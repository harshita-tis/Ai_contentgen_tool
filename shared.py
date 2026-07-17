"""
shared.py — Core app setup: Flask app/db instance, config, auth, DB models,
shop-config management, and Shopify helper functions used by BOTH the
content_generation and image_search modules.

Both feature modules do `from shared import app, db, ...` and register their
routes directly on this single shared `app` object.
"""
from flask import Flask, render_template, request, jsonify, Response, stream_with_context, Blueprint, redirect, url_for, session, flash
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime, timezone
import openai
import requests
import os
import json
import uuid
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from flask_cors import CORS
from functools import wraps
import re
import logging
import time
from html import escape as html_escape
import hashlib
import asyncio
import requests as _req
from urllib.parse import urlparse

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

load_dotenv()
shopify_bp = Blueprint('shopify', __name__)
app = Flask(__name__)
CORS(
    app,
    supports_credentials=True,
    origins=[
        "https://spark236.myshopify.com",
        "https://42e8-2405-201-3009-5c56-1b93-652d-7596-880f.ngrok-free.app",
        "https://genuinereplacementparts.com"
    ],
)

# ─── MySQL Configuration ──────────────────────────────────────────────────────
_DB_HOST     = os.getenv("HOST", "localhost")
_DB_USER     = os.getenv("USER", "root")
_DB_PASSWORD = os.getenv("DATABASE_PASSWORD", "")
_DB_NAME     = os.getenv("DATABASE", "content_gen")
model = os.getenv("OPENAI_MODEL", "gpt-4o")

app.config['SQLALCHEMY_DATABASE_URI'] = (
    f"mysql+pymysql://{_DB_USER}:{_DB_PASSWORD}@{_DB_HOST}/{_DB_NAME}?charset=utf8mb4"
)
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {
    'pool_recycle': 280,
    'pool_pre_ping': True,
}
app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'grp-content-secret-key-2024')
db = SQLAlchemy(app)

# ─── User Credentials & Role Definitions ─────────────────────────────────────
USERS = {
    'grp_user':     {'password': '12345',      'role': 'user'},
    'grp_reviewer': {'password': 'review@123', 'role': 'reviewer'},
}

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(name)s | %(message)s")
logger = logging.getLogger(__name__)


# ─── Auth Decorators & Small Helpers ─────────────────────────────────────
# ─── Auth Decorators ──────────────────────────────────────────────────────────

def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'username' not in session:
            if request.path.startswith('/api/'):
                return jsonify({'error': 'Authentication required'}), 401
            return redirect(url_for('login_page', next='/'))
        return f(*args, **kwargs)
    return decorated


def reviewer_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'username' not in session:
            if request.path.startswith('/api/'):
                return jsonify({'error': 'Authentication required'}), 401
            return redirect(url_for('login_page', next='/'))
        if session.get('role') != 'reviewer':
            if request.path.startswith('/api/'):
                return jsonify({'error': 'Reviewer access required'}), 403
            flash('Access denied: Reviewer role required.', 'error')
            return redirect('/')
        return f(*args, **kwargs)
    return decorated


def _get_active_shop() -> 'ShopConfig | None':
    """
    Resolves the *current user's* active shop.
    Per-session: each logged-in user gets their own active-shop choice,
    stored in their Flask session, so one user switching shops never
    affects another concurrent user/session.
    Falls back to the DB-wide default shop (legacy `is_active` flag) and
    then to the first created shop if the session has no selection yet
    or points at a shop that no longer exists.
    """
    try:
        shop_id = session.get('active_shop_id')
        if shop_id:
            shop = ShopConfig.query.get(shop_id)
            if shop:
                return shop
            # Session pointed at a shop that's been deleted — clear it and fall through.
            session.pop('active_shop_id', None)

        shop = ShopConfig.query.filter_by(is_active=True).first() \
            or ShopConfig.query.order_by(ShopConfig.created_at.asc()).first()
        if shop:
            session['active_shop_id'] = shop.id
        return shop
    except Exception:
        return None


def _shopify_base_url(domain: str = '') -> str:
    d = (domain or '').strip().rstrip('/')
    if d and not d.startswith(('http://', 'https://')):
        d = 'https://' + d
    return d


def _utc_now():
    return datetime.now(timezone.utc)



# ─── Shared Constants & Shopify GraphQL Queries ──────────────────────────
API_KEY = os.getenv('OPENAI_API_KEY')

# In-memory cancel flags: job_id -> threading.Event
# Set the event to signal a running job to stop ASAP.
_cancel_events: dict[str, threading.Event] = {}


_GQL_PRODUCT = """
query GetProduct($id: ID!) {
  product(id: $id) {
    id
    title
    handle
    productType
    vendor
    tags
    bodyHtml
    images(first: 1) {
      edges {
        node {
          url
          altText
        }
      }
    }
    variants(first: 1) {
      edges {
        node {
          sku
          image {
            url
          }
        }
      }
    }
  }
}
"""

_GQL_PRODUCT_BY_SKU = """
query GetProductBySku($query: String!) {
  products(first: 1, query: $query) {
    edges {
      node {
        id
        title
        images(first: 1) {
          edges {
            node {
              url
              altText
            }
          }
        }
        variants(first: 5) {
          edges {
            node {
              sku
              image {
                url
              }
            }
          }
        }
      }
    }
  }
}
"""

_GQL_PRODUCT_UPDATE = """
mutation ProductUpdate($input: ProductInput!) {
  productUpdate(input: $input) {
    product {
      id
      seo {
        title
        description
      }
    }
    userErrors { field message }
  }
}
"""

_GQL_METAFIELDS_SET = """
mutation MetafieldsSet($metafields: [MetafieldsSetInput!]!) {
  metafieldsSet(metafields: $metafields) {
    metafields { id namespace key }
    userErrors { field message }
  }
}
"""

_GQL_PRODUCT_BY_HANDLE = """
query ProductByHandle($handle: String!) {
  productByHandle(handle: $handle) {
    id
    title
    handle
    productType
    vendor
    tags
    bodyHtml  
    images(first: 1) {
      edges {
        node {
          url
          altText
        }
      }
    }
    variants(first: 5) {
      edges {
        node {
          sku
          image {
            url
          }
        }
      }     
    }
  }
}
"""

SHOPIFY_METAFIELD_TYPES = {
    'symptoms_installation_guide': 'multi_line_text_field',
    'technical_specifications':    'multi_line_text_field',
    'bullet_points':               'single_line_text_field',
    'common_causes':               'multi_line_text_field',
    'short_description':           'multi_line_text_field',
}

# SEO fields pushed via ProductUpdate input (not metafields)
SEO_SECTIONS = {'meta_title', 'meta_description'}

COST_PER_INPUT_TOKEN  = 0.150 / 1_000_000
COST_PER_OUTPUT_TOKEN = 0.600 / 1_000_000

# Max parallel workers for HTML template formatting inside _generate_section_batch.
# Each worker makes one OpenAI API call, so keep this within your RPM quota.
MAX_FORMAT_WORKERS = int(os.getenv('MAX_FORMAT_WORKERS', '2'))

DEFAULT_PROMPTS = {
    "short_description": """You are a professional product copywriter.
Product Title: {product_title}
Part Number: {part_number}
Write 5-6 short product description items. Output ONLY plain text, one item per line.""",

    "product_description": """You are a professional product copywriter.
Product Title: {product_title}
Part Number: {part_number}
Write a detailed description showing features and benefits concisely.""",

    "technical_specifications": """You are a technical documentation specialist.
Product Title: {product_title}
Part Number: {part_number}
List 5 key technical specifications configurations.""",

    "common_problems": """You are a product support specialist.
Product Title: {product_title}
Part Number: {part_number}
List 4-6 common symptoms or failure causes.""",

    "installation_guide": """You are a technical writer.
Product Title: {product_title}
Part Number: {part_number}
Write short actionable installation steps.""",

    "meta_title": """You are an SEO specialist.
Product Title: {product_title}
Part Number: {part_number}
Write a single SEO-optimized meta title for this product page. It must be under 60 characters, include the part number, and clearly describe the product. Output ONLY the meta title text, nothing else.""",

    "meta_description": """You are an SEO specialist.
Product Title: {product_title}
Part Number: {part_number}
Write a single SEO-optimized meta description for this product page. It must be between 140-160 characters, include the part number, highlight key benefits, and include a call to action. Output ONLY the meta description text, nothing else.""",
}

SECTION_LABELS = {
    "short_description": "Short Description",
    "product_description": "Product Description",
    "technical_specifications": "Technical Specifications",
    "common_problems": "Common Problems & Symptoms",
    "installation_guide": "Installation Guide",
    "meta_title": "SEO Meta Title",
    "meta_description": "SEO Meta Description",
}

SECTIONS = list(DEFAULT_PROMPTS.keys())



# ─── Database Models ──────────────────────────────────────────────────────
# ─── Database Models ──────────────────────────────────────────────────────────

class GeneratedContent(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    batch_id = db.Column(db.String(36), index=True)
    product_title = db.Column(db.String(255), nullable=False)
    part_number = db.Column(db.String(100), nullable=False)
    part_type = db.Column(db.String(100))
    brand = db.Column(db.String(100))
    appliance_type = db.Column(db.String(100))
    section = db.Column(db.String(100), nullable=False)
    prompt_type = db.Column(db.String(20), nullable=False)
    prompt_used = db.Column(db.Text, nullable=False)
    plain_text = db.Column(db.Text)
    html_text = db.Column(db.Text)
    product_image_url = db.Column(db.String(1024))
    input_tokens = db.Column(db.Integer)
    output_tokens = db.Column(db.Integer)
    total_tokens = db.Column(db.Integer)
    model_used = db.Column(db.String(100))
    created_at = db.Column(db.DateTime, default=_utc_now)

    def to_dict(self):
        return {
            'id': self.id,
            'batch_id': self.batch_id,
            'product_title': self.product_title,
            'part_number': self.part_number,
            'part_type': self.part_type,
            'brand': self.brand,
            'appliance_type': self.appliance_type,
            'section': self.section,
            'section_label': SECTION_LABELS.get(self.section, self.section),
            'prompt_type': self.prompt_type,
            'prompt_used': self.prompt_used,
            'plain_text': self.plain_text,
            'html_text': self.html_text,
            'product_image_url': self.product_image_url,
            'input_tokens': self.input_tokens,
            'output_tokens': self.output_tokens,
            'total_tokens': self.total_tokens,
            'model_used': self.model_used,
            'created_at': self.created_at.strftime('%Y-%m-%d %H:%M:%S')
        }


class ProductHistory(db.Model):
    __tablename__ = 'product_history'
    id = db.Column(db.Integer, primary_key=True)
    batch_id = db.Column(db.String(36), index=True)
    product_title = db.Column(db.String(255), nullable=False)
    part_number = db.Column(db.String(100), nullable=False)
    part_type = db.Column(db.String(100))
    brand = db.Column(db.String(100))
    appliance_type = db.Column(db.String(100))  
    source = db.Column(db.String(20), default='manual')
    shopify_url = db.Column(db.String(1024))
    product_image_url = db.Column(db.String(1024))
    total_input_tokens = db.Column(db.Integer, default=0)
    total_output_tokens = db.Column(db.Integer, default=0)
    total_tokens = db.Column(db.Integer, default=0)
    cost_usd = db.Column(db.Float, default=0.0)
    sections_generated = db.Column(db.Integer, default=0)
    model_used = db.Column(db.String(100))
    created_at = db.Column(db.DateTime, default=_utc_now)

    def to_dict(self):
        return {
            'id': self.id,
            'batch_id': self.batch_id,
            'product_title': self.product_title,
            'part_number': self.part_number,
            'part_type': self.part_type,
            'brand': self.brand,
            'appliance_type': self.appliance_type,
            'source': self.source,
            'shopify_url': self.shopify_url or '',
            'product_image_url': self.product_image_url,
            'total_input_tokens': self.total_input_tokens,
            'total_output_tokens': self.total_output_tokens,
            'total_tokens': self.total_tokens,
            'cost_usd': round(self.cost_usd, 6),
            'sections_generated': self.sections_generated,
            'model_used': self.model_used,
            'created_at': self.created_at.strftime('%Y-%m-%d %H:%M:%S')
        }


class BatchHistory(db.Model):
    __tablename__ = 'batch_history'
    id = db.Column(db.Integer, primary_key=True)
    batch_id = db.Column(db.String(36), unique=True, nullable=False, index=True)
    source = db.Column(db.String(20), default='manual')
    product_count = db.Column(db.Integer, default=0)
    total_input_tokens = db.Column(db.Integer, default=0)
    total_output_tokens = db.Column(db.Integer, default=0)
    total_tokens = db.Column(db.Integer, default=0)
    cost_usd = db.Column(db.Float, default=0.0)
    model_used = db.Column(db.String(100))
    created_at = db.Column(db.DateTime, default=_utc_now)

    def to_dict(self):
        return {
            'id': self.id,
            'batch_id': self.batch_id,
            'source': self.source,
            'product_count': self.product_count,
            'total_input_tokens': self.total_input_tokens,
            'total_output_tokens': self.total_output_tokens,
            'total_tokens': self.total_tokens,
            'cost_usd': round(self.cost_usd, 6),
            'model_used': self.model_used,
            'created_at': self.created_at.strftime('%Y-%m-%d %H:%M:%S')
        }


class ReviewStatus(db.Model):
    __tablename__ = 'review_status'
    id = db.Column(db.Integer, primary_key=True)
    product_title = db.Column(db.String(255), nullable=False)
    part_number = db.Column(db.String(100), nullable=False)
    status = db.Column(db.String(20), default='pending')
    reviewer = db.Column(db.String(100))
    reviewed_at = db.Column(db.DateTime)
    published_at = db.Column(db.DateTime)
    updated_at = db.Column(db.DateTime, default=_utc_now, onupdate=_utc_now)

    __table_args__ = (db.UniqueConstraint('product_title', 'part_number', name='uq_review_product'),)

    def to_dict(self):
        return {
            'id': self.id,
            'product_title': self.product_title,
            'part_number': self.part_number,
            'status': self.status,
            'reviewer': self.reviewer,
            'reviewed_at': self.reviewed_at.strftime('%Y-%m-%d %H:%M:%S') if self.reviewed_at else None,
            'published_at': self.published_at.strftime('%Y-%m-%d %H:%M:%S') if self.published_at else None,
            'updated_at': self.updated_at.strftime('%Y-%m-%d %H:%M:%S') if self.updated_at else None,
        }


# ─── Server-Side Job Models ───────────────────────────────────────────────────

class GenerationJob(db.Model):
    """
    Tracks the lifecycle of a bulk generation request so work continues
    on the server even if the client tab is closed/refreshed.
    """
    __tablename__ = 'generation_job'
    id         = db.Column(db.Integer, primary_key=True)
    job_id     = db.Column(db.String(36), unique=True, nullable=False, index=True)
    batch_id   = db.Column(db.String(36), nullable=False, index=True)
    status     = db.Column(db.String(20), default='pending')   # pending|running|done|error|cancelled
    total_products = db.Column(db.Integer, default=0)
    done_products  = db.Column(db.Integer, default=0)
    error_message  = db.Column(db.Text)
    # Full request payload (products/sections/prompts/templates) needed to actually
    # run the job. Persisting this lets a separate worker process (see worker.py)
    # pick the job up and run it — the web request process that created it doesn't
    # have to stay alive. This is what makes jobs survive Passenger/cPanel killing
    # the web worker process mid-generation.
    payload    = db.Column(db.Text(16777215))
    created_at = db.Column(db.DateTime, default=_utc_now)
    updated_at = db.Column(db.DateTime, default=_utc_now, onupdate=_utc_now)

    def to_dict(self):
        return {
            'job_id': self.job_id,
            'batch_id': self.batch_id,
            'status': self.status,
            'total_products': self.total_products,
            'done_products': self.done_products,
            'error_message': self.error_message,
            'created_at': self.created_at.strftime('%Y-%m-%d %H:%M:%S'),
        }


class JobEvent(db.Model):
    """
    Stores each SSE event emitted during a job so late/reconnecting clients
    can replay the full event history and catch up seamlessly.
    """
    __tablename__ = 'job_event'
    id         = db.Column(db.Integer, primary_key=True)
    job_id     = db.Column(db.String(36), nullable=False, index=True)
    event_type = db.Column(db.String(50), nullable=False)   # batch_start|product_done|batch_end
    payload    = db.Column(db.Text(16777215), nullable=False)  # MEDIUMTEXT — product_done payloads can exceed 65 KB
    seq        = db.Column(db.Integer, nullable=False)       # monotonic sequence per job
    created_at = db.Column(db.DateTime, default=_utc_now)


class ShopConfig(db.Model):
    __tablename__ = 'shop_config'
    id             = db.Column(db.Integer, primary_key=True)
    name           = db.Column(db.String(255), nullable=False)
    domain         = db.Column(db.String(255), nullable=False, unique=True)
    website_domain = db.Column(db.String(255), nullable=True)   # public storefront, e.g. genuinereplacementparts.com
    access_token   = db.Column(db.Text, nullable=False)
    api_version    = db.Column(db.String(20))
    metafields   = db.Column(db.Text, default='[]')
    sections     = db.Column(db.Text, default='[]')
    is_active    = db.Column(db.Boolean, default=False, nullable=False)
    created_at   = db.Column(db.DateTime, default=_utc_now)
    updated_at   = db.Column(db.DateTime, default=_utc_now, onupdate=_utc_now)

    def to_dict(self, hide_token=True, active_id=None):
        return {
            'id':             self.id,
            'name':           self.name,
            'domain':         self.domain,
            'website_domain': self.website_domain or '',
            'access_token':   '•' * 12 if hide_token else self.access_token,
            'api_version':    self.api_version or '',
            'metafields':   json.loads(self.metafields or '[]'),
            'sections':     json.loads(self.sections or '[]'),
            # `is_active` reflects the CURRENT caller's session selection when
            # active_id is supplied; otherwise falls back to the DB-wide default.
            'is_active':    (self.id == active_id) if active_id is not None else self.is_active,
            'created_at':   self.created_at.strftime('%Y-%m-%d %H:%M:%S'),
            'updated_at':   self.updated_at.strftime('%Y-%m-%d %H:%M:%S'),
        }


class ImageSearchGeneration(db.Model):
    __tablename__ = 'image_search_generation'
    id             = db.Column(db.Integer, primary_key=True)
    generation_id  = db.Column(db.String(36), unique=True, nullable=False, index=True,
                               default=lambda: str(uuid.uuid4()))
    batch_id       = db.Column(db.String(36), nullable=True, index=True)
    source         = db.Column(db.String(20), default='manual')   # 'manual' | 'csv'
    review_status  = db.Column(db.String(20), default='pending', index=True)  # 'pending' | 'reviewed' | 'published'
    shop_id        = db.Column(db.Integer, db.ForeignKey('shop_config.id'), nullable=True)
    product_title  = db.Column(db.String(512), nullable=True)
    part_number    = db.Column(db.String(128), nullable=True)
    brand          = db.Column(db.String(128), nullable=True)
    part_type      = db.Column(db.String(128), nullable=True)
    appliance_type = db.Column(db.String(128), nullable=True)
    shopify_url    = db.Column(db.String(1024), nullable=True)
    prompt_used    = db.Column(db.Text, nullable=True)
    query_used     = db.Column(db.Text, nullable=True)
    image_urls     = db.Column(db.Text, nullable=True)
    selected_urls  = db.Column(db.Text, nullable=True)
    input_tokens   = db.Column(db.Integer, default=0)
    output_tokens  = db.Column(db.Integer, default=0)
    created_at     = db.Column(db.DateTime, default=_utc_now)


class AppSetting(db.Model):
    """Generic key-value store for app-wide settings (e.g. saved image search prompt)."""
    __tablename__ = 'app_setting'
    id    = db.Column(db.Integer, primary_key=True)
    key   = db.Column(db.String(100), unique=True, nullable=False)
    value = db.Column(db.Text, nullable=False)

    @classmethod
    def get(cls, key, default=None):
        row = cls.query.filter_by(key=key).first()
        return row.value if row else default

    @classmethod
    def set(cls, key, value):
        row = cls.query.filter_by(key=key).first()
        if row:
            row.value = value
        else:
            db.session.add(cls(key=key, value=value))
        db.session.commit()
# ─── Shop Config Routes (used by both modules' UIs) ──────────────────────
# ─── Shop Config Routes ───────────────────────────────────────────────────────

@app.route('/shop-config')
@reviewer_required
def shop_config_page():
    return render_template('shop_config.html')


@app.route('/api/shops', methods=['GET'])
@login_required
def list_shops():
    shops = ShopConfig.query.order_by(ShopConfig.created_at.asc()).all()
    current = _get_active_shop()
    current_id = current.id if current else None
    return jsonify({'shops': [s.to_dict(active_id=current_id) for s in shops]})


@app.route('/api/shops', methods=['POST'])
@reviewer_required
def create_shop():
    body = request.json or {}
    name           = (body.get('name') or '').strip()
    domain         = (body.get('domain') or '').strip().lower().rstrip('/')
    website_domain = (body.get('website_domain') or '').strip().lower().rstrip('/')
    access_token   = (body.get('access_token') or '').strip()
    api_version    = (body.get('api_version') or '').strip()
    metafields     = body.get('metafields', [])
    sections       = body.get('sections', [])

    if not name or not domain or not access_token:
        return jsonify({'error': 'name, domain, and access_token are required'}), 400

    if ShopConfig.query.filter_by(domain=domain).first():
        return jsonify({'error': f'A shop with domain "{domain}" already exists'}), 409

    shop = ShopConfig(
        name=name,
        domain=domain,
        website_domain=website_domain or None,
        access_token=access_token,
        api_version=api_version or None,
        metafields=json.dumps(metafields),
        sections=json.dumps(sections),
        is_active=ShopConfig.query.count() == 0,
    )
    db.session.add(shop)
    db.session.commit()
    return jsonify({'success': True, 'shop': shop.to_dict()}), 201


@app.route('/api/shops/<int:shop_id>', methods=['PUT'])
@reviewer_required
def update_shop(shop_id):
    shop = ShopConfig.query.get_or_404(shop_id)
    body = request.json or {}
    if 'name' in body:            shop.name           = (body['name'] or '').strip()
    if 'domain' in body:          shop.domain         = (body['domain'] or '').strip().lower().rstrip('/')
    if 'website_domain' in body:  shop.website_domain = (body['website_domain'] or '').strip().lower().rstrip('/') or None
    if body.get('access_token'):  shop.access_token   = body['access_token'].strip()
    if 'api_version' in body:     shop.api_version    = (body.get('api_version') or '').strip() or None
    if 'metafields' in body:      shop.metafields     = json.dumps(body['metafields'])
    if 'sections' in body:        shop.sections       = json.dumps(body['sections'])
    db.session.commit()
    return jsonify({'success': True, 'shop': shop.to_dict()})


@app.route('/api/shops/<int:shop_id>', methods=['DELETE'])
@reviewer_required
def delete_shop(shop_id):
    shop = ShopConfig.query.get_or_404(shop_id)
    was_active = shop.is_active
    db.session.delete(shop)
    db.session.commit()
    if was_active:
        nxt = ShopConfig.query.order_by(ShopConfig.created_at.asc()).first()
        if nxt:
            nxt.is_active = True
            db.session.commit()
    return jsonify({'success': True})


@app.route('/api/shops/<int:shop_id>/set-active', methods=['POST'])
@login_required
def set_active_shop(shop_id):
    shop = ShopConfig.query.get_or_404(shop_id)
    # Per-session only — does NOT touch other users' active-shop selection.
    session['active_shop_id'] = shop.id
    return jsonify({'success': True, 'shop': shop.to_dict(active_id=shop.id)})


_GQL_METAFIELD_DEFINITIONS = """
query {
  metafieldDefinitions(first: 100, ownerType: PRODUCT) {
    edges {
      node {
        namespace
        key
        name
        type { name }
      }
    }
  }
}
"""

@app.route('/api/shops/<int:shop_id>/metafields', methods=['GET'])
@reviewer_required
def get_shop_metafields(shop_id):
    shop = ShopConfig.query.get_or_404(shop_id)
    domain       = shop.domain.strip().rstrip('/')
    access_token = shop.access_token.strip()
    api_version  = (shop.api_version or '2024-01').strip()

    if not domain or not access_token:
        return jsonify({'success': False, 'error': 'Shop credentials are incomplete'}), 400

    endpoint = f'https://{domain}/admin/api/{api_version}/graphql.json'
    headers  = {
        'Content-Type':           'application/json',
        'X-Shopify-Access-Token': access_token,
    }

    try:
        resp = requests.post(endpoint, json={'query': _GQL_METAFIELD_DEFINITIONS}, headers=headers, timeout=15)
    except requests.exceptions.Timeout:
        return jsonify({'success': False, 'error': 'Request to Shopify timed out'}), 504
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

    if resp.status_code == 401:
        return jsonify({'success': False, 'error': 'Invalid access token (401 Unauthorized)'}), 200
    if not resp.ok:
        return jsonify({'success': False, 'error': f'Shopify returned HTTP {resp.status_code}'}), 200

    body   = resp.json()
    errors = body.get('errors')
    if errors:
        msg = errors[0].get('message', str(errors)) if isinstance(errors, list) else str(errors)
        return jsonify({'success': False, 'error': f'Shopify GraphQL error: {msg}'}), 200

    edges = ((body.get('data') or {}).get('metafieldDefinitions', {}).get('edges') or [])

    metafields = []
    for edge in edges:
        node = edge.get('node') or {}
        ns   = node.get('namespace', '')
        key  = node.get('key', '')
        name = node.get('name', '')
        mf_type = (node.get('type') or {}).get('name', 'single_line_text_field')
        label = f'{ns}.{key}' + (f' — {name}' if name else '')
        metafields.append({
            'namespace': ns,
            'key':       key,
            'name':      name,
            'type':      mf_type,
            'label':     label,
        })

    return jsonify({'success': True, 'metafields': metafields, 'total': len(metafields)})


@app.route('/api/shops/fetch-metafields-direct', methods=['POST'])
@reviewer_required
def fetch_metafields_direct():
    body         = request.json or {}
    domain       = (body.get('domain') or '').strip().lower().rstrip('/')
    access_token = (body.get('access_token') or '').strip()
    api_version  = (body.get('api_version') or '').strip() or '2024-01'

    if not domain or not access_token:
        return jsonify({'success': False, 'error': 'domain and access_token are required'}), 400

    endpoint = f'https://{domain}/admin/api/{api_version}/graphql.json'
    headers  = {
        'Content-Type':           'application/json',
        'X-Shopify-Access-Token': access_token,
    }

    try:
        resp = requests.post(endpoint, json={'query': _GQL_METAFIELD_DEFINITIONS}, headers=headers, timeout=15)
    except requests.exceptions.Timeout:
        return jsonify({'success': False, 'error': 'Request to Shopify timed out'}), 504
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

    if resp.status_code == 401:
        return jsonify({'success': False, 'error': 'Invalid access token (401 Unauthorized)'}), 200
    if not resp.ok:
        return jsonify({'success': False, 'error': f'Shopify returned HTTP {resp.status_code}'}), 200

    body_json = resp.json()
    errors    = body_json.get('errors')
    if errors:
        msg = errors[0].get('message', str(errors)) if isinstance(errors, list) else str(errors)
        return jsonify({'success': False, 'error': f'Shopify GraphQL error: {msg}'}), 200

    edges = ((body_json.get('data') or {}).get('metafieldDefinitions', {}).get('edges') or [])

    metafields = []
    for edge in edges:
        node    = edge.get('node') or {}
        ns      = node.get('namespace', '')
        key     = node.get('key', '')
        name    = node.get('name', '')
        mf_type = (node.get('type') or {}).get('name', 'single_line_text_field')
        label   = f'{ns}.{key}' + (f' — {name}' if name else '')
        metafields.append({'namespace': ns, 'key': key, 'name': name, 'type': mf_type, 'label': label})

    return jsonify({'success': True, 'metafields': metafields, 'total': len(metafields)})


@app.route('/api/shops/<int:shop_id>/test-connection', methods=['POST'])
@reviewer_required
def test_shop_connection_by_id(shop_id):
    shop = ShopConfig.query.get_or_404(shop_id)
    domain       = shop.domain.strip().rstrip('/')
    access_token = shop.access_token.strip()
    api_version  = (shop.api_version or '2024-01').strip()
    url = f'https://{domain}/admin/api/{api_version}/shop.json'
    try:
        resp = requests.get(url, headers={'X-Shopify-Access-Token': access_token}, timeout=10)
        if resp.status_code == 401:
            return jsonify({'success': False, 'error': 'Invalid access token (401 Unauthorized)'}), 200
        if resp.status_code == 404:
            return jsonify({'success': False, 'error': 'Store domain not found (404)'}), 200
        if not resp.ok:
            return jsonify({'success': False, 'error': f'Shopify returned HTTP {resp.status_code}'}), 200
        shop_name = resp.json().get('shop', {}).get('name', domain)
        return jsonify({'success': True, 'shop_name': shop_name})
    except requests.exceptions.Timeout:
        return jsonify({'success': False, 'error': 'Request timed out'}), 200
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 200


@app.route('/api/shops/test-connection', methods=['POST'])
@reviewer_required
def test_shop_connection():
    body         = request.json or {}
    domain       = (body.get('domain') or '').strip().lower().rstrip('/')
    access_token = (body.get('access_token') or '').strip()
    api_version  = (body.get('api_version') or '').strip() or '2024-01'

    if not domain or not access_token:
        return jsonify({'error': 'domain and access_token are required'}), 400

    url = f'https://{domain}/admin/api/{api_version}/shop.json'
    try:
        resp = requests.get(url, headers={'X-Shopify-Access-Token': access_token}, timeout=10)
        if resp.status_code == 401:
            return jsonify({'success': False, 'error': 'Invalid access token (401 Unauthorized)'}), 200
        if resp.status_code == 404:
            return jsonify({'success': False, 'error': 'Store domain not found (404)'}), 200
        if not resp.ok:
            return jsonify({'success': False, 'error': f'Shopify returned HTTP {resp.status_code}'}), 200
        shop_name = resp.json().get('shop', {}).get('name', domain)
        return jsonify({'success': True, 'shop_name': shop_name})
    except requests.exceptions.Timeout:
        return jsonify({'success': False, 'error': 'Request timed out'}), 200
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 200


_image_cache: dict = {}


# ─── Shopify product-fetch helpers shared by content_generation & image_search ──
def _extract_admin_product_id(raw_url: str) -> str | None:
    m = re.search(r'admin\.shopify\.com/store/[^/]+/products/(\d+)', raw_url)
    if m: return m.group(1)
    m = re.search(r'myshopify\.com/admin/products/(\d+)', raw_url)
    if m: return m.group(1)
    return None
 
def _is_storefront_url(raw_url: str) -> bool:
    return bool(_extract_products_path_segment(raw_url)) and not _extract_admin_product_id(raw_url)
 
def _shopify_graphql_with_creds(query: str, variables: dict, domain: str, token: str, api_version: str) -> dict:
    endpoint = f"{_shopify_base_url(domain)}/admin/api/{api_version}/graphql.json"
    headers  = {'Content-Type': 'application/json', 'X-Shopify-Access-Token': token}
    resp = requests.post(endpoint, json={'query': query, 'variables': variables or {}}, headers=headers, timeout=45)
    if not resp.ok: raise RuntimeError(f'Shopify API HTTP {resp.status_code}')
    return resp.json()

def _fetch_by_product_id(product_id: str, _domain: str = '', _token: str = '', _api_version: str = '') -> dict:
    domain, token, api_version = _domain.strip(), _token.strip(), _api_version.strip() or '2024-01'
    if not domain or not token:
        shop = _get_active_shop()
        if not shop: raise ValueError("No active shop configured.")
        domain, token, api_version = shop.domain.strip(), shop.access_token.strip(), (shop.api_version or '2024-01').strip()

    body = _shopify_graphql_with_creds(_GQL_PRODUCT, {'id': f"gid://shopify/Product/{product_id}"}, domain, token, api_version)
    product = (body.get('data') or {}).get('product')
    if not product: raise ValueError("Product not found.")
    return _parse_gql_product(product)
 
def _fetch_by_storefront_url(raw_url: str) -> dict:
    url = raw_url.strip().split('#')[0].split('?')[0].rstrip('/')
    if re.search(r'/products/[^/]+$', url) and not url.endswith('.json'): url += '.json'
    resp = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
    if not resp.ok: raise ValueError("Shopify storefront error.")
    return _parse_rest_product(resp.json()['product'])
 
def _webcate_from_tags(tags) -> str:
    """Extract the value of the 'webcate:<value>' tag from a list or comma-separated string of tags."""
    if isinstance(tags, str):
        tag_list = [t.strip() for t in tags.split(',')]
        logger.info(f"tag_list-:{tag_list}")
    else:
        tag_list = list(tags or [])

    for tag in tag_list:
        if tag.lower().startswith('webcat:'):
            logger.info(f"tag-:{tag}")
            return tag.split(':', 1)[1].strip()
    return ''

def _subcate_from_tags(tags) -> str:
    """Extract the value of the 'subcate:<value>' tag from a list or comma-separated string of tags."""
    if isinstance(tags, str):
        tag_list = [t.strip() for t in tags.split(',')]
        logger.info(f"tag_list-:{tag_list}")
    else:
        tag_list = list(tags or [])
    for tag in tag_list:
        if tag.lower().startswith('subcate:'):
            logger.info(f"tag-:{tag}")
            return tag.split(':', 1)[1].strip()
    return ''

def _parse_gql_product(product: dict) -> dict:
    sku = ''
    edges = (product.get('variants') or {}).get('edges') or []
    if edges: sku = (edges[0].get('node') or {}).get('sku') or ''
    image_url = ''
    if edges:
        v_img = ((edges[0].get('node') or {}).get('image') or {}).get('url', '')
        if v_img: image_url = v_img
    if not image_url:
        img_edges = (product.get('images') or {}).get('edges') or []
        if img_edges: image_url = img_edges[0]['node'].get('url', '')

    body_html  = product.get('bodyHtml') or ''
    plain_body = re.sub(r'<[^>]+>', ' ', body_html).strip()
    tags = product.get('tags') or []
    return {
        'title': product.get('title', ''), 'part_number': sku, 'brand': product.get('vendor', ''),
        'part_type': _webcate_from_tags(tags), 'appliance_type': _subcate_from_tags(tags), 'description': plain_body,
        'tags': ', '.join(tags), 'handle': product.get('handle', ''), 'product_image_url': image_url
    }

def _fetch_by_handle_admin(handle: str, _domain: str = '', _token: str = '', _api_version: str = '') -> dict:
    domain, token, api_version = _domain.strip(), _token.strip(), _api_version.strip() or '2024-01'
    if not domain or not token:
        shop = _get_active_shop()
        if not shop: raise ValueError('No active shop config found.')
        domain, token, api_version = shop.domain.strip(), shop.access_token.strip(), (shop.api_version or '2024-01').strip()
    body = _shopify_graphql_with_creds(_GQL_PRODUCT_BY_HANDLE, {'handle': handle}, domain, token, api_version)
    return _parse_gql_product((body.get('data') or {}).get('productByHandle'))

def _parse_rest_product(product: dict) -> dict:
    variant   = (product.get('variants') or [{}])[0]
    sku       = variant.get('sku') or str(product.get('id', ''))
    body_html = product.get('body_html') or ''
    plain_body = re.sub(r'<[^>]+>', ' ', body_html).strip()
    image_url = ''
    images = product.get('images') or []
    if images: image_url = images[0].get('src', '')
    tags = product.get('tags', '')
    return {
        'title': product.get('title', ''), 'part_number': sku, 'brand': product.get('vendor', ''),
        'part_type': _webcate_from_tags(tags), 'appliance_type': _subcate_from_tags(tags), 'description': plain_body,
        'tags': tags, 'handle': product.get('handle', ''), 'product_image_url': image_url
    }
 
def _fetch_shopify_product(raw_url: str, _domain: str = '', _token: str = '', _api_version: str = '') -> dict:
    product_id = _extract_admin_product_id(raw_url)
    if product_id: return _fetch_by_product_id(product_id, _domain=_domain, _token=_token, _api_version=_api_version)
    seg = _extract_products_path_segment(raw_url)
    if not seg: raise ValueError("Invalid layout URL structure link.")
    if seg.isdigit(): return _fetch_by_product_id(seg, _domain=_domain, _token=_token, _api_version=_api_version)

    domain, token = _domain.strip(), _token.strip()
    if not domain or not token:
        active_shop = _get_active_shop()
        if active_shop:
            domain, token, _api_version = active_shop.domain.strip(), active_shop.access_token.strip(), (active_shop.api_version or '2024-01').strip()
    if domain and token:
        try: return _fetch_by_handle_admin(seg, _domain=domain, _token=token, _api_version=_api_version)
        except Exception: pass
    if _is_storefront_url(raw_url): return _fetch_by_storefront_url(raw_url)
    raise ValueError("Could not access data paths.")

def fetch_shopify_image_by_product_url(shopify_url: str, _domain: str = '', _token: str = '', _api_version: str = '') -> str:
    u = (shopify_url or '').strip()
    if not u: return ''
    try:
        p = _fetch_shopify_product(u, _domain=_domain, _token=_token, _api_version=_api_version)
        return (p.get('product_image_url') or '').strip()
    except Exception: return ''

def fetch_shopify_image_by_handle(handle: str, _domain: str = '', _token: str = '', _api_version: str = '') -> str:
    """
    Look up a product's image straight from its Shopify handle (the slug in
    /products/<handle>) via the Admin GraphQL API, using the same
    _fetch_by_handle_admin -> _parse_gql_product path (and its variant-image
    -> product-image fallback) as the URL-based lookup.

    Use this wherever a product handle is known instead of trying to resolve
    an image by SKU -- there is no SKU-based fetch path wired up in this
    codebase (see _GQL_PRODUCT_BY_SKU, which is defined but never called).
    """
    h = (handle or '').strip().strip('/')
    if not h: return ''
    try:
        p = _fetch_by_handle_admin(h, _domain=_domain, _token=_token, _api_version=_api_version)
        return (p.get('product_image_url') or '').strip()
    except Exception: return ''
 
def _extract_products_path_segment(url: str) -> str | None:
    m = re.search(r'/products/([^/?#]+)', (url or '').strip(), re.I)
    if not m: return None
    seg = m.group(1).strip().rstrip('/')
    return seg[:-5] if seg.lower().endswith('.json') else seg



# ─── Login / Logout / Landing page ───────────────────────────────────────
@app.route('/login', methods=['GET', 'POST'])
def login_page():
    if 'username' in session: return redirect('/')
    error = None
    if request.method == 'POST':
        username = (request.form.get('username') or '').strip()
        password = (request.form.get('password') or '').strip()
        user = USERS.get(username)
        if user and user['password'] == password:
            session['username'] = username
            session['role'] = user['role']
            return redirect(request.form.get('next') or request.args.get('next') or '/')
        error = 'Invalid username or password.'
    return render_template('login.html', error=error, next=request.args.get('next', '/'))


@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login_page'))


@app.route('/')
@login_required
def index():
    return render_template('index.html', sections=SECTION_LABELS)
  



# ─── DB migrations, run at startup by run.py ─────────────────────────────
def run_migrations():
    from sqlalchemy import inspect, text
    with app.app_context():
        db.create_all()  # creates generation_job and job_event tables automatically
        inspector = inspect(db.engine)
        existing_cols = {c['name'] for c in inspector.get_columns('generated_content')}
        gc_new_cols = {
            'appliance_type':    'VARCHAR(100)',
            'batch_id':          'VARCHAR(36)',
            'product_image_url': 'VARCHAR(1024)',
        }
        with db.engine.connect() as conn:
            for col, col_type in gc_new_cols.items():
                if col not in existing_cols:
                    conn.execute(text(f'ALTER TABLE generated_content ADD COLUMN {col} {col_type}'))

            if 'product_history' in inspector.get_table_names():
                ph_cols = {c['name'] for c in inspector.get_columns('product_history')}
                ph_new_cols = {'batch_id': 'VARCHAR(36)', 'product_image_url': 'VARCHAR(1024)', 'shopify_url': 'VARCHAR(1024)'}
                for col, col_type in ph_new_cols.items():
                    if col not in ph_cols:
                        conn.execute(text(f'ALTER TABLE product_history ADD COLUMN {col} {col_type}'))

            if 'shop_config' in inspector.get_table_names():
                sc_cols = {c['name'] for c in inspector.get_columns('shop_config')}
                if 'sections' not in sc_cols:
                    conn.execute(text("ALTER TABLE shop_config ADD COLUMN sections TEXT DEFAULT '[]'"))
                if 'website_domain' not in sc_cols:
                    conn.execute(text('ALTER TABLE shop_config ADD COLUMN website_domain VARCHAR(255) NULL'))

            # Add batch_id to image_search_generation (new column for batch grouping)
            if 'image_search_generation' in inspector.get_table_names():
                isg_cols = {c['name'] for c in inspector.get_columns('image_search_generation')}
                if 'batch_id' not in isg_cols:
                    conn.execute(text('ALTER TABLE image_search_generation ADD COLUMN batch_id VARCHAR(36) NULL'))
                    conn.execute(text('ALTER TABLE image_search_generation ADD INDEX ix_image_search_generation_batch_id (batch_id)'))
                if 'source' not in isg_cols:
                    conn.execute(text("ALTER TABLE image_search_generation ADD COLUMN source VARCHAR(20) DEFAULT 'manual'"))
                if 'review_status' not in isg_cols:
                    conn.execute(text("ALTER TABLE image_search_generation ADD COLUMN review_status VARCHAR(20) DEFAULT 'pending'"))
                    conn.execute(text('ALTER TABLE image_search_generation ADD INDEX ix_image_search_generation_review_status (review_status)'))

            # Widen job_event.payload from TEXT (64 KB) to MEDIUMTEXT (16 MB)
            # product_done payloads with full HTML can easily exceed 64 KB.
            if 'job_event' in inspector.get_table_names():
                col_info = {c['name']: c for c in inspector.get_columns('job_event')}
                if 'payload' in col_info:
                    col_type_str = str(col_info['payload']['type']).upper()
                    if 'MEDIUMTEXT' not in col_type_str and 'LONGTEXT' not in col_type_str:
                        conn.execute(text('ALTER TABLE job_event MODIFY COLUMN payload MEDIUMTEXT NOT NULL'))

            conn.commit()                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               