"""
content_generation/app.py — Everything related to AI content generation:
prompt building, OpenAI calls, batch/streaming generation jobs, history,
review & publish-to-Shopify, and Shopify product lookups used while
generating content.

Importing this module registers its routes on the shared Flask `app`.
"""
from shared import (
    app, db, login_required, reviewer_required, _get_active_shop, _shopify_base_url, _utc_now,
    GeneratedContent, ProductHistory, BatchHistory, ReviewStatus, GenerationJob, JobEvent, ShopConfig,
    SHOPIFY_METAFIELD_TYPES, SEO_SECTIONS, COST_PER_INPUT_TOKEN, COST_PER_OUTPUT_TOKEN,
    MAX_FORMAT_WORKERS, DEFAULT_PROMPTS, SECTION_LABELS, SECTIONS,
    _GQL_PRODUCT, _GQL_PRODUCT_BY_SKU, _GQL_PRODUCT_UPDATE, _GQL_METAFIELDS_SET, _GQL_PRODUCT_BY_HANDLE,
    _extract_admin_product_id, _is_storefront_url, _shopify_graphql_with_creds,
    _fetch_by_product_id, _fetch_by_storefront_url, _fetch_by_handle_admin,
    _webcate_from_tags, _subcate_from_tags, _parse_gql_product, _parse_rest_product,
    _fetch_shopify_product, fetch_shopify_image_by_product_url, _extract_products_path_segment,
    model, API_KEY, _cancel_events,
)
from flask import render_template, request, jsonify, Response, stream_with_context, session, flash, redirect, url_for
from datetime import datetime, timezone
import openai, requests, os, json, uuid, threading, re, logging, time, hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from html import escape as html_escape
from shared import BASE_URL

logger = logging.getLogger(__name__)

def fetch_shopify_image_by_sku(part_number: str, _domain: str = '', _token: str = '', _api_version: str = '') -> str:
    if not part_number:
        return ''
    with _image_cache_lock:
        if part_number in _image_cache:
            return _image_cache[part_number]

    domain      = (_domain or '').strip()
    token       = (_token or '').strip()
    api_version = (_api_version or '').strip()

    if not domain or not token:
        shop = _get_active_shop()
        if not shop or not shop.domain or not shop.access_token:
            return ''
        domain      = shop.domain.strip()
        token       = shop.access_token.strip()
        api_version = (shop.api_version or '2024-01').strip()

    if not api_version:
        api_version = '2024-01'

    try:
        endpoint = f"{_shopify_base_url(domain)}/admin/api/{api_version}/graphql.json"
        headers = {
            'Content-Type':           'application/json',
            'X-Shopify-Access-Token': token,
        }
        payload = {
            'query':     _GQL_PRODUCT_BY_SKU,
            'variables': {'query': f'sku:{part_number}'},
        }
        resp = requests.post(endpoint, json=payload, headers=headers, timeout=10)
        if not resp.ok:
            return ''

        body = resp.json()
        edges = ((body.get('data') or {}).get('products', {}).get('edges') or [])
        if not edges:
            with _image_cache_lock:
                _image_cache[part_number] = ''
            return ''

        product_node = edges[0]['node']
        image_url = ''
        for variant_edge in (product_node.get('variants', {}).get('edges') or []):
            v = variant_edge['node']
            if v.get('sku', '').strip().lower() == part_number.strip().lower():
                variant_img = (v.get('image') or {}).get('url', '')
                if variant_img:
                    image_url = variant_img
                    break

        if not image_url:
            img_edges = (product_node.get('images', {}).get('edges') or [])
            if img_edges:
                image_url = img_edges[0]['node'].get('url', '')

        with _image_cache_lock:
            _image_cache[part_number] = image_url
        return image_url
    except Exception:
        return ''


def _shopify_graphql(query: str, variables: dict | None = None) -> dict:
    shop = _get_active_shop()
    if not shop or not shop.domain or not shop.access_token:
        raise ValueError('No active Shopify shop configured. Add and activate a shop in Shop Config.')
    api_version = (shop.api_version or '2024-01').strip()
    endpoint = f"{_shopify_base_url(shop.domain)}/admin/api/{api_version}/graphql.json"
    headers = {
        'Content-Type':           'application/json',
        'X-Shopify-Access-Token': shop.access_token,
    }
    resp = requests.post(endpoint, json={'query': query, 'variables': variables or {}}, headers=headers, timeout=45)
    if not resp.ok:
        raise RuntimeError(f'Shopify API HTTP {resp.status_code}: {resp.text[:500]}')
    return resp.json()


def _shopify_product_gid_from_product_url(shopify_url: str) -> str | None:
    u = (shopify_url or '').strip()
    if not u:
        return None
    admin_id = _extract_admin_product_id(u)
    if admin_id:
        return f'gid://shopify/Product/{admin_id}'
    seg = _extract_products_path_segment(u)
    if not seg:
        return None
    if seg.isdigit():
        return f'gid://shopify/Product/{seg}'
    body = _shopify_graphql(_GQL_PRODUCT_BY_HANDLE, {'handle': seg})
    if body.get('errors'):
        raise RuntimeError(f'Shopify productByHandle: {body["errors"]}')
    node = (body.get('data') or {}).get('productByHandle')
    return node.get('id') if node else None


def _shopify_product_gid_from_sku(part_number: str) -> str | None:
    """Resolve a Shopify product GID by SKU when no URL is available."""
    if not part_number:
        return None
    body = _shopify_graphql(_GQL_PRODUCT_BY_SKU, {'query': f'sku:{part_number}'})
    edges = ((body.get('data') or {}).get('products', {}).get('edges') or [])
    if not edges:
        return None
    return edges[0]['node'].get('id')


def _latest_shopify_url_for_product(product_title: str, part_number: str) -> str:
    row = (ProductHistory.query
           .filter_by(product_title=product_title, part_number=part_number)
           .filter(ProductHistory.shopify_url.isnot(None))
           .filter(ProductHistory.shopify_url != '')
           .order_by(ProductHistory.created_at.desc())
           .first())
    return (row.shopify_url or '').strip() if row else ''


def _latest_generated_record(product_title: str, part_number: str, section: str):
    return (GeneratedContent.query
            .filter_by(product_title=product_title, part_number=part_number, section=section)
            .order_by(GeneratedContent.created_at.desc())
            .first())


def _record_html_for_shopify(rec) -> str:
    if not rec:
        return ''
    return (rec.html_text or '').strip()


def _get_metafield_cfg_by_key() -> dict:
    """
    Reads the active shop's saved metafields (namespace, key, type) and returns
    a dict keyed by metafield key, e.g.:
      { 'bullet_points': {'namespace': 'descriptors', 'key': 'bullet_points', 'type': 'multi_line_text_field'} }
    Falls back to namespace='custom' + SHOPIFY_METAFIELD_TYPES for any missing entry.
    """
    defaults = {
        k: {'namespace': 'custom', 'key': k, 'type': t}
        for k, t in SHOPIFY_METAFIELD_TYPES.items()
    }
    try:
        shop = _get_active_shop()
        if not shop:
            return defaults
        saved = json.loads(shop.metafields or '[]')
        for m in saved:
            mf_key = (m.get('key') or '').strip()
            if not mf_key:
                continue
            defaults[mf_key] = {
                'namespace': (m.get('namespace') or 'custom').strip(),
                'key':       mf_key,
                'type':      (m.get('type') or SHOPIFY_METAFIELD_TYPES.get(mf_key, 'multi_line_text_field')).strip(),
            }
    except Exception as e:
        logger.warning(f'_get_metafield_cfg_by_key failed: {e}')
    return defaults


def publish_generated_content_to_shopify(product_title: str, part_number: str, shopify_url: str = '') -> dict:
    gid = _shopify_product_gid_from_product_url(shopify_url) if shopify_url else None
    if not gid:
        # Fall back to SKU-based lookup using the active shop
        gid = _shopify_product_gid_from_sku(part_number)
    if not gid:
        raise ValueError(f'Could not resolve a Shopify product for part number "{part_number}". Check that the SKU exists in your Shopify store.')

    body_html    = _record_html_for_shopify(_latest_generated_record(product_title, part_number, 'product_description'))
    mf_specs     = _record_html_for_shopify(_latest_generated_record(product_title, part_number, 'technical_specifications'))
    mf_bullets   = _record_html_for_shopify(_latest_generated_record(product_title, part_number, 'short_description'))
    mf_causes    = _record_html_for_shopify(_latest_generated_record(product_title, part_number, 'common_problems'))
    mf_install   = _record_html_for_shopify(_latest_generated_record(product_title, part_number, 'installation_guide'))

    # Fetch SEO plain text (not HTML) for meta title and meta description
    meta_title_rec = _latest_generated_record(product_title, part_number, 'meta_title')
    meta_desc_rec  = _latest_generated_record(product_title, part_number, 'meta_description')
    meta_title_val = (meta_title_rec.plain_text or '').strip() if meta_title_rec else ''
    meta_desc_val  = (meta_desc_rec.plain_text  or '').strip() if meta_desc_rec  else ''

    if not any([body_html, mf_specs, mf_bullets, mf_causes, mf_install, meta_title_val, meta_desc_val]):
        raise ValueError('No generated content found for this product — nothing to publish.')

    # Always update title; include descriptionHtml only if generated
    # Include SEO fields if meta title or meta description were generated
    product_input = {'id': gid, 'title': product_title}
    if body_html:
        product_input['descriptionHtml'] = body_html
    if meta_title_val or meta_desc_val:
        seo_input = {}
        if meta_title_val:
            seo_input['title'] = meta_title_val[:255]  # Shopify SEO title limit
        if meta_desc_val:
            seo_input['description'] = meta_desc_val[:512]  # Shopify SEO description limit
        product_input['seo'] = seo_input

    pu = _shopify_graphql(_GQL_PRODUCT_UPDATE, {'input': product_input})
    if pu.get('errors'):
        raise RuntimeError(f'Shopify productUpdate: {pu["errors"]}')
    pu_user_errors = (((pu.get('data') or {}).get('productUpdate') or {}).get('userErrors') or [])
    if pu_user_errors:
        raise RuntimeError(f'Shopify productUpdate userErrors: {pu_user_errors}')

    # Build a dynamic section → metafield config from the shop's saved sections.
    # Each saved section has: { name, metafield, namespace, key, target, type, ... }
    # We match on normalized section name against SECTION_LABELS values.
    def _build_dynamic_section_mf_cfg() -> dict:
        """
        Returns { section_db_key: { namespace, key, type } } built from the active
        shop's saved sections, matched by normalizing the section's 'name' field
        against SECTION_LABELS values. Falls back to hardcoded defaults if no
        shop section config is found for a given section.
        """
        # Inverted: normalized label → section DB key  e.g. "short description" → "short_description"
        label_to_section_key = {v.strip().lower(): k for k, v in SECTION_LABELS.items()}

        result = {}
        try:
            shop = _get_active_shop()
            if shop:
                saved_sections = json.loads(shop.sections or '[]')
                for s in saved_sections:
                    s_name = (s.get('name') or '').strip().lower()
                    section_key = label_to_section_key.get(s_name)
                    if not section_key:
                        continue
                    # Only push sections that target a metafield (not bodyHtml / SEO)
                    if (s.get('target') or '').strip().lower() != 'metafield':
                        continue
                    ns  = (s.get('namespace') or 'custom').strip()
                    key = (s.get('key') or '').strip()
                    typ = (s.get('type') or SHOPIFY_METAFIELD_TYPES.get(key, 'multi_line_text_field')).strip()
                    if key:
                        result[section_key] = {'namespace': ns, 'key': key, 'type': typ}
        except Exception as e:
            logger.warning(f'_build_dynamic_section_mf_cfg failed: {e}')

        # Hardcoded fallback for any section not covered by shop config
        fallback = {
            'technical_specifications': {'namespace': 'custom', 'key': 'technical_specifications',    'type': 'multi_line_text_field'},
            'short_description':        {'namespace': 'custom', 'key': 'short_description',           'type': 'single_line_text_field'},
            'common_problems':          {'namespace': 'custom', 'key': 'common_causes',               'type': 'multi_line_text_field'},
            'installation_guide':       {'namespace': 'custom', 'key': 'symptoms_installation_guide', 'type': 'multi_line_text_field'},
        }
        for k, v in fallback.items():
            result.setdefault(k, v)
        return result

    section_mf_cfg = _build_dynamic_section_mf_cfg()
    logger.info(f"section_mf_cfg (dynamic): {section_mf_cfg}")

    metafields_payload = []
    for section_key, val in (
        ('technical_specifications', mf_specs),
        ('short_description',        mf_bullets),
        ('common_problems',          mf_causes),
        ('installation_guide',       mf_install),
    ):
        if not (val or '').strip():
            continue
        cfg = section_mf_cfg.get(section_key)
        if not cfg:
            logger.warning(f'No metafield config found for section "{section_key}", skipping.')
            continue
        # single_line_text_field rejects newlines — collapse to one line while keeping HTML tags
        mf_value = val
        if cfg['type'] == 'single_line_text_field':
            mf_value = ' '.join(val.split())
        metafields_payload.append({
            'ownerId':   gid,
            'namespace': cfg['namespace'],
            'key':       cfg['key'],
            'type':      cfg['type'],
            'value':     mf_value,
        })

    if metafields_payload:
        ms = _shopify_graphql(_GQL_METAFIELDS_SET, {'metafields': metafields_payload})
        if ms.get('errors'):
            raise RuntimeError(f'Shopify metafieldsSet: {ms["errors"]}')
        ms_user_errors = (((ms.get('data') or {}).get('metafieldsSet') or {}).get('userErrors') or [])
        if ms_user_errors:
            logger.info(f"error-:{ms_user_errors}")
            raise RuntimeError(f'Shopify metafieldsSet userErrors: {ms_user_errors}')

    return {
        'product_gid':          gid,
        'body_html_updated':    bool(body_html),
        'metafields_set':       len(metafields_payload),
        'seo_title_updated':    bool(meta_title_val),
        'seo_description_updated': bool(meta_desc_val),
        'shopify_url':          (shopify_url or '')[:512],
    }


def _get_shop_section_templates_by_name() -> dict:
    """
    Returns {normalized_name: template_string} from the active shop's saved sections.
    """
    try:
        shop = _get_active_shop()
        if not shop:
            return {}
        sections = json.loads(shop.sections or '[]')
        # Map using the 'name' field normalized to lowercase
        return {s['name'].strip().lower(): s.get('template', '') for s in sections if s.get('name')}
    except Exception as e:
        logger.error(f"Error reading templates by name: {e}")
        return {}


def _get_shop_section_prompts_by_name() -> dict:
    """
    Returns {normalized_name: prompt_string} from the active shop's saved sections.
    """
    try:
        shop = _get_active_shop()
        if not shop:
            return {}
        sections = json.loads(shop.sections or '[]')
        # Map using the 'name' field normalized to lowercase
        return {s['name'].strip().lower(): s.get('prompt', '') for s in sections if s.get('name')}
    except Exception as e:
        logger.error(f"Error reading prompts by name: {e}")

# ─── LLM Structure Frame Injection Pipeline ──────────────────────────────

def _plain_text_to_html(plain_content: str) -> str:
    """
    Converts plain text lines (bullet points, numbered lists, or paragraphs)
    into clean <ul><li> HTML. Falls back to <p> tags for paragraph-style content.
    """
    cleaned = (plain_content or '').strip()
    if not cleaned:
        return ''

    # Normalize bullet separators
    if '•' in cleaned and '\n' not in cleaned:
        cleaned = cleaned.replace('•', '\n•')

    lines = [line.strip().lstrip('•-*0123456789.) ').strip() for line in cleaned.splitlines() if line.strip()]
    lines = [l for l in lines if l]

    if not lines:
        return f'<div>{plain_content}</div>'

    # If there are multiple lines, render as <ul><li> list
    if len(lines) > 1:
        items = ''.join(f'<li>{line}</li>' for line in lines)
        return f'<ul>{items}</ul>'

    # Single block of text — wrap in a <p>
    return f'<p>{lines[0]}</p>'

def _parse_retry_wait(exc, fallback: float = 5.0) -> float:
    """
    Extracts the suggested wait time from an OpenAI RateLimitError.
    Handles both string messages and the nested error dict OpenAI returns.
    e.g. "Please try again in 1.83s" -> 1.83 + 0.5 buffer
         "Please try again in 2m30s" -> 150.5
    Falls back to `fallback` if no time found.
    """
    # Try to get the full message string from the exception body
    try:
        msg = exc.body.get("error", {}).get("message", "") if hasattr(exc, "body") and exc.body else str(exc)
    except Exception:
        msg = str(exc)

    # Pattern handles: "1.83s", "2m30s", "2m 30s"
    m = re.search(r"in\s+(?:(\d+)m\s*)?(\d+(?:\.\d+)?)s", msg)
    if m:
        minutes = float(m.group(1) or 0)
        seconds = float(m.group(2))
        total = minutes * 60 + seconds + 0.5   # +0.5s safety buffer
        return max(total, 1.0)                  # never wait less than 1s
    return fallback


def format_content_via_template(plain_content, text_template, product_title, part_number, product_image_url=''):
    """
    LEGACY PASSTHROUGH — now a no-op.

    Content + HTML generation are merged into a single API call inside
    _generate_section_task and _generate_section_batch via
    build_merged_prompt() / _build_batch_section_prompt().

    When a template was used the caller already has final HTML in
    `plain_content`; when there was no template we convert plain text
    to basic HTML here without any extra API round-trip.

    Returns: (plain_content, html_text, 0, 0)  — token counts are 0
    because no extra API call is made here any more.
    """
    if not (text_template or '').strip():
        # No template: convert plain text to basic HTML locally (free, instant)
        html_text = _plain_text_to_html(plain_content)
        return plain_content, html_text, 0, 0

    # Template was used: the generation prompt already produced HTML.
    # plain_content already contains the final HTML output; pass it through.
    html_text = plain_content
    # Clean up any stray backtick fences the model may have emitted
    if html_text.startswith("```html"):
        html_text = html_text.split("```html", 1)[1].split("```", 1)[0].strip()
    elif html_text.startswith("```"):
        html_text = html_text.split("```", 1)[1].split("```", 1)[0].strip()
    return plain_content, html_text, 0, 0


def build_prompt(section, product_title, part_number, user_prompt='', brand='', appliance_type='', part_type=''):
    if user_prompt:
        has_placeholders = any(p in user_prompt for p in [
            '{product_title}', '{part_number}',
            '{BRAND}', '{APPLIANCE_TYPE}', '{PART_TYPE}', '{PART_NUMBER}',
        ])
        if has_placeholders:
            final = (user_prompt
                .replace('{product_title}', product_title)
                .replace('{part_number}', part_number)
                .replace('{BRAND}', brand)
                .replace('{APPLIANCE_TYPE}', appliance_type)
                .replace('{PART_TYPE}', part_type)
                .replace('{PART_NUMBER}', part_number))
        else:
            final = f"Product Title: {product_title}\nPart Number: {part_number}\n\n{user_prompt}"
        return 'user', final
    elif section in DEFAULT_PROMPTS:
        tmpl = DEFAULT_PROMPTS[section]
        return 'default', tmpl.format(product_title=product_title, part_number=part_number)
    else:
        final = f"Product Title: {product_title}\nPart Number: {part_number}\n\nGenerate content text lines for '{section}'."
        return 'default', final


def build_merged_prompt(section, product_title, part_number, user_prompt='',
                        brand='', appliance_type='', part_type='',
                        template_text='', product_image_url=''):
    """
    Returns (prompt_type, final_prompt) where the single prompt instructs the
    model to BOTH generate the content AND format it into the HTML template in
    one shot — eliminating the second 'format_content_via_template' API call.

    When no template is provided the prompt asks for plain text only (same as
    before), and the caller converts it to basic HTML locally via
    _plain_text_to_html().
    """
    prompt_type, base_prompt = build_prompt(
        section, product_title, part_number, user_prompt,
        brand=brand, appliance_type=appliance_type, part_type=part_type
    )

    if not (template_text or '').strip() or section in SEO_SECTIONS:
        # No template — return plain-text prompt unchanged
        return prompt_type, base_prompt

    hydrated_template = (
        template_text
        .replace('{product_title}', product_title)
        .replace('{part_number}', part_number)
    )

    merged_prompt = (
        f"{base_prompt}\n\n"
        f"─── OUTPUT FORMAT ───\n"
        f"You must embed your generated content directly into the HTML template below.\n"
        f"RULE 1: Preserve the exact HTML structure, CSS classes, attributes, and any "
        f"custom elements (buttons, 'Read More' tags, etc.) from the template.\n"
        f"RULE 2: Map paragraphs to <p> tags and bullet items to <li> elements as the "
        f"template structure dictates.\n"
        f"RULE 3: If the template contains an <img> tag with an empty src, set it to "
        f'src="{product_image_url}".\n'
        f"RULE 4: Output ONLY the final HTML. Do NOT wrap it in markdown backticks.\n\n"
        f"─── HTML TEMPLATE ───\n{hydrated_template}\n\n"
        f"Generate the completed HTML now:"
    )
    return prompt_type, merged_prompt


_openai_client: openai.OpenAI | None = None
_openai_client_lock = threading.Lock()

def get_openai_client() -> openai.OpenAI:
    """
    Return a module-level shared OpenAI client so that the underlying
    httpx connection pool is reused across calls instead of creating a
    new TCP connection for every request.  Thread-safe lazy initialisation.

    max_retries=0 disables the SDK's built-in retry so ONLY our controlled
    retry logic in _generate_section_task / format_content_via_template fires.
    """
    global _openai_client
    if _openai_client is None:
        with _openai_client_lock:
            if _openai_client is None:
                import httpx
                _openai_client = openai.OpenAI(
                    api_key=API_KEY,
                    http_client=httpx.Client(limits=httpx.Limits(
                        max_connections=20,
                        max_keepalive_connections=10,
                    )),
                    max_retries=0,
                )
    return _openai_client


def _generate_section_task(section, product_title, part_number, custom_prompts, template_text='', product_image_url='',
                           brand='', appliance_type='', part_type=''):
    user_prompt = custom_prompts.get(section, '')

    # ── Merged prompt: content generation + HTML formatting in ONE API call ───
    has_template = bool((template_text or '').strip()) and section not in SEO_SECTIONS
    prompt_type, final_prompt = build_merged_prompt(
        section, product_title, part_number, user_prompt,
        brand=brand, appliance_type=appliance_type, part_type=part_type,
        template_text=template_text, product_image_url=product_image_url
    )

    max_retries = 4

    for attempt in range(max_retries):
        try:
            _client = get_openai_client()
            resp = _client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": final_prompt}],
                temperature=0.7,
                max_tokens=2500
            )
            raw_output = resp.choices[0].message.content.strip()
            usage = resp.usage

            if section in SEO_SECTIONS:
                # SEO fields are always plain text — no HTML
                plain_text = raw_output
                html_text  = raw_output
            elif has_template:
                # Model already returned final HTML — clean up any stray fences
                html_text = raw_output
                if html_text.startswith("```html"):
                    html_text = html_text.split("```html", 1)[1].split("```", 1)[0].strip()
                elif html_text.startswith("```"):
                    html_text = html_text.split("```", 1)[1].split("```", 1)[0].strip()
                # Store a plain-text version by stripping tags (for DB plain_text column)
                plain_text = re.sub(r'<[^>]+>', ' ', html_text).strip()
            else:
                # No template: convert plain text → basic HTML locally (no extra API call)
                plain_text = raw_output
                html_text  = _plain_text_to_html(raw_output)

            total_in  = usage.prompt_tokens
            total_out = usage.completion_tokens

            return section, {
                'plain_text': plain_text, 'html_text': html_text,
                'input_tokens': total_in,
                'output_tokens': total_out,
                'total_tokens': total_in + total_out,
                'prompt_type': prompt_type,
                'prompt_used': final_prompt,
                'error': None
            }

        except openai.RateLimitError as e:
            if attempt < max_retries - 1:
                wait = _parse_retry_wait(e, fallback=5.0 * (2 ** attempt))
                logger.warning(
                    f"Rate limit hit for section='{section}' part='{part_number}', "
                    f"retrying in {wait:.1f}s (attempt {attempt + 1}/{max_retries})"
                )
                time.sleep(wait)
            else:
                logger.error(f"Rate limit exceeded after {max_retries} attempts for section='{section}' part='{part_number}'")
                return section, {
                    'plain_text': '', 'html_text': '', 'record_id': None,
                    'input_tokens': 0, 'output_tokens': 0, 'total_tokens': 0,
                    'prompt_type': prompt_type, 'prompt_used': final_prompt,
                    'error': f'Rate limit exceeded after {max_retries} retries: {str(e)}'
                }

        except Exception as e:
            return section, {
                'plain_text': '', 'html_text': '', 'record_id': None,
                'input_tokens': 0, 'output_tokens': 0, 'total_tokens': 0,
                'prompt_type': prompt_type, 'prompt_used': final_prompt,
                'error': str(e)
            }


# ─── Batch LLM call: one section, N products ─────────────────────────────────

def _build_batch_section_prompt(section: str, products: list[dict], custom_prompts: dict,
                                template_text: str = '', product_image_urls: dict | None = None,
                                client_ids: list[str] | None = None) -> tuple[str, str]:
    """
    Build a single prompt that asks the model to generate content for `section`
    for all products at once.

    When a template_text is provided (and section is not an SEO section) the
    prompt instructs the model to output the content **already embedded in the
    HTML template** for each product — eliminating the second API call that
    format_content_via_template used to make.

    Returns (prompt_type, prompt_text).

    NOTE: part_number is NOT guaranteed unique within a batch (the same OEM
    part_number frequently cross-references multiple distinct listings/models).
    Keying the model's JSON reply by part_number causes silent key collisions —
    duplicate JSON keys mean only the last product's content survives parsing,
    and every other product sharing that part_number ends up with the wrong
    (last-written) content. To avoid this, each product is assigned a unique
    `client_id` (see `_client_id_for_product`) and the model is instructed to
    key its JSON reply by that id instead:
        { "<client_id>": "<generated plain text OR final HTML>", ... }
    """
    if product_image_urls is None:
        product_image_urls = {}
    if client_ids is None or len(client_ids) != len(products):
        client_ids = [str(i) for i in range(len(products))]

    has_template = bool((template_text or '').strip()) and section not in SEO_SECTIONS

    user_prompt_tmpl = custom_prompts.get(section, '')
    if user_prompt_tmpl:
        prompt_type = 'user'
    elif section in DEFAULT_PROMPTS:
        prompt_type = 'default'
        user_prompt_tmpl = DEFAULT_PROMPTS[section]
    else:
        prompt_type = 'default'
        user_prompt_tmpl = f"Generate content text lines for '{{section}}'."

    # Check if the template uses per-product placeholders that must be substituted
    # individually (brand, appliance_type, part_type differ per product).
    PER_PRODUCT_PLACEHOLDERS = ('{BRAND}', '{APPLIANCE_TYPE}', '{PART_TYPE}', '{PART_NUMBER}')
    has_per_product_placeholders = any(ph in user_prompt_tmpl for ph in PER_PRODUCT_PLACEHOLDERS)

    if has_per_product_placeholders:
        per_product_blocks = []
        for cid, p in zip(client_ids, products):
            substituted = (user_prompt_tmpl
                .replace('{product_title}', p.get('product_title', ''))
                .replace('{part_number}',   p.get('part_number', ''))
                .replace('{BRAND}',         p.get('brand', ''))
                .replace('{APPLIANCE_TYPE}',p.get('appliance_type', ''))
                .replace('{PART_TYPE}',     p.get('part_type', ''))
                .replace('{PART_NUMBER}',   p.get('part_number', '')))
            per_product_blocks.append(
                f'[id="{cid}" part_number="{p.get("part_number","")}" title="{p.get("product_title","")}"]\n{substituted}'
            )
        per_product_instruction = (
            "For each product below, follow the specific instruction provided for it:\n\n"
            + "\n\n".join(per_product_blocks)
            + "\n\n"
        )
        products_block = ""
    elif '{product_title}' in user_prompt_tmpl or '{part_number}' in user_prompt_tmpl:
        per_product_instruction = (
            "For each product listed below apply the following instruction template:\n"
            f"{user_prompt_tmpl}\n\n"
        )
        products_block = "PRODUCTS:\n" + "\n".join(
            f'- id="{cid}" part_number="{p.get("part_number","")}" title="{p.get("product_title","")}"'
            f' brand="{p.get("brand","")}" appliance_type="{p.get("appliance_type","")}"'
            f' part_type="{p.get("part_type","")}"'
            for cid, p in zip(client_ids, products)
        )
    else:
        per_product_instruction = (
            f"For each product listed below:\n{user_prompt_tmpl}\n\n"
        )
        products_block = "PRODUCTS:\n" + "\n".join(
            f'- id="{cid}" part_number="{p.get("part_number","")}" title="{p.get("product_title","")}"'
            f' brand="{p.get("brand","")}" appliance_type="{p.get("appliance_type","")}"'
            f' part_type="{p.get("part_type","")}"'
            for cid, p in zip(client_ids, products)
        )

    # ── Build HTML template instruction block (merged approach) ──────────────
    if has_template:
        # Build a per-product hydrated template snippet so image URLs are correct
        template_blocks = []
        for cid, p in zip(client_ids, products):
            pn    = p.get('part_number', '')
            pt    = p.get('product_title', '')
            img   = product_image_urls.get(cid, '') or product_image_urls.get(pn, '')
            hydrated = (
                template_text
                .replace('{product_title}', pt)
                .replace('{part_number}', pn)
            )
            # If template has an empty src img tag, pre-fill it
            if img:
                hydrated = re.sub(r'src=""', f'src="{img}"', hydrated)
                hydrated = re.sub(r"src=''", f"src='{img}'", hydrated)
            template_blocks.append(f'[id="{cid}" part_number="{pn}"]\n{hydrated}')

        template_instruction = (
            "\n\n─── OUTPUT FORMAT (HTML) ───\n"
            "For EACH product, embed your generated content into the corresponding "
            "HTML template below. Rules:\n"
            "1. Preserve the exact HTML structure, CSS classes, attributes, and any "
            "custom elements (buttons, 'Read More' tags, etc.).\n"
            "2. Map paragraphs to <p> tags and bullet items to <li> elements.\n"
            "3. If an <img> tag has an empty src, populate it with the image URL "
            "already pre-filled in each template block below.\n"
            "4. Output ONLY valid HTML per product (no markdown backticks).\n\n"
            "─── PER-PRODUCT TEMPLATES ───\n"
            + "\n\n".join(template_blocks)
        )

        output_rules = (
            "\n\nOUTPUT RULES:\n"
            "1. Reply ONLY with a single valid JSON object.\n"
            "2. Each key is the exact `id` value shown in brackets above for that "
            "product — NOT the part_number (multiple products can share the same "
            "part_number, so part_number must never be used as the JSON key).\n"
            "3. Each value is the FINAL HTML string for that product (with content "
            "embedded into the template).\n"
            "4. Use \\n inside strings for line-breaks. Do NOT add extra JSON keys.\n"
            "5. Do NOT wrap the JSON in markdown backticks.\n"
            'Example:\n{"' + (client_ids[0] if client_ids else '0') + '": "<div>...</div>", "'
            + (client_ids[1] if len(client_ids) > 1 else '1') + '": "<div>...</div>"}'
        )
    else:
        template_instruction = ""
        output_rules = (
            "\n\nOUTPUT RULES:\n"
            "1. Reply ONLY with a single valid JSON object.\n"
            "2. Each key is the exact `id` value shown above for that product — NOT "
            "the part_number (multiple products can share the same part_number, so "
            "part_number must never be used as the JSON key).\n"
            "3. Each value is the generated plain text for that product (no HTML).\n"
            "4. Use \\n inside strings to represent line-breaks. Do NOT add extra JSON keys.\n"
            "5. Do NOT wrap the JSON in markdown backticks.\n"
            "Example:\n"
            '{"' + (client_ids[0] if client_ids else '0') + '": "Line 1\\nLine 2", "'
            + (client_ids[1] if len(client_ids) > 1 else '1') + '": "Line 1\\nLine 2"}'
        )

    prompt = (
        "You are a professional product copywriter.\n\n"
        + per_product_instruction
        + (products_block + "\n\n" if products_block else "")
        + template_instruction
        + output_rules
    )
    return prompt_type, prompt


def _generate_section_batch(
    section: str,
    products: list[dict],
    custom_prompts: dict,
    template_text: str = '',
    product_image_urls: dict | None = None,
    max_retries: int = 4,
    cancel_event: 'threading.Event | None' = None,
    client_ids: list[str] | None = None,
) -> dict:
    """
    Generate `section` content for all `products` in ONE API call.

    Returns a dict keyed by client_id (NOT part_number — part_number can repeat
    within a batch when the same OEM part cross-references multiple distinct
    listings, and a plain-part_number key would collide, silently dropping all
    but the last product's content):
        {
          "<client_id>": {
              "plain_text": ..., "html_text": ...,
              "input_tokens": ..., "output_tokens": ..., "total_tokens": ...,
              "prompt_type": ..., "prompt_used": ..., "error": None | str
          }
        }

    Falls back product-by-product if JSON parsing fails after retries.
    """
    if product_image_urls is None:
        product_image_urls = {}
    if client_ids is None or len(client_ids) != len(products):
        client_ids = [str(i) for i in range(len(products))]

    part_numbers = [p.get('part_number', '') for p in products]
    n_products   = len(products)

    # ── LOG: confirm we are batching N products into 1 call ──────────────────
    logger.info(
        f"[BATCH-API] ▶ SINGLE API CALL | section='{section}' | "
        f"products={n_products} | part_numbers={part_numbers} | client_ids={client_ids}"
    )

    prompt_type, final_prompt = _build_batch_section_prompt(
        section, products, custom_prompts,
        template_text=template_text,
        product_image_urls=product_image_urls,
        client_ids=client_ids,
    )

    # ── Call the model with retry on rate-limit ──────────────────────────────
    raw_text = ''
    usage_in = usage_out = 0
    last_error: Exception | None = None

    for attempt in range(max_retries):
        logger.info(
            f"[BATCH-API] → Sending request to OpenAI (streaming) | section='{section}' | "
            f"attempt={attempt + 1}/{max_retries} | products_in_prompt={n_products} | "
            f"part_numbers={part_numbers}"
        )
        try:
            _client = get_openai_client()

            # ── Streaming call: first token arrives in ~1s; chunks accumulate ──
            stream = _client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": final_prompt}],
                temperature=0.7,
                max_tokens=4000,   # 5 products × ~600 HTML tokens each; was 1500 which caused constant truncation
                stream=True,
                stream_options={"include_usage": True},  # get token counts at end
            )

            chunks = []
            usage_in = usage_out = 0
            for chunk in stream:
                # ── Cancel check: abort stream mid-flight ────────────
                if cancel_event and cancel_event.is_set():
                    logger.info(f"[BATCH-API] ⏹ Cancel signal | section='{section}'")
                    try:
                        stream.close()
                    except Exception:
                        pass
                    return {cid: {
                        'plain_text': '', 'html_text': '',
                        'input_tokens': 0, 'output_tokens': 0, 'total_tokens': 0,
                        'prompt_type': 'default', 'prompt_used': '', 'error': 'Cancelled'
                    } for cid in client_ids}
                # Accumulate content delta
                delta = chunk.choices[0].delta.content if chunk.choices else None
                if delta:
                    chunks.append(delta)
                # Final chunk carries usage when stream_options include_usage=True
                if chunk.usage:
                    usage_in  = chunk.usage.prompt_tokens
                    usage_out = chunk.usage.completion_tokens

            raw_text   = "".join(chunks).strip()
            logger.info(f"Raw text: {raw_text}")
            last_error = None

            logger.info(
                f"[BATCH-API] ✓ Stream complete | section='{section}' | "
                f"products_requested={n_products} | "
                f"input_tokens={usage_in} | output_tokens={usage_out} | "
                f"total_tokens={usage_in + usage_out} | "
                f"~tokens_per_product={(usage_in + usage_out) // max(n_products, 1)}"
            )
            break

        except openai.RateLimitError as e:
            last_error = e
            if attempt < max_retries - 1:
                wait = _parse_retry_wait(e, fallback=5.0 * (2 ** attempt))
                logger.warning(
                    f"[BATCH-API] ⚠ Rate limit | section='{section}' | "
                    f"attempt={attempt + 1}/{max_retries} | waiting={wait:.1f}s"
                )
                # Check cancel before sleeping on rate-limit backoff
                if cancel_event and cancel_event.is_set():
                    return {cid: {
                        'plain_text': '', 'html_text': '',
                        'input_tokens': 0, 'output_tokens': 0, 'total_tokens': 0,
                        'prompt_type': 'default', 'prompt_used': '', 'error': 'Cancelled'
                    } for cid in client_ids}
                time.sleep(wait)
            else:
                logger.error(
                    f"[BATCH-API] ✗ Rate limit EXCEEDED | section='{section}' | "
                    f"all {max_retries} attempts failed"
                )

        except Exception as e:
            last_error = e
            logger.error(
                f"[BATCH-API] ✗ API error | section='{section}' | "
                f"attempt={attempt + 1} | error={e}"
            )
            break   # non-rate-limit errors won't improve on retry

    # ── If the API call itself failed, fall back to individual calls ──────────
    if last_error is not None or not raw_text:
        logger.warning(
            f"[BATCH-API] ⚠ FALLING BACK to per-product calls | section='{section}' | "
            f"reason={'API error' if last_error else 'empty response'} | "
            f"will now make {n_products} individual API calls instead of 1"
        )
        results = {}
        for cid, p in zip(client_ids, products):
            pn = p.get('part_number', '')
            logger.info(
                f"[BATCH-API][FALLBACK] → Individual call | section='{section}' | "
                f"client_id='{cid}' part_number='{pn}'"
            )
            sec, res = _generate_section_task(
                section,
                p.get('product_title', ''),
                pn,
                custom_prompts,
                template_text,
                product_image_urls.get(cid, '') or product_image_urls.get(pn, ''),
                brand=p.get('brand', ''),
                appliance_type=p.get('appliance_type', ''),
                part_type=p.get('part_type', ''),
            )
            logger.info(
                f"[BATCH-API][FALLBACK] ✓ Done | section='{section}' | client_id='{cid}' | "
                f"tokens={res.get('total_tokens', 0)} | error={res.get('error')}"
            )
            results[cid] = res
        return results

    # ── Strip markdown fences if the model ignored our instruction ────────────
    cleaned = raw_text
    if cleaned.startswith('```json'):
        cleaned = cleaned.split('```json', 1)[1].split('```', 1)[0].strip()
        logger.debug(f"[BATCH-API] Stripped ```json fence | section='{section}'")
    elif cleaned.startswith('```'):
        cleaned = cleaned.split('```', 1)[1].split('```', 1)[0].strip()
        logger.debug(f"[BATCH-API] Stripped ``` fence | section='{section}'")

    # ── Parse JSON ────────────────────────────────────────────────────────────
    try:
        parsed: dict = json.loads(cleaned)
        if not isinstance(parsed, dict):
            raise ValueError(f"Expected a JSON object, got {type(parsed)}")

        returned_keys  = list(parsed.keys())
        missing_keys   = [cid for cid in client_ids if cid not in parsed]
        extra_keys     = [k for k in returned_keys if k not in client_ids]
        logger.info(
            f"[BATCH-API] ✓ JSON parsed | section='{section}' | "
            f"products_requested={n_products} | products_returned={len(returned_keys)} | "
            f"returned_keys={returned_keys} | "
            f"missing={missing_keys if missing_keys else 'none'} | "
            f"unexpected_keys={extra_keys if extra_keys else 'none'}"
        )

    except (json.JSONDecodeError, ValueError) as parse_err:
        logger.warning(
            f"[BATCH-API] ✗ JSON parse FAILED | section='{section}' | error={parse_err} | "
            f"raw_response_preview={raw_text[:300]!r} | "
            f"FALLING BACK to {n_products} individual API calls"
        )
        results = {}
        for cid, p in zip(client_ids, products):
            pn = p.get('part_number', '')
            logger.info(
                f"[BATCH-API][FALLBACK] → Individual call | section='{section}' | "
                f"client_id='{cid}' part_number='{pn}'"
            )
            sec, res = _generate_section_task(
                section,
                p.get('product_title', ''),
                pn,
                custom_prompts,
                template_text,
                product_image_urls.get(cid, '') or product_image_urls.get(pn, ''),
                brand=p.get('brand', ''),
                appliance_type=p.get('appliance_type', ''),
                part_type=p.get('part_type', ''),
            )
            logger.info(
                f"[BATCH-API][FALLBACK] ✓ Done | section='{section}' | client_id='{cid}' | "
                f"tokens={res.get('total_tokens', 0)} | error={res.get('error')}"
            )
            results[cid] = res
        return results

    # ── Per-product token allocation (evenly split; exact counts unavailable) ─
    n = max(len(products), 1)
    per_in  = usage_in  // n
    per_out = usage_out // n

    has_template = bool((template_text or '').strip()) and section not in SEO_SECTIONS

    # ── Build results directly — no second API call needed ───────────────────
    results: dict = {}
    for cid, p in zip(client_ids, products):
        pn            = p.get('part_number', '')
        product_title = p.get('product_title', '')
        raw_value     = parsed.get(cid, '')

        if not raw_value:
            logger.warning(
                f"[BATCH-API] ✗ No content in response | section='{section}' | "
                f"client_id='{cid}' part_number='{pn}' | all returned keys={list(parsed.keys())}"
            )
            results[cid] = {
                'plain_text': '', 'html_text': '',
                'input_tokens': per_in, 'output_tokens': per_out,
                'total_tokens': per_in + per_out,
                'prompt_type': prompt_type, 'prompt_used': final_prompt,
                'error': 'No content returned by model for this product.'
            }
            continue

        logger.info(
            f"[BATCH-API] ✓ Content mapped | section='{section}' | "
            f"client_id='{cid}' part_number='{pn}' | value_len={len(raw_value)} chars | "
            f"allocated_tokens=in:{per_in} out:{per_out}"
        )

        if section in SEO_SECTIONS:
            plain_text = raw_value
            html_text  = raw_value
        elif has_template:
            # Model already returned final HTML embedded in the template
            html_text = raw_value
            if html_text.startswith("```html"):
                html_text = html_text.split("```html", 1)[1].split("```", 1)[0].strip()
            elif html_text.startswith("```"):
                html_text = html_text.split("```", 1)[1].split("```", 1)[0].strip()
            # Strip tags for the plain_text column
            plain_text = re.sub(r'<[^>]+>', ' ', html_text).strip()
        else:
            # No template — convert plain text to basic HTML locally
            plain_text = raw_value
            html_text  = _plain_text_to_html(raw_value)

        results[cid] = {
            'plain_text': plain_text, 'html_text': html_text,
            'input_tokens':  per_in,
            'output_tokens': per_out,
            'total_tokens':  per_in + per_out,
            'prompt_type': prompt_type, 'prompt_used': final_prompt,
            'error': None,
        }

    logger.info(
        f"[BATCH-API] ✓ SECTION COMPLETE | section='{section}' | "
        f"1 API call handled {n_products} products | "
        f"success={len([r for r in results.values() if not r.get('error')])} | "
        f"errors={len([r for r in results.values() if r.get('error')])} | "
        f"total_tokens_this_call={usage_in + usage_out}"
    )

    return results



@app.route('/history')
@login_required
def history_page():
    return render_template('history.html',BASE_URL=BASE_URL)


@app.route('/review')
@reviewer_required
def review_page():
    return render_template('review.html',BASE_URL=BASE_URL)


@app.route('/api/default-prompt/<section>')
@login_required
def get_default_prompt(section):
    return jsonify({'prompt': DEFAULT_PROMPTS.get(section, '')})


@app.route('/api/generate', methods=['POST'])
@login_required
def generate_content():
    data = request.json
    product_title = data.get('product_title', '').strip()
    part_number   = data.get('part_number', '').strip()
    part_type     = data.get('part_type', '').strip()
    brand         = data.get('brand', '').strip()
    appliance_type = data.get('appliance_type', '').strip()
    section       = data.get('section', '').strip() # e.g., 'product_description'
    user_prompt   = data.get('user_prompt', '').strip()
    template_text = data.get('template_text', '').strip()
    product_image_url = data.get('product_image_url', '').strip()

    if not product_title or not part_number or not section:
        return jsonify({'error': 'Product title, part number, and section are required.'}), 400
    if not API_KEY:
        return jsonify({'error': 'OpenAI API key is required.'}), 400

    # Get the exact display name (e.g., 'Product Description' or 'Short Product Description')
    display_name = SECTION_LABELS.get(section, section).strip().lower()
    
    # Quick handling for custom variations in naming structures
    if "short" in display_name:
        display_name = "short description"

    # Fall back directly to the active shop's saved templates and prompts matching by Name
    shop_templates = _get_shop_section_templates_by_name()
    shop_prompts = _get_shop_section_prompts_by_name()

    if not template_text:
        template_text = shop_templates.get(display_name, '')

    if not user_prompt:
        user_prompt = shop_prompts.get(display_name, '')

    # ── Build a single merged prompt (content + HTML in one shot) ───────────
    prompt_type, final_prompt = build_merged_prompt(
        section, product_title, part_number, user_prompt,
        brand=brand, appliance_type=appliance_type, part_type=part_type,
        template_text=template_text, product_image_url=product_image_url
    )
    has_template = bool(template_text) and section not in SEO_SECTIONS

    try:
        client = get_openai_client()
        response = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": final_prompt}],
            temperature=0.7,
            max_tokens=2500
        )
        raw_output = response.choices[0].message.content.strip()
        usage = response.usage

        if section in SEO_SECTIONS:
            plain_text = raw_output
            html_text  = raw_output
        elif has_template:
            html_text = raw_output
            if html_text.startswith("```html"):
                html_text = html_text.split("```html", 1)[1].split("```", 1)[0].strip()
            elif html_text.startswith("```"):
                html_text = html_text.split("```", 1)[1].split("```", 1)[0].strip()
            plain_text = re.sub(r'<[^>]+>', ' ', html_text).strip()
        else:
            plain_text = raw_output
            html_text  = _plain_text_to_html(raw_output)

        total_in  = usage.prompt_tokens
        total_out = usage.completion_tokens

        record = GeneratedContent(
            batch_id=str(uuid.uuid4()),
            product_title=product_title, part_number=part_number,
            part_type=part_type, brand=brand, appliance_type=appliance_type,
            section=section, prompt_type=prompt_type, prompt_used=final_prompt,
            plain_text=plain_text, html_text=html_text,
            input_tokens=total_in, output_tokens=total_out,
            total_tokens=total_in + total_out, model_used=model
        )
        db.session.add(record)
        db.session.commit()

        return jsonify({
            'success': True, 'id': record.id,
            'plain_text': plain_text, 'html_text': html_text,
            'input_tokens': total_in,
            'output_tokens': total_out,
            'total_tokens': total_in + total_out,
            'prompt_type': prompt_type,
            'section_label': SECTION_LABELS.get(section, section)
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


def _client_id_for_product(prod: dict, fallback: str | int = '') -> str:
    """
    Stable string client id for matching generation events to frontend part cards.
    JSON may carry id as int or str; normalize to str. Fall back to index, then
    title|part_number so each product stays distinct.
    """
    raw = prod.get('id')
    if raw is None or raw == '':
        raw = prod.get('index', fallback)
    if raw is None or raw == '':
        pn = (prod.get('part_number') or '').strip()
        pt = (prod.get('product_title') or '').strip()
        if pn or pt:
            return f'{pt}|{pn}'
        return str(fallback)
    return str(raw)


def _run_generation_job(job_id: str, batch_id: str, products: list, active_sections: list,
                        section_prompts: dict, section_templates: dict,
                        cancel_event: threading.Event = None):
    """
    Background worker that performs the full bulk generation for a job.
    All results are persisted to the DB and emitted events are stored in JobEvent
    so clients can reconnect at any time and replay missed events.
    """

    # ── Helpers scoped to this job ────────────────────────────────────────────

    _seq_counter = [0]

    def _store_event(event_type: str, payload: dict):
        """Persist one SSE event to JobEvent and update the job's updated_at."""
        _seq_counter[0] += 1
        with app.app_context():
            ev = JobEvent(job_id=job_id, event_type=event_type,
                          payload=json.dumps(payload), seq=_seq_counter[0])
            db.session.add(ev)
            db.session.commit()

    def _update_job(status: str, done_products: int = None, error: str = None):
        with app.app_context():
            job = GenerationJob.query.filter_by(job_id=job_id).first()
            if job:
                job.status = status
                if done_products is not None:
                    job.done_products = done_products
                if error:
                    job.error_message = error
                job.updated_at = _utc_now()
                db.session.commit()

    # ── Constants ────────────────────────────────────────────────────────────
    BATCH_SIZE = 5
    # cPanel/shared-hosting note: this used to be a hardcoded 7 (one thread per
    # section, all firing simultaneously = 7 live OpenAI HTTPS connections at
    # once, per running job). CloudLinux LVE plans commonly kill processes that
    # open too many concurrent connections/threads. Default lowered to 3 and
    # made tunable via GEN_SECTION_WORKERS so you can turn it up on a real VPS.
    MAX_WORKERS = int(os.getenv('GEN_SECTION_WORKERS', '3'))
    # No forced inter-batch delay; rate-limit retry logic in _generate_section_batch
    # will naturally pause if OpenAI returns a 429.  Set > 0 only if you regularly
    # saturate your TPM quota on large jobs.
    INTER_BATCH_DELAY = 0

    # Use the pre-registered cancel event (created in the request process)
    _cancel_ev = cancel_event or threading.Event()

    def _is_cancelled() -> bool:
        """Return True if a stop has been requested (either in-memory or via DB)."""
        if _cancel_ev.is_set():
            return True
        # Fallback: check DB (handles multi-process / gunicorn worker restarts)
        try:
            with app.app_context():
                _j = GenerationJob.query.filter_by(job_id=job_id).first()
                return bool(_j and _j.status == 'cancelled')
        except Exception:
            return False

    try:
        _update_job('running')
        logger.info(
            "[JOB %s] init | batch_id=%s products=%d sections=%s",
            job_id, batch_id, len(products), active_sections
        )
        _store_event('batch_start', {'batch_id': batch_id, 'total_products': len(products)})

        grand_total_in  = 0
        grand_total_out = 0
        db_lock = threading.Lock()
        done_count = 0

        with app.app_context():
            _shop = _get_active_shop()
            _shop_domain      = (_shop.domain.strip()       if _shop else '')
            _shop_token       = (_shop.access_token.strip() if _shop else '')
            _shop_api_version = ((_shop.api_version or '2024-01').strip() if _shop else '2024-01')

        product_batches = [products[i:i + BATCH_SIZE] for i in range(0, len(products), BATCH_SIZE)]
        logger.info(f"[JOB {job_id}] Starting: {len(products)} products | {len(product_batches)} batches | workers={MAX_WORKERS}")

        for batch_index, batch in enumerate(product_batches):
            # ── Cancellation check (between batches) ─────────────────────
            if _is_cancelled():
                logger.info(f"[JOB {job_id}] Cancelled — stopping before batch {batch_index + 1}")
                _store_event('batch_end', {
                    'batch_id': batch_id, 'cancelled': True,
                    'total_input_tokens': grand_total_in,
                    'total_output_tokens': grand_total_out,
                    'total_tokens': grand_total_in + grand_total_out,
                    'cost_usd': 0
                })
                return

            if batch_index > 0:
                time.sleep(INTER_BATCH_DELAY)

            logger.info(f"[JOB {job_id}][BATCH {batch_index + 1}/{len(product_batches)}] ▶ {len(batch)} products")
            logger.info(
                "[JOB %s][BATCH %d] product_keys=%s",
                job_id,
                batch_index + 1,
                [
                    {
                        'client_id': _client_id_for_product(p, i),
                        'title': (p.get('product_title') or '').strip(),
                        'part_number': (p.get('part_number') or '').strip(),
                        'brand': (p.get('brand') or '').strip(),
                        'shopify_url': (p.get('shopify_url') or '').strip(),
                    }
                    for i, p in enumerate(batch)
                ],
            )

            product_data = {_client_id_for_product(prod, i): {
                'prod': prod, 'sections': {}, 'total_in': 0, 'total_out': 0,
                'done_sections': 0
            } for i, prod in enumerate(batch)}

            # Step 1: resolve images
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as img_executor:
                for i, prod in enumerate(batch):
                    client_id      = _client_id_for_product(prod, i)
                    part_number    = prod.get('part_number', '').strip()
                    shopify_url_in = (prod.get('shopify_url') or '').strip()
                    if shopify_url_in:
                        img_future = img_executor.submit(
                            fetch_shopify_image_by_product_url,
                            shopify_url_in, _shop_domain, _shop_token, _shop_api_version
                        )
                    else:
                        img_future = img_executor.submit(
                            fetch_shopify_image_by_sku,
                            part_number, _shop_domain, _shop_token, _shop_api_version
                        )
                    product_data[client_id]['img_future'] = img_future

            # Collect image URLs. Keyed by client_id (NOT part_number) — the same
            # part_number can legitimately appear on multiple distinct products
            # in a batch (OEM cross-reference), and a part_number-keyed map would
            # silently overwrite entries for those products.
            image_url_map: dict[str, str] = {}
            for i, prod in enumerate(batch):
                client_id   = _client_id_for_product(prod, i)
                img_fut     = product_data[client_id].get('img_future')
                img_url     = ''
                if img_fut:
                    try: img_url = img_fut.result(timeout=5)
                    except Exception: pass
                image_url_map[client_id] = img_url
                product_data[client_id]['image_url'] = img_url
            logger.info(
                "[JOB %s][BATCH %d] image_url_map=%s",
                job_id, batch_index + 1, image_url_map
            )

            # Step 2: one API call per section for ALL products in this batch
            valid_batch = []
            valid_client_ids = []
            for i, prod in enumerate(batch):
                client_id     = _client_id_for_product(prod, i)
                product_title = prod.get('product_title', '').strip()
                part_number   = prod.get('part_number', '').strip()
                if not product_title or not part_number:
                    product_data[client_id]['error'] = 'Product title and part number are required.'
                    continue
                valid_batch.append(prod)
                valid_client_ids.append(client_id)

            batch_merged_prompts = dict(section_prompts)
            for prod in valid_batch:
                for k, v in (prod.get('prompts') or {}).items():
                    if v and not batch_merged_prompts.get(k):
                        batch_merged_prompts[k] = v

            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as sec_executor:
                section_futures = {
                    sec_executor.submit(
                        _generate_section_batch,
                        section,
                        valid_batch,
                        batch_merged_prompts,
                        section_templates.get(section, ''),
                        image_url_map,
                        4,            # max_retries
                        _cancel_ev,   # cancel signal
                        valid_client_ids,  # unique keys — part_number is not reliably unique
                    ): section
                    for section in active_sections
                }

                for sec_future in as_completed(section_futures):
                    section = section_futures[sec_future]
                    try:
                        section_results_by_id: dict = sec_future.result()
                    except Exception as exc:
                        logger.error(f"[JOB {job_id}] Section '{section}' future raised: {exc}")
                        section_results_by_id = {}

                    # ── Cancel check inside section loop ────────────────
                    if _is_cancelled():
                        logger.info(f"[JOB {job_id}] Cancelled mid-batch — cancelling remaining section futures")
                        for _f in section_futures:
                            _f.cancel()
                        break

                    for prod, client_id in zip(valid_batch, valid_client_ids):
                        part_number = prod.get('part_number', '').strip()
                        res         = section_results_by_id.get(client_id, {
                            'plain_text': '', 'html_text': '',
                            'input_tokens': 0, 'output_tokens': 0, 'total_tokens': 0,
                            'prompt_type': 'default', 'prompt_used': '',
                            'error': f'No result returned for client_id={client_id}'
                        })
                        pd = product_data[client_id]
                        pd['sections'][section] = res
                        pd['done_sections'] += 1
                        pd['total_in']  += res.get('input_tokens', 0)
                        pd['total_out'] += res.get('output_tokens', 0)
                    logger.info(
                        "[JOB %s][BATCH %d][SECTION %s] result_keys=%s",
                        job_id,
                        batch_index + 1,
                        section,
                        list(section_results_by_id.keys()) if isinstance(section_results_by_id, dict) else [],
                    )

            # Step 3: persist + store events
            for prod, client_id in zip(valid_batch, valid_client_ids):
                pd             = product_data[client_id]
                product_title  = prod.get('product_title', '').strip()
                part_number    = prod.get('part_number', '').strip()
                part_type      = prod.get('part_type', '').strip()
                brand          = prod.get('brand', '').strip()
                appliance_type = prod.get('appliance_type', '').strip()
                source         = prod.get('source', 'manual')
                shopify_url    = (prod.get('shopify_url') or '').strip()
                product_image_url = image_url_map.get(client_id, '')

                total_in     = pd['total_in']
                total_out    = pd['total_out']
                total_tokens = total_in + total_out
                cost         = total_in * COST_PER_INPUT_TOKEN + total_out * COST_PER_OUTPUT_TOKEN

                section_results = {}
                with db_lock:
                    with app.app_context():
                        for s in active_sections:
                            r = pd['sections'].get(s, {})
                            if not r or r.get('error'):
                                section_results[s] = {
                                    'plain_text': '', 'html_text': '', 'record_id': None,
                                    'input_tokens': 0, 'output_tokens': 0, 'total_tokens': 0,
                                    'prompt_type': r.get('prompt_type', 'default'),
                                    'prompt_used': r.get('prompt_used', ''),
                                    'error': r.get('error', 'Unknown error')
                                }
                            else:
                                record = GeneratedContent(
                                    batch_id=batch_id,
                                    product_title=product_title, part_number=part_number,
                                    part_type=part_type, brand=brand, appliance_type=appliance_type,
                                    section=s, prompt_type=r['prompt_type'], prompt_used=r['prompt_used'],
                                    plain_text=r['plain_text'], html_text=r['html_text'],
                                    product_image_url=product_image_url,
                                    input_tokens=r['input_tokens'], output_tokens=r['output_tokens'],
                                    total_tokens=r['total_tokens'], model_used=model
                                )
                                db.session.add(record)
                                db.session.flush()
                                section_results[s] = {
                                    'plain_text': r['plain_text'], 'html_text': r['html_text'],
                                    'record_id': record.id,
                                    'input_tokens': r['input_tokens'],
                                    'output_tokens': r['output_tokens'],
                                    'total_tokens': r['total_tokens'],
                                    'prompt_type': r['prompt_type'],
                                    'prompt_used': r['prompt_used']
                                }

                        history_row = ProductHistory(
                            batch_id=batch_id,
                            product_title=product_title, part_number=part_number,
                            part_type=part_type, brand=brand, appliance_type=appliance_type,
                            source=source, shopify_url=shopify_url or None,
                            product_image_url=product_image_url,
                            total_input_tokens=total_in, total_output_tokens=total_out,
                            total_tokens=total_tokens, cost_usd=cost,
                            sections_generated=len([s for s in section_results.values() if not s.get('error')]),
                            model_used=model
                        )
                        db.session.add(history_row)
                        db.session.commit()

                grand_total_in  += total_in
                grand_total_out += total_out
                done_count      += 1

                event_payload = {
                    'id': client_id,
                    'product_title': product_title,
                    'part_number': part_number,
                    'product_image_url': product_image_url,
                    'sections': section_results,
                    'total_input_tokens': total_in,
                    'total_output_tokens': total_out,
                    'total_tokens': total_tokens,
                    'cost_usd': round(cost, 6),
                    'error': pd.get('error')
                }
                logger.info(
                    "[JOB %s] product_done payload | client_id=%s title=%r part_number=%s "
                    "sections=%s totals={in:%s,out:%s,total:%s,cost:%s} error=%r",
                    job_id,
                    client_id,
                    product_title,
                    part_number,
                    list(section_results.keys()),
                    total_in,
                    total_out,
                    total_tokens,
                    round(cost, 6),
                    pd.get('error'),
                )
                _store_event('product_done', event_payload)
                _update_job('running', done_products=done_count)

        # All batches done — write BatchHistory + final event
        with app.app_context():
            grand_total = grand_total_in + grand_total_out
            grand_cost  = grand_total_in * COST_PER_INPUT_TOKEN + grand_total_out * COST_PER_OUTPUT_TOKEN
            batch_row   = BatchHistory(
                batch_id=batch_id,
                source=products[0].get('source', 'manual') if products else 'manual',
                product_count=len(products),
                total_input_tokens=grand_total_in,
                total_output_tokens=grand_total_out,
                total_tokens=grand_total,
                cost_usd=grand_cost,
                model_used=model
            )
            db.session.add(batch_row)
            db.session.commit()

        _store_event('batch_end', {
            'batch_id': batch_id,
            'total_input_tokens': grand_total_in,
            'total_output_tokens': grand_total_out,
            'total_tokens': grand_total_in + grand_total_out,
            'cost_usd': round(grand_cost, 6)
        })
        _update_job('done', done_products=done_count)
        logger.info(f"[JOB {job_id}] ✓ Complete — {len(products)} products processed")

    except Exception as exc:
        logger.error(f"[JOB {job_id}] Fatal error: {exc}")
        _update_job('error', error=str(exc))

    finally:
        _cancel_events.pop(job_id, None)  # free memory


@app.route('/api/generate-stream', methods=['POST'])
@login_required
def generate_stream():
    """
    Launches a background generation job and immediately returns a job_id.
    The client connects to /api/jobs/<job_id>/stream to receive SSE events,
    which can be reconnected at any time — generation continues server-side
    regardless of whether the client tab is open.
    """
    if not API_KEY:
        return jsonify({'error': 'OpenAI API key is required.'}), 400

    body     = request.json or {}
    products = body.get('products', [])
    logger.info(
        "generate-stream request | products=%d first_products=%s sections_config_keys=%s",
        len(products),
        [
            {
                'id': (p or {}).get('id'),
                'title': (p or {}).get('product_title'),
                'part_number': (p or {}).get('part_number'),
                'brand': (p or {}).get('brand'),
                'shopify_url': (p or {}).get('shopify_url'),
                'source': (p or {}).get('source'),
            }
            for p in products[:5]
        ],
        [s.get('key') for s in (body.get('sections_config') or []) if isinstance(s, dict)],
    )
    if not products:
        return jsonify({'error': 'No products provided.'}), 400

    sections_config = body.get('sections_config', [])
    if sections_config:
        active_sections   = [s['key'] for s in sections_config if s.get('key')]
        section_prompts   = {s['key']: s.get('prompt', '')    for s in sections_config}
        section_templates = {s['key']: s.get('template', '') for s in sections_config}
    else:
        active_sections   = SECTIONS
        section_prompts   = {}
        section_templates = {}

    shop_templates = _get_shop_section_templates_by_name()
    shop_prompts   = _get_shop_section_prompts_by_name()
    for sec_key in active_sections:
        display_name = SECTION_LABELS.get(sec_key, sec_key).strip().lower()
        if "short" in display_name:
            display_name = "short description"
        if not section_templates.get(sec_key):
            section_templates[sec_key] = shop_templates.get(display_name, '')
        if not section_prompts.get(sec_key):
            section_prompts[sec_key] = shop_prompts.get(display_name, '')

    job_id   = str(uuid.uuid4())
    batch_id = str(uuid.uuid4())

    # IMPORTANT (cPanel/Passenger): we deliberately do NOT spawn a thread or
    # submit to an executor here anymore. On Passenger, the process serving
    # this request can be recycled/killed (memory limit, idle timeout, max
    # requests) at any time — when that happens, any background thread it
    # owns is killed with it (that's the "Child process ... killed by signal:
    # 15" you saw). Instead we just persist everything the job needs and mark
    # it 'pending'; a separate, independent process (worker.py, run via cron
    # or a long-running command) polls for pending jobs and executes them.
    # This is the same reason JobEvent/GenerationJob already read back from
    # the DB rather than from in-memory state — the job is designed to be
    # resumable/executable from any process.
    with app.app_context():
        job = GenerationJob(
            job_id=job_id, batch_id=batch_id,
            status='pending', total_products=len(products),
            payload=json.dumps({
                'products': products,
                'active_sections': active_sections,
                'section_prompts': section_prompts,
                'section_templates': section_templates,
            }),
        )
        db.session.add(job)
        db.session.commit()
    logger.info(
        "generate-stream queued | job_id=%s batch_id=%s total_products=%d active_sections=%s",
        job_id, batch_id, len(products), active_sections
    )

    return jsonify({'job_id': job_id, 'batch_id': batch_id, 'total_products': len(products)})


@app.route('/api/jobs/<job_id>/stream')
@login_required
def job_stream(job_id: str):
    """
    SSE endpoint for a running or completed job.
    On connect it replays all stored events from seq > last_event_id (the
    standard EventSource reconnect header), then streams new events as they
    arrive.  Safe to reconnect at any time — even after the job completes.
    """
    last_event_id = request.args.get('lastEventId', type=int, default=0)
    if not last_event_id:
        header_val = request.headers.get('Last-Event-ID')
        if header_val:
            try:
                last_event_id = int(header_val)
            except (TypeError, ValueError):
                last_event_id = 0

    def event_stream():
        # Replay already-stored events the client hasn't seen yet
        with app.app_context():
            past = (JobEvent.query
                    .filter_by(job_id=job_id)
                    .filter(JobEvent.seq > last_event_id)
                    .order_by(JobEvent.seq)
                    .all())
            for ev in past:
                yield f"id: {ev.seq}\nevent: {ev.event_type}\ndata: {ev.payload}\n\n"

        # Poll for new events until job is finished
        last_seq = (past[-1].seq if past else last_event_id)
        tick = 0
        while True:
            tick += 1
            if tick % 50 == 0:
                yield ': keepalive\n\n'

            with app.app_context():
                job = GenerationJob.query.filter_by(job_id=job_id).first()
                if not job:
                    break

                new_events = (JobEvent.query
                              .filter_by(job_id=job_id)
                              .filter(JobEvent.seq > last_seq)
                              .order_by(JobEvent.seq)
                              .all())
                for ev in new_events:
                    last_seq = ev.seq
                    yield f"id: {ev.seq}\nevent: {ev.event_type}\ndata: {ev.payload}\n\n"

                if job.status in ('done', 'error'):
                    if not new_events:
                        break

            time.sleep(0.3)

    return Response(stream_with_context(event_stream()), mimetype='text/event-stream',
                    headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no', 'Connection': 'keep-alive'})


@app.route('/api/jobs/<job_id>/events')
@login_required
def job_events(job_id: str):
    """JSON polling fallback — returns stored events after a given seq."""
    after_seq = request.args.get('after', type=int, default=0)
    with app.app_context():
        job = GenerationJob.query.filter_by(job_id=job_id).first()
        if not job:
            return jsonify({'error': 'Job not found'}), 404
        events = (JobEvent.query
                  .filter_by(job_id=job_id)
                  .filter(JobEvent.seq > after_seq)
                  .order_by(JobEvent.seq)
                  .all())
        return jsonify({
            'job_id': job_id,
            'status': job.status,
            'events': [
                {'seq': e.seq, 'type': e.event_type, 'data': json.loads(e.payload)}
                for e in events
            ],
        })


@app.route('/api/jobs/<job_id>/status')
@login_required
def job_status(job_id: str):
    """Simple polling endpoint — returns current job state without streaming."""
    with app.app_context():
        job = GenerationJob.query.filter_by(job_id=job_id).first()
        if not job:
            return jsonify({'error': 'Job not found'}), 404
        return jsonify(job.to_dict())


@app.route('/api/jobs/<job_id>/cancel', methods=['POST'])
@login_required
def cancel_job(job_id: str):
    """Marks a job as cancelled so the background worker stops after the current batch."""
    with app.app_context():
        job = GenerationJob.query.filter_by(job_id=job_id).first()
        if not job:
            return jsonify({'error': 'Job not found'}), 404
        if job.status not in ('pending', 'running'):
            return jsonify({'error': 'Job is already ' + job.status}), 400
        job.status = 'cancelled'
        job.updated_at = _utc_now()
        db.session.commit()
        # Signal the in-memory event so the worker thread wakes up immediately
        ev = _cancel_events.get(job_id)
        if ev:
            ev.set()
        return jsonify({'job_id': job_id, 'status': 'cancelled'})


@app.route('/api/jobs')
@login_required
def list_jobs():
    """Returns the most recent 50 jobs so the UI can show in-progress or resumable jobs."""
    with app.app_context():
        jobs = (GenerationJob.query
                .order_by(GenerationJob.created_at.desc())
                .limit(50).all())
        return jsonify({'jobs': [j.to_dict() for j in jobs]})


@app.route('/api/product-sections')
@login_required
def get_product_sections():
    product_title = request.args.get('product_title', '').strip()
    part_number   = request.args.get('part_number', '').strip()
    batch_id      = request.args.get('batch_id', '').strip()
    logger.info(
        "product-sections request | title=%r part_number=%s batch_id=%s",
        product_title, part_number, batch_id
    )
    if not product_title or not part_number:
        return jsonify({'error': 'product_title and part_number are required'}), 400

    sections = {}
    for section in SECTIONS:
        q = GeneratedContent.query.filter_by(
            product_title=product_title, part_number=part_number, section=section
        )
        if batch_id:
            q = q.filter_by(batch_id=batch_id)
        record = q.order_by(GeneratedContent.created_at.desc()).first()
        if record:
            sections[section] = {
                'record_id': record.id,
                'plain_text': record.plain_text, 'html_text': record.html_text,
                'prompt_used': record.prompt_used, 'prompt_type': record.prompt_type,
                'input_tokens': record.input_tokens, 'output_tokens': record.output_tokens,
                'total_tokens': record.total_tokens, 'model_used': record.model_used,
                'created_at': record.created_at.strftime('%Y-%m-%d %H:%M:%S'),
            }
    logger.info(
        "product-sections response | title=%r part_number=%s batch_id=%s section_record_ids=%s",
        product_title,
        part_number,
        batch_id,
        {k: v.get('record_id') for k, v in sections.items()},
    )
    return jsonify({'sections': sections})


@app.route('/api/history')
@login_required
def get_history():
    page     = request.args.get('page', 1, type=int)
    per_page = 20
    records  = GeneratedContent.query.order_by(GeneratedContent.created_at.desc()).paginate(page=page, per_page=per_page, error_out=False)
    return jsonify({'records': [r.to_dict() for r in records.items], 'total': records.total, 'pages': records.pages, 'current_page': page})


@app.route('/api/product-history')
@login_required
def get_product_history():
    page     = request.args.get('page', 1, type=int)
    per_page = 20
    records  = ProductHistory.query.order_by(ProductHistory.created_at.desc()).paginate(page=page, per_page=per_page, error_out=False)
    return jsonify({'records': [r.to_dict() for r in records.items], 'total': records.total, 'pages': records.pages, 'current_page': page})


@app.route('/api/batch-history')
@login_required
def get_batch_history():
    page     = request.args.get('page', 1, type=int)
    per_page = 20
    records  = BatchHistory.query.order_by(BatchHistory.created_at.desc()).paginate(page=page, per_page=per_page, error_out=False)
    return jsonify({'records': [r.to_dict() for r in records.items], 'total': records.total, 'pages': records.pages, 'current_page': page})


@app.route('/api/batch-products/<batch_id>')
@login_required
def get_batch_products(batch_id):
    records = ProductHistory.query.filter_by(batch_id=batch_id).order_by(ProductHistory.id).all()
    return jsonify({'products': [r.to_dict() for r in records]})


@app.route('/api/update/<int:record_id>', methods=['PUT'])
@login_required
def update_content(record_id):
    data       = request.json
    plain_text = data.get('plain_text', '').strip()
    html_text  = data.get('html_text', '').strip()
    record = GeneratedContent.query.get(record_id)
    if not record: return jsonify({'error': 'Record not found.'}), 404
    if not plain_text and not html_text: return jsonify({'error': 'No content provided for update.'}), 400
    if plain_text: record.plain_text = plain_text
    if html_text:  record.html_text  = html_text
    db.session.commit()
    return jsonify({'success': True, 'id': record.id, 'plain_text': record.plain_text, 'html_text': record.html_text, 'updated_at': _utc_now().strftime('%Y-%m-%d %H:%M:%S')})
@app.route('/api/update-title', methods=['PUT'])
@reviewer_required
def update_product_title():
    """
    Renames a product's title everywhere it's used as an identity key:
    ProductHistory, GeneratedContent, and ReviewStatus rows are all keyed
    by (product_title, part_number), so all three must be updated together
    or lookups (and the review list) will break.
    """
    data          = request.json or {}
    old_title     = (data.get('product_title') or '').strip()
    part_number   = (data.get('part_number') or '').strip()
    new_title     = (data.get('new_title') or '').strip()

    if not old_title or not part_number:
        return jsonify({'error': 'product_title and part_number are required'}), 400
    if not new_title:
        return jsonify({'error': 'new_title cannot be empty'}), 400
    if new_title == old_title:
        return jsonify({'success': True, 'product_title': new_title, 'part_number': part_number, 'unchanged': True})

    # Guard against colliding with a different product that already has this title/part_number
    conflict = ProductHistory.query.filter_by(product_title=new_title, part_number=part_number).first()
    if conflict:
        return jsonify({'error': f'Another product already exists with the title "{new_title}" and part number "{part_number}".'}), 409

    try:
        ProductHistory.query.filter_by(
            product_title=old_title, part_number=part_number
        ).update({'product_title': new_title}, synchronize_session=False)

        GeneratedContent.query.filter_by(
            product_title=old_title, part_number=part_number
        ).update({'product_title': new_title}, synchronize_session=False)

        rv = ReviewStatus.query.filter_by(product_title=old_title, part_number=part_number).first()
        if rv:
            rv.product_title = new_title
            rv.updated_at = _utc_now()

        db.session.commit()
    except Exception as e:
        db.session.rollback()
        logger.exception('[update_product_title] failed for old_title=%r part_number=%s', old_title, part_number)
        return jsonify({'success': False, 'error': str(e)}), 500

    return jsonify({'success': True, 'product_title': new_title, 'part_number': part_number})


@app.route('/api/update-bulk', methods=['PUT'])
@login_required
def update_content_bulk():
    data = request.json or {}
    sections = data.get('sections', {})
    if not sections: return jsonify({'error': 'No sections provided.'}), 400
    updated = []; errors = []
    for section_key, payload in sections.items():
        record_id  = payload.get('record_id')
        plain_text = payload.get('plain_text', '').strip()
        html_text  = payload.get('html_text', '').strip()
        if not record_id: continue
        record = GeneratedContent.query.get(record_id)
        if not record: continue
        if plain_text: record.plain_text = plain_text
        if html_text:  record.html_text  = html_text
        updated.append(record_id)
    if updated: db.session.commit()
    return jsonify({'success': True, 'updated': updated, 'errors': errors, 'updated_at': _utc_now().strftime('%Y-%m-%d %H:%M:%S')})


@app.route('/api/review/products')
@reviewer_required
def review_products():
    page     = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 30, type=int)
    shop_name= session.get("active_shop_id")
    logger.info(f"shop_id-:{shop_name}")

    from sqlalchemy import func

    # Subquery: per (product_title, part_number) get the latest created_at
    # and the batch_id that corresponds to that latest record
    latest_subq = (
        db.session.query(
            ProductHistory.product_title,
            ProductHistory.part_number,
            ProductHistory.brand,
            ProductHistory.appliance_type,
            ProductHistory.part_type,
            func.max(ProductHistory.created_at).label('latest_created_at'),
        )
        .group_by(
            ProductHistory.product_title,
            ProductHistory.part_number,
        )
        .subquery()
    )

    # Join back to ProductHistory to fetch batch_id and source for the latest record
    ph_alias = db.aliased(ProductHistory)
    batch_subq = (
        db.session.query(
            latest_subq.c.product_title,
            latest_subq.c.part_number,
            latest_subq.c.brand,
            latest_subq.c.appliance_type,
            latest_subq.c.part_type,
            latest_subq.c.latest_created_at,
            ph_alias.batch_id,
            ph_alias.source,
            ph_alias.shopify_url,
        )
        .join(
            ph_alias,
            (ph_alias.product_title == latest_subq.c.product_title)
            & (ph_alias.part_number  == latest_subq.c.part_number)
            & (ph_alias.created_at   == latest_subq.c.latest_created_at),
        )
        .subquery()
    )

    total_count = db.session.query(func.count()).select_from(batch_subq).scalar()
    rows = (
        db.session.query(batch_subq)
        .order_by(batch_subq.c.latest_created_at.desc())
        .offset((page - 1) * per_page)
        .limit(per_page)
        .all()
    )

    records = []
    for row in rows:
        rv = ReviewStatus.query.filter_by(
            product_title=row.product_title,
            part_number=row.part_number,
        ).first()

        # Resolve batch source: prefer BatchHistory table, fall back to ProductHistory.source
        batch_source = row.source or 'Manual'
        if row.batch_id:
            bh = BatchHistory.query.filter_by(batch_id=row.batch_id).first()
            if bh and bh.source:
                batch_source = bh.source

        records.append({
            'product_title':     row.product_title,
            'part_number':       row.part_number,
            'brand':             row.brand,
            'appliance_type':    row.appliance_type,
            'part_type':         row.part_type,
            'latest_created_at': row.latest_created_at.strftime('%Y-%m-%d %H:%M:%S') if row.latest_created_at else None,
            'batch_id':          row.batch_id or '',
            'batch_source':      batch_source,
            'shopify_url':       (row.shopify_url or '').strip() or _latest_shopify_url_for_product(row.product_title, row.part_number),
            'review_status':     rv.status if rv else 'pending',
            'reviewer':          rv.reviewer if rv else None,
            'reviewed_at':       rv.reviewed_at.strftime('%Y-%m-%d %H:%M:%S') if rv and rv.reviewed_at else None,
            'published_at':      rv.published_at.strftime('%Y-%m-%d %H:%M:%S') if rv and rv.published_at else None,
        })

    pages = (total_count + per_page - 1) // per_page
    return jsonify({'records': records, 'total': total_count, 'pages': pages, 'current_page': page})


def _publish_single_reviewed_product(product_title: str, part_number: str, reviewer: str, shopify_url_override: str = ''):
    resolved_url = (shopify_url_override or '').strip() or _latest_shopify_url_for_product(product_title, part_number)
    shopify_info = publish_generated_content_to_shopify(product_title, part_number, resolved_url)
    rv = ReviewStatus.query.filter_by(product_title=product_title, part_number=part_number).first()
    now = _utc_now()
    if not rv:
        rv = ReviewStatus(product_title=product_title, part_number=part_number)
        db.session.add(rv)
    rv.status = 'published'
    rv.reviewer = (reviewer or '').strip() or rv.reviewer
    rv.updated_at = now
    rv.published_at = now
    db.session.commit()
    return shopify_info, rv


@app.route('/api/review/publish-all', methods=['POST'])
@reviewer_required
def publish_all_eligible_reviews():
    data     = request.json or {}
    reviewer = (data.get('reviewer') or '').strip()

    from sqlalchemy import func
    subq = (db.session.query(ProductHistory.product_title, ProductHistory.part_number, func.max(ProductHistory.created_at).label('latest_created_at'))
            .group_by(ProductHistory.product_title, ProductHistory.part_number).subquery())
    rows = db.session.query(subq).order_by(subq.c.latest_created_at.desc()).all()

    published = skipped_wrong_state = failed = 0
    errors = []

    for row in rows:
        rv = ReviewStatus.query.filter_by(product_title=row.product_title, part_number=row.part_number).first()
        st = rv.status if rv else 'pending'
        if st == 'published': continue
        if st not in ('pending', 'reviewed'):
            skipped_wrong_state += 1; continue

        try:
            _publish_single_reviewed_product(row.product_title, row.part_number, reviewer, '')
            published += 1
        except Exception as e:
            db.session.rollback(); failed += 1
            if len(errors) < 40:
                errors.append({'product_title': row.product_title, 'part_number': row.part_number, 'error': str(e)})

    return jsonify({'success': True, 'published': published, 'skipped_other_state': skipped_wrong_state, 'failed': failed, 'errors': errors})


@app.route('/api/review/status', methods=['POST'])
@reviewer_required
def update_review_status():
    data          = request.json or {}
    product_title = data.get('product_title', '').strip()
    part_number   = data.get('part_number', '').strip()
    status        = data.get('status', '').strip()
    reviewer      = data.get('reviewer', '').strip()

    if not product_title or not part_number: return jsonify({'error': 'product_title and part_number are required'}), 400
    if status not in ('pending', 'reviewed', 'published'): return jsonify({'error': 'status must be pending, reviewed, or published'}), 400

    if status == 'published':
        try:
            shopify_info, rv = _publish_single_reviewed_product(product_title, part_number, reviewer, (data.get('shopify_url') or '').strip())
            return jsonify({'success': True, 'review': rv.to_dict(), 'shopify': shopify_info})
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)}), 500

    rv = ReviewStatus.query.filter_by(product_title=product_title, part_number=part_number).first()
    now = _utc_now()
    if not rv:
        rv = ReviewStatus(product_title=product_title, part_number=part_number)
        db.session.add(rv)

    rv.status   = status
    rv.reviewer = reviewer or rv.reviewer
    rv.updated_at = now
    if status == 'reviewed' and not rv.reviewed_at: rv.reviewed_at = now
    if status == 'pending':
        rv.reviewed_at  = None
        rv.published_at = None

    db.session.commit()
    return jsonify({'success': True, 'review': rv.to_dict()})



@app.route('/api/shopify-product', methods=['POST'])
@login_required
def api_shopify_product():
    body = request.json or {}
    url = (body.get('url') or '').strip()
    try:
        product = _fetch_shopify_product(url)
        return jsonify({'success': True, 'product': product})
    except Exception as e:
        logger.exception(f"[api_shopify_product] failed for url={url}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/shopify-products-batch', methods=['POST'])
@login_required
def fetch_shopify_products_batch():
    body = request.json or {}
    urls = body.get('urls') or []
    if not urls: return jsonify({'error': 'urls array is required'}), 400
    if len(urls) > 100: return jsonify({'error': 'Maximum 100 URLs per batch'}), 400
    products, errors = [], []
    for raw_url in urls:
        raw_url = (raw_url or '').strip()
        if not raw_url:
            errors.append({'url': raw_url, 'error': 'Empty URL'})
            continue
        try:
            product = _fetch_shopify_product(raw_url)
            product['source_url'] = raw_url
            products.append(product)
        except Exception as e:
            errors.append({'url': raw_url, 'error': str(e)})
    return jsonify({'success': True, 'products': products, 'errors': errors, 'fetched': len(products), 'failed': len(errors)})

@app.route('/api/product-image', methods=['POST'])
@login_required
def get_product_image():
    body = request.json or {}
    part_number = (body.get('part_number') or '').strip()
    if not part_number: return jsonify({'error': 'part_number is required'}), 400
    try:
        image_url = fetch_shopify_image_by_sku(part_number)
        return jsonify({'success': True, 'part_number': part_number, 'image_url': image_url})
    except Exception as e: return jsonify({'error': str(e)}), 500