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
    RepairImageGeneration,
    SHOPIFY_METAFIELD_TYPES, SEO_SECTIONS, COST_PER_INPUT_TOKEN, COST_PER_OUTPUT_TOKEN,
    MAX_FORMAT_WORKERS, DEFAULT_PROMPTS, SECTION_LABELS, SECTIONS,
    _GQL_PRODUCT, _GQL_PRODUCT_BY_SKU, _GQL_PRODUCT_UPDATE, _GQL_METAFIELDS_SET, _GQL_PRODUCT_BY_HANDLE,
    _extract_admin_product_id, _is_storefront_url, _shopify_graphql_with_creds,
    _fetch_by_product_id, _fetch_by_storefront_url, _fetch_by_handle_admin,
    _webcate_from_tags, _subcate_from_tags, _parse_gql_product, _parse_rest_product,
    _fetch_shopify_product, fetch_shopify_image_by_product_url, _extract_products_path_segment,
    model, API_KEY, _cancel_events,_GQL_METAFIELD_DEFINITIONS
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

    # Path segment in storefront URL (/products/...) is ALWAYS the handle.
    # Query productByHandle first (handles can be alphanumeric OR numeric like "9178024596").
    try:
        body = _shopify_graphql(_GQL_PRODUCT_BY_HANDLE, {'handle': seg})
        node = (body.get('data') or {}).get('productByHandle')
        if node and node.get('id'):
            return node.get('id')
    except Exception as e:
        logger.warning(f"[_shopify_product_gid_from_product_url] handle lookup failed for '{seg}': {e}")

    # Fallback for numeric segment if handle query returned null: try as product ID
    if seg.isdigit():
        try:
            body_id = _shopify_graphql(_GQL_PRODUCT, {'id': f'gid://shopify/Product/{seg}'})
            node_id = (body_id.get('data') or {}).get('product')
            if node_id and node_id.get('id'):
                return node_id.get('id')
        except Exception as e:
            logger.warning(f"[_shopify_product_gid_from_product_url] ID lookup failed for '{seg}': {e}")

    return None


def _shopify_product_gid_from_sku(part_number: str) -> str | None:
    """Resolve a Shopify product GID by SKU when no URL is available."""
    if not part_number:
        return None
    body = _shopify_graphql(_GQL_PRODUCT_BY_SKU, {'query': f'sku:{part_number}'})
    edges = ((body.get('data') or {}).get('products', {}).get('edges') or [])
    if not edges:
        return None
    return edges[0]['node'].get('id')


def _batch_id_for_shopify_url(product_title: str, part_number: str, shopify_url: str) -> str:
    """
    Resolve which batch's data corresponds to a specific shopify_url, so publishing can pull
    the GeneratedContent that was actually generated for THIS listing — instead of whichever
    batch for this (title, part_number) happens to have the newest created_at. This matters
    whenever the same product_title + part_number was generated more than once for different
    Shopify listings (e.g. a "GRP" listing and an "appliance part" listing).
    """
    if not shopify_url:
        return ''
    row = (ProductHistory.query
           .filter_by(product_title=product_title, part_number=part_number, shopify_url=shopify_url)
           .order_by(ProductHistory.created_at.desc())
           .first())
    if row:
        logger.info(
            "[PUBLISH][_batch_id_for_shopify_url] title=%r part_number=%r shopify_url=%r -> batch_id=%s (product_history.id=%s)",
            product_title, part_number, shopify_url, row.batch_id, row.id,
        )
        return row.batch_id or ''
    logger.warning(
        "[PUBLISH][_batch_id_for_shopify_url] NO ProductHistory row matches title=%r part_number=%r shopify_url=%r "
        "— cannot scope content to a specific batch, will fall back to globally-latest content (may be wrong listing).",
        product_title, part_number, shopify_url,
    )
    return ''


def _latest_shopify_url_for_product(product_title: str, part_number: str) -> str:
    candidates = (ProductHistory.query
           .filter_by(product_title=product_title, part_number=part_number)
           .filter(ProductHistory.shopify_url.isnot(None))
           .filter(ProductHistory.shopify_url != '')
           .order_by(ProductHistory.created_at.desc())
           .all())
    logger.info(
        "[PUBLISH][_latest_shopify_url_for_product] title=%r part_number=%r candidates=%s",
        product_title, part_number,
        [{'id': c.id, 'batch_id': c.batch_id, 'shopify_url': c.shopify_url, 'created_at': str(c.created_at)} for c in candidates],
    )
    row = candidates[0] if candidates else None
    if row:
        logger.info(
            "[PUBLISH][_latest_shopify_url_for_product] SELECTED id=%s batch_id=%s shopify_url=%r (%d other candidate(s) ignored)",
            row.id, row.batch_id, row.shopify_url, len(candidates) - 1,
        )
    return (row.shopify_url or '').strip() if row else ''


def _latest_generated_record(product_title: str, part_number: str, section: str, batch_id: str = ''):
    query = GeneratedContent.query.filter_by(product_title=product_title, part_number=part_number, section=section)
    if batch_id:
        query = query.filter_by(batch_id=batch_id)

    candidates = query.order_by(GeneratedContent.created_at.desc()).all()

    if not candidates and batch_id:
        # Nothing generated for this section under the resolved batch — do NOT silently fall back
        # to a different batch's content, that's the exact bug we're fixing. Just report it missing.
        logger.warning(
            "[PUBLISH][_latest_generated_record] batch_id=%s has NO record for title=%r part_number=%r section=%r "
            "(section will be left blank for this publish rather than pulling from a different batch)",
            batch_id, product_title, part_number, section,
        )
        return None

    if len(candidates) > 1 and not batch_id:
        # More than one GeneratedContent row exists for this (title, part_number, section) and we
        # have no batch_id to disambiguate — i.e. this product was generated more than once
        # (different batch/listing), and we're about to pick only the newest one.
        logger.warning(
            "[PUBLISH][_latest_generated_record] MULTIPLE candidates (no batch_id given) for title=%r part_number=%r section=%r -> %s",
            product_title, part_number, section,
            [{'id': c.id, 'batch_id': c.batch_id, 'created_at': str(c.created_at)} for c in candidates],
        )

    record = candidates[0] if candidates else None
    if record:
        logger.info(
            "[PUBLISH][_latest_generated_record] SELECTED id=%s batch_id=%s title=%r part_number=%r section=%r created_at=%s scoped=%s",
            record.id, record.batch_id, product_title, part_number, section, record.created_at, bool(batch_id),
        )
    else:
        logger.info(
            "[PUBLISH][_latest_generated_record] NO RECORD FOUND for title=%r part_number=%r section=%r batch_id=%r",
            product_title, part_number, section, batch_id,
        )
    return record


def _record_html_for_shopify(rec) -> str:
    if not rec:
        return ''
    return (rec.html_text or '').strip()

def _live_metafield_types() -> dict:
    """
    Queries the ACTIVE shop's real metafield definitions from Shopify and
    returns {'namespace.key': 'type_name'}. This is the ground truth Shopify
    enforces on write — different stores can define the same key with
    different types, so this must be checked per-store, not hardcoded.
    Returns {} on any failure (callers fall back to their existing defaults).
    """
    try:
        body = _shopify_graphql(_GQL_METAFIELD_DEFINITIONS)
        edges = ((body.get('data') or {}).get('metafieldDefinitions', {}).get('edges') or [])
        return {
            f"{(e['node'].get('namespace') or '')}.{(e['node'].get('key') or '')}": (e['node'].get('type') or {}).get('name')
            for e in edges
        }
    except Exception as e:
        logger.warning(f'_live_metafield_types failed: {e}')
        return {}

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


# def publish_generated_content_to_shopify(product_title: str, part_number: str, shopify_url: str = '') -> dict:
#     logger.info(
#         "[PUBLISH] START title=%r part_number=%r shopify_url=%r",
#         product_title, part_number, shopify_url,
#     )
#     gid = _shopify_product_gid_from_product_url(shopify_url) if shopify_url else None
#     if not gid:
#         # Fall back to SKU-based lookup using the active shop
#         logger.info("[PUBLISH] No gid from shopify_url, falling back to SKU lookup for part_number=%r", part_number)
#         gid = _shopify_product_gid_from_sku(part_number)
#     if not gid:
#         raise ValueError(f'Could not resolve a Shopify product for part number "{part_number}". Check that the SKU exists in your Shopify store.')
#     logger.info("[PUBLISH] Resolved target product gid=%s for shopify_url=%r", gid, shopify_url)

#     # Resolve which batch's content actually belongs to this shopify_url, so we don't pull
#     # content generated for a different listing of the same (title, part_number).
#     target_batch_id = _batch_id_for_shopify_url(product_title, part_number, shopify_url)

#     body_html    = _record_html_for_shopify(_latest_generated_record(product_title, part_number, 'product_description', target_batch_id))
#     mf_specs     = _record_html_for_shopify(_latest_generated_record(product_title, part_number, 'technical_specifications', target_batch_id))
#     mf_bullets   = _record_html_for_shopify(_latest_generated_record(product_title, part_number, 'short_description', target_batch_id))
#     mf_causes    = _record_html_for_shopify(_latest_generated_record(product_title, part_number, 'common_problems', target_batch_id))
#     mf_install   = _record_html_for_shopify(_latest_generated_record(product_title, part_number, 'installation_guide', target_batch_id))

#     # Fetch SEO plain text (not HTML) for meta title and meta description
#     meta_title_rec = _latest_generated_record(product_title, part_number, 'meta_title', target_batch_id)
#     meta_desc_rec  = _latest_generated_record(product_title, part_number, 'meta_description', target_batch_id)
#     meta_title_val = (meta_title_rec.plain_text or '').strip() if meta_title_rec else ''
#     meta_desc_val  = (meta_desc_rec.plain_text  or '').strip() if meta_desc_rec  else ''

#     if not any([body_html, mf_specs, mf_bullets, mf_causes, mf_install, meta_title_val, meta_desc_val]):
#         raise ValueError('No generated content found for this product — nothing to publish.')

#     logger.info(
#         "[PUBLISH] About to write to gid=%s | body_html_len=%d meta_title=%r meta_desc_len=%d "
#         "| specs_len=%d bullets_len=%d causes_len=%d install_len=%d",
#         gid, len(body_html), meta_title_val, len(meta_desc_val),
#         len(mf_specs), len(mf_bullets), len(mf_causes), len(mf_install),
#     )

#     # Always update title; include descriptionHtml only if generated
#     # Include SEO fields if meta title or meta description were generated
#     product_input = {'id': gid, 'title': product_title}
#     if body_html:
#         product_input['descriptionHtml'] = body_html
#     if meta_title_val or meta_desc_val:
#         seo_input = {}
#         if meta_title_val:
#             seo_input['title'] = meta_title_val[:255]  # Shopify SEO title limit
#         if meta_desc_val:
#             seo_input['description'] = meta_desc_val[:512]  # Shopify SEO description limit
#         product_input['seo'] = seo_input

#     pu = _shopify_graphql(_GQL_PRODUCT_UPDATE, {'input': product_input})
#     if pu.get('errors'):
#         raise RuntimeError(f'Shopify productUpdate: {pu["errors"]}')
#     pu_user_errors = (((pu.get('data') or {}).get('productUpdate') or {}).get('userErrors') or [])
#     if pu_user_errors:
#         raise RuntimeError(f'Shopify productUpdate userErrors: {pu_user_errors}')

#     # Build a dynamic section → metafield config from the shop's saved sections.
#     # Each saved section has: { name, metafield, namespace, key, target, type, ... }
#     # We match on normalized section name against SECTION_LABELS values.
#     def _build_dynamic_section_mf_cfg() -> dict:
#         """
#         Returns { section_db_key: { namespace, key, type } } built from the active
#         shop's saved sections, matched by normalizing the section's 'name' field
#         against SECTION_LABELS values. Falls back to hardcoded defaults if no
#         shop section config is found for a given section.
#         """
#         # Inverted: normalized label → section DB key  e.g. "short description" → "short_description"
#         label_to_section_key = {v.strip().lower(): k for k, v in SECTION_LABELS.items()}

#         result = {}
#         try:
#             shop = _get_active_shop()
#             if shop:
#                 saved_sections = json.loads(shop.sections or '[]')
#                 for s in saved_sections:
#                     s_name = (s.get('name') or '').strip().lower()
#                     section_key = label_to_section_key.get(s_name)
#                     if not section_key:
#                         continue
#                     # Only push sections that target a metafield (not bodyHtml / SEO)
#                     if (s.get('target') or '').strip().lower() != 'metafield':
#                         continue
#                     ns  = (s.get('namespace') or 'custom').strip()
#                     key = (s.get('key') or '').strip()
#                     typ = (s.get('type') or SHOPIFY_METAFIELD_TYPES.get(key, 'multi_line_text_field')).strip()
#                     if key:
#                         result[section_key] = {'namespace': ns, 'key': key, 'type': typ}
#         except Exception as e:
#             logger.warning(f'_build_dynamic_section_mf_cfg failed: {e}')

#         # Hardcoded fallback for any section not covered by shop config
#         fallback = {
#             'technical_specifications': {'namespace': 'custom', 'key': 'technical_specifications',    'type': 'multi_line_text_field'},
#             'short_description':        {'namespace': 'custom', 'key': 'short_description',           'type': 'multi_line_text_field'},
#             'common_problems':          {'namespace': 'custom', 'key': 'common_causes',               'type': 'multi_line_text_field'},
#             'installation_guide':       {'namespace': 'custom', 'key': 'symptoms_installation_guide', 'type': 'multi_line_text_field'},
#         }
#         for k, v in fallback.items():
#             result.setdefault(k, v)
#         return result

#     section_mf_cfg = _build_dynamic_section_mf_cfg()
#     logger.info(f"section_mf_cfg (dynamic): {section_mf_cfg}")

#     metafields_payload = []
#     for section_key, val in (
#         ('technical_specifications', mf_specs),
#         ('short_description',        mf_bullets),
#         ('common_problems',          mf_causes),
#         ('installation_guide',       mf_install),
#     ):
#         if not (val or '').strip():
#             continue
#         cfg = section_mf_cfg.get(section_key)
#         if not cfg:
#             logger.warning(f'No metafield config found for section "{section_key}", skipping.')
#             continue
#         # single_line_text_field rejects newlines — collapse to one line while keeping HTML tags
#         mf_value = val
#         if cfg['type'] == 'single_line_text_field':
#             mf_value = ' '.join(val.split())
#         metafields_payload.append({
#             'ownerId':   gid,
#             'namespace': cfg['namespace'],
#             'key':       cfg['key'],
#             'type':      cfg['type'],
#             'value':     mf_value,
#         })

#     if metafields_payload:
#         ms = _shopify_graphql(_GQL_METAFIELDS_SET, {'metafields': metafields_payload})
#         if ms.get('errors'):
#             raise RuntimeError(f'Shopify metafieldsSet: {ms["errors"]}')
#         ms_user_errors = (((ms.get('data') or {}).get('metafieldsSet') or {}).get('userErrors') or [])
#         if ms_user_errors:
#             logger.info(f"error-:{ms_user_errors}")
#             raise RuntimeError(f'Shopify metafieldsSet userErrors: {ms_user_errors}')

#     return {
#         'product_gid':          gid,
#         'body_html_updated':    bool(body_html),
#         'metafields_set':       len(metafields_payload),
#         'seo_title_updated':    bool(meta_title_val),
#         'seo_description_updated': bool(meta_desc_val),
#         'shopify_url':          (shopify_url or '')[:512],
#     }


def publish_generated_content_to_shopify(product_title: str, part_number: str, shopify_url: str = '') -> dict:
    logger.info(
        "[PUBLISH] START title=%r part_number=%r shopify_url=%r",
        product_title, part_number, shopify_url,
    )
    gid = _shopify_product_gid_from_product_url(shopify_url) if shopify_url else None
    if not gid:
        # Fall back to SKU-based lookup using the active shop
        logger.info("[PUBLISH] No gid from shopify_url, falling back to SKU lookup for part_number=%r", part_number)
        gid = _shopify_product_gid_from_sku(part_number)
    if not gid:
        raise ValueError(f'Could not resolve a Shopify product for part number "{part_number}". Check that the SKU exists in your Shopify store.')
    logger.info("[PUBLISH] Resolved target product gid=%s for shopify_url=%r", gid, shopify_url)

    # Resolve which batch's content actually belongs to this shopify_url, so we don't pull
    # content generated for a different listing of the same (title, part_number).
    target_batch_id = _batch_id_for_shopify_url(product_title, part_number, shopify_url)

    body_html    = _record_html_for_shopify(_latest_generated_record(product_title, part_number, 'product_description', target_batch_id))
    mf_specs     = _record_html_for_shopify(_latest_generated_record(product_title, part_number, 'technical_specifications', target_batch_id))
    mf_bullets   = _record_html_for_shopify(_latest_generated_record(product_title, part_number, 'short_description', target_batch_id))
    mf_causes    = _record_html_for_shopify(_latest_generated_record(product_title, part_number, 'common_problems', target_batch_id))
    mf_install   = _record_html_for_shopify(_latest_generated_record(product_title, part_number, 'installation_guide', target_batch_id))

    # Fetch SEO plain text (not HTML) for meta title and meta description
    meta_title_rec = _latest_generated_record(product_title, part_number, 'meta_title', target_batch_id)
    meta_desc_rec  = _latest_generated_record(product_title, part_number, 'meta_description', target_batch_id)
    meta_title_val = (meta_title_rec.plain_text or '').strip() if meta_title_rec else ''
    meta_desc_val  = (meta_desc_rec.plain_text  or '').strip() if meta_desc_rec  else ''

    if not any([body_html, mf_specs, mf_bullets, mf_causes, mf_install, meta_title_val, meta_desc_val]):
        raise ValueError('No generated content found for this product — nothing to publish.')

    logger.info(
        "[PUBLISH] About to write to gid=%s | body_html_len=%d meta_title=%r meta_desc_len=%d "
        "| specs_len=%d bullets_len=%d causes_len=%d install_len=%d",
        gid, len(body_html), meta_title_val, len(meta_desc_val),
        len(mf_specs), len(mf_bullets), len(mf_causes), len(mf_install),
    )

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
    live_types = _live_metafield_types()
    logger.info(f"section_mf_cfg (dynamic): {section_mf_cfg}")
    logger.info(f"live_types (from Shopify definitions): {live_types}")

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

        # If Shopify already has a live definition for this namespace.key, its type WINS.
        # This is what stops single_line/multi_line mismatches permanently, across every
        # store, without needing a hardcoded per-store guess.
        live_type = live_types.get(f"{cfg['namespace']}.{cfg['key']}")
        if live_type and live_type != cfg['type']:
            logger.info(
                '[PUBLISH] Overriding configured type for %s.%s: %s -> %s (live Shopify definition)',
                cfg['namespace'], cfg['key'], cfg['type'], live_type,
            )
            cfg = {**cfg, 'type': live_type}

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

        active_shop = _get_active_shop()
        gen_shop_id = active_shop.id if active_shop else None

        gen_batch_id = str(uuid.uuid4())
        record = GeneratedContent(
            shop_id=gen_shop_id,
            batch_id=gen_batch_id,
            product_title=product_title, part_number=part_number,
            part_type=part_type, brand=brand, appliance_type=appliance_type,
            section=section, prompt_type=prompt_type, prompt_used=final_prompt,
            plain_text=plain_text, html_text=html_text,
            input_tokens=total_in, output_tokens=total_out,
            total_tokens=total_in + total_out, model_used=model
        )
        db.session.add(record)

        cost = (total_in * COST_PER_INPUT_TOKEN) + (total_out * COST_PER_OUTPUT_TOKEN)
        ph_row = ProductHistory.query.filter_by(product_title=product_title, part_number=part_number).order_by(ProductHistory.created_at.desc()).first()
        if ph_row:
            if not ph_row.shop_id and gen_shop_id:
                ph_row.shop_id = gen_shop_id
            ph_row.total_input_tokens = (ph_row.total_input_tokens or 0) + total_in
            ph_row.total_output_tokens = (ph_row.total_output_tokens or 0) + total_out
            ph_row.total_tokens = (ph_row.total_tokens or 0) + (total_in + total_out)
            ph_row.cost_usd = (ph_row.cost_usd or 0.0) + cost
            ph_row.sections_generated = (ph_row.sections_generated or 0) + 1
        else:
            ph_row = ProductHistory(
                shop_id=gen_shop_id,
                batch_id=gen_batch_id,
                product_title=product_title, part_number=part_number,
                part_type=part_type, brand=brand, appliance_type=appliance_type,
                source='manual',
                total_input_tokens=total_in, total_output_tokens=total_out,
                total_tokens=total_in + total_out, cost_usd=cost,
                sections_generated=1, model_used=model
            )
            db.session.add(ph_row)
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
    completed_client_ids = set()

    with app.app_context():
        # Check highest existing sequence number for this job so resumed sequence numbers don't clash
        last_ev = JobEvent.query.filter_by(job_id=job_id).order_by(JobEvent.seq.desc()).first()
        if last_ev:
            _seq_counter[0] = last_ev.seq

        # Identify products that were already completed in a previous attempt of this job
        past_done_events = JobEvent.query.filter_by(job_id=job_id, event_type='product_done').all()
        for ev in past_done_events:
            try:
                ev_data = json.loads(ev.payload)
                cid = ev_data.get('id')
                if cid:
                    completed_client_ids.add(str(cid))
            except Exception:
                pass

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
        done_count = len(completed_client_ids)
        _update_job('running', done_products=done_count)
        logger.info(
            "[JOB %s] init | batch_id=%s products=%d sections=%s | completed_so_far=%d",
            job_id, batch_id, len(products), active_sections, done_count
        )

        with app.app_context():
            has_start = JobEvent.query.filter_by(job_id=job_id, event_type='batch_start').first()
            if not has_start:
                _store_event('batch_start', {'batch_id': batch_id, 'total_products': len(products)})

        grand_total_in  = 0
        grand_total_out = 0
        db_lock = threading.Lock()

        with app.app_context():
            _job_obj = GenerationJob.query.filter_by(job_id=job_id).first()
            _shop = _get_active_shop()
            job_payload = json.loads(_job_obj.payload) if (_job_obj and _job_obj.payload) else {}
            job_shop_id       = job_payload.get('shop_id') or (_job_obj.shop_id if _job_obj else None) or (_shop.id if _shop else None)
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

            _update_job('running')

            batch_start_idx = batch_index * BATCH_SIZE
            uncompleted_batch = []
            for i, prod in enumerate(batch):
                client_id = _client_id_for_product(prod, batch_start_idx + i)
                if client_id not in completed_client_ids:
                    uncompleted_batch.append(prod)

            if not uncompleted_batch:
                logger.info(f"[JOB {job_id}][BATCH {batch_index + 1}/{len(product_batches)}] All products in batch already completed — skipping")
                continue

            if batch_index > 0:
                time.sleep(INTER_BATCH_DELAY)

            logger.info(f"[JOB {job_id}][BATCH {batch_index + 1}/{len(product_batches)}] ▶ {len(uncompleted_batch)} uncompleted product(s)")
            logger.info(
                "[JOB %s][BATCH %d] product_keys=%s",
                job_id,
                batch_index + 1,
                [
                    {
                        'client_id': _client_id_for_product(p, batch_start_idx + i),
                        'title': (p.get('product_title') or '').strip(),
                        'part_number': (p.get('part_number') or '').strip(),
                        'brand': (p.get('brand') or '').strip(),
                        'shopify_url': (p.get('shopify_url') or '').strip(),
                    }
                    for i, p in enumerate(uncompleted_batch)
                ],
            )

            product_data = {_client_id_for_product(prod, batch_start_idx + i): {
                'prod': prod, 'sections': {}, 'total_in': 0, 'total_out': 0,
                'done_sections': 0
            } for i, prod in enumerate(uncompleted_batch)}

            # Step 1: resolve images
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as img_executor:
                for i, prod in enumerate(uncompleted_batch):
                    client_id      = _client_id_for_product(prod, batch_start_idx + i)
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

            # Collect image URLs. Keyed by client_id (NOT part_number)
            image_url_map: dict[str, str] = {}
            for i, prod in enumerate(uncompleted_batch):
                client_id   = _client_id_for_product(prod, batch_start_idx + i)
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

            # Step 2: one API call per section for ALL uncompleted products in this batch
            valid_batch = []
            valid_client_ids = []
            for i, prod in enumerate(uncompleted_batch):
                client_id     = _client_id_for_product(prod, batch_start_idx + i)
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
                        valid_client_ids,  # unique keys
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
                                    shop_id=job_shop_id,
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
                            shop_id=job_shop_id,
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
                completed_client_ids.add(client_id)
<<<<<<< HEAD
                done_count      = len(completed_client_ids) 
=======
                done_count      = len(completed_client_ids)
>>>>>>> added repaior image pages

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
                shop_id=job_shop_id,
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
    active_shop = _get_active_shop()
    stream_shop_id = (body.get('shop_id') or session.get('active_shop_id') or (active_shop.id if active_shop else None))
    with app.app_context():
        job = GenerationJob(
            job_id=job_id, shop_id=stream_shop_id, batch_id=batch_id,
            status='pending', total_products=len(products),
            payload=json.dumps({
                'shop_id': stream_shop_id,
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


# @app.route('/api/product-sections')
# @login_required
# def get_product_sections():
#     product_title = request.args.get('product_title', '').strip()
#     part_number   = request.args.get('part_number', '').strip()
#     batch_id      = request.args.get('batch_id', '').strip()
#     logger.info(
#         "product-sections request | title=%r part_number=%s batch_id=%s",
#         product_title, part_number, batch_id
#     )
#     if not product_title or not part_number:
#         return jsonify({'error': 'product_title and part_number are required'}), 400

#     sections = {}
#     for section in SECTIONS:
#         q = GeneratedContent.query.filter_by(
#             product_title=product_title, part_number=part_number, section=section
#         )
#         if batch_id:
#             q = q.filter_by(batch_id=batch_id)
#         record = q.order_by(GeneratedContent.created_at.desc()).first()
#         if record:
#             sections[section] = {
#                 'record_id': record.id,
#                 'plain_text': record.plain_text, 'html_text': record.html_text,
#                 'prompt_used': record.prompt_used, 'prompt_type': record.prompt_type,
#                 'input_tokens': record.input_tokens, 'output_tokens': record.output_tokens,
#                 'total_tokens': record.total_tokens, 'model_used': record.model_used,
#                 'created_at': record.created_at.strftime('%Y-%m-%d %H:%M:%S'),
#             }
#     logger.info(
#         "product-sections response | title=%r part_number=%s batch_id=%s section_record_ids=%s",
#         product_title,
#         part_number,
#         batch_id,
#         {k: v.get('record_id') for k, v in sections.items()},
#     )
#     return jsonify({'sections': sections})

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
            product_title=product_title,
            part_number=part_number,
            section=section
        )

        if batch_id:
            q = q.filter_by(batch_id=batch_id)

        record = q.order_by(GeneratedContent.created_at.desc()).first()

        if record:
            # Backfill html_text for legacy records that only have plain_text
            html_val = (record.html_text or '').strip()
            if not html_val and (record.plain_text or '').strip():
                html_val = _plain_text_to_html(record.plain_text)

            sections[section] = {
                'record_id': record.id,
                'section_label': SECTION_LABELS.get(record.section, record.section),
                'plain_text': record.plain_text,
                'html_text': html_val,
                'prompt_used': record.prompt_used,
                'prompt_type': record.prompt_type,
                'input_tokens': record.input_tokens,
                'output_tokens': record.output_tokens,
                'total_tokens': record.total_tokens,
                'model_used': record.model_used,
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
    shop_id  = request.args.get('shop_id', type=int) or session.get('active_shop_id')
    q = GeneratedContent.query
    if shop_id: q = q.filter_by(shop_id=shop_id)
    records  = q.order_by(GeneratedContent.created_at.desc()).paginate(page=page, per_page=per_page, error_out=False)
    return jsonify({'records': [r.to_dict() for r in records.items], 'total': records.total, 'pages': records.pages, 'current_page': page})


@app.route('/api/product-history')
@login_required
def get_product_history():
    page     = request.args.get('page', 1, type=int)
    per_page = 20
    shop_id  = request.args.get('shop_id', type=int) or session.get('active_shop_id')
    q = ProductHistory.query
    if shop_id: q = q.filter_by(shop_id=shop_id)
    records  = q.order_by(ProductHistory.created_at.desc()).paginate(page=page, per_page=per_page, error_out=False)
    return jsonify({'records': [r.to_dict() for r in records.items], 'total': records.total, 'pages': records.pages, 'current_page': page})


@app.route('/api/batch-history')
@login_required
def get_batch_history():
    page     = request.args.get('page', 1, type=int)
    per_page = 20
    shop_id  = request.args.get('shop_id', type=int) or session.get('active_shop_id')
    q = BatchHistory.query
    if shop_id: q = q.filter_by(shop_id=shop_id)
    records  = q.order_by(BatchHistory.created_at.desc()).paginate(page=page, per_page=per_page, error_out=False)
    return jsonify({'records': [r.to_dict() for r in records.items], 'total': records.total, 'pages': records.pages, 'current_page': page})


@app.route('/api/batch-products/<batch_id>')
@login_required
def get_batch_products(batch_id):
    from sqlalchemy import or_
    shop_id = request.args.get('shop_id', type=int) or session.get('active_shop_id')
    q = ProductHistory.query.filter_by(batch_id=batch_id)
    if shop_id:
        active_shop = ShopConfig.query.get(shop_id)
        if active_shop and active_shop.domain:
            d = active_shop.domain.strip().lower()
            q = q.filter(or_(ProductHistory.shop_id == shop_id, ProductHistory.shopify_url.ilike(f'%{d}%')))
        else:
            q = q.filter(ProductHistory.shop_id == shop_id)
    
    rows = q.order_by(ProductHistory.created_at.desc(), ProductHistory.id.desc()).all()

    seen = set()
    unique_records = []
    for r in rows:
        key = (r.product_title.strip().lower(), r.part_number.strip().lower())
        if key not in seen:
            seen.add(key)
            unique_records.append(r)

    return jsonify({'products': [r.to_dict() for r in unique_records]})


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


@app.route('/api/review/batches')
@reviewer_required
def review_batches():
    """
    Batch-grouped replacement for /api/review/products + client-side grouping.

    Returns real batches (batch_id -> its own products), filtered in SQL and
    paginated by BATCH (not by raw ProductHistory row), so the count/items the
    frontend renders always matches what's actually in product_history /
    batch_history — no client-side reconstruction, no per_page=9999 dump.
    """
    from sqlalchemy import func, or_, and_

    page     = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 10, type=int)   # BATCHES per page
    status   = (request.args.get('status') or 'all').strip().lower()
    search   = (request.args.get('search') or '').strip().lower()
    shop_id  = request.args.get('shop_id', type=int) or session.get('active_shop_id')
    shop_domains = [d.strip().lower() for d in (request.args.get('shop_domains') or '').split(',') if d.strip()]

    review_join_cond = and_(
        ReviewStatus.product_title == ProductHistory.product_title,
        ReviewStatus.part_number == ProductHistory.part_number,
    )

    def _apply_filters(q):
        if status in ('pending', 'reviewed', 'published'):
            if status == 'pending':
                q = q.filter(or_(ReviewStatus.status == 'pending', ReviewStatus.status.is_(None)))
            else:
                q = q.filter(ReviewStatus.status == status)
        if search:
            like = f'%{search}%'
            q = q.filter(or_(
                func.lower(ProductHistory.product_title).like(like),
                func.lower(ProductHistory.part_number).like(like),
            ))
        if shop_id:
            if shop_domains:
                q = q.filter(or_(ProductHistory.shop_id == shop_id, *[ProductHistory.shopify_url.ilike(f'%{d}%') for d in shop_domains]))
            else:
                q = q.filter(ProductHistory.shop_id == shop_id)
        elif shop_domains:
            q = q.filter(or_(*[ProductHistory.shopify_url.ilike(f'%{d}%') for d in shop_domains]))
        return q

    # Step 1: cheap aggregate — one row per batch, not per product — so
    # pagination and item counts are computed without pulling product rows.
    agg_q = (
        db.session.query(
            ProductHistory.batch_id,
            func.count(ProductHistory.id).label('item_count'),
            func.max(ProductHistory.created_at).label('latest_created_at'),
        )
        .outerjoin(ReviewStatus, review_join_cond)
        .filter(ProductHistory.batch_id.isnot(None), ProductHistory.batch_id != '')
    )
    agg_q = _apply_filters(agg_q).group_by(ProductHistory.batch_id)

    all_batch_summaries = agg_q.order_by(func.max(ProductHistory.created_at).desc()).all()
    total_batches = len(all_batch_summaries)
    pages = max(1, (total_batches + per_page - 1) // per_page)
    page = max(1, min(page, pages))

    start = (page - 1) * per_page
    page_summaries = all_batch_summaries[start:start + per_page]
    page_batch_ids = [s.batch_id for s in page_summaries]

    # Step 2: fetch the actual product rows, but ONLY for the batches on this page.
    products_by_batch = {bid: [] for bid in page_batch_ids}
    if page_batch_ids:
        detail_q = (
            db.session.query(ProductHistory, ReviewStatus)
            .outerjoin(ReviewStatus, review_join_cond)
            .filter(ProductHistory.batch_id.in_(page_batch_ids))
        )
        detail_q = _apply_filters(detail_q).order_by(ProductHistory.batch_id, ProductHistory.id)

        seen_by_batch = {bid: set() for bid in page_batch_ids}
        for row, rv in detail_q.all():
            bid = row.batch_id
            prod_key = (row.product_title.strip().lower(), row.part_number.strip().lower())
            if prod_key in seen_by_batch[bid]:
                continue
            seen_by_batch[bid].add(prod_key)

            products_by_batch[bid].append({
                "product_title": row.product_title,
                "part_number": row.part_number,
                "brand": row.brand,
                "appliance_type": row.appliance_type,
                "part_type": row.part_type,
                "latest_created_at": row.created_at.strftime("%Y-%m-%d %H:%M:%S") if row.created_at else None,
                "batch_id": row.batch_id or "",
                "shopify_url": (row.shopify_url or "").strip(),
                "review_status": rv.status if rv else "pending",
                "reviewer": rv.reviewer if rv else None,
                "reviewed_at": rv.reviewed_at.strftime("%Y-%m-%d %H:%M:%S") if rv and rv.reviewed_at else None,
                "published_at": rv.published_at.strftime("%Y-%m-%d %H:%M:%S") if rv and rv.published_at else None,
            })

    bh_map = {}
    job_map = {}
    if page_batch_ids:
        job_map = {job.batch_id: job for job in GenerationJob.query.filter(GenerationJob.batch_id.in_(page_batch_ids)).all()}
        bh_map = {bh.batch_id: bh for bh in BatchHistory.query.filter(BatchHistory.batch_id.in_(page_batch_ids)).all()}

    batches_out = []
    for s in page_summaries:
        job = job_map.get(s.batch_id)
        bh = bh_map.get(s.batch_id)
        prod_count = (job.total_products if (job and job.total_products) else (bh.product_count if (bh and bh.product_count) else s.item_count))
        batches_out.append({
            "batch_id": s.batch_id,
            "source": (bh.source if bh else None) or "manual",
            "product_count": prod_count,                                   # product count from GenerationJob
            "item_count": s.item_count,                                   # count AFTER current filters
            "latest_created_at": s.latest_created_at.strftime("%Y-%m-%d %H:%M:%S") if s.latest_created_at else None,
            "products": products_by_batch.get(s.batch_id, []),
        })

    return jsonify({
        "batches": batches_out,
        "total": total_batches,
        "pages": pages,
        "current_page": page,
    })


@app.route('/api/review/products')
@reviewer_required
def review_products():
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 30, type=int)
    shop_id = request.args.get('shop_id', type=int) or session.get("active_shop_id")
    logger.info(f"shop_id: {shop_id}")

    # Base query
    query = ProductHistory.query
    if shop_id:
        active_shop = ShopConfig.query.get(shop_id)
        if active_shop and active_shop.domain:
            d = active_shop.domain.strip().lower()
            query = query.filter(or_(ProductHistory.shop_id == shop_id, ProductHistory.shopify_url.ilike(f'%{d}%')))
        else:
            query = query.filter(ProductHistory.shop_id == shop_id)

    total_count = query.count()

    rows = (
        query.order_by(ProductHistory.created_at.desc())
        .offset((page - 1) * per_page)
        .limit(per_page)
        .all()
    )

    # ── Bulk-load ReviewStatus + BatchHistory instead of querying per row ──
    # (previously: 2 queries per row = up to ~2*per_page extra round-trips)
    pt_pn_pairs = {(r.product_title, r.part_number) for r in rows}
    review_by_key = {}
    if pt_pn_pairs:
        titles = {t for t, _ in pt_pn_pairs}
        rv_rows = ReviewStatus.query.filter(ReviewStatus.product_title.in_(titles)).all()
        for rv in rv_rows:
            key = (rv.product_title, rv.part_number)
            if key in pt_pn_pairs:
                review_by_key[key] = rv

    batch_ids = {r.batch_id for r in rows if r.batch_id}
    batch_source_by_id = {}
    if batch_ids:
        bh_rows = BatchHistory.query.filter(BatchHistory.batch_id.in_(batch_ids)).all()
        batch_source_by_id = {bh.batch_id: bh.source for bh in bh_rows if bh.source}

    records = []

    for row in rows:
        rv = review_by_key.get((row.product_title, row.part_number))

        # Resolve batch source
        batch_source = row.source or "Manual"

        if row.batch_id:
            src = batch_source_by_id.get(row.batch_id)
            if src:
                batch_source = src

        records.append({
            "product_title": row.product_title,
            "part_number": row.part_number,
            "brand": row.brand,
            "appliance_type": row.appliance_type,
            "part_type": row.part_type,
            "latest_created_at": row.created_at.strftime("%Y-%m-%d %H:%M:%S") if row.created_at else None,
            "batch_id": row.batch_id or "",
            "batch_source": batch_source,
            "shopify_url": (row.shopify_url or "").strip(),
            "review_status": rv.status if rv else "pending",
            "reviewer": rv.reviewer if rv else None,
            "reviewed_at": rv.reviewed_at.strftime("%Y-%m-%d %H:%M:%S") if rv and rv.reviewed_at else None,
            "published_at": rv.published_at.strftime("%Y-%m-%d %H:%M:%S") if rv and rv.published_at else None,
        })

    pages = (total_count + per_page - 1) // per_page

    return jsonify({
        "records": records,
        "total": total_count,
        "pages": pages,
        "current_page": page,
    })


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
    if not url:
        logger.warning("[api_shopify_product] Received request with empty or missing 'url'")
        return jsonify({'success': False, 'error': 'Product URL is required'}), 400
    logger.info(f"[api_shopify_product] Fetch request received for url='{url}'")
    try:
        product = _fetch_shopify_product(url)
        logger.info(f"[api_shopify_product] SUCCESS for url='{url}' -> title='{product.get('title')}', part_number='{product.get('part_number')}'")
        return jsonify({'success': True, 'product': product})
    except Exception as e:
        logger.error(f"[api_shopify_product] FAILED for url='{url}': {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e), 'url': url}), 400

@app.route('/api/shopify-products-batch', methods=['POST'])
@login_required
def fetch_shopify_products_batch():
    body = request.json or {}
    urls = body.get('urls') or []
    if not urls: return jsonify({'error': 'urls array is required'}), 400
    if len(urls) > 100: return jsonify({'error': 'Maximum 100 URLs per batch'}), 400
    logger.info(f"[fetch_shopify_products_batch] Processing batch request of {len(urls)} URLs")
    products, errors = [], []
    for idx, raw_url in enumerate(urls):
        raw_url = (raw_url or '').strip()
        if not raw_url:
            err_msg = 'Empty URL'
            logger.warning(f"[fetch_shopify_products_batch] Row #{idx+1}: {err_msg}")
            errors.append({'row': idx + 1, 'url': raw_url, 'error': err_msg})
            continue
        try:
            product = _fetch_shopify_product(raw_url)
            product['source_url'] = raw_url
            products.append(product)
            logger.info(f"[fetch_shopify_products_batch] Row #{idx+1} SUCCESS for '{raw_url}'")
        except Exception as e:
            logger.error(f"[fetch_shopify_products_batch] Row #{idx+1} FAILED for '{raw_url}': {e}")
            errors.append({'row': idx + 1, 'url': raw_url, 'error': str(e)})
    logger.info(f"[fetch_shopify_products_batch] Batch complete: {len(products)} fetched, {len(errors)} failed")
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


@app.route('/api/admin/backfill-shop-ids', methods=['POST'])
@login_required
def backfill_shop_ids():
    """
    Backfills null shop_id columns across ProductHistory, GeneratedContent,
    BatchHistory, ReviewStatus, and GenerationJob using shopify_url matching
    or active shop fallback.
    """
    shops = ShopConfig.query.all()
    if not shops:
        return jsonify({'error': 'No shop configurations found'}), 400

    shop_map = {}
    for s in shops:
        if s.domain:
            shop_map[s.domain.strip().lower()] = s.id
        if s.website_domain:
            shop_map[s.website_domain.strip().lower()] = s.id

    default_shop_id = shops[0].id if len(shops) == 1 else None
    active_shop = _get_active_shop()
    if active_shop:
        default_shop_id = active_shop.id

    updated_ph = 0
    updated_gc = 0
    updated_bh = 0
    updated_rv = 0
    updated_gj = 0

    # 1. ProductHistory
    ph_nulls = ProductHistory.query.filter(ProductHistory.shop_id.is_(None)).all()
    for ph in ph_nulls:
        matched_id = None
        if ph.shopify_url:
            url_lower = ph.shopify_url.lower()
            for dom, sid in shop_map.items():
                if dom in url_lower:
                    matched_id = sid
                    break
        if not matched_id and default_shop_id:
            matched_id = default_shop_id
        if matched_id:
            ph.shop_id = matched_id
            updated_ph += 1

    # 2. GeneratedContent
    gc_nulls = GeneratedContent.query.filter(GeneratedContent.shop_id.is_(None)).all()
    if gc_nulls:
        ph_shop_by_batch = {ph.batch_id: ph.shop_id for ph in ProductHistory.query.filter(ProductHistory.shop_id.isnot(None)).all() if ph.batch_id}
        ph_shop_by_prod = {(ph.product_title, ph.part_number): ph.shop_id for ph in ProductHistory.query.filter(ProductHistory.shop_id.isnot(None)).all()}

        for gc in gc_nulls:
            sid = ph_shop_by_batch.get(gc.batch_id) or ph_shop_by_prod.get((gc.product_title, gc.part_number)) or default_shop_id
            if sid:
                gc.shop_id = sid
                updated_gc += 1

    # 3. BatchHistory
    bh_nulls = BatchHistory.query.filter(BatchHistory.shop_id.is_(None)).all()
    if bh_nulls:
        ph_shop_by_batch = {ph.batch_id: ph.shop_id for ph in ProductHistory.query.filter(ProductHistory.shop_id.isnot(None)).all() if ph.batch_id}
        for bh in bh_nulls:
            sid = ph_shop_by_batch.get(bh.batch_id) or default_shop_id
            if sid:
                bh.shop_id = sid
                updated_bh += 1

    # 4. ReviewStatus
    rv_nulls = ReviewStatus.query.filter(ReviewStatus.shop_id.is_(None)).all()
    if rv_nulls:
        ph_shop_by_prod = {(ph.product_title, ph.part_number): ph.shop_id for ph in ProductHistory.query.filter(ProductHistory.shop_id.isnot(None)).all()}
        for rv in rv_nulls:
            sid = ph_shop_by_prod.get((rv.product_title, rv.part_number)) or default_shop_id
            if sid:
                rv.shop_id = sid
                updated_rv += 1

    # 5. GenerationJob
    gj_nulls = GenerationJob.query.filter(GenerationJob.shop_id.is_(None)).all()
    if gj_nulls:
        ph_shop_by_batch = {ph.batch_id: ph.shop_id for ph in ProductHistory.query.filter(ProductHistory.shop_id.isnot(None)).all() if ph.batch_id}
        for gj in gj_nulls:
            sid = ph_shop_by_batch.get(gj.batch_id) or default_shop_id
            if sid:
                gj.shop_id = sid
                updated_gj += 1

    return jsonify({
        'success': True,
        'updated': {
            'product_history': updated_ph,
            'generated_content': updated_gc,
            'batch_history': updated_bh,
            'review_status': updated_rv,
            'generation_job': updated_gj,
        }
    })


# ─── REPAIR IMAGE GENERATION & REVIEW ROUTES ───────────────────────────────

@app.route('/generate-repair')
@login_required
def generate_repair_page():
    return render_template('generate_repair.html', BASE_URL=BASE_URL)


@app.route('/review-repair')
@reviewer_required
def review_repair_page():
    return render_template('review_repair.html', BASE_URL=BASE_URL)


def _find_partselect_url(part_number, brand='', product_title='', shopify_url=''):
    if shopify_url and ('partselect.com' in shopify_url.lower() or 'partselect.ca' in shopify_url.lower()):
        logger.info("[PARTSELECT-SEARCH] Direct PartSelect URL provided: %s", shopify_url)
        return shopify_url

    if not part_number:
        return None

    queries = [
        f"site:partselect.com {part_number}",
        f"partselect.com {part_number}",
        f"{brand} {part_number} partselect".strip()
    ]
    pattern = re.compile(r"partselect\.(?:com|ca)/PS\d+-", re.I)

    try:
        from ddgs import DDGS
        with DDGS() as ddgs:
            for q in queries:
                try:
                    logger.info("[PARTSELECT-SEARCH] Searching DDG for: '%s'", q)
                    results = list(ddgs.text(q, max_results=6))
                    for r in results:
                        href = r.get('href', '')
                        if pattern.search(href):
                            logger.info("[PARTSELECT-SEARCH] Found PartSelect product URL: %s", href)
                            return href
                except Exception as e:
                    logger.warning("[PARTSELECT-SEARCH] DDG query '%s' error: %s", q, e)
                    continue
    except Exception as e:
        logger.warning("[PARTSELECT-SEARCH] DDGS module search failed: %s", e)

    return None


def _scrape_partselect_repair_data(url):
    logger.info("[PARTSELECT-SCRAPER] Scraping PartSelect page: %s", url)
    difficulty = None
    duration = None
    try:
        from playwright.sync_api import sync_playwright
        from bs4 import BeautifulSoup

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                viewport={'width': 1400, 'height': 900},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36'
            )
            page = context.new_page()
            page.add_init_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined});")
            page.goto(url, wait_until='domcontentloaded', timeout=30000)
            try:
                page.wait_for_selector('text=Difficulty', timeout=5000)
            except Exception:
                page.wait_for_timeout(3000)
            html = page.content()
            browser.close()

        soup = BeautifulSoup(html, 'html.parser')
        text = soup.get_text(' ', strip=True)

        review_marker = re.search(r'\d+\s+Reviews?', text)
        price_match_idx = text.find('Price Match', review_marker.end()) if review_marker else -1

        if review_marker and price_match_idx != -1:
            badge_text = text[review_marker.end():price_match_idx]
        else:
            badge_text = text

        levels = ['Very Difficult', 'Really Easy', 'Very Easy', 'Difficult', 'Medium', 'Easy']
        for level in levels:
            if level in badge_text:
                difficulty = level
                break

        duration_pattern = re.compile(
            r'(Less than \d+\s*(?:mins?|minutes?|hours?|hrs?)'
            r'|\d+\s*-\s*\d+\s*(?:mins?|minutes?|hours?|hrs?)'
            r'|\d+\s*(?:mins?|minutes?|hours?|hrs?))',
            re.I
        )
        match = duration_pattern.search(badge_text)
        if match:
            duration = match.group(1)

    except Exception as e:
        logger.error("[PARTSELECT-SCRAPER] Error scraping %s: %s", url, e)

    return difficulty, duration


def _generate_repair_details_from_ai(product_title, part_number, brand='', appliance_type='', part_type='', shopify_url=''):
    ps_difficulty = None
    ps_duration = None
    ps_url = _find_partselect_url(part_number, brand, product_title, shopify_url)
    if ps_url:
        ps_difficulty, ps_duration = _scrape_partselect_repair_data(ps_url)

    if ps_difficulty and ps_duration:
        logger.info("[PARTSELECT-SCRAPER] [SOURCE: REAL PARTSELECT SCRAPED DATA] Difficulty: '%s', Repair Time: '%s' from URL: %s", ps_difficulty, ps_duration, ps_url)
    else:
        logger.info("[PARTSELECT-SCRAPER] PartSelect scraped badge data not found for '%s'. Fallback to AI.", part_number)

    client = get_openai_client()
    logger.info("[REPAIR-AI] Requesting features & safety details from OpenAI for product='%s', part_number='%s'...", product_title, part_number)
    prompt = f"""You are an expert electronics and appliance repair technician.

Given the following appliance part:
- Title: {product_title}
- Part Number: {part_number}
- Brand: {brand}
- Appliance Type: {appliance_type}
- Part Type: {part_type}

Provide accurate repair details, difficulty, safety tip, and 4 key features in JSON format matching this exact schema:

{{
  "product_name": "{product_title}",
  "part_number": "{part_number}",
  "repair_difficulty": "{ps_difficulty.upper() if ps_difficulty else 'EASY'}",
  "estimated_time": "{ps_duration if ps_duration else '15–30 Minutes'}",
  "diy": "YES",
  "safety_tip": "A single clear safety warning (15-25 words) for replacing this component safely (e.g. disconnect power, wear gloves, discharge capacitors).",
  "features": [
    {{
      "title": "OEM QUALITY",
      "description": "Short 1-sentence bullet point describing OEM quality."
    }},
    {{
      "title": "RELIABLE PERFORMANCE",
      "description": "Short 1-sentence bullet point describing reliable performance."
    }},
    {{
      "title": "DURABLE CONSTRUCTION",
      "description": "Short 1-sentence bullet point describing durable construction."
    }},
    {{
      "title": "PERFECT FIT",
      "description": "Short 1-sentence bullet point describing perfect fit and compatibility."
    }}
  ]
}}

Return ONLY valid raw JSON.
"""
    try:
        response = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            temperature=0.4,
            max_tokens=500
        )
        content = response.choices[0].message.content.strip()
        logger.info("[REPAIR-AI] Raw OpenAI JSON Response for '%s': %s", part_number, content)
        data = json.loads(content)

        if ps_duration:
            repair_time = ps_duration
            logger.info("[REPAIR-AI] [SOURCE: REAL PARTSELECT SCRAPED] repair_time = '%s'", repair_time)
        else:
            ai_estimated_time = data.get('estimated_time') or data.get('repair_time')
            if ai_estimated_time:
                repair_time = str(ai_estimated_time).strip()
                logger.info("[REPAIR-AI] [SOURCE: AI GENERATED FALLBACK] repair_time = '%s'", repair_time)
            else:
                repair_time = '15–30 Minutes'
                logger.warning("[REPAIR-AI] [SOURCE: SCRIPT FALLBACK DEFAULT] repair_time missing! Defaulting to '%s'", repair_time)

        if ps_difficulty:
            repair_difficulty = ps_difficulty.upper()
            logger.info("[REPAIR-AI] [SOURCE: REAL PARTSELECT SCRAPED] repair_difficulty = '%s'", repair_difficulty)
        else:
            ai_difficulty = data.get('repair_difficulty')
            if ai_difficulty:
                repair_difficulty = str(ai_difficulty).strip().upper()
                logger.info("[REPAIR-AI] [SOURCE: AI GENERATED FALLBACK] repair_difficulty = '%s'", repair_difficulty)
            else:
                repair_difficulty = 'EASY'
                logger.warning("[REPAIR-AI] [SOURCE: SCRIPT FALLBACK DEFAULT] repair_difficulty missing! Defaulting to '%s'", repair_difficulty)

        ai_safety = data.get('safety_tip')
        if ai_safety:
            safety_tip = str(ai_safety).strip()
            logger.info("[REPAIR-AI] [SOURCE: AI GENERATED] safety_tip = '%s'", safety_tip)
        else:
            safety_tip = 'Always disconnect power supply and discharge capacitors before starting installation.'
            logger.warning("[REPAIR-AI] [SOURCE: SCRIPT FALLBACK DEFAULT] safety_tip missing! Defaulting to script fallback.")

        features_list = data.get('features', [])
        feat_map = {}
        if isinstance(features_list, list):
            for f in features_list:
                if isinstance(f, dict) and f.get('title'):
                    t = f['title'].strip().upper()
                    feat_map[t] = f.get('description', '')

        oem = feat_map.get('OEM QUALITY') or data.get('oem_quality') or 'Build to meet original equipment standards'
        reliable = feat_map.get('RELIABLE PERFORMANCE') or data.get('reliable_performance') or 'Featuring a simple design, you can easily install this part.'
        durable = feat_map.get('DURABLE CONSTRUCTION') or data.get('durable_construction') or 'Built with robust materials, it offers excellent resistance to wear.'
        fit = feat_map.get('PERFECT FIT') or data.get('perfect_fit') or 'Tailored to fit your specific model, this part integrates seamlessly.'

        logger.info("[REPAIR-AI] Key Features:\n  - OEM QUALITY: %s\n  - RELIABLE PERFORMANCE: %s\n  - DURABLE CONSTRUCTION: %s\n  - PERFECT FIT: %s", oem, reliable, durable, fit)

        return {
            'repair_time': repair_time,
            'repair_difficulty': repair_difficulty,
            'oem_quality': oem,
            'reliable_performance': reliable,
            'durable_construction': durable,
            'perfect_fit': fit,
            'safety_tip': safety_tip,
            'diy': data.get('diy', 'YES'),
            'features': features_list if features_list else [
                {"title": "OEM QUALITY", "description": oem},
                {"title": "RELIABLE PERFORMANCE", "description": reliable},
                {"title": "DURABLE CONSTRUCTION", "description": durable},
                {"title": "PERFECT FIT", "description": fit}
            ]
        }
    except Exception as e:
        logger.error("[REPAIR-AI] [SOURCE: SCRIPT FALLBACK EXECUTED] Error generating repair details for part '%s': %s. Using scraped or default values!", part_number, e)
        final_diff = ps_difficulty.upper() if ps_difficulty else 'EASY'
        final_dur = ps_duration if ps_duration else '15–30 Minutes'
        return {
            'repair_time': final_dur,
            'repair_difficulty': final_diff,
            'oem_quality': 'Build to meet original equipment standards',
            'reliable_performance': 'Featuring a simple design, you can easily install this part.',
            'durable_construction': 'Built with robust materials, it offers excellent resistance to wear.',
            'perfect_fit': 'Tailored to fit your specific model, this part integrates seamlessly.',
            'safety_tip': 'Always disconnect power supply and discharge capacitors before starting installation.',
            'diy': 'YES',
            'features': [
                {"title": "OEM QUALITY", "description": "Build to meet original equipment standards"},
                {"title": "RELIABLE PERFORMANCE", "description": "Featuring a simple design, you can easily install this part."},
                {"title": "DURABLE CONSTRUCTION", "description": "Built with robust materials, it offers excellent resistance to wear."},
                {"title": "PERFECT FIT", "description": "Tailored to fit your specific model, this part integrates seamlessly."}
            ]
        }


@app.route('/api/generate-repair', methods=['POST'])
@login_required
def generate_repair_api():
    data = request.json or {}
    products = data.get('products', [])
    shop_id = data.get('shop_id') or None
    
    if not products or not isinstance(products, list):
        return jsonify({'error': 'Products list is required.'}), 400

    active_shop = _get_active_shop()
    if not shop_id and active_shop:
        shop_id = active_shop.id

    batch_id = str(uuid.uuid4())
    results = []

    for item in products:
        p_title = (item.get('product_title') or '').strip()
        p_number = (item.get('part_number') or '').strip()
        if not p_title or not p_number:
            continue

        brand = (item.get('brand') or '').strip()
        appliance_type = (item.get('appliance_type') or '').strip()
        part_type = (item.get('part_type') or '').strip()
        shopify_url = (item.get('shopify_url') or '').strip()
        product_image_url = (item.get('product_image_url') or '').strip()

        if not product_image_url and shopify_url:
            product_image_url = fetch_shopify_image_by_product_url(shopify_url)
        if not product_image_url and p_number:
            product_image_url = fetch_shopify_image_by_sku(p_number)

        ai_details = _generate_repair_details_from_ai(p_title, p_number, brand, appliance_type, part_type, shopify_url)

        rec = RepairImageGeneration(
            batch_id=batch_id,
            shop_id=shop_id,
            product_title=p_title,
            part_number=p_number,
            brand=brand,
            appliance_type=appliance_type,
            part_type=part_type,
            shopify_url=shopify_url,
            product_image_url=product_image_url,
            repair_time=ai_details['repair_time'],
            repair_difficulty=ai_details['repair_difficulty'],
            oem_quality=ai_details['oem_quality'],
            reliable_performance=ai_details['reliable_performance'],
            durable_construction=ai_details['durable_construction'],
            perfect_fit=ai_details['perfect_fit'],
            safety_tip=ai_details['safety_tip'],
            review_status='pending'
        )
        db.session.add(rec)
        db.session.commit()

        results.append({
            'id': rec.id,
            'generation_id': rec.generation_id,
            'batch_id': rec.batch_id,
            'product_title': rec.product_title,
            'part_number': rec.part_number,
            'brand': rec.brand,
            'appliance_type': rec.appliance_type,
            'part_type': rec.part_type,
            'shopify_url': rec.shopify_url,
            'product_image_url': rec.product_image_url,
            'repair_time': rec.repair_time,
            'repair_difficulty': rec.repair_difficulty,
            'oem_quality': rec.oem_quality,
            'reliable_performance': rec.reliable_performance,
            'durable_construction': rec.durable_construction,
            'perfect_fit': rec.perfect_fit,
            'safety_tip': rec.safety_tip,
            'review_status': rec.review_status,
            'created_at': rec.created_at.strftime('%Y-%m-%d %H:%M:%S') if rec.created_at else ''
        })

    return jsonify({
        'success': True,
        'batch_id': batch_id,
        'count': len(results),
        'results': results
    })


@app.route('/api/review-repair/products', methods=['GET'])
@reviewer_required
def get_review_repair_products():
    shop_id = request.args.get('shop_id', type=int)
    batch_id = request.args.get('batch_id', type=str)
    status = request.args.get('status', default='all', type=str)
    search = request.args.get('search', default='', type=str).strip().lower()

    query = RepairImageGeneration.query

    if shop_id:
        query = query.filter(RepairImageGeneration.shop_id == shop_id)
    if batch_id and batch_id != 'all':
        query = query.filter(RepairImageGeneration.batch_id == batch_id)
    if status and status != 'all':
        query = query.filter(RepairImageGeneration.review_status == status)

    records = query.order_by(RepairImageGeneration.created_at.desc()).all()

    if search:
        records = [
            r for r in records
            if (r.product_title and search in r.product_title.lower()) or
               (r.part_number and search in r.part_number.lower()) or
               (r.brand and search in r.brand.lower())
        ]

    out = []
    for r in records:
        out.append({
            'id': r.id,
            'generation_id': r.generation_id,
            'batch_id': r.batch_id,
            'shop_id': r.shop_id,
            'product_title': r.product_title,
            'part_number': r.part_number,
            'brand': r.brand,
            'appliance_type': r.appliance_type,
            'part_type': r.part_type,
            'shopify_url': r.shopify_url,
            'product_image_url': r.product_image_url,
            'generated_image_url': r.generated_image_url,
            'repair_time': r.repair_time,
            'repair_difficulty': r.repair_difficulty,
            'oem_quality': r.oem_quality,
            'reliable_performance': r.reliable_performance,
            'durable_construction': r.durable_construction,
            'perfect_fit': r.perfect_fit,
            'safety_tip': r.safety_tip,
            'review_status': r.review_status,
            'created_at': r.created_at.strftime('%Y-%m-%d %H:%M:%S') if r.created_at else ''
        })

    return jsonify({'products': out})


@app.route('/api/review-repair/batches', methods=['GET'])
@reviewer_required
def get_review_repair_batches():
    shop_id = request.args.get('shop_id', type=int)
    query = RepairImageGeneration.query
    if shop_id:
        query = query.filter(RepairImageGeneration.shop_id == shop_id)

    records = query.all()
    batch_map = {}
    for r in records:
        bid = r.batch_id or 'single'
        if bid not in batch_map:
            batch_map[bid] = {
                'batch_id': bid,
                'count': 0,
                'latest_created_at': r.created_at,
                'products': []
            }
        batch_map[bid]['count'] += 1
        batch_map[bid]['products'].append({
            'id': r.id,
            'product_title': r.product_title,
            'part_number': r.part_number,
            'review_status': r.review_status,
            'generated_image_url': r.generated_image_url
        })
        if r.created_at and (not batch_map[bid]['latest_created_at'] or r.created_at > batch_map[bid]['latest_created_at']):
            batch_map[bid]['latest_created_at'] = r.created_at

    batches = list(batch_map.values())
    batches.sort(key=lambda x: x['latest_created_at'] or datetime.min, reverse=True)

    out = []
    for b in batches:
        out.append({
            'batch_id': b['batch_id'],
            'count': b['count'],
            'created_at': b['latest_created_at'].strftime('%Y-%m-%d %H:%M:%S') if b['latest_created_at'] else '',
            'products': b['products']
        })

    return jsonify({'batches': out})


@app.route('/api/review-repair/update', methods=['POST', 'PUT'])
@reviewer_required
def update_review_repair():
    data = request.json or {}
    record_id = data.get('id')
    if not record_id:
        return jsonify({'error': 'ID is required.'}), 400

    rec = RepairImageGeneration.query.get(record_id)
    if not rec:
        return jsonify({'error': 'Record not found.'}), 404

    if 'product_title' in data: rec.product_title = data['product_title']
    if 'part_number' in data: rec.part_number = data['part_number']
    if 'brand' in data: rec.brand = data['brand']
    if 'appliance_type' in data: rec.appliance_type = data['appliance_type']
    if 'part_type' in data: rec.part_type = data['part_type']
    if 'shopify_url' in data: rec.shopify_url = data['shopify_url']
    if 'product_image_url' in data: rec.product_image_url = data['product_image_url']
    if 'generated_image_url' in data:
        g_url = data['generated_image_url'] or ''
        b_url = (os.getenv("BASE_URL") or BASE_URL or "").strip().rstrip('/')
        if b_url and g_url.startswith('/static/') and not g_url.startswith(b_url):
            g_url = f"{b_url}{g_url}"
        rec.generated_image_url = g_url
    if 'repair_time' in data: rec.repair_time = data['repair_time']
    if 'repair_difficulty' in data: rec.repair_difficulty = data['repair_difficulty']
    if 'oem_quality' in data: rec.oem_quality = data['oem_quality']
    if 'reliable_performance' in data: rec.reliable_performance = data['reliable_performance']
    if 'durable_construction' in data: rec.durable_construction = data['durable_construction']
    if 'perfect_fit' in data: rec.perfect_fit = data['perfect_fit']
    if 'safety_tip' in data: rec.safety_tip = data['safety_tip']
    if 'review_status' in data: rec.review_status = data['review_status']

    db.session.commit()
    return jsonify({'success': True, 'product': rec.to_dict()})


@app.route('/api/review-repair/generate-image/<int:record_id>', methods=['POST'])
@reviewer_required
def generate_review_repair_image(record_id):
    rec = RepairImageGeneration.query.get(record_id)
    if not rec:
        return jsonify({'error': 'Record not found.'}), 404

    try:
        gen_url = _generate_repair_infographic_image(rec)
        return jsonify({
            'success': True,
            'generated_image_url': gen_url,
            'product': rec.to_dict()
        })
    except Exception as e:
        logger.error("[GENERATE-IMAGE-API] Failed to generate image for id %s: %s", record_id, e)
        return jsonify({'error': f'Image generation failed: {str(e)}'}), 500


@app.route('/api/review-repair/publish/<int:record_id>', methods=['POST'])
@reviewer_required
def publish_review_repair_image(record_id):
    rec = RepairImageGeneration.query.get(record_id)
    if not rec:
        return jsonify({'error': 'Record not found.'}), 404

    try:
        published_src = _publish_repair_image_to_shopify(rec)
        return jsonify({
            'success': True,
            'message': 'Image published to Shopify successfully!',
            'image_src': published_src,
            'product': rec.to_dict()
        })
    except Exception as e:
        logger.error("[PUBLISH-REPAIR-API] Failed to publish for id %s: %s", record_id, e)
        return jsonify({'error': f'Publishing to Shopify failed: {str(e)}'}), 500


def _generate_repair_infographic_image(rec: RepairImageGeneration) -> str:
    base_dir = os.path.dirname(os.path.abspath(__file__))
    
    product_data = {
        "product_name": rec.product_title or "Appliance Part",
        "part_number": rec.part_number or "",
        "product_image": rec.product_image_url or "",
        "repair_difficulty": rec.repair_difficulty or "EASY",
        "estimated_time": rec.repair_time or "15–30 Minutes",
        "diy": "YES",
        "safety_tip": rec.safety_tip or "Always disconnect power supply before replacing part.",
        "features": [
            {"title": "OEM QUALITY", "description": rec.oem_quality or "Build to meet original equipment standards"},
            {"title": "RELIABLE PERFORMANCE", "description": rec.reliable_performance or "Featuring a simple design, you can easily install this part."},
            {"title": "DURABLE CONSTRUCTION", "description": rec.durable_construction or "Built with robust materials, it offers excellent resistance to wear."},
            {"title": "PERFECT FIT", "description": rec.perfect_fit or "Tailored to fit your specific model, this part integrates seamlessly."}
        ]
    }

    out_dir = os.path.join(base_dir, "..", "static", "generated_images")
    os.makedirs(out_dir, exist_ok=True)

    out_filename = f"repair_{rec.id}_{int(time.time())}.png"
    out_path = os.path.join(out_dir, out_filename)

    app_root = os.path.abspath(os.path.join(base_dir, ".."))
    template_path = os.path.join(app_root, "template.png")
    if not os.path.exists(template_path):
        template_path = os.path.join(os.getcwd(), "template.png")

    from PIL import Image, ImageDraw, ImageFont
    import io

    # Generate poster using Pillow layout from test.py
    base = Image.open(template_path).convert("RGB")
    draw = ImageDraw.Draw(base)
    W, H = base.size

    # Fonts setup with safe fallback checking
    def get_valid_fnt(preferred_path, fallback_path, size):
        if preferred_path and os.path.exists(preferred_path):
            try:
                return ImageFont.truetype(preferred_path, size)
            except Exception:
                pass
        if fallback_path and os.path.exists(fallback_path):
            try:
                return ImageFont.truetype(fallback_path, size)
            except Exception:
                pass
        return ImageFont.load_default()

    font_bold_path = "/home/tis-lap-22/.local/share/fonts/montserrat/Montserrat-Bold.ttf"
    fallback_bold_path = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
    font_reg_path = "/home/tis-lap-22/.local/share/fonts/montserrat/Montserrat-Regular.ttf"
    fallback_reg_path = "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"

    def fnt_bold(s): return get_valid_fnt(font_bold_path, fallback_bold_path, s)
    def fnt_reg(s): return get_valid_fnt(font_reg_path, fallback_reg_path, s)

    def draw_ct(d, cx, y, txt, f, fill): d.text((cx, y), txt, font=f, fill=fill, anchor="mm")
    def draw_wt(d, box, txt, f, fill, line_spacing=8, align="center"):
        x0, y0, x1, y1 = box
        mw = x1 - x0
        words = str(txt).split()
        lines, current = [], ""
        for w in words:
            trial = (current + " " + w).strip()
            bbox = d.textbbox((0, 0), trial, font=f)
            if bbox[2] - bbox[0] <= mw or not current:
                current = trial
            else:
                lines.append(current)
                current = w
        if current: lines.append(current)
        y = y0
        for line in lines:
            bbox = d.textbbox((0, 0), line, font=f)
            w, h = bbox[2] - bbox[0], bbox[3] - bbox[1]
            x = x0 + (mw - w) / 2 if align == "center" else x0
            d.text((x, y), line, font=f, fill=fill)
            y += h + line_spacing
        return y

    # Draw Text Elements
    p_name = product_data["product_name"]
    p_num = product_data["part_number"]
    subtitle = f"{p_name} ({p_num})" if p_num else p_name

    draw_ct(draw, W / 2, 82, "Repair Information", fnt_bold(68), (2, 55, 150))

    # Badge
    b_font = fnt_bold(26)
    bbox = draw.textbbox((0, 0), subtitle, font=b_font)
    tw = bbox[2] - bbox[0]
    bw, bh = max(tw + 80, 200), 56
    bx0, by0 = (W - bw) / 2, 165
    draw.rounded_rectangle((bx0, by0, bx0 + bw, by0 + bh), radius=28, fill=(3, 30, 110))
    draw_ct(draw, W / 2, by0 + bh / 2, subtitle, b_font, (255, 255, 255))

    # Cards
    h_font, p_font, b_font_reg = fnt_bold(21), fnt_bold(34), fnt_reg(18)
    big_f, unit_f = fnt_bold(62), fnt_bold(30)

    # Top Left
    draw_ct(draw, 168, 412, "REPAIR DIFFICULTY", h_font, (0, 43, 148))
    draw.rounded_rectangle((83, 464, 253, 521), radius=16, fill=(33, 160, 55))
    draw_ct(draw, 168, 492, str(product_data["repair_difficulty"]).upper(), p_font, (255, 255, 255))
    draw_wt(draw, (48, 545, 288, 650), "Designed for Best Fit: Engineered for proper alignment and seamless integration.", b_font_reg, (70, 82, 130), line_spacing=5)

    # Bottom Left
    draw_ct(draw, 168, 820, "DIY FRIENDLY", h_font, (0, 43, 148))
    draw.rounded_rectangle((83, 872, 240, 928), radius=16, fill=(33, 160, 55))
    draw_ct(draw, 168, 900, "YES", p_font, (255, 255, 255))
    draw_wt(draw, (48, 950, 288, 1050), "Easy DIY Installation: Features a straightforward replacement process for quick restoration.", b_font_reg, (70, 82, 130), line_spacing=5)

    # Top Right
    est_time = str(product_data["estimated_time"]).strip()
    time_big = est_time.split(" ", 1)[0] if " " in est_time else est_time
    time_unit = est_time.split(" ", 1)[1].upper() if " " in est_time else "MINUTES"
    draw_ct(draw, 1063, 412, "ESTIMATED REPAIR TIME", h_font, (0, 43, 148))
    draw_ct(draw, 1063, 495, time_big, big_f, (0, 43, 148))
    draw_ct(draw, 1063, 553, time_unit, unit_f, (0, 43, 148))
    draw_wt(draw, (943, 590, 1183, 650), "Approximate time for most installations.", b_font_reg, (70, 82, 130), line_spacing=5)

    # Bottom Right
    draw_ct(draw, 1063, 817, "SAFETY TIP", h_font, (0, 43, 148))
    draw_wt(draw, (943, 862, 1183, 1050), product_data["safety_tip"], b_font_reg, (70, 82, 130), line_spacing=5)

    # Center Circle Fill (Solid White) & Product Paste
    cx, cy, radius = 617, 580, 280
    draw.ellipse((cx - radius + 8, cy - radius + 8, cx + radius - 8, cy + radius - 8), fill=(255, 255, 255))

    if product_data["product_image"]:
        try:
            resp = requests.get(product_data["product_image"], headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
            if resp.status_code == 200:
                p_img = Image.open(io.BytesIO(resp.content)).convert("RGBA")
                try:
                    import rembg
                    p_img = rembg.remove(p_img)
                except Exception:
                    pass

                if p_img.mode != 'RGBA':
                    p_img = p_img.convert('RGBA')

                # Make near-white pixels transparent so no square box artifacts remain
                datas = p_img.getdata()
                newData = []
                for item in datas:
                    r, g, b = item[0], item[1], item[2]
                    a = item[3] if len(item) == 4 else 255
                    if r > 240 and g > 240 and b > 240:
                        newData.append((255, 255, 255, 0))
                    else:
                        newData.append((r, g, b, a))
                p_img.putdata(newData)

                bbox = p_img.getbbox()
                if bbox:
                    p_img = p_img.crop(bbox)

                rad = radius - 30
                w, h = p_img.size
                if w > 0 and h > 0:
                    import math
                    scale = (2 * rad) / math.sqrt(w * w + h * h)
                    nw, nh = int(w * scale), int(h * scale)
                    p_img_resized = p_img.resize((nw, nh), Image.LANCZOS)
                    px, py = int(cx - nw / 2), int(cy - nh / 2)
                    base.paste(p_img_resized, (px, py), p_img_resized)
        except Exception as e:
            logger.warning("[GENERATE-POSTER] Center product paste failed: %s", e)

    draw = ImageDraw.Draw(base)

    # Bottom bar features
    bar_h_font, bar_b_font = fnt_bold(13), fnt_reg(13)
    sections_x = [(146, 325), (435, 645), (751, 945), (1060, 1228)]
    feats = product_data["features"]
    for (tx0, tx1), item in zip(sections_x, feats[:4]):
        hdr = item.get("title", "")
        desc = item.get("description", "")
        draw.text((tx0, 1095), hdr, font=bar_h_font, fill=(255, 255, 255))
        hb = draw.textbbox((0, 0), hdr, font=bar_h_font)
        hh = hb[3] - hb[1]
        draw_wt(draw, (tx0, 1095 + hh + 9, tx1, 1235), desc, bar_b_font, (215, 225, 245), align="left", line_spacing=3)

    base = base.resize((1024, 1024), Image.LANCZOS)
    base.save(out_path)

    rel_path = f"/static/generated_images/{out_filename}"
    base_url = (os.getenv("BASE_URL") or BASE_URL or "").strip().rstrip('/')
    if base_url:
        full_url = f"{base_url}{rel_path}" if not rel_path.startswith(base_url) else rel_path
    else:
        full_url = rel_path

    rec.generated_image_url = full_url
    if rec.review_status == 'pending':
        rec.review_status = 'reviewed'
    db.session.commit()

    return full_url


def _publish_repair_image_to_shopify(rec: RepairImageGeneration):
    import base64
    effective_url = rec.shopify_url or ''
    part_number = rec.part_number or ''

    gid = None
    if effective_url:
        try:
            gid = _shopify_product_gid_from_product_url(effective_url)
        except Exception as e:
            logger.warning("[PUBLISH-REPAIR] GID from URL failed: %s", e)
    if not gid and part_number:
        try:
            gid = _shopify_product_gid_from_sku(part_number)
        except Exception as e:
            logger.warning("[PUBLISH-REPAIR] GID from SKU failed: %s", e)

    if not gid:
        raise ValueError("Could not resolve Shopify product. Make sure the Shop URL or Part Number matches a product in your active shop.")

    numeric_id = gid.split('/')[-1]

    shop = None
    if getattr(rec, 'shop_id', None):
        shop = ShopConfig.query.get(rec.shop_id)
    if not shop:
        shop = _get_active_shop()
    if not shop:
        raise ValueError("No active Shopify shop configured")

    domain = shop.domain.strip()
    token = shop.access_token.strip()
    api_version = (shop.api_version or '2024-01').strip()

    endpoint = f"{_shopify_base_url(domain)}/admin/api/{api_version}/products/{numeric_id}/images.json"
    headers = {
        'Content-Type': 'application/json',
        'X-Shopify-Access-Token': token,
    }

    img_url = rec.generated_image_url or rec.product_image_url
    if not img_url:
        raise ValueError("No generated image or product image available to publish.")

    base_dir = os.path.dirname(os.path.abspath(__file__))
    if '/static/' in img_url:
        static_rel = img_url[img_url.find('/static/'):].lstrip('/')
        local_path = os.path.join(base_dir, "..", static_rel)
        if not os.path.exists(local_path):
            local_path = static_rel
        if os.path.exists(local_path):
            with open(local_path, "rb") as f:
                b64_str = base64.b64encode(f.read()).decode('utf-8')
            payload = {'image': {'attachment': b64_str}}
        else:
            payload = {'image': {'src': img_url}}
    elif img_url.startswith('http'):
        payload = {'image': {'src': img_url}}
    else:
        with open(img_url, "rb") as f:
            b64_str = base64.b64encode(f.read()).decode('utf-8')
        payload = {'image': {'attachment': b64_str}}

    resp = requests.post(endpoint, json=payload, headers=headers, timeout=25)
    if not resp.ok:
        raise ValueError(f"Shopify returned HTTP {resp.status_code}: {resp.text[:300]}")

    created_img = resp.json().get('image', {})
    rec.review_status = 'published'
    db.session.commit()
    return created_img.get('src', img_url)