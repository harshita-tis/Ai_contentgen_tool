"""
image_search/app.py — Web scraping pipeline that finds candidate product
images, plus all image-search review/publish/history routes.

Importing this module registers its routes on the shared Flask `app`.
"""
from shared import (
    app, db, login_required, _get_active_shop, _shopify_base_url, _utc_now,
    ShopConfig, ImageSearchGeneration,AppSetting,
    _GQL_PRODUCT, _GQL_PRODUCT_BY_SKU, _GQL_PRODUCT_UPDATE, _GQL_METAFIELDS_SET, _GQL_PRODUCT_BY_HANDLE,
    _extract_admin_product_id, _is_storefront_url, _shopify_graphql_with_creds,
    _fetch_by_product_id, _fetch_by_storefront_url, _fetch_by_handle_admin,
    _webcate_from_tags, _subcate_from_tags, _parse_gql_product, _parse_rest_product,
    _fetch_shopify_product, fetch_shopify_image_by_product_url, _extract_products_path_segment,API_KEY
)
from flask import render_template, request, jsonify, session
from datetime import datetime, timezone
import requests, os, json, uuid, threading, re, logging, time, hashlib, asyncio
from urllib.parse import urlparse
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
import openai
from flask import render_template
from shared import BASE_URL

logger = logging.getLogger(__name__)


DEFAULT_IMAGE_SEARCH_PROMPT = """Find product listing pages for this appliance replacement part:
Brand: {brand}
Part Number: {part_number}
Title: {title}

Search these queries:
"{part_number}"
"{brand} {part_number}"
"site:appliancepartspros.com {part_number}"
"site:repairclinic.com {part_number}"
"site:partselect.com {part_number}"
"site:encompass.com {part_number}"

Return ONLY a JSON array of direct product page URLs — NOT image URLs, NOT category pages.
Example: ["https://www.repairclinic.com/PartDetail/1234567"]
Return at most 8 URLs."""


def _is_cpanel_like_env() -> bool:
    """
    Heuristic: shared hosting / cPanel commonly runs under Passenger and enforces
    strict CPU/memory/runtime limits. We default to conservative concurrency there.
    """
    return bool(
        os.getenv("CPANEL")
        or os.getenv("PASSENGER_APP_ENV")
        or os.getenv("PASSENGER_BASE_URI")
        or os.getenv("DOCUMENT_ROOT")
    )



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
                # Shared hosting is sensitive to high concurrency and long-running
                # sockets. Keep pools small and set explicit timeouts.
                default_max_conn = 5 if _is_cpanel_like_env() else 20
                default_keepalive = 2 if _is_cpanel_like_env() else 10

                max_conn = int(os.getenv("OPENAI_MAX_CONNECTIONS", str(default_max_conn)))
                max_keepalive = int(os.getenv("OPENAI_MAX_KEEPALIVE_CONNECTIONS", str(default_keepalive)))
                timeout_s = float(os.getenv("OPENAI_HTTP_TIMEOUT", "90"))
                _openai_client = openai.OpenAI(
                    api_key=API_KEY,
                    http_client=httpx.Client(limits=httpx.Limits(
                        max_connections=max_conn,
                        max_keepalive_connections=max_keepalive,
                    ), timeout=httpx.Timeout(timeout_s)),
                    max_retries=0,
                )
    return _openai_client

# ──────────────────────────────────────────────────────────────
# SECTION A — IMAGE FILTERING HELPERS
# ──────────────────────────────────────────────────────────────

_BLOCK_PATTERNS = [
    r'\.gif($|\?)',
    r'logo', r'banner', r'sprite', r'icon', r'badge',
    r'placeholder', r'blank\.', r'empty\.',
    r'no.?image', r'image.?not.?available', r'noimage', r'no_image',
    r'pd-no-image', r'image_coming_soon', r'coming.?soon',
    r'Group_\d+', r'rotating', r'manufacturerLogo',
    r'/sharedImages/', r'social', r'payment', r'shipping',
    r'star[_\-]', r'rating', r'cart', r'checkout',
    r'header', r'footer', r'zen\.png',
    r'arrow', r'chevron', r'background',
    r'trustpilot', r'captcha', r'recaptcha',
    # payment / wallet icons
    r'applepay', r'apple.?pay', r'gpay', r'google.?pay',
    r'paypal', r'venmo', r'klarna', r'afterpay', r'affirm',
    r'visa', r'mastercard', r'amex', r'discover',
    r'credit.?card', r'debit.?card', r'wallet',
    r'\.svg($|\?)',
]
_BLOCK_RE = re.compile("|".join(_BLOCK_PATTERNS), re.IGNORECASE)

_SKIP_ALT_CLASS = [
    "logo", "icon", "banner", "badge", "sprite", "avatar",
    "flag", "arrow", "star", "rating", "cart", "payment",
    "social", "background", "header", "footer",
    "applepay", "gpay", "paypal", "visa", "mastercard",
    "no image", "not available", "coming soon",
]


def _is_blocked(url, alt="", cls=""):
    if _BLOCK_RE.search(url):
        return True
    combined = (alt + " " + cls).lower()
    return any(kw in combined for kw in _SKIP_ALT_CLASS)


def _score_image(url, part_number, alt="", width=None, height=None):
    """Higher score = more likely to be the actual product photo."""
    score     = 0
    url_lower = url.lower()
    part_lower = part_number.lower()
    alt_lower  = (alt or "").lower()

    if part_lower in url_lower:   score += 100   # part number in filename
    if part_lower in alt_lower:   score += 60    # part number in alt text

    for pat in [r'/product', r'/item', r'/sku', r'/part', r'/p/',
                r'main.?image', r'primary', r'hero', r'/files/',
                r'/media/', r'/images?/', r'_full', r'_large',
                r'_main', r'_zoom']:
        if re.search(pat, url_lower):
            score += 10
            break

    if re.search(r'cdn\.|\.kxcdn\.com|cloudfront|akamai|fastly', url_lower):
        score += 5

    if re.search(r'\.(jpg|jpeg|webp)($|\?)', url_lower):
        score += 8
    elif re.search(r'\.png($|\?)', url_lower):
        score += 3

    if width and height:
        try:
            w = int(str(width).replace("px", ""))
            h = int(str(height).replace("px", ""))
            if   w >= 400 and h >= 400: score += 20
            elif w >= 200 and h >= 200: score += 10
            elif w < 80  or  h < 80:   score -= 25
        except (ValueError, TypeError):
            pass

    return score


def _deduplicate(items):
    seen, result = set(), []
    for item in items:
        key = hashlib.md5(item["url"].encode()).hexdigest()
        if key not in seen:
            seen.add(key)
            result.append(item)
    return result


# ──────────────────────────────────────────────────────────────
# SECTION B — PAGE FETCHER (requests → Playwright fallback)
# ──────────────────────────────────────────────────────────────

_BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0 Safari/537.36"
)


async def _fetch_html(url: str, browser=None) -> tuple[str, str]:
    """
    Returns (html, method).
    Tries requests first (timeout=8s); if page looks JS-rendered falls back to Playwright.
    Accepts an optional pre-launched browser to avoid repeated browser.launch() overhead.
    """
    html = ""
    try:
        r = requests.get(url, headers={"User-Agent": _BROWSER_UA}, timeout=8)
        html = r.text
    except Exception as e:
        logger.warning(f"requests failed for {url}: {e}")

    soup_check = BeautifulSoup(html, "html.parser")
    if len(html) < 5000 or len(soup_check.find_all("img")) == 0:
        logger.info(f"Playwright fallback for {url}")
        try:
            _own_browser = False
            if browser is None:
                p_inst = await async_playwright().__aenter__()
                browser = await p_inst.chromium.launch(headless=True)
                _own_browser = True
            page = await browser.new_page()
            try:
                # domcontentloaded is much faster than networkidle; sufficient for static images
                await page.goto(url, wait_until="domcontentloaded", timeout=12000)
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight / 2)")
                await page.wait_for_timeout(800)
                html = await page.content()
            finally:
                await page.close()
                if _own_browser:
                    await browser.close()
            return html, "playwright"
        except Exception as e:
            logger.warning(f"Playwright failed for {url}: {e}")
            return html, "requests_partial"

    return html, "requests"


# ──────────────────────────────────────────────────────────────
# SECTION C — SCRAPE + SCORE IMAGES FROM ONE PAGE
# ──────────────────────────────────────────────────────────────

def _extract_images_from_html(
    html: str,
    page_url: str,
    part_number: str,
    brand: str,
    title: str,
) -> list[dict]:
    """
    Returns [{url, score, source, title, domain}] sorted by score desc.
    Works on any website — no site-specific logic.
    """
    results    = []
    soup       = BeautifulSoup(html, "html.parser")
    part_lower = part_number.lower()
    brand_lower= brand.lower()
    title_lower= (title or "").lower()
    domain     = urlparse(page_url).netloc.replace("www.", "")

    # Real <title> tag of the page each image was found on — this is what the
    # frontend shows next to the page URL, distinct from the user-supplied
    # search title which is the same for every page in the batch.
    page_title = ""
    if soup.title and soup.title.string:
        page_title = re.sub(r'\s+', ' ', soup.title.string).strip()
    if not page_title:
        og_title_tag = soup.find("meta", property="og:title")
        if og_title_tag and og_title_tag.get("content"):
            page_title = og_title_tag["content"].strip()

    # ── Pass 1: OG / Twitter meta tags ──────────────────────────────────
    for prop in ["og:image", "twitter:image"]:
        tag = soup.find("meta", property=prop) or soup.find("meta", attrs={"name": prop})
        if tag and tag.get("content"):
            url = tag["content"]
            if url.startswith("//"):
                url = "https:" + url
            if url.startswith("http") and not _is_blocked(url):
                s = _score_image(url, part_number) + 30
                results.append({"url": url, "score": s,
                                 "source": domain, "title": title, "domain": domain,
                                 "page_url": page_url, "page_title": page_title})

    # ── Pass 2: JSON-LD structured data ─────────────────────────────────
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data  = json.loads(script.string or "")
            nodes = data if isinstance(data, list) else [data]
            for node in nodes:
                for subnode in node.get("@graph", [node]):
                    img = subnode.get("image")
                    candidates = []
                    if isinstance(img, str):
                        candidates = [img]
                    elif isinstance(img, list):
                        candidates = [i if isinstance(i, str) else i.get("url", "") for i in img]
                    elif isinstance(img, dict):
                        candidates = [img.get("url", "")]
                    for u in candidates:
                        if u and u.startswith("http") and not _is_blocked(u):
                            s = _score_image(u, part_number) + 25
                            results.append({"url": u, "score": s,
                                             "source": domain, "title": title, "domain": domain,
                                             "page_url": page_url, "page_title": page_title})
        except Exception:
            pass

    # ── Pass 3: Every <img> tag with proximity scoring ───────────────────
    for img_tag in soup.find_all("img"):
        src = (
            img_tag.get("src") or
            img_tag.get("data-src") or
            img_tag.get("data-lazy-src") or
            img_tag.get("data-original") or
            img_tag.get("data-zoom-image") or
            img_tag.get("data-full-size-url")
        )
        if not src:
            continue

        # Normalise URL
        if src.startswith("//"):
            src = "https:" + src
        elif src.startswith("/"):
            parsed = urlparse(page_url)
            src = f"{parsed.scheme}://{parsed.netloc}{src}"
        if not src.startswith("http"):
            continue

        alt    = img_tag.get("alt") or ""
        cls    = " ".join(img_tag.get("class") or [])
        width  = img_tag.get("width")
        height = img_tag.get("height")

        if _is_blocked(src, alt, cls):
            continue

        # Walk up to 5 parent levels — score how close part number / title is
        context_score = 0
        node = img_tag.parent
        for depth in range(5):
            if node is None:
                break
            node_text = node.get_text(separator=" ", strip=True).lower()

            if part_lower in node_text:
                context_score += max(50 - depth * 10, 10)
                break
            # title words match (at least 3 chars each)
            title_words = [w for w in title_lower.split() if len(w) >= 3]
            if title_words and sum(1 for w in title_words if w in node_text) >= min(2, len(title_words)):
                context_score += max(30 - depth * 8, 8)
                break
            if brand_lower in node_text and part_lower[:6] in node_text:
                context_score += max(25 - depth * 6, 6)
                break

            node = getattr(node, "parent", None)

        base  = _score_image(src, part_number, alt, width, height)
        total = base + context_score

        if total < 5:
            continue

        results.append({"url": src, "score": total,
                         "source": domain, "title": title, "domain": domain,
                         "page_url": page_url, "page_title": page_title})

    results.sort(key=lambda x: x["score"], reverse=True)
    return _deduplicate(results)


# ──────────────────────────────────────────────────────────────
# SECTION D — OPENAI WEB SEARCH → PRODUCT PAGE URLs
# ──────────────────────────────────────────────────────────────

def _find_product_pages(
    part_number: str,
    brand: str,
    title: str,
    part_type: str,
    appliance_type: str,
    broad: bool = False,
    exclude_domains: list[str] | None = None,
) -> tuple[list[str], int, int]:
    """
    Asks OpenAI (web_search_preview) to find product PAGES for this part.

    broad=False (default) — restricts to the known parts-retailer sites
        (appliancepartspros.com, repairclinic.com, partselect.com, encompass.com).
    broad=True — "Search More" mode. Drops the site: restriction entirely and
        searches the open web for ANY page selling/showing this part — used when
        the pages we already scraped didn't have usable images. exclude_domains
        lets us skip domains we already tried (e.g. the competitor page that had
        no images), so we don't just find the same dead end again.

    Returns (page_urls, input_tokens, output_tokens).
    """
    client = get_openai_client()

    query = " ".join(filter(None, [brand, part_number, title, part_type, appliance_type]))
    exclude_domains = [d.strip().lower() for d in (exclude_domains or []) if d and d.strip()]

    if broad:
        exclude_line = (
            f"Do NOT return pages from these domains, we already checked them and they had no usable images: "
            f"{', '.join(exclude_domains)}.\n"
            if exclude_domains else ""
        )
        prompt = (
            f"Find product listing / product detail pages ANYWHERE on the internet for this appliance "
            f"replacement part — retailer sites, manufacturer sites, marketplaces, distributor sites, "
            f"forums with product photos, anywhere it might be sold or pictured. Do not limit the search "
            f"to any specific site — search broadly across the whole web.\n"
            f"Brand: {brand}\n"
            f"Part Number: {part_number}\n"
            f"Title: {title}\n\n"
            f"Search queries to try:\n"
            f'"{part_number}"\n'
            f'"{brand} {part_number}"\n'
            f'"{part_number}" buy OR shop OR replacement part\n\n'
            f"{exclude_line}"
            f"Return ONLY a JSON array of direct product page URLs — NOT image URLs, NOT category/search pages.\n"
            f'Example: ["https://www.example.com/product/1234567"]\n'
            f"Return at most 8 URLs."
        )
    else:
        prompt = (
            f"Find product listing pages for this appliance replacement part:\n"
            f"Brand: {brand}\n"
            f"Part Number: {part_number}\n"
            f"Title: {title}\n\n"
            f"Search these queries:\n"
            f'"{part_number}"\n'
            f'"{brand} {part_number}"\n'
            f'"site:appliancepartspros.com {part_number}"\n'
            f'"site:repairclinic.com {part_number}"\n'
            f'"site:partselect.com {part_number}"\n'
            f'"site:encompass.com {part_number}"\n\n'
            f"Return ONLY a JSON array of direct product page URLs — NOT image URLs, NOT category pages.\n"
            f'Example: ["https://www.repairclinic.com/PartDetail/1234567"]\n'
            f"Return at most 8 URLs."
        )

    resp = client.responses.create(
        model="gpt-4o",
        tools=[{"type": "web_search_preview"}],
        input=[{"role": "user", "content": prompt}],
    )

    usage        = getattr(resp, "usage", None)
    in_tok       = getattr(usage, "input_tokens",  0) if usage else 0
    out_tok      = getattr(usage, "output_tokens", 0) if usage else 0

    raw_text = ""
    for item in (resp.output or []):
        if getattr(item, "type", "") == "message":
            for block in (getattr(item, "content", None) or []):
                if getattr(block, "type", "") == "output_text":
                    raw_text += getattr(block, "text", "")

    urls = []
    try:
        match = re.search(r'\[.*?\]', raw_text, re.DOTALL)
        if match:
            urls = json.loads(match.group())
            urls = [u for u in urls if isinstance(u, str) and u.startswith("http")]
    except Exception as e:
        logger.warning(f"Could not parse product page URLs: {e}\nRaw: {raw_text[:300]}")

    if exclude_domains:
        urls = [u for u in urls if urlparse(u).netloc.replace("www.", "").lower() not in exclude_domains]

    logger.info(f"Found {len(urls)} product page(s) for {part_number} (broad={broad}): {urls}")
    return urls, in_tok, out_tok


# ──────────────────────────────────────────────────────────────
# SECTION E — MAIN ASYNC PIPELINE
# ──────────────────────────────────────────────────────────────

async def _scrape_images_async(
    part_number: str,
    brand: str,
    title: str,
    part_type: str,
    appliance_type: str,
    top_n: int = 10,
    broad: bool = False,
    exclude_domains: list[str] | None = None,
) -> tuple[list[dict], list[str], int, int]:
    """
    Full pipeline:
      1. OpenAI finds product page URLs (broad=True → whole web, skipping exclude_domains)
      2. Each page is fetched in parallel (requests → Playwright fallback, shared browser)
      3. Images are extracted and scored by proximity to part number / title
      4. Top N validated image dicts are returned (HEAD checks run concurrently)

    Returns:
      images       — list of {url, title, source, page_url, page_title} dicts
      page_urls    — product pages that were scraped      (for audit log)
      input_tokens
      output_tokens
    """
    # Step 1 — find product pages
    page_urls, in_tok, out_tok = _find_product_pages(
        part_number, brand, title, part_type, appliance_type,
        broad=broad, exclude_domains=exclude_domains,
    )

    if not page_urls:
        logger.warning(f"No product pages found for part {part_number}")
        return [], [], in_tok, out_tok

    # Step 2 — scrape all pages in parallel, sharing one Playwright browser
    all_candidates: list[dict] = []

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        try:
            async def _fetch_and_extract(page_url):
                try:
                    html, method = await _fetch_html(page_url, browser=browser)
                    candidates   = _extract_images_from_html(html, page_url, part_number, brand, title)
                    logger.info(f"[{method}] {page_url} → {len(candidates)} candidate(s)")
                    return candidates
                except Exception as e:
                    logger.warning(f"Failed to scrape {page_url}: {e}")
                    return []

            results = await asyncio.gather(*[_fetch_and_extract(u) for u in page_urls[:8]])
            for cands in results:
                all_candidates.extend(cands)
        finally:
            await browser.close()

    # Step 3 — global re-rank + deduplicate
    all_candidates.sort(key=lambda x: x["score"], reverse=True)
    all_candidates = _deduplicate(all_candidates)

    # Step 4 — validate (HEAD request) concurrently and pick top N
    # Pre-filter with blocklist before making any network calls
    candidates_to_check = [c for c in all_candidates if not _is_blocked(c["url"])]

    async def _validate(c):
        """Returns the image dict if valid, else None."""
        try:
            loop = asyncio.get_event_loop()
            r = await loop.run_in_executor(
                None,
                lambda: requests.head(c["url"], timeout=4, allow_redirects=True)
            )
            final_url = r.url if hasattr(r, 'url') else c["url"]
            if _is_blocked(final_url):
                logger.debug(f"✗ blocked (redirect)  {final_url}")
                return None
            ct = r.headers.get("content-type", "")
            if "image" in ct.lower():
                logger.info(f"✅ score={c['score']}  {c['url']}")
                return {
                    "url":        c["url"],
                    "title":      c.get("title") or title,
                    "source":     c.get("source") or "",
                    "page_url":   c.get("page_url") or "",
                    "page_title": c.get("page_title") or "",
                }
            else:
                logger.debug(f"✗ not image ({ct})  {c['url']}")
                return None
        except Exception:
            logger.debug(f"✗ unreachable  {c['url']}")
            return None

    # Run HEAD checks concurrently — check up to top_n * 2 candidates to fill top_n slots
    check_limit = min(len(candidates_to_check), top_n * 2)
    validated = await asyncio.gather(*[_validate(c) for c in candidates_to_check[:check_limit]])

    final = [v for v in validated if v is not None][:top_n]

    logger.info(f"Final images for {part_number}: {len(final)}")
    return final, page_urls, in_tok, out_tok


# cPanel/shared-hosting note: each scrape run launches a full headless Chromium
# process, which is the single heaviest thing this app does (RAM + process count).
# This semaphore caps how many scrape runs (and therefore browsers) can be in
# flight across ALL requests at once. Tune via MAX_CONCURRENT_BROWSERS env var —
# keep this at 1 on small/shared cPanel plans.
MAX_CONCURRENT_BROWSERS = int(os.getenv('MAX_CONCURRENT_BROWSERS', '1'))
_browser_slot = threading.Semaphore(MAX_CONCURRENT_BROWSERS)


def _run_scrape_pipeline(
    part_number: str,
    brand: str,
    title: str,
    part_type: str,
    appliance_type: str,
    top_n: int = 10,
    broad: bool = False,
    exclude_domains: list[str] | None = None,
) -> tuple[list[dict], list[str], int, int]:
    """Sync wrapper — safe to call from a Flask route.

    Blocks (without pegging CPU) until a browser "slot" is free, so we never
    have more than MAX_CONCURRENT_BROWSERS Chromium processes running at once
    regardless of how many requests come in simultaneously.
    """
    with _browser_slot:
        return asyncio.run(
            _scrape_images_async(
                part_number, brand, title, part_type, appliance_type, top_n,
                broad=broad, exclude_domains=exclude_domains,
            )
        )


# ──────────────────────────────────────────────────────────────

# ──────────────────────────────────────────────────────────────

@app.route('/api/image-search/search', methods=['POST'])
@login_required
def image_search_endpoint():
    body           = request.json or {}
    logger.info(
        "image-search request | title=%r part_number=%r brand=%r part_type=%r appliance_type=%r "
        "source=%r batch_id=%r shopify_urls=%r",
        body.get('title'),
        body.get('part_number'),
        body.get('brand'),
        body.get('part_type'),
        body.get('appliance_type'),
        body.get('source'),
        body.get('batch_id'),
        body.get('shopify_urls'),
    )

    title          = (body.get('title')          or '').strip()
    part_number    = (body.get('part_number')    or '').strip()
    brand          = (body.get('brand')          or '').strip()
    part_type      = (body.get('part_type')      or '').strip()
    appliance_type = (body.get('appliance_type') or '').strip()
    # Accept either a list of URLs or a single legacy string
    shopify_urls_raw = body.get('shopify_urls') or []
    if isinstance(shopify_urls_raw, str):
        shopify_urls_raw = [u.strip() for u in shopify_urls_raw.split(',') if u.strip()]
    shopify_urls   = [u.strip() for u in shopify_urls_raw if (u or '').strip()]
    # Store as JSON array; keep legacy shopify_url column pointing at first entry
    shopify_url    = shopify_urls[0] if shopify_urls else ''
    batch_id       = (body.get('batch_id')       or '').strip() or str(uuid.uuid4())
    source         = (body.get('source')         or 'manual').strip().lower()
    if source not in ('manual', 'csv'):
        source = 'manual'

    if not part_number:
        return jsonify({'error': 'part_number is required'}), 400

    try:
        images, page_urls, in_tok, out_tok = _run_scrape_pipeline(
            part_number    = part_number,
            brand          = brand,
            title          = title,
            part_type      = part_type,
            appliance_type = appliance_type,
            top_n          = 10,
        )
    except Exception as e:
        logger.error(f"Scrape pipeline error: {e}")
        return jsonify({'error': str(e)}), 500

    logger.info(
        "image-search pipeline result | batch_id=%s part_number=%s title=%r query=%r page_count=%d image_count=%d "
        "pages=%s image_preview=%s tokens_in=%s tokens_out=%s",
        batch_id,
        part_number,
        title,
        " ".join(filter(None, [brand, title, part_number, part_type, appliance_type])),
        len(page_urls),
        len(images),
        page_urls,
        [
            {
                'url': (img or {}).get('url', ''),
                'source': (img or {}).get('source', ''),
                'title': (img or {}).get('title', ''),
                'domain': (img or {}).get('domain', ''),
                'page_url': (img or {}).get('page_url', ''),
                'page_title': (img or {}).get('page_title', ''),
                'score': (img or {}).get('score'),
            }
            for img in images[:5]
        ],
        in_tok,
        out_tok,
    )

    query_used = " ".join(filter(None, [brand, title, part_number, part_type, appliance_type]))

    gen = ImageSearchGeneration(
        batch_id       = batch_id,
        source         = source,
        shop_id        = None,   # no longer tied to a single shop
        product_title  = title,
        part_number    = part_number,
        brand          = brand,
        part_type      = part_type,
        appliance_type = appliance_type,
        shopify_url    = json.dumps(shopify_urls) if shopify_urls else None,  # JSON list
        prompt_used    = json.dumps(page_urls),   # audit: pages that were scraped
        query_used     = query_used,
        image_urls     = json.dumps(images),
        selected_urls  = json.dumps([]),
        input_tokens   = in_tok,
        output_tokens  = out_tok,
    )
    db.session.add(gen)
    db.session.commit()

    logger.info(
        "image-search saved generation | generation_id=%s batch_id=%s part_number=%s title=%r review_status=%s "
        "shopify_urls=%s selected_urls=%s image_count=%d db_image_preview=%s",
        gen.generation_id,
        gen.batch_id,
        gen.part_number,
        gen.product_title,
        gen.review_status,
        shopify_urls,
        [],
        len(images),
        [
            {
                'url': (img or {}).get('url', ''),
                'source': (img or {}).get('source', ''),
                'title': (img or {}).get('title', ''),
                'page_url': (img or {}).get('page_url', ''),
                'page_title': (img or {}).get('page_title', ''),
            }
            for img in images[:5]
        ],
    )

    return jsonify({
        'success':       True,
        'generation_id': gen.generation_id,
        'batch_id':      gen.batch_id,
        'source':        gen.source,
        'query_used':    query_used,
        'images':        images,
        'total':         len(images),
        'input_tokens':  in_tok,
        'output_tokens': out_tok,
        'shopify_urls':  shopify_urls,
    })


# ──────────────────────────────────────────────────────────────
# SECTION G — REMAINING IMAGE SEARCH ROUTES
# ──────────────────────────────────────────────────────────────

@app.route('/api/image-search/save-selection', methods=['POST'])
@login_required
def save_image_selection():
    body          = request.json or {}
    generation_id = (body.get('generation_id') or '').strip()
    selected_urls = body.get('selected_urls') or []

    if not generation_id:
        return jsonify({'error': 'generation_id is required'}), 400

    gen = ImageSearchGeneration.query.filter_by(generation_id=generation_id).first()
    if not gen:
        return jsonify({'error': 'Generation not found'}), 404

    gen.selected_urls = json.dumps(selected_urls)
    db.session.commit()
    return jsonify({'success': True})


@app.route('/api/image-search/search-more', methods=['POST'])
@login_required
def image_search_more():
    """
    "Search More" — used from the review page when the pages we already
    scraped (usually a specific competitor product page) turned out to have
    no usable images. Re-runs the pipeline in broad mode: no site: restriction,
    searches the open web, and skips domains we already tried for this
    generation so we don't just land on the same dead page again.

    Body: { generation_id }
    Merges newly found images into the existing generation and returns the
    full (deduplicated) image list.
    """
    body          = request.json or {}
    generation_id = (body.get('generation_id') or '').strip()

    if not generation_id:
        return jsonify({'error': 'generation_id is required'}), 400

    gen = ImageSearchGeneration.query.filter_by(generation_id=generation_id).first()
    if not gen:
        return jsonify({'error': 'Generation not found'}), 404

    try:
        existing_images = json.loads(gen.image_urls) if gen.image_urls else []
    except Exception:
        existing_images = []
    try:
        already_scraped_pages = json.loads(gen.prompt_used) if gen.prompt_used else []
    except Exception:
        already_scraped_pages = []

    # Domains we've already tried and got nothing useful from — includes both
    # the pages the normal pipeline scraped and the original competitor URL(s)
    # this generation started from.
    exclude_domains = set()
    for u in already_scraped_pages:
        try:
            exclude_domains.add(urlparse(u).netloc.replace("www.", "").lower())
        except Exception:
            pass
    try:
        original_urls = json.loads(gen.shopify_url) if gen.shopify_url else []
        if isinstance(original_urls, str):
            original_urls = [original_urls]
    except Exception:
        original_urls = [gen.shopify_url] if gen.shopify_url else []
    for u in original_urls:
        try:
            exclude_domains.add(urlparse(u).netloc.replace("www.", "").lower())
        except Exception:
            pass

    logger.info(
        "image-search search-more | generation_id=%s part_number=%s exclude_domains=%s",
        generation_id, gen.part_number, sorted(exclude_domains),
    )

    try:
        new_images, new_page_urls, in_tok, out_tok = _run_scrape_pipeline(
            part_number    = gen.part_number    or '',
            brand          = gen.brand          or '',
            title          = gen.product_title  or '',
            part_type      = gen.part_type      or '',
            appliance_type = gen.appliance_type or '',
            top_n          = 10,
            broad          = True,
            exclude_domains= list(exclude_domains),
        )
    except Exception as e:
        logger.error(f"Search-more pipeline error: {e}")
        return jsonify({'error': str(e)}), 500

    # Merge, deduplicating by image url
    seen_urls = {img.get('url') for img in existing_images if img.get('url')}
    merged_images = list(existing_images)
    added = 0
    for img in new_images:
        if img.get('url') and img['url'] not in seen_urls:
            merged_images.append(img)
            seen_urls.add(img['url'])
            added += 1

    gen.image_urls    = json.dumps(merged_images)
    gen.prompt_used   = json.dumps(list(dict.fromkeys(already_scraped_pages + new_page_urls)))
    gen.input_tokens  = (gen.input_tokens or 0) + in_tok
    gen.output_tokens = (gen.output_tokens or 0) + out_tok
    db.session.commit()

    logger.info(
        "image-search search-more result | generation_id=%s new_found=%d added=%d total=%d",
        generation_id, len(new_images), added, len(merged_images),
    )

    return jsonify({
        'success':       True,
        'generation_id': generation_id,
        'images':        merged_images,
        'added':         added,
        'total':         len(merged_images),
        'input_tokens':  in_tok,
        'output_tokens': out_tok,
    })


@app.route('/api/image-search/mark-reviewed', methods=['POST'])
@login_required
def mark_image_search_reviewed():
    """
    Marks one generation item, or every item in a batch, as reviewed/pending.
    Body: { generation_id } OR { batch_id }, plus optional { status: 'pending'|'reviewed' }
    """
    body          = request.json or {}
    generation_id = (body.get('generation_id') or '').strip()
    batch_id      = (body.get('batch_id')      or '').strip()
    status        = (body.get('status') or 'reviewed').strip().lower()
    if status not in ('pending', 'reviewed', 'published'):
        status = 'reviewed'

    if not generation_id and not batch_id:
        return jsonify({'error': 'generation_id or batch_id is required'}), 400

    if generation_id:
        gen = ImageSearchGeneration.query.filter_by(generation_id=generation_id).first()
        if not gen:
            return jsonify({'error': 'Generation not found'}), 404
        gen.review_status = status
        db.session.commit()
        return jsonify({'success': True, 'updated': 1})

    rows = ImageSearchGeneration.query.filter_by(batch_id=batch_id).all()
    if not rows:
        return jsonify({'error': 'Batch not found'}), 404
    for g in rows:
        g.review_status = status
    db.session.commit()
    return jsonify({'success': True, 'updated': len(rows)})


@app.route('/api/image-search/publish-image', methods=['POST'])
@login_required
def publish_image_to_shopify():
    """
    Attach a single image URL to a Shopify product.
    Uses the REST Images API (POST /products/{id}/images.json) — simplest and
    most reliable; no staged uploads needed for external URLs.
    """
    body          = request.json or {}
    generation_id = (body.get('generation_id') or '').strip()
    image_url     = (body.get('image_url')     or '').strip()
    shopify_url   = (body.get('shopify_url')   or '').strip()

    if not image_url:
        return jsonify({'error': 'image_url is required'}), 400
    if not generation_id:
        return jsonify({'error': 'generation_id is required'}), 400

    gen = ImageSearchGeneration.query.filter_by(generation_id=generation_id).first()
    if not gen:
        return jsonify({'error': 'Generation not found'}), 404

    # Resolve Shopify product numeric ID
    effective_url = shopify_url or gen.shopify_url or ''
    part_number   = gen.part_number or ''

    gid = None
    if effective_url:
        try:
            gid = _shopify_product_gid_from_product_url(effective_url)
        except Exception as e:
            logger.warning(f'publish-image: GID from URL failed: {e}')
    if not gid and part_number:
        try:
            gid = _shopify_product_gid_from_sku(part_number)
        except Exception as e:
            logger.warning(f'publish-image: GID from SKU failed: {e}')
    if not gid:
        return jsonify({'error': 'Could not resolve Shopify product. Make sure the Shop URL or Part Number matches a product in your store.'}), 400

    # Extract numeric ID from GID  e.g. gid://shopify/Product/12345  →  12345
    numeric_id = gid.split('/')[-1]

    shop = _get_active_shop()
    if not shop:
        return jsonify({'error': 'No active Shopify shop configured'}), 400

    domain      = shop.domain.strip()
    token       = shop.access_token.strip()
    api_version = (shop.api_version or '2024-01').strip()

    endpoint = f"{_shopify_base_url(domain)}/admin/api/{api_version}/products/{numeric_id}/images.json"
    headers  = {
        'Content-Type':           'application/json',
        'X-Shopify-Access-Token': token,
    }
    payload = {'image': {'src': image_url}}

    try:
        resp = requests.post(endpoint, json=payload, headers=headers, timeout=20)
    except Exception as e:
        return jsonify({'error': f'Shopify request failed: {e}'}), 500

    if resp.status_code == 422:
        detail = resp.json().get('errors', resp.text[:300])
        return jsonify({'error': f'Shopify rejected the image: {detail}'}), 422
    if not resp.ok:
        return jsonify({'error': f'Shopify returned HTTP {resp.status_code}: {resp.text[:300]}'}), 500

    created_image = resp.json().get('image', {})
    logger.info(f"Published image to product {numeric_id}: {created_image.get('src','')}")

    gen.review_status = 'published'
    db.session.commit()

    return jsonify({
        'success':   True,
        'image_id':  created_image.get('id'),
        'image_src': created_image.get('src', image_url),
    })


@app.route('/api/image-search/history', methods=['GET'])
@login_required
def image_search_history():
    page     = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 20, type=int)
    rows     = (ImageSearchGeneration.query
                .order_by(ImageSearchGeneration.created_at.desc())
                .paginate(page=page, per_page=per_page, error_out=False))
    items = []
    for g in rows.items:
        items.append({
            'generation_id':  g.generation_id,
            'product_title':  g.product_title,
            'part_number':    g.part_number,
            'brand':          g.brand,
            'query_used':     g.query_used,
            'image_urls':     json.loads(g.image_urls  or '[]'),
            'selected_urls':  json.loads(g.selected_urls or '[]'),
            'input_tokens':   g.input_tokens,
            'output_tokens':  g.output_tokens,
            'created_at':     g.created_at.strftime('%Y-%m-%d %H:%M:%S'),
        })
    return jsonify({'success': True, 'history': items,
                    'total': rows.total, 'pages': rows.pages, 'page': page})


@app.route('/image-search')
@login_required
def image_search_page():
    return render_template('image_search.html',BASE_URL=BASE_URL)


"""
PATCH — Add these routes to app.py BEFORE the `if __name__ == '__main__':` block.
Also add the new route for review_image_search.html page.
"""

# ──────────────────────────────────────────────────────────────
# IMAGE SEARCH REVIEW ROUTES
# ──────────────────────────────────────────────────────────────

@app.route('/review-image-search')
@login_required
def review_image_search_page():
    return render_template('review_image_search.html',BASE_URL=BASE_URL)


@app.route('/api/image-search/batches', methods=['GET'])
@login_required
def image_search_batches():
    """
    Returns a paginated list of image-search batches grouped by batch_id,
    mirroring the content-generation batch structure.

    Each batch entry includes summary stats (product count, total images,
    selected images, tokens) and the list of generation items within it.
    """
    page     = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 50, type=int)

    # Fetch all rows ordered newest-first so we can group by batch_id in Python.
    # For very large tables, consider a SQL GROUP BY approach instead.
    all_rows = (ImageSearchGeneration.query
                .order_by(ImageSearchGeneration.created_at.desc())
                .all())

    # Group rows by batch_id (rows without a batch_id get their own singleton batch)
    from collections import OrderedDict
    batch_map = OrderedDict()   # batch_id → list of rows, insertion-order = newest first
    for g in all_rows:
        bid = g.batch_id or g.generation_id   # legacy rows: treat each row as its own batch
        if bid not in batch_map:
            batch_map[bid] = []
        batch_map[bid].append(g)

    # Build the paged batch list
    batch_ids  = list(batch_map.keys())
    total      = len(batch_ids)
    pages      = max(1, (total + per_page - 1) // per_page)
    page       = max(1, min(page, pages))
    start      = (page - 1) * per_page
    paged_ids  = batch_ids[start: start + per_page]

    # Pre-fetch shop names for badge display
    shop_names = {s.id: (s.name or s.domain) for s in ShopConfig.query.all()}

    batches = []
    for bid in paged_ids:
        rows = batch_map[bid]
        total_images   = 0
        total_selected = 0
        total_in_tok   = 0
        total_out_tok  = 0
        items          = []
        any_pending    = False
        for g in rows:
            image_urls    = json.loads(g.image_urls    or '[]')
            selected_urls = json.loads(g.selected_urls or '[]')
            img_cnt = len(image_urls)
            sel_cnt = len(selected_urls)
            total_images   += img_cnt
            total_selected += sel_cnt
            total_in_tok   += g.input_tokens  or 0
            total_out_tok  += g.output_tokens or 0
            r_status = g.review_status or 'pending'
            if r_status == 'pending':
                any_pending = True
            items.append({
                'generation_id':  g.generation_id,
                'product_title':  g.product_title  or '',
                'part_number':    g.part_number     or '',
                'brand':          g.brand           or '',
                'part_type':      g.part_type       or '',
                'appliance_type': g.appliance_type  or '',
                'shopify_url':    g.shopify_url     or '',
                'query_used':     g.query_used      or '',
                'image_count':    img_cnt,
                'selected_count': sel_cnt,
                'review_status':  r_status,
                'input_tokens':   g.input_tokens    or 0,
                'output_tokens':  g.output_tokens   or 0,
                'created_at':     g.created_at.strftime('%Y-%m-%d %H:%M:%S'),
            })

        # Batch-level timestamps (newest row = rows[0] because ordered desc above)
        batches.append({
            'batch_id':       bid,
            'source':         rows[0].source or 'manual',
            'shop_name':      shop_names.get(rows[0].shop_id, ''),
            'status':         'pending' if any_pending else 'reviewed',
            'product_count':  len(rows),
            'image_count':    total_images,
            'selected_count': total_selected,
            'input_tokens':   total_in_tok,
            'output_tokens':  total_out_tok,
            'created_at':     rows[-1].created_at.strftime('%Y-%m-%d %H:%M:%S'),  # oldest in batch
            'updated_at':     rows[0].created_at.strftime('%Y-%m-%d %H:%M:%S'),   # newest in batch
            'items':          items,
        })

    return jsonify({
        'success': True,
        'batches': batches,
        'total':   total,
        'pages':   pages,
        'page':    page,
    })


@app.route('/api/image-search/generation/<generation_id>', methods=['GET'])
@login_required
def image_search_generation_detail(generation_id):
    """Returns full detail for one generation, including all candidate image URLs."""
    gen = ImageSearchGeneration.query.filter_by(generation_id=generation_id).first()
    if not gen:
        return jsonify({'error': 'Not found'}), 404

    image_urls   = json.loads(gen.image_urls    or '[]')
    selected_raw = json.loads(gen.selected_urls or '[]')
    # Normalise to plain strings while preserving saved order (no set conversion)
    _seen = set()
    selected_ordered = []
    for _u in selected_raw:
        _url = _u if isinstance(_u, str) else _u.get('url', '')
        if _url and _url not in _seen:
            _seen.add(_url)
            selected_ordered.append(_url)

    logger.info(
        "image-search detail response | generation_id=%s batch_id=%s title=%r part_number=%s review_status=%s "
        "image_count=%d selected_count=%d image_preview=%s",
        gen.generation_id,
        gen.batch_id,
        gen.product_title,
        gen.part_number,
        gen.review_status,
        len(image_urls),
        len(selected_ordered),
        [
            {
                'url': (img or {}).get('url', '') if isinstance(img, dict) else img,
                'source': (img or {}).get('source', '') if isinstance(img, dict) else '',
                'title': (img or {}).get('title', '') if isinstance(img, dict) else '',
            }
            for img in image_urls[:5]
        ],
    )

    return jsonify({
        'success':        True,
        'generation_id':  gen.generation_id,
        'batch_id':       gen.batch_id        or '',
        'source':         gen.source          or 'manual',
        'review_status':  gen.review_status   or 'pending',
        'product_title':  gen.product_title  or '',
        'part_number':    gen.part_number     or '',
        'brand':          gen.brand           or '',
        'part_type':      gen.part_type       or '',
        'appliance_type': gen.appliance_type  or '',
        'shopify_url':    gen.shopify_url     or '',
        'query_used':     gen.query_used      or '',
        'images':         image_urls,
        'selected_urls':  selected_ordered,
        'input_tokens':   gen.input_tokens    or 0,
        'output_tokens':  gen.output_tokens   or 0,
        'created_at':     gen.created_at.strftime('%Y-%m-%d %H:%M:%S'),
    })

@app.route('/history-image-search')
@login_required
def history_image_search_page():
    return render_template('history_image_search.html',BASE_URL=BASE_URL)


@app.route('/api/image-search/publish-to-shops', methods=['POST'])
@login_required
def publish_image_to_shops():
    """
    Publishes one or more image URLs to one or more shops.

    Body:
      generation_id  – the ImageSearchGeneration row
      image_urls     – list of image URLs to publish (in order)
      shop_ids       – list of ShopConfig IDs to publish to

    For each shop the product is resolved via the shopify_url list stored on
    the generation row: the URL whose domain contains shop.website_domain is
    used first; if none matches we fall back to SKU lookup.
    """
    body          = request.json or {}
    generation_id = (body.get('generation_id') or '').strip()
    image_urls    = body.get('image_urls') or []
    shop_ids      = body.get('shop_ids') or []

    if not generation_id:
        return jsonify({'error': 'generation_id is required'}), 400
    if not image_urls:
        return jsonify({'error': 'image_urls is required'}), 400
    if not shop_ids:
        return jsonify({'error': 'shop_ids is required'}), 400

    gen = ImageSearchGeneration.query.filter_by(generation_id=generation_id).first()
    if not gen:
        return jsonify({'error': 'Generation not found'}), 404

    # Parse the list of shopify_urls stored on this generation
    raw_shopify = gen.shopify_url or '[]'
    try:
        all_shopify_urls = json.loads(raw_shopify) if raw_shopify.startswith('[') else [raw_shopify]
    except Exception:
        all_shopify_urls = [raw_shopify] if raw_shopify else []
    all_shopify_urls = [u for u in all_shopify_urls if u]

    part_number = gen.part_number or ''

    shops = ShopConfig.query.filter(ShopConfig.id.in_(shop_ids)).all()
    if not shops:
        return jsonify({'error': 'No valid shops found for the given shop_ids'}), 400

    results = []

    for shop in shops:
        shop_domain    = shop.domain.strip()
        shop_token     = shop.access_token.strip()
        api_version    = (shop.api_version or '2024-01').strip()
        website_domain = (shop.website_domain or '').strip().rstrip('/')

        # ── Resolve which shopify_url belongs to this shop ──────────────
        matched_url = ''
        if website_domain:
            for su in all_shopify_urls:
                parsed_host = urlparse(su).netloc.replace('www.', '')
                wd_clean    = website_domain.replace('www.', '').rstrip('/')
                if wd_clean in parsed_host or parsed_host in wd_clean:
                    matched_url = su
                    break

        # ── Resolve product GID for this shop ────────────────────────────
        gid = None
        if matched_url:
            try:
                # Resolve using this specific shop's credentials
                product_id = _extract_admin_product_id(matched_url)
                if product_id:
                    gid = f'gid://shopify/Product/{product_id}'
                else:
                    seg = _extract_products_path_segment(matched_url)
                    if seg:
                        if seg.isdigit():
                            gid = f'gid://shopify/Product/{seg}'
                        else:
                            body_gql = _shopify_graphql_with_creds(
                                _GQL_PRODUCT_BY_HANDLE, {'handle': seg},
                                shop_domain, shop_token, api_version
                            )
                            node = (body_gql.get('data') or {}).get('productByHandle')
                            if node:
                                gid = node.get('id')
            except Exception as e:
                logger.warning(f'publish-to-shops: GID from URL failed for shop {shop.id}: {e}')

        if not gid and part_number:
            try:
                endpoint = f"{_shopify_base_url(shop_domain)}/admin/api/{api_version}/graphql.json"
                headers  = {'Content-Type': 'application/json', 'X-Shopify-Access-Token': shop_token}
                resp_sku = requests.post(
                    endpoint,
                    json={'query': _GQL_PRODUCT_BY_SKU, 'variables': {'query': f'sku:{part_number}'}},
                    headers=headers, timeout=15
                )
                if resp_sku.ok:
                    edges = ((resp_sku.json().get('data') or {}).get('products', {}).get('edges') or [])
                    if edges:
                        gid = edges[0]['node'].get('id')
            except Exception as e:
                logger.warning(f'publish-to-shops: GID from SKU failed for shop {shop.id}: {e}')

        if not gid:
            results.append({
                'shop_id':   shop.id,
                'shop_name': shop.name,
                'success':   False,
                'error':     'Could not resolve Shopify product for this shop.',
                'published': 0,
            })
            continue

        # ── Attach each image URL via REST Images API ─────────────────────
        numeric_id = gid.split('/')[-1]
        endpoint_img = f"{_shopify_base_url(shop_domain)}/admin/api/{api_version}/products/{numeric_id}/images.json"
        headers_img  = {'Content-Type': 'application/json', 'X-Shopify-Access-Token': shop_token}

        published_count = 0
        shop_errors     = []
        for img_url in image_urls:
            try:
                r = requests.post(endpoint_img, json={'image': {'src': img_url}}, headers=headers_img, timeout=20)
                if r.status_code == 422:
                    detail = r.json().get('errors', r.text[:200])
                    shop_errors.append(f'{img_url}: {detail}')
                elif not r.ok:
                    shop_errors.append(f'{img_url}: HTTP {r.status_code}')
                else:
                    published_count += 1
            except Exception as e:
                shop_errors.append(f'{img_url}: {e}')

        results.append({
            'shop_id':      shop.id,
            'shop_name':    shop.name,
            'success':      published_count > 0,
            'published':    published_count,
            'errors':       shop_errors,
            'matched_url':  matched_url,
        })

    # Mark generation as published if at least one shop succeeded
    if any(r['success'] for r in results):
        gen.review_status = 'published'
        db.session.commit()

    return jsonify({'success': True, 'results': results})



@app.route('/api/image-search/default-prompt', methods=['GET'])
@login_required
def get_default_image_search_prompt():
    """Returns the saved prompt from DB, falling back to the hardcoded default."""
    prompt = AppSetting.get('image_search_prompt', DEFAULT_IMAGE_SEARCH_PROMPT)
    return jsonify({'prompt': prompt})

@app.route('/api/image-search/save-prompt', methods=['POST'])
@login_required
def save_default_image_search_prompt():
    """Persists the user-edited prompt as the new default in the DB."""
    body   = request.json or {}
    prompt = (body.get('prompt') or '').strip()
    if not prompt:
        return jsonify({'error': 'prompt is required'}), 400
    AppSetting.set('image_search_prompt', prompt)
    return jsonify({'success': True})
    
if __name__ == '__main__':
    run_migrations()