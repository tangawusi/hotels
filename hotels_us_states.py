#!/usr/bin/env python3
from __future__ import annotations

import argparse
import concurrent.futures
import csv
import json
import gzip
import os
import random
import re
import threading
import time
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple
from urllib import error as urllib_error
from urllib import request as urllib_request
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse


STATE_ABBR_TO_NAME: Dict[str, str] = {
    "AL": "Alabama",
    "AK": "Alaska",
    "AZ": "Arizona",
    "AR": "Arkansas",
    "CA": "California",
    "CO": "Colorado",
    "CT": "Connecticut",
    "DE": "Delaware",
    "FL": "Florida",
    "GA": "Georgia",
    "HI": "Hawaii",
    "ID": "Idaho",
    "IL": "Illinois",
    "IN": "Indiana",
    "IA": "Iowa",
    "KS": "Kansas",
    "KY": "Kentucky",
    "LA": "Louisiana",
    "ME": "Maine",
    "MD": "Maryland",
    "MA": "Massachusetts",
    "MI": "Michigan",
    "MN": "Minnesota",
    "MS": "Mississippi",
    "MO": "Missouri",
    "MT": "Montana",
    "NE": "Nebraska",
    "NV": "Nevada",
    "NH": "New Hampshire",
    "NJ": "New Jersey",
    "NM": "New Mexico",
    "NY": "New York",
    "NC": "North Carolina",
    "ND": "North Dakota",
    "OH": "Ohio",
    "OK": "Oklahoma",
    "OR": "Oregon",
    "PA": "Pennsylvania",
    "RI": "Rhode Island",
    "SC": "South Carolina",
    "SD": "South Dakota",
    "TN": "Tennessee",
    "TX": "Texas",
    "UT": "Utah",
    "VT": "Vermont",
    "VA": "Virginia",
    "WA": "Washington",
    "WV": "West Virginia",
    "WI": "Wisconsin",
    "WY": "Wyoming",
}

OVERPASS_ENDPOINTS: Sequence[str] = (
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass.openstreetmap.ru/api/interpreter",
)

TOURISM_VALUES = ("hotel", "motel", "guest_house", "hostel", "resort", "apartment")

AGGREGATOR_BLACKLIST: Set[str] = {
    "booking.com",
    "expedia.com",
    "tripadvisor.com",
    "hotels.com",
    "priceline.com",
    "agoda.com",
    "kayak.com",
    "trivago.com",
    "orbitz.com",
    "travelocity.com",
    "hotwire.com",
    "airbnb.com",
    "vrbo.com",
    "facebook.com",
    "instagram.com",
    "x.com",
    "twitter.com",
    "yelp.com",
    "google.com",
    "maps.google.com",
}

BRAND_BLACKLIST: Set[str] = {
    "hilton.com",
    "marriott.com",
    "ihg.com",
    "choicehotels.com",
    "wyndhamhotels.com",
    "hyatt.com",
    "bestwestern.com",
    "accor.com",
}

STOPWORDS: Set[str] = {
    "hotel",
    "motel",
    "inn",
    "lodge",
    "suites",
    "suite",
    "resort",
    "spa",
    "the",
    "and",
    "at",
    "of",
    "in",
    "on",
    "by",
    "a",
    "an",
    "extended",
    "stay",
}

TRACKING_PARAMS_PREFIX = ("utm_", "fbclid", "gclid", "mc_", "msclkid")


def detect_base_dir() -> str:
    candidates = ["/content", os.getcwd(), os.path.expanduser("~")]
    for candidate in candidates:
        if os.path.exists(candidate) and os.access(candidate, os.W_OK):
            return os.path.abspath(candidate)
    return os.path.abspath(os.getcwd())


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def atomic_write_json(path: str, payload: Dict[str, Any]) -> None:
    ensure_dir(os.path.dirname(path))
    temp = f"{path}.tmp"
    with open(temp, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2, sort_keys=True)
    os.replace(temp, path)


def load_json(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        if isinstance(data, dict):
            return data
    except Exception:
        return {}
    return {}


def is_colab() -> bool:
    return os.path.exists("/content") and "COLAB_RELEASE_TAG" in os.environ


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect US lodging websites by state")
    parser.add_argument("--mode", choices=["hotel_urls", "unique_domains"], default="hotel_urls")
    parser.add_argument("--per-state", type=int, default=1000)
    parser.add_argument("--liveness-workers", type=int, default=0)
    parser.add_argument("--search-workers", type=int, default=12)
    parser.add_argument("--wikidata-workers", type=int, default=10)
    parser.add_argument("--places-workers", type=int, default=6)
    parser.add_argument("--blacklist-brands", action="store_true")
    parser.add_argument("--save-interval", type=int, default=100)
    parser.add_argument("--timeout", type=float, default=12.0)
    return parser.parse_args()


class Progress:
    def __init__(self) -> None:
        self._tqdm = None
        self._lock = threading.Lock()

    def iterable(self, iterable: Iterable[Any], total: Optional[int], desc: str) -> Iterable[Any]:
        if self._tqdm is None:
            try:
                self._tqdm = __import__("tqdm").tqdm
            except Exception:
                self._tqdm = False
        if self._tqdm:
            return self._tqdm(iterable, total=total, desc=desc)
        return iterable

    def log(self, message: str) -> None:
        with self._lock:
            print(message, flush=True)


class CacheStore:
    def __init__(self, cache_dir: str) -> None:
        self.cache_dir = cache_dir
        ensure_dir(cache_dir)
        self.domain_live_cache_path = os.path.join(cache_dir, "domain_live_cache.json")
        self.url_live_cache_path = os.path.join(cache_dir, "url_live_cache.json")
        self.verified_cache_path = os.path.join(cache_dir, "verified_cache.json")
        self.search_cache_path = os.path.join(cache_dir, "search_cache.json")

        self.domain_live_cache = load_json(self.domain_live_cache_path)
        self.url_live_cache = load_json(self.url_live_cache_path)
        self.verified_cache = load_json(self.verified_cache_path)
        self.search_cache = load_json(self.search_cache_path)

        self._lock = threading.Lock()

    def get(self, cache_name: str, key: str) -> Optional[Any]:
        cache = getattr(self, cache_name)
        with self._lock:
            return cache.get(key)

    def set(self, cache_name: str, key: str, value: Any) -> None:
        cache = getattr(self, cache_name)
        with self._lock:
            cache[key] = value

    def save_all(self) -> None:
        with self._lock:
            atomic_write_json(self.domain_live_cache_path, self.domain_live_cache)
            atomic_write_json(self.url_live_cache_path, self.url_live_cache)
            atomic_write_json(self.verified_cache_path, self.verified_cache)
            atomic_write_json(self.search_cache_path, self.search_cache)


class HttpClient:
    def __init__(self, timeout: float) -> None:
        self.timeout = timeout
        self.default_headers = {
            "User-Agent": "Mozilla/5.0 (compatible; HotelsUSCollector/1.0)",
            "Accept-Language": "en-US,en;q=0.8",
            "Accept-Encoding": "gzip, deflate",
        }

    @staticmethod
    def _decode_body(data: bytes, headers: Dict[str, str]) -> bytes:
        encoding = headers.get("Content-Encoding", "").lower()
        if "gzip" not in encoding:
            return data
        try:
            return gzip.decompress(data)
        except OSError:
            return data

    def _build_url(self, url: str, params: Optional[Dict[str, Any]]) -> str:
        if not params:
            return url
        parsed = urlparse(url)
        existing = parse_qsl(parsed.query, keep_blank_values=True)
        merged = existing + [(k, str(v)) for k, v in params.items()]
        query = urlencode(merged, doseq=True)
        return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, query, parsed.fragment))

    def request(
        self,
        method: str,
        url: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Any] = None,
        headers: Optional[Dict[str, str]] = None,
        stream: bool = False,
        allow_redirects: bool = True,
        retries: int = 3,
    ) -> Optional["SimpleResponse"]:
        for attempt in range(retries):
            request_url = self._build_url(url, params)
            req_data: Optional[bytes] = None
            if data is not None:
                if isinstance(data, dict):
                    req_data = urlencode(data, doseq=True).encode("utf-8")
                elif isinstance(data, str):
                    req_data = data.encode("utf-8")
                elif isinstance(data, bytes):
                    req_data = data
                else:
                    req_data = str(data).encode("utf-8")
            merged_headers = dict(self.default_headers)
            if headers:
                merged_headers.update(headers)
            if isinstance(data, dict):
                merged_headers.setdefault("Content-Type", "application/x-www-form-urlencoded")

            class _NoRedirect(urllib_request.HTTPRedirectHandler):
                def redirect_request(self, req, fp, code, msg, hdrs, newurl):
                    return None

            handlers: List[Any] = []
            if not allow_redirects:
                handlers.append(_NoRedirect())
            opener = urllib_request.build_opener(*handlers)
            try:
                req = urllib_request.Request(request_url, data=req_data, headers=merged_headers, method=method.upper())
                with opener.open(req, timeout=self.timeout) as response:
                    payload = response.read()
                    header_map = {k: v for k, v in response.headers.items()}
                    body = self._decode_body(payload, header_map)
                    return SimpleResponse(response.status, response.geturl(), header_map, body)
            except urllib_error.HTTPError as http_err:
                try:
                    payload = http_err.read()
                except Exception:
                    payload = b""
                header_map = {k: v for k, v in (http_err.headers.items() if http_err.headers else [])}
                body = self._decode_body(payload, header_map)
                return SimpleResponse(http_err.code, http_err.geturl(), header_map, body)
            except (urllib_error.URLError, TimeoutError, ValueError):
                sleep_for = min(5.0, (2**attempt) + random.random())
                time.sleep(sleep_for)
        return None


class SimpleResponse:
    def __init__(self, status_code: int, url: str, headers: Dict[str, str], body: bytes) -> None:
        self.status_code = status_code
        self.url = url
        self.headers = headers
        self._body = body
        self.encoding = self._detect_encoding(headers)

    @staticmethod
    def _detect_encoding(headers: Dict[str, str]) -> Optional[str]:
        content_type = headers.get("Content-Type", "")
        match = re.search(r"charset=([^;\s]+)", content_type, flags=re.I)
        if match:
            return match.group(1).strip('"\'')
        return None

    @property
    def text(self) -> str:
        return self._body.decode(self.encoding or "utf-8", errors="ignore")

    def json(self) -> Dict[str, Any]:
        return json.loads(self.text)

    def iter_content(self, chunk_size: int = 8192) -> Iterable[bytes]:
        for idx in range(0, len(self._body), max(1, chunk_size)):
            yield self._body[idx : idx + chunk_size]


def normalize_url(url: str) -> str:
    if not url:
        return ""
    candidate = url.strip()
    if not candidate:
        return ""
    if not re.match(r"^https?://", candidate, flags=re.I):
        candidate = "https://" + candidate
    parsed = urlparse(candidate)
    scheme = parsed.scheme.lower() if parsed.scheme else "https"
    netloc = parsed.netloc.lower()
    if netloc.startswith("www."):
        netloc = netloc[4:]
    query_items = []
    for key, value in parse_qsl(parsed.query, keep_blank_values=True):
        lowered = key.lower()
        if lowered in TRACKING_PARAMS_PREFIX:
            continue
        if any(lowered.startswith(prefix) for prefix in TRACKING_PARAMS_PREFIX):
            continue
        query_items.append((key, value))
    query = urlencode(query_items, doseq=True)
    path = re.sub(r"/+", "/", parsed.path or "/")
    if path != "/" and path.endswith("/"):
        path = path[:-1]
    rebuilt = urlunparse((scheme, netloc, path, "", query, ""))
    return rebuilt


def root_domain(value: str) -> str:
    parsed = urlparse(value if re.match(r"^https?://", value, flags=re.I) else "https://" + value)
    domain = parsed.netloc.lower()
    if domain.startswith("www."):
        domain = domain[4:]
    return domain.split(":")[0]


def is_blacklisted(domain: str, blacklist_brands: bool) -> bool:
    all_blacklist = set(AGGREGATOR_BLACKLIST)
    if blacklist_brands:
        all_blacklist |= BRAND_BLACKLIST
    for blocked in all_blacklist:
        if domain == blocked or domain.endswith("." + blocked):
            return True
    return False


def overpass_query_for_state(state_abbr: str) -> str:
    tourism_regex = "|".join(TOURISM_VALUES)
    return f"""
[out:json][timeout:60];
area["ISO3166-2"="US-{state_abbr}"]["admin_level"="4"]->.searchArea;
(
  nwr["tourism"~"^({tourism_regex})$"](area.searchArea);
);
out tags center 5000;
""".strip()


def parse_overpass_elements(payload: Dict[str, Any], state_abbr: str) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    for element in payload.get("elements", []):
        tags = element.get("tags", {}) or {}
        name = (tags.get("name") or "").strip()
        city = (tags.get("addr:city") or tags.get("is_in:city") or "").strip()
        website = (tags.get("website") or tags.get("contact:website") or "").strip()
        wikidata = (tags.get("wikidata") or "").strip()
        brand = (tags.get("brand") or "").strip()
        if not name:
            continue
        rows.append(
            {
                "state": state_abbr,
                "hotel_name": name,
                "city": city,
                "website": website,
                "wikidata": wikidata,
                "brand": brand,
                "source": "OpenStreetMap",
            }
        )
    return rows


def fetch_overpass_for_state(client: HttpClient, state_abbr: str) -> List[Dict[str, str]]:
    query = overpass_query_for_state(state_abbr)
    endpoints = list(OVERPASS_ENDPOINTS)
    random.shuffle(endpoints)
    for idx, endpoint in enumerate(endpoints):
        response = client.request("POST", endpoint, data={"data": query}, retries=2)
        if response is None:
            continue
        if response.status_code >= 500:
            time.sleep(min(5, (2**idx) + random.random()))
            continue
        if response.status_code != 200:
            continue
        try:
            payload = response.json()
            return parse_overpass_elements(payload, state_abbr)
        except ValueError:
            continue
    return []


def fetch_wikidata_websites(client: HttpClient, qid: str) -> List[str]:
    url = "https://www.wikidata.org/wiki/Special:EntityData/{}.json".format(qid)
    response = client.request("GET", url, retries=3)
    if response is None or response.status_code != 200:
        return []
    try:
        payload = response.json()
    except ValueError:
        return []
    entity = payload.get("entities", {}).get(qid, {})
    claims = entity.get("claims", {})
    urls: List[str] = []
    for prop in ("P856",):
        for claim in claims.get(prop, []) or []:
            mainsnak = claim.get("mainsnak", {})
            datavalue = mainsnak.get("datavalue", {})
            value = datavalue.get("value")
            if isinstance(value, str):
                urls.append(value)
    return urls


def places_text_search(client: HttpClient, api_key: str, query: str) -> Optional[str]:
    endpoint = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    response = client.request("GET", endpoint, params={"query": query, "key": api_key}, retries=2)
    if response is None or response.status_code != 200:
        return None
    try:
        data = response.json()
    except ValueError:
        return None
    results = data.get("results") or []
    if not results:
        return None
    place_id = results[0].get("place_id")
    if not place_id:
        return None
    details_endpoint = "https://maps.googleapis.com/maps/api/place/details/json"
    details = client.request(
        "GET",
        details_endpoint,
        params={"place_id": place_id, "fields": "website", "key": api_key},
        retries=2,
    )
    if details is None or details.status_code != 200:
        return None
    try:
        payload = details.json()
    except ValueError:
        return None
    return payload.get("result", {}).get("website")


def ddg_discovery(client: HttpClient, query: str) -> List[str]:
    endpoint = "https://duckduckgo.com/html/"
    response = client.request("GET", endpoint, params={"q": query}, retries=2)
    if response is None or response.status_code != 200:
        return []
    html = response.text
    matches = re.findall(r'href="(https?://[^"]+)"', html, flags=re.I)
    cleaned: List[str] = []
    for value in matches:
        if "duckduckgo.com" in value:
            continue
        cleaned.append(value)
    time.sleep(0.4 + random.random() * 0.6)
    return cleaned[:10]


def infer_brand_urls(brand: str, city: str) -> List[str]:
    if not brand:
        return []
    slug_city = re.sub(r"[^a-z0-9]+", "-", city.lower()).strip("-")
    b = brand.lower()
    candidates: List[str] = []
    if "hilton" in b:
        candidates.append(f"https://www.hilton.com/en/hotels/{slug_city}/")
    if "marriott" in b:
        candidates.append(f"https://www.marriott.com/en-us/hotels/{slug_city}/")
    if "hyatt" in b:
        candidates.append(f"https://www.hyatt.com/en-US/hotel/{slug_city}")
    if "wyndham" in b:
        candidates.append(f"https://www.wyndhamhotels.com/{slug_city}")
    return candidates


def tokenize_name(name: str) -> List[str]:
    words = re.findall(r"[a-z0-9]+", name.lower())
    tokens: List[str] = []
    for w in words:
        if len(w) < 4:
            continue
        if w in STOPWORDS:
            continue
        tokens.append(w)
    return sorted(set(tokens))


def html_contains_tokens(html: str, tokens: Sequence[str]) -> bool:
    text = html.lower()
    matched = 0
    for token in tokens:
        if token in text:
            matched += 1
        if matched >= 2:
            return True
    return False


def fetch_html_snippet(client: HttpClient, url: str, max_bytes: int = 400_000) -> Optional[str]:
    response = client.request("GET", url, stream=True, allow_redirects=True, retries=2)
    if response is None:
        return None
    if response.status_code >= 500:
        return None
    chunks: List[bytes] = []
    total = 0
    try:
        for chunk in response.iter_content(chunk_size=8192):
            if not chunk:
                continue
            chunks.append(chunk)
            total += len(chunk)
            if total >= max_bytes:
                break
    except (urllib_error.URLError, TimeoutError, ValueError):
        return None
    data = b"".join(chunks)
    try:
        return data.decode(response.encoding or "utf-8", errors="ignore")
    except Exception:
        return data.decode("utf-8", errors="ignore")


def liveness_check(client: HttpClient, cache: CacheStore, url: str) -> bool:
    key = normalize_url(url)
    cached = cache.get("url_live_cache", key)
    if isinstance(cached, bool):
        return cached
    candidates = [key]
    parsed = urlparse(key)
    if parsed.scheme == "https":
        candidates.append(urlunparse(("http", parsed.netloc, parsed.path, parsed.params, parsed.query, parsed.fragment)))
    for candidate in candidates:
        head = client.request("HEAD", candidate, allow_redirects=True, retries=2)
        if head is not None and head.status_code < 500:
            cache.set("url_live_cache", key, True)
            cache.set("domain_live_cache", root_domain(candidate), True)
            return True
        get_resp = client.request("GET", candidate, allow_redirects=True, retries=2)
        if get_resp is not None and get_resp.status_code < 500:
            cache.set("url_live_cache", key, True)
            cache.set("domain_live_cache", root_domain(candidate), True)
            return True
    cache.set("url_live_cache", key, False)
    cache.set("domain_live_cache", root_domain(key), False)
    return False


def verify_candidate(client: HttpClient, cache: CacheStore, hotel_name: str, url: str) -> bool:
    norm = normalize_url(url)
    vkey = f"{hotel_name.lower()}::{norm}"
    cached = cache.get("verified_cache", vkey)
    if isinstance(cached, bool):
        return cached
    if not liveness_check(client, cache, norm):
        cache.set("verified_cache", vkey, False)
        return False
    tokens = tokenize_name(hotel_name)
    if len(tokens) < 2:
        cache.set("verified_cache", vkey, False)
        return False
    html = fetch_html_snippet(client, norm)
    if not html:
        cache.set("verified_cache", vkey, False)
        return False
    ok = html_contains_tokens(html, tokens)
    cache.set("verified_cache", vkey, ok)
    return ok


def discover_candidates_for_record(
    client: HttpClient,
    record: Dict[str, str],
    state_name: str,
    api_key: Optional[str],
    cache: CacheStore,
) -> List[Tuple[str, str]]:
    out: List[Tuple[str, str]] = []
    if record.get("website"):
        out.append((record["website"], "OpenStreetMap"))

    qid = record.get("wikidata")
    if qid:
        websites = fetch_wikidata_websites(client, qid)
        out.extend((u, "Wikidata") for u in websites)

    if api_key:
        query = f"{record.get('hotel_name', '')} {record.get('city', '')} {state_name}".strip()
        cache_key = f"places::{query}"
        cached = cache.get("search_cache", cache_key)
        place_site = cached if isinstance(cached, str) else places_text_search(client, api_key, query)
        if isinstance(place_site, str) and place_site:
            cache.set("search_cache", cache_key, place_site)
            out.append((place_site, "GooglePlaces"))

    ddg_query = f"{record.get('hotel_name', '')} {record.get('city', '')} {record.get('state', '')} official site".strip()
    cache_key = f"ddg::{ddg_query}"
    cached_links = cache.get("search_cache", cache_key)
    if isinstance(cached_links, list):
        links = [str(v) for v in cached_links]
    else:
        links = ddg_discovery(client, ddg_query)
        cache.set("search_cache", cache_key, links)
    out.extend((u, "DuckDuckGo") for u in links)

    out.extend((u, "BrandInference") for u in infer_brand_urls(record.get("brand", ""), record.get("city", "")))

    seen: Set[str] = set()
    deduped: List[Tuple[str, str]] = []
    for url, source in out:
        nurl = normalize_url(url)
        if not nurl:
            continue
        if nurl in seen:
            continue
        seen.add(nurl)
        deduped.append((nurl, source))
    return deduped


def make_row(
    mode: str,
    state: str,
    hotel_name: str,
    city: str,
    url: str,
    source: str,
    verified: bool,
) -> Dict[str, str]:
    domain = root_domain(url)
    if mode == "hotel_urls":
        return {
            "State": state,
            "Hotel Name": hotel_name,
            "City": city,
            "Property URL": url,
            "Domain": domain,
            "Source": source,
            "Verified": str(bool(verified)),
        }
    return {
        "State": state,
        "Hotel Name": hotel_name,
        "City": city,
        "Domain": domain,
        "Source": source,
        "Verified": str(bool(verified)),
    }


def write_csv(path: str, rows: List[Dict[str, str]], mode: str) -> None:
    ensure_dir(os.path.dirname(path))
    if mode == "hotel_urls":
        columns = ["State", "Hotel Name", "City", "Property URL", "Domain", "Source", "Verified"]
    else:
        columns = ["State", "Hotel Name", "City", "Domain", "Source", "Verified"]
    with open(path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def default_liveness_workers(value: int) -> int:
    if value > 0:
        return value
    return 80 if is_colab() else 50


def process_state(
    state_abbr: str,
    state_name: str,
    args: argparse.Namespace,
    client: HttpClient,
    cache: CacheStore,
    progress: Progress,
) -> List[Dict[str, str]]:
    records = fetch_overpass_for_state(client, state_abbr)
    if not records:
        return []

    mode = args.mode
    per_state = args.per_state
    blacklist_brands = bool(args.blacklist_brands)

    output: List[Dict[str, str]] = []
    dedupe_key: Set[str] = set()
    lock = threading.Lock()
    api_key = os.environ.get("GOOGLE_PLACES_API_KEY")
    save_counter = 0

    def worker(record: Dict[str, str]) -> None:
        nonlocal save_counter
        candidates = discover_candidates_for_record(client, record, state_name, api_key, cache)
        for candidate_url, source in candidates:
            dom = root_domain(candidate_url)
            if is_blacklisted(dom, blacklist_brands):
                continue
            verified = verify_candidate(client, cache, record["hotel_name"], candidate_url)
            if not verified:
                continue
            key = normalize_url(candidate_url) if mode == "hotel_urls" else dom
            with lock:
                if key in dedupe_key:
                    continue
                if len(output) >= per_state:
                    return
                dedupe_key.add(key)
                output.append(
                    make_row(
                        mode,
                        state_abbr,
                        record.get("hotel_name", ""),
                        record.get("city", ""),
                        normalize_url(candidate_url),
                        source,
                        verified,
                    )
                )
                save_counter += 1
                if save_counter % max(1, args.save_interval) == 0:
                    cache.save_all()

    max_workers = max(args.search_workers, args.wikidata_workers, args.places_workers)
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [pool.submit(worker, rec) for rec in records]
        for f in progress.iterable(concurrent.futures.as_completed(futures), total=len(futures), desc=f"{state_abbr} discovery"):
            _ = f.result()
            if len(output) >= per_state:
                break

    output.sort(key=lambda r: (r.get("Hotel Name", ""), r.get("Domain", "")))
    return output[:per_state]


def run() -> int:
    args = parse_args()
    _ = default_liveness_workers(args.liveness_workers)

    base_dir = detect_base_dir()
    state_dir = os.path.join(base_dir, "state_outputs")
    cache_dir = os.path.join(base_dir, "cache")
    ensure_dir(state_dir)
    ensure_dir(cache_dir)

    cache = CacheStore(cache_dir)
    client = HttpClient(timeout=args.timeout)
    progress = Progress()

    all_rows: List[Dict[str, str]] = []

    for state_abbr, state_name in progress.iterable(STATE_ABBR_TO_NAME.items(), total=len(STATE_ABBR_TO_NAME), desc="States"):
        progress.log(f"Processing {state_abbr} - {state_name}")
        rows = process_state(state_abbr, state_name, args, client, cache, progress)
        if not rows:
            progress.log(f"No rows for {state_abbr}")
            continue
        all_rows.extend(rows)
        per_state_path = os.path.join(state_dir, f"{state_abbr}_{args.mode}_{len(rows)}.csv")
        write_csv(per_state_path, rows, args.mode)
        cache.save_all()
        progress.log(f"Saved {len(rows)} rows for {state_abbr}")

    master_path = os.path.join(base_dir, f"us_hotels_{args.mode}_50states_{args.per_state}_each.csv")
    write_csv(master_path, all_rows, args.mode)
    cache.save_all()
    progress.log(f"Complete. Wrote {len(all_rows)} rows to {master_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
