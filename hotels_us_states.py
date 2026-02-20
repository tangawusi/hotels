#!/usr/bin/env python3
from __future__ import annotations

import concurrent.futures as cf
import csv, gzip, json, os, random, re, threading, time
from typing import Any, Dict, List, Optional, Sequence, Set
from urllib import error as ue
from urllib import request as ur
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse, unquote


STATE = {
    "AL":"Alabama","AK":"Alaska","AZ":"Arizona","AR":"Arkansas","CA":"California","CO":"Colorado","CT":"Connecticut",
    "DE":"Delaware","FL":"Florida","GA":"Georgia","HI":"Hawaii","ID":"Idaho","IL":"Illinois","IN":"Indiana","IA":"Iowa",
    "KS":"Kansas","KY":"Kentucky","LA":"Louisiana","ME":"Maine","MD":"Maryland","MA":"Massachusetts","MI":"Michigan",
    "MN":"Minnesota","MS":"Mississippi","MO":"Missouri","MT":"Montana","NE":"Nebraska","NV":"Nevada","NH":"New Hampshire",
    "NJ":"New Jersey","NM":"New Mexico","NY":"New York","NC":"North Carolina","ND":"North Dakota","OH":"Ohio","OK":"Oklahoma",
    "OR":"Oregon","PA":"Pennsylvania","RI":"Rhode Island","SC":"South Carolina","SD":"South Dakota","TN":"Tennessee",
    "TX":"Texas","UT":"Utah","VT":"Vermont","VA":"Virginia","WA":"Washington","WV":"West Virginia","WI":"Wisconsin","WY":"Wyoming",
}

OVERPASS: Sequence[str] = (
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass.openstreetmap.ru/api/interpreter",
)
TOURISM = ("hotel", "motel", "guest_house", "hostel", "resort", "apartment")

AGG_BLACKLIST: Set[str] = {
    "booking.com","expedia.com","tripadvisor.com","hotels.com","priceline.com","agoda.com","kayak.com","trivago.com",
    "orbitz.com","travelocity.com","hotwire.com","airbnb.com","vrbo.com","facebook.com","instagram.com","x.com","twitter.com",
    "yelp.com","google.com","maps.google.com",
}
BRAND_BLACKLIST: Set[str] = {
    "hilton.com","marriott.com","ihg.com","choicehotels.com","wyndhamhotels.com","hyatt.com","bestwestern.com","accor.com",
}
TRACK_PREFIX = ("utm_", "fbclid", "gclid", "mc_", "msclkid")


CFG = {
    "per_state": 1500,
    "workers": 24,
    "timeout": 12.0,
    "blacklist_aggregators": True,
    "blacklist_brands": False,
    "ddg_delay_min": 0.35,
    "ddg_delay_max": 0.85,
    "save_interval": 300,
}


def base_dir() -> str:
    for p in ("/content", os.getcwd(), os.path.expanduser("~")):
        if os.path.exists(p) and os.access(p, os.W_OK):
            return os.path.abspath(p)
    return os.path.abspath(os.getcwd())


def ensure(p: str) -> None:
    os.makedirs(p, exist_ok=True)


def jload(p: str) -> Dict[str, Any]:
    if not os.path.exists(p):
        return {}
    try:
        with open(p, "r", encoding="utf-8") as f:
            x = json.load(f)
        return x if isinstance(x, dict) else {}
    except Exception:
        return {}


def jsave(p: str, data: Dict[str, Any]) -> None:
    ensure(os.path.dirname(p))
    tmp = p + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, sort_keys=True)
    os.replace(tmp, p)


class Cache:
    def __init__(self, d: str):
        ensure(d)
        self.path = os.path.join(d, "cache.json")
        self.data = jload(self.path)
        self.lock = threading.Lock()

    def get(self, k: str):
        with self.lock:
            return self.data.get(k)

    def set(self, k: str, v: Any):
        with self.lock:
            self.data[k] = v

    def save(self):
        with self.lock:
            jsave(self.path, self.data)


class Resp:
    def __init__(self, code: int, url: str, headers: Dict[str, str], body: bytes):
        self.code, self.url, self.headers, self.body = code, url, headers, body

    @property
    def text(self) -> str:
        ct = self.headers.get("Content-Type", "")
        m = re.search(r"charset=([^;\s]+)", ct, flags=re.I)
        enc = m.group(1).strip('"\'' ) if m else "utf-8"
        return self.body.decode(enc, errors="ignore")

    def json(self) -> Dict[str, Any]:
        return json.loads(self.text)


class HTTP:
    def __init__(self, timeout: float):
        self.timeout = timeout
        self.h = {
            "User-Agent": "Mozilla/5.0 (compatible; HotelsDomains/1.0)",
            "Accept-Language": "en-US,en;q=0.8",
            "Accept-Encoding": "gzip, deflate",
        }

    def req(self, method: str, url: str, *, params=None, data=None, retries=3) -> Optional[Resp]:
        if params:
            pu = urlparse(url)
            q = parse_qsl(pu.query, keep_blank_values=True) + [(k, str(v)) for k, v in params.items()]
            url = urlunparse((pu.scheme, pu.netloc, pu.path, pu.params, urlencode(q, doseq=True), pu.fragment))
        req_data = None
        if data is not None:
            if isinstance(data, dict):
                req_data = urlencode(data, doseq=True).encode("utf-8")
            elif isinstance(data, (bytes, bytearray)):
                req_data = bytes(data)
            else:
                req_data = str(data).encode("utf-8")

        for a in range(retries):
            try:
                r = ur.Request(url, data=req_data, headers=self.h, method=method.upper())
                with ur.urlopen(r, timeout=self.timeout) as resp:
                    b = resp.read()
                    hdr = {k: v for k, v in resp.headers.items()}
                    if "gzip" in hdr.get("Content-Encoding", "").lower():
                        try:
                            b = gzip.decompress(b)
                        except OSError:
                            pass
                    return Resp(resp.status, resp.geturl(), hdr, b)
            except ue.HTTPError as e:
                try:
                    b = e.read()
                except Exception:
                    b = b""
                hdr = {k: v for k, v in (e.headers.items() if e.headers else [])}
                if "gzip" in hdr.get("Content-Encoding", "").lower():
                    try:
                        b = gzip.decompress(b)
                    except OSError:
                        pass
                return Resp(e.code, e.geturl(), hdr, b)
            except Exception:
                time.sleep(min(5.0, (2**a) + random.random()))
        return None


def norm_url(u: str) -> str:
    u = (u or "").strip()
    if not u:
        return ""
    if not re.match(r"^https?://", u, flags=re.I):
        u = "https://" + u
    p = urlparse(u)
    netloc = (p.netloc or "").lower()
    if netloc.startswith("www."):
        netloc = netloc[4:]
    # strip tracking params
    q = [(k, v) for k, v in parse_qsl(p.query, keep_blank_values=True)
         if k.lower() not in TRACK_PREFIX and not any(k.lower().startswith(x) for x in TRACK_PREFIX)]
    return urlunparse((p.scheme.lower() or "https", netloc, p.path or "/", "", urlencode(q, doseq=True), ""))


def domain(u: str) -> str:
    u = u if re.match(r"^https?://", u, flags=re.I) else "https://" + u
    d = (urlparse(u).netloc or "").lower()
    if d.startswith("www."):
        d = d[4:]
    return d.split(":")[0].strip()


def blacklisted(d: str) -> bool:
    if not d:
        return True
    bl: Set[str] = set()
    if CFG["blacklist_aggregators"]:
        bl |= AGG_BLACKLIST
    if CFG["blacklist_brands"]:
        bl |= BRAND_BLACKLIST
    return any(d == b or d.endswith("." + b) for b in bl)


def overpass_q(abbr: str) -> str:
    t = "|".join(TOURISM)
    return f"""
[out:json][timeout:60];
area["ISO3166-2"="US-{abbr}"]["admin_level"="4"]->.a;
(nwr["tourism"~"^({t})$"](area.a););
out tags center 5000;
""".strip()


def fetch_osm(http: HTTP, abbr: str) -> List[Dict[str, str]]:
    q = overpass_q(abbr)
    eps = list(OVERPASS)
    random.shuffle(eps)
    for i, ep in enumerate(eps):
        r = http.req("POST", ep, data={"data": q}, retries=2)
        if not r or r.code != 200:
            if r and r.code >= 500:
                time.sleep(min(5, (2**i) + random.random()))
            continue
        try:
            data = r.json()
        except Exception:
            continue
        out = []
        for el in data.get("elements", []) or []:
            tags = el.get("tags", {}) or {}
            name = (tags.get("name") or "").strip()
            if not name:
                continue
            out.append({
                "state": abbr,
                "hotel_name": name,
                "city": (tags.get("addr:city") or tags.get("is_in:city") or "").strip(),
                "website": (tags.get("website") or tags.get("contact:website") or "").strip(),
                "wikidata": (tags.get("wikidata") or "").strip(),
            })
        return out
    return []


def wikidata_sites(http: HTTP, qid: str) -> List[str]:
    r = http.req("GET", f"https://www.wikidata.org/wiki/Special:EntityData/{qid}.json", retries=3)
    if not r or r.code != 200:
        return []
    try:
        ent = r.json().get("entities", {}).get(qid, {})
        claims = ent.get("claims", {})
    except Exception:
        return []
    urls = []
    for c in claims.get("P856", []) or []:
        v = (((c.get("mainsnak", {}) or {}).get("datavalue", {}) or {}).get("value"))
        if isinstance(v, str) and v.strip():
            urls.append(v.strip())
    return urls


def places_site(http: HTTP, api_key: str, q: str) -> Optional[str]:
    r = http.req("GET", "https://maps.googleapis.com/maps/api/place/textsearch/json", params={"query": q, "key": api_key}, retries=2)
    if not r or r.code != 200:
        return None
    try:
        res = (r.json().get("results") or [])
        pid = (res[0] or {}).get("place_id") if res else None
    except Exception:
        return None
    if not pid:
        return None
    d = http.req("GET", "https://maps.googleapis.com/maps/api/place/details/json",
                 params={"place_id": pid, "fields": "website", "key": api_key}, retries=2)
    if not d or d.code != 200:
        return None
    try:
        w = d.json().get("result", {}).get("website")
        return w.strip() if isinstance(w, str) and w.strip() else None
    except Exception:
        return None


def ddg(http: HTTP, q: str) -> List[str]:
    r = http.req("GET", "https://duckduckgo.com/html/", params={"q": q}, retries=2)
    if not r or r.code != 200:
        return []
    h = r.text
    urls = [unquote(x) for x in re.findall(r'uddg=([^"&]+)', h, flags=re.I)]
    urls += [x for x in re.findall(r'href="(https?://[^"]+)"', h, flags=re.I) if "duckduckgo.com" not in x]
    # de-dupe keep order
    seen, out = set(), []
    for u in urls:
        u = (u or "").strip()
        if not u or u in seen:
            continue
        seen.add(u)
        out.append(u)
    time.sleep(CFG["ddg_delay_min"] + random.random() * max(0.0, CFG["ddg_delay_max"] - CFG["ddg_delay_min"]))
    return out[:15]


def candidates(http: HTTP, cache: Cache, rec: Dict[str, str], state_name: str) -> List[str]:
    out: List[str] = []
    if rec.get("website"):
        out.append(rec["website"])

    qid = rec.get("wikidata")
    if qid:
        out += wikidata_sites(http, qid)

    api_key = os.environ.get("GOOGLE_PLACES_API_KEY")
    if api_key:
        q = f"{rec.get('hotel_name','')} {rec.get('city','')} {state_name}".strip()
        ck = f"places::{q}"
        cached = cache.get(ck)
        if isinstance(cached, str) and cached.strip():
            out.append(cached.strip())
        else:
            s = places_site(http, api_key, q)
            if s:
                cache.set(ck, s)
                out.append(s)

    q = f"{rec.get('hotel_name','')} {rec.get('city','')} {rec.get('state','')} official site".strip()
    ck = f"ddg::{q}"
    cached = cache.get(ck)
    if isinstance(cached, list):
        out += [str(x) for x in cached if str(x).strip()]
    else:
        links = ddg(http, q)
        cache.set(ck, links)
        out += links

    # normalize + dedupe
    seen, ded = set(), []
    for u in out:
        nu = norm_url(u)
        if not nu or nu in seen:
            continue
        seen.add(nu)
        ded.append(nu)
    return ded


def write_csv(path: str, domains: List[str]) -> None:
    ensure(os.path.dirname(path))
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["Domain"])
        for d in domains:
            w.writerow([d])


def process_state(http: HTTP, cache: Cache, abbr: str, name: str) -> List[str]:
    recs = fetch_osm(http, abbr)
    if not recs:
        return []
    per = int(CFG["per_state"])
    lock = threading.Lock()
    out: List[str] = []
    seen: Set[str] = set()
    save_ctr = 0

    def work(rec: Dict[str, str]) -> None:
        nonlocal save_ctr
        for u in candidates(http, cache, rec, name):
            d = domain(u)
            if blacklisted(d):
                continue
            with lock:
                if d in seen:
                    continue
                if len(out) >= per:
                    return
                seen.add(d)
                out.append(d)
                save_ctr += 1
                if save_ctr % max(1, int(CFG["save_interval"])) == 0:
                    cache.save()

    with cf.ThreadPoolExecutor(max_workers=max(4, int(CFG["workers"]))) as pool:
        futs = [pool.submit(work, r) for r in recs]
        for f in cf.as_completed(futs):
            _ = f.result()
            if len(out) >= per:
                break

    return sorted(set(out))[:per]


def main() -> int:
    bd = base_dir()
    out_dir = os.path.join(bd, "state_outputs")
    cache_dir = os.path.join(bd, "cache_domains")
    ensure(out_dir); ensure(cache_dir)

    http = HTTP(timeout=float(CFG["timeout"]))
    cache = Cache(cache_dir)

    all_dom: Set[str] = set()

    for abbr, name in STATE.items():
        print(f"Processing {abbr} - {name}", flush=True)
        doms = process_state(http, cache, abbr, name)
        if not doms:
            print(f"No domains for {abbr}", flush=True)
            continue
        for d in doms:
            all_dom.add(d)
        p = os.path.join(out_dir, f"{abbr}_domains_{len(doms)}.csv")
        write_csv(p, doms)
        cache.save()
        print(f"Saved {len(doms)} domains for {abbr}", flush=True)

    master = os.path.join(bd, "us_hotels_domains_all.csv")
    write_csv(master, sorted(all_dom))
    cache.save()
    print(f"Done. {len(all_dom)} unique domains -> {master}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
