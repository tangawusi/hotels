#!/usr/bin/env python3
from __future__ import annotations

import csv
import gzip
import json
import os
import random
import re
import time
from typing import Dict, List, Optional, Sequence, Set
from urllib import request as ur
from urllib import error as ue
from urllib.parse import urlparse

# --- States ---
STATE: Dict[str, str] = {
    "AL":"Alabama","AK":"Alaska","AZ":"Arizona","AR":"Arkansas","CA":"California","CO":"Colorado","CT":"Connecticut",
    "DE":"Delaware","FL":"Florida","GA":"Georgia","HI":"Hawaii","ID":"Idaho","IL":"Illinois","IN":"Indiana","IA":"Iowa",
    "KS":"Kansas","KY":"Kentucky","LA":"Louisiana","ME":"Maine","MD":"Maryland","MA":"Massachusetts","MI":"Michigan",
    "MN":"Minnesota","MS":"Mississippi","MO":"Missouri","MT":"Montana","NE":"Nebraska","NV":"Nevada","NH":"New Hampshire",
    "NJ":"New Jersey","NM":"New Mexico","NY":"New York","NC":"North Carolina","ND":"North Dakota","OH":"Ohio","OK":"Oklahoma",
    "OR":"Oregon","PA":"Pennsylvania","RI":"Rhode Island","SC":"South Carolina","SD":"South Dakota","TN":"Tennessee",
    "TX":"Texas","UT":"Utah","VT":"Vermont","VA":"Virginia","WA":"Washington","WV":"West Virginia","WI":"Wisconsin","WY":"Wyoming",
}

# --- Overpass endpoints (rotate) ---
OVERPASS: Sequence[str] = (
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass.openstreetmap.ru/api/interpreter",
)

TOURISM = ("hotel", "motel", "guest_house", "hostel", "resort", "apartment")

# --- Optional blacklist: keep on if you ONLY want property sites ---
BLACKLIST: Set[str] = {
    "booking.com","expedia.com","tripadvisor.com","hotels.com","priceline.com","agoda.com","kayak.com","trivago.com",
    "orbitz.com","travelocity.com","hotwire.com","airbnb.com","vrbo.com",
    "facebook.com","instagram.com","x.com","twitter.com","yelp.com",
}

CFG = {
    "per_state": 2000,      # best-effort cap per state
    "timeout": 60,          # network timeout
    "retries": 4,           # per endpoint
    "sleep_floor": 0.6,     # politeness between state queries
    "use_blacklist": True,  # set False if you want EVERYTHING
}


UA = "Mozilla/5.0 (compatible; HotelsDomainsOnly/2.0)"


def ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)


def domain_from_url(u: str) -> str:
    if not u:
        return ""
    u = u.strip()
    if not u:
        return ""
    if not re.match(r"^https?://", u, flags=re.I):
        u = "https://" + u
    d = (urlparse(u).netloc or "").lower()
    if d.startswith("www."):
        d = d[4:]
    d = d.split(":")[0].strip()
    return d


def is_blacklisted(d: str) -> bool:
    if not d:
        return True
    return any(d == b or d.endswith("." + b) for b in BLACKLIST)


def overpass_query(state_abbr: str) -> str:
    # Important: only fetch objects that ALREADY HAVE website tags.
    # This avoids “no domains” outcomes due to discovery failures.
    t = "|".join(TOURISM)
    return f"""
[out:json][timeout:120];
area["ISO3166-2"="US-{state_abbr}"]["admin_level"="4"]->.a;
(
  nwr["tourism"~"^({t})$"]["website"](area.a);
  nwr["tourism"~"^({t})$"]["contact:website"](area.a);
);
out tags 50000;
""".strip()


def http_post_form(url: str, form: Dict[str, str], timeout: int, retries: int) -> Optional[dict]:
    data = "&".join(f"{k}={ur.quote(v)}" for k, v in form.items()).encode("utf-8")
    headers = {
        "User-Agent": UA,
        "Accept": "application/json",
        "Accept-Encoding": "gzip, deflate",
        "Content-Type": "application/x-www-form-urlencoded",
    }

    for attempt in range(retries):
        try:
            req = ur.Request(url, data=data, headers=headers, method="POST")
            with ur.urlopen(req, timeout=timeout) as resp:
                raw = resp.read()
                enc = (resp.headers.get("Content-Encoding") or "").lower()
                if "gzip" in enc:
                    try:
                        raw = gzip.decompress(raw)
                    except OSError:
                        pass
                return json.loads(raw.decode("utf-8", errors="ignore"))
        except ue.HTTPError as e:
            # Many Overpass failures are 429/5xx — backoff and retry.
            code = getattr(e, "code", None)
            if code in (429, 502, 503, 504) or (isinstance(code, int) and code >= 500):
                time.sleep(min(8.0, (2 ** attempt) + random.random()))
                continue
            # Non-retryable-ish
            return None
        except Exception:
            time.sleep(min(8.0, (2 ** attempt) + random.random()))
    return None


def fetch_state_domains(state_abbr: str) -> List[str]:
    q = overpass_query(state_abbr)
    endpoints = list(OVERPASS)
    random.shuffle(endpoints)

    payload = None
    for ep in endpoints:
        payload = http_post_form(ep, {"data": q}, timeout=int(CFG["timeout"]), retries=int(CFG["retries"]))
        if payload and isinstance(payload.get("elements"), list):
            break

    if not payload:
        return []

    seen: Set[str] = set()
    out: List[str] = []

    for el in payload.get("elements", []):
        tags = el.get("tags") or {}
        w1 = (tags.get("website") or "").strip()
        w2 = (tags.get("contact:website") or "").strip()
        for w in (w1, w2):
            d = domain_from_url(w)
            if not d:
                continue
            if CFG["use_blacklist"] and is_blacklisted(d):
                continue
            if d in seen:
                continue
            seen.add(d)
            out.append(d)
            if len(out) >= int(CFG["per_state"]):
                return out

    return out


def write_csv(path: str, domains: List[str]) -> None:
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["Domain"])
        for d in domains:
            w.writerow([d])


def main() -> int:
    base = os.path.abspath(os.getcwd() if os.access(os.getcwd(), os.W_OK) else os.path.expanduser("~"))
    out_dir = os.path.join(base, "state_outputs_domains")
    ensure_dir(out_dir)

    all_domains: Set[str] = set()
    total_states = 0

    for abbr, name in STATE.items():
        total_states += 1
        print(f"[{total_states}/50] {abbr} {name} ...", flush=True)
        doms = fetch_state_domains(abbr)
        print(f"  -> {len(doms)} domains", flush=True)

        if doms:
            for d in doms:
                all_domains.add(d)
            write_csv(os.path.join(out_dir, f"{abbr}_domains_{len(doms)}.csv"), doms)

        time.sleep(CFG["sleep_floor"] + random.random() * 0.6)

    master = os.path.join(base, "us_hotels_domains_master.csv")
    write_csv(master, sorted(all_domains))
    print(f"\nDONE: {len(all_domains)} unique domains -> {master}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
