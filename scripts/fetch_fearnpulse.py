"""
fetch_fearnpulse.py
Pulls historical spot rate time series from Fearnleys' public Hasura
GraphQL endpoint (the same backend that serves fearnpulse.com).

Confirmed via browser devtools (Sep 2026):
  - Endpoint: https://pbrokerapp.hasura.app/v1/graphql
  - No auth header / API key required for these queries. The CORS
    allow-list in the OPTIONS preflight scopes browser access to
    https://fearnpulse.com, but that's a browser-only restriction --
    it does not block server-to-server calls from this script.
  - A single query with a wide dateFrom/dateTo window returns every
    point in that window in one call (confirmed: requesting roughly
    the last year returned the full year, no pagination). This is a
    one-time backfill + light weekly top-up, not an incremental
    weekly scrape.

QUERY confirmed via "View source" on the raw request payload (Sep 2026)
-- rate_type / rate_subtype / route are nested inside an `info` object
on rate_meta; rate_unit sits directly on rate_meta. An earlier version
of this script had all four flat on rate_meta, which Hasura rejected
with "field 'route' not found in type: 'rate_meta_bool_exp'". Fixed.

Known gap: ROUTES is seeded only from the LPG Rates panel visible on
screen. Fearnleys' site also covers tanker / dry bulk rates elsewhere
on the page (visible in the background of one screenshot: "VLCC /
Strait of Hormuz...") with different rate_type values. Add those once
you've got their exact route strings the same way you got the LPG
ones -- the query shape below should work unchanged for any sector.
"""

import csv
import time
from datetime import date

import requests

ENDPOINT = "https://pbrokerapp.hasura.app/v1/graphql"

QUERY = """
query GetRatesByMetaForRange($dateFrom: date!, $dateTo: date!, $rateType: String!, $rateSubtype: String!, $route: [String!]!, $rateUnit: String = "usd") {
  rate_meta(
    where: {info: {rate_type: {_eq: $rateType}, rate_subtype: {_eq: $rateSubtype}, route: {_in: $route}}, rate_unit: {_eq: $rateUnit}}
  ) {
    rates(where: {date: {_gte: $dateFrom, _lte: $dateTo}}, order_by: {date: desc}) {
      date
      rate
      __typename
    }
    __typename
  }
}
"""

# (label, rate_type, rate_subtype, route, rate_unit)
#
# Everything below is scoped deliberately to what's confirmed to appear
# on fearnpulse.com's own public weekly report (cross-checked against
# the printed PDF, Week 36 2026) -- not everything the --catalog dump
# surfaced. The catalog has ~360 series total; this list uses ~62 of
# them. Left out on purpose (all confirmed present in the catalog, all
# excluded by choice, not by accident):
#   - NEWBUILDING/PRICES: container TEU prices (1,900-21,000 TEU),
#     VLGC/MGC LPG newbuilding prices -- not on the printed report
#   - S&P: Chinese/Japanese buyer-market splits (DRY-5-CN/JP etc.),
#     WET-RESALE, DRY-RESALE -- not on the printed report
#   - BULK/TC: 'Capesize (182 000 dwt)' -- the printed report's
#     Capesize row is explicitly labeled "180'", so the 180k variant
#     below is the one that matches; 182k is a real series but a
#     different, unpublished one
#   - The entire LNG desk, full Baltic tanker route/TCE matrices
#     (BITR-1-4, TD1/TD3C/TD6/TD7/TD17/TD19/TD-20), and everything
#     else in the 360-series catalog not itemized below
ROUTES = [
    # ---- LPG Spot, USD/Month (confirmed via --discover) ----
    ("LPG_SPOT_ETH",             "LPG", "SPOT", "ETH (8-12 000 cbm)", "usd"),
    ("LPG_SPOT_HDY_SR",          "LPG", "SPOT", "HDY SR (20-22 000 cbm)", "usd"),
    ("LPG_SPOT_LGC",             "LPG", "SPOT", "LGC (60 000 cbm)", "usd"),
    ("LPG_SPOT_MGC",             "LPG", "SPOT", "MGC (38 000 cbm)", "usd"),
    ("LPG_SPOT_COASTER_EUROPE",  "LPG", "SPOT", "COASTER Europe (3 500-5 000 cbm)", "usd"),
    ("LPG_SPOT_COASTER_ASIA",    "LPG", "SPOT", "COASTER Asia", "usd"),
    ("LPG_SPOT_HDY_ETH",         "LPG", "SPOT", "HDY ETH (21-22 000 cbm)", "usd"),
    ("LPG_SPOT_SR",              "LPG", "SPOT", "SR (6 500 cbm)", "usd"),
    ("LPG_SPOT_VLGC",            "LPG", "SPOT", "VLGC (84 000 cbm)", "usd"),

    # ---- LPG FOB Propane / Butane, USD/Tonne (confirmed via --catalog) ----
    ("LPG_FOB_PROPANE_NSEA",      "LPG", "LPG/FOB-PROPANE", "FOB North Sea/ANSI", "usd"),
    ("LPG_FOB_PROPANE_SAUDI",     "LPG", "LPG/FOB-PROPANE", "Saudi Arabia/CP", "usd"),
    ("LPG_FOB_PROPANE_BELVIEU",   "LPG", "LPG/FOB-PROPANE", "MT Belvieu (US Gulf)", "usd"),
    ("LPG_FOB_PROPANE_SONATRACH", "LPG", "LPG/FOB-PROPANE", "Sonatrach/Bethioua", "usd"),
    ("LPG_FOB_BUTANE_NSEA",       "LPG", "LPG/FOB-BUTANE", "FOB North Sea/ANSI", "usd"),
    ("LPG_FOB_BUTANE_SAUDI",      "LPG", "LPG/FOB-BUTANE", "Saudi Arabia/CP", "usd"),
    ("LPG_FOB_BUTANE_BELVIEU",    "LPG", "LPG/FOB-BUTANE", "MT Belvieu (US Gulf)", "usd"),
    ("LPG_FOB_BUTANE_SONATRACH",  "LPG", "LPG/FOB-BUTANE", "Sonatrach/Bethioua", "usd"),

    # ---- Tanker Dirty spot, Worldscale -- matches PDF's 9 routes exactly ----
    ("TANK_DIRTY_MEG_WEST",      "TANK", "Dirty", "MEG/WEST", "ws"),
    ("TANK_DIRTY_MEG_JAPAN",     "TANK", "Dirty", "MEG/Japan", "ws"),
    ("TANK_DIRTY_MEG_SINGAPORE", "TANK", "Dirty", "MEG/Singapore", "ws"),
    ("TANK_DIRTY_WAF_FEAST",     "TANK", "Dirty", "WAF/FEAST", "ws"),
    ("TANK_DIRTY_WAF_USAC",      "TANK", "Dirty", "WAF/USAC", "ws"),
    ("TANK_DIRTY_SIDI_KERIR",    "TANK", "Dirty", "Sidi Kerir/W Med", "ws"),
    ("TANK_DIRTY_N_AFR_EUROMED", "TANK", "Dirty", "N. Afr/Euromed", "ws"),
    ("TANK_DIRTY_UK_CONT",       "TANK", "Dirty", "UK/Cont", "ws"),
    ("TANK_DIRTY_CARIBS_USG",    "TANK", "Dirty", "Caribs/USG", "ws"),

    # ---- Tanker 1 Year T/C, USD/Day -- matches PDF's ECO/SCRUBBER section ----
    ("TANK_1YRTC_VLCC",    "TANK", "1 Year T/C", "VLCC", "usd"),
    ("TANK_1YRTC_SUEZMAX", "TANK", "1 Year T/C", "Suezmax", "usd"),
    ("TANK_1YRTC_AFRAMAX", "TANK", "1 Year T/C", "Aframax", "usd"),

    # ---- VLCC weekly activity counts -- matches PDF's "VLCCs" section ----
    ("TANK_VLCC_FIXED_LASTWEEK", "TANK", "WEEKLY VLCC", "VLCCs fixed in all areas last week", "usd"),
    ("TANK_VLCC_AVAIL_MEG_30D",  "TANK", "WEEKLY VLCC", "VLCCs available in MEG next 30 days", "usd"),

    # ---- Dry Bulk 1 Year T/C, USD/Day -- matches "1 Year T/C Dry Bulk" ----
    ("BULK_TC_CAPESIZE",     "BULK", "TC", "Capesize (180 000 dwt)", "usd"),
    ("BULK_TC_NEWCASTLEMAX", "BULK", "TC", "Newcastlemax (208 000 dwt)", "usd"),
    ("BULK_TC_PANAMAX",      "BULK", "TC", "Panamax (75 000 dwt)", "usd"),
    ("BULK_TC_KAMSARMAX",    "BULK", "TC", "Kamsarmax (82 000 dwt)", "usd"),
    ("BULK_TC_SUPRAMAX",     "BULK", "TC", "Supramax (58 000 dwt)", "usd"),
    ("BULK_TC_ULTRAMAX",     "BULK", "TC", "Ultramax (64 000 dwt)", "usd"),
    ("BULK_TC_HANDYSIZE",    "BULK", "TC", "Handysize (38 000 dwt)", "usd"),

    # ---- Newbuilding prices, USD millions -- matches the printed "Prices" table ----
    ("NB_VLCC",         "NEWBUILDING", "PRICES", "VLCC", "usd"),
    ("NB_SUEZMAX",      "NEWBUILDING", "PRICES", "Suezmax", "usd"),
    ("NB_AFRAMAX",      "NEWBUILDING", "PRICES", "Aframax", "usd"),
    ("NB_PRODUCT",      "NEWBUILDING", "PRICES", "Product", "usd"),
    ("NB_NEWCASTLEMAX", "NEWBUILDING", "PRICES", "Newcastlemax", "usd"),
    ("NB_KAMSARMAX",    "NEWBUILDING", "PRICES", "Kamsarmax", "usd"),
    ("NB_ULTRAMAX",     "NEWBUILDING", "PRICES", "Ultramax", "usd"),
    ("NB_LNGC_MEGI",    "NEWBUILDING", "PRICES", "LNGC (MEGI) (cbm)", "usd"),

    # ---- S&P secondhand prices, USD millions -- matches the printed "Prices" table ----
    ("SP_DRY5_CAPESIZE",    "S&P", "DRY-5", "Capesize", "usd"),
    ("SP_DRY5_KAMSARMAX",   "S&P", "DRY-5", "Kamsarmax", "usd"),
    ("SP_DRY5_ULTRAMAX",    "S&P", "DRY-5", "Ultramax", "usd"),
    ("SP_DRY5_HANDYSIZE",   "S&P", "DRY-5", "Handysize", "usd"),
    ("SP_DRY10_CAPESIZE",   "S&P", "DRY-10", "Capesize", "usd"),
    ("SP_DRY10_KAMSARMAX",  "S&P", "DRY-10", "Kamsarmax", "usd"),
    ("SP_DRY10_ULTRAMAX",   "S&P", "DRY-10", "Ultramax", "usd"),
    ("SP_DRY10_HANDYSIZE",  "S&P", "DRY-10", "Handysize", "usd"),
    ("SP_WET5_MR",          "S&P", "WET-5", "MR", "usd"),
    ("SP_WET5_AFRAMAX_LR2", "S&P", "WET-5", "Aframax / LR2", "usd"),
    ("SP_WET5_SUEZMAX",     "S&P", "WET-5", "Suezmax", "usd"),
    ("SP_WET5_VLCC",        "S&P", "WET-5", "VLCC", "usd"),
    ("SP_WET10_MR",         "S&P", "WET-10", "MR", "usd"),
    ("SP_WET10_AFRAMAX_LR2","S&P", "WET-10", "Aframax / LR2", "usd"),
    ("SP_WET10_SUEZMAX",    "S&P", "WET-10", "Suezmax", "usd"),
    ("SP_WET10_VLCC",       "S&P", "WET-10", "VLCC", "usd"),

    # ---- LNG, USD/Day -- confirmed by direct test: latest values matched
    # the printed PDF's $58,000 / $20,000 / $20,000 exactly ----
    ("LNG_1YRTC_MEGI_XDF",         "LNG", "BROKER", "1 yr TC MEGI / XDF", "usd"),
    ("LNG_SPOT_EAST_174K_2STROKE", "LNG", "BROKER", "Spot East 174k 2-stroke", "usd"),
    ("LNG_SPOT_WEST_174K_2STROKE", "LNG", "BROKER", "Spot West 174k 2-stroke", "usd"),

    # ---- NOT yet added: ----
    # Dry Bulk SPOT rates + Baltic Dry Index (TCE Cont/Far East,
    # Australia/China, Pacific RV, Transatlantic RV, BDI itself) --
    # NOT present anywhere in the 360-series catalog, and a text search
    # of every already-loaded response body found no match either. Very
    # likely a different transport entirely (possibly a WebSocket
    # subscription rather than a one-shot query) or a different backend
    # (a licensed Baltic Exchange feed rather than Fearnleys' own
    # rate_meta). See note below on how to check.
]

HEADERS = {
    "Content-Type": "application/json",
    # No auth/API-key header turned out to be needed -- the earlier
    # failure was a pure GraphQL schema-validation error (400 with a
    # clean error body), which only happens after the request is
    # accepted and parsed. If that were an auth problem it would have
    # shown up as a 401/403 instead.
}


def fetch_route(label, rate_type, rate_subtype, route, rate_unit,
                 date_from="1900-01-01", date_to=None):
    # date_from defaults absurdly early on purpose. A tighter guess like
    # "2000-01-01" (the earlier version of this script) risks silently
    # truncating any series whose real history goes back further -- no
    # error, just missing rows. Since an overly wide range costs nothing
    # (Hasura just returns whatever data actually exists, however far
    # back that goes), there's no reason to guess a floor at all.
    date_to = date_to or date.today().isoformat()
    payload = {
        "operationName": "GetRatesByMetaForRange",
        "query": QUERY,
        "variables": {
            "dateFrom": date_from,
            "dateTo": date_to,
            "rateType": rate_type,
            "rateSubtype": rate_subtype,
            "route": [route],
            "rateUnit": rate_unit,
        },
    }
    resp = requests.post(ENDPOINT, json=payload, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    body = resp.json()
    if "errors" in body:
        print(f"  [{label}] GraphQL error: {body['errors']}")
        return []
    rate_meta = body.get("data", {}).get("rate_meta", [])
    rows = []
    for meta in rate_meta:
        for r in meta.get("rates", []):
            rows.append({"label": label, "route": route, "date": r["date"], "rate": r["rate"]})
    return rows


def main():
    all_rows = []
    for label, rate_type, rate_subtype, route, rate_unit in ROUTES:
        print(f"Fetching {label} ({route})...")
        rows = fetch_route(label, rate_type, rate_subtype, route, rate_unit)
        print(f"  -> {len(rows)} rows"
              + ("  [EMPTY - check route string / query shape]" if not rows else ""))
        all_rows.extend(rows)
        time.sleep(0.5)  # polite pacing -- shared backend, not just fearnpulse's own traffic

    out_path = "fearnpulse_rates.csv"
    with open(out_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["label", "route", "date", "rate"])
        writer.writeheader()
        writer.writerows(all_rows)
    print(f"\nWrote {len(all_rows)} rows to {out_path}")


DISCOVERY_QUERY = """
query ListRouteOptions($rateType: String!, $rateSubtype: String!, $rateUnit: String = "usd") {
  rate_meta(
    where: {info: {rate_type: {_eq: $rateType}, rate_subtype: {_eq: $rateSubtype}}, rate_unit: {_eq: $rateUnit}}
  ) {
    info {
      route
      rate_type
      rate_subtype
      __typename
    }
    rate_unit
    __typename
  }
}
"""


def discover_routes(rate_type="LPG", rate_subtype="SPOT", rate_unit="usd"):
    """UNVERIFIED GUESS. Built on the assumption that `info` is
    selectable the same way it's filterable, since we now know it's a
    real nested object (confirmed by the working query -- unlike the
    earlier flat-field guess that failed). If this comes back with a
    GraphQL error, that assumption was wrong: fall back to clicking
    each remaining card on fearnpulse.com and capturing its `route`
    value via devtools Payload -> View source, same as was done for
    ETH and COASTER Europe."""
    payload = {
        "operationName": "ListRouteOptions",
        "query": DISCOVERY_QUERY,
        "variables": {"rateType": rate_type, "rateSubtype": rate_subtype, "rateUnit": rate_unit},
    }
    resp = requests.post(ENDPOINT, json=payload, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    body = resp.json()
    if "errors" in body:
        print(f"Discovery query failed (this was a guess -- expected outcome if wrong): {body['errors']}")
        print("Fall back to clicking each remaining card on fearnpulse.com and capturing")
        print("its `route` value via devtools Payload -> View source.")
        return []
    rows = body.get("data", {}).get("rate_meta", [])
    if not rows:
        print(f"Query succeeded but returned 0 routes for rate_type={rate_type!r}, "
              f"rate_subtype={rate_subtype!r} -- possibly the subtype/type strings "
              f"themselves need adjusting too.")
        return []
    print(f"Found {len(rows)} route(s) for rate_type={rate_type!r}, rate_subtype={rate_subtype!r}:")
    for r in rows:
        info = r["info"]
        print(f"  route={info['route']!r}")
    return rows


CATALOG_QUERY = """
query ListAllSeries {
  rate_meta {
    info {
      rate_type
      rate_subtype
      route
      __typename
    }
    rate_unit
    __typename
  }
}
"""


def list_all_series():
    """UNVERIFIED GUESS, one step past discover_routes(): drops the
    `where` argument on rate_meta entirely, on the assumption Hasura
    treats a missing `where` as "no filter" (the normal behavior for
    an auto-generated Hasura query field, where `where` is optional).
    If this works, it returns every (rate_type, rate_subtype, route,
    rate_unit) combination the whole fearnpulse.com backend has --
    tankers, dry bulk, gas, newbuilding, S&P, market brief, all of it
    -- in one call, no more card-clicking needed for any section.

    If it errors, `where` might be required after all: fall back to
    seeding discover_routes() with one confirmed rate_type per report
    section (one card click each, same as was done for LPG) instead.

    Note this only covers whatever actually lives in rate_meta/rates.
    A few things on the printed report are categorical or count-based
    rather than a $ value with a change (Newbuilding "Activity Levels":
    Strong/Increasing/Slow/Moderate; "Fixed in all areas last week" /
    "Available in MEG next 30 days" under VLCCs). Those are very
    likely a different query/table entirely and won't show up here
    even if this works cleanly for everything else."""
    payload = {
        "operationName": "ListAllSeries",
        "query": CATALOG_QUERY,
        "variables": {},
    }
    resp = requests.post(ENDPOINT, json=payload, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    body = resp.json()
    if "errors" in body:
        print(f"Catalog query failed (this was a guess): {body['errors']}")
        print("Fall back to per-section seeding: click one card in each report")
        print("section (Tankers, Dry Bulk, LPG FOB, LNG, Newbuilding, S&P, Market")
        print("Brief), capture its rate_type via Payload -> View source, then call")
        print("discover_routes(rate_type=..., rate_subtype=...) for each.")
        return []
    rows = body.get("data", {}).get("rate_meta", [])
    print(f"Found {len(rows)} total series across the whole backend:")
    seen = set()
    for r in rows:
        info = r["info"]
        key = (info["rate_type"], info["rate_subtype"], info["route"], r["rate_unit"])
        if key not in seen:
            seen.add(key)
            print(f"  rate_type={info['rate_type']!r:20} rate_subtype={info['rate_subtype']!r:14} "
                  f"route={info['route']!r:40} unit={r['rate_unit']!r}")
    return rows


if __name__ == "__main__":
    import sys
    if "--catalog" in sys.argv:
        list_all_series()
    elif "--discover" in sys.argv:
        discover_routes()
    else:
        main()

