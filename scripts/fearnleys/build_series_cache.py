#!/usr/bin/env python3
"""Per-label monthly series cache for the Fearnleys desk tab.

Reads scripts/fearnleys_rates_full.csv (real Hasura-sourced long history:
label, rate_type, rate_subtype, route, unit, date, rate) and emits
data/derived/fearnleys_series_monthly.json:

  {label: {type, subtype, route, unit, n, first, last, latest, prev,
           chg_pct, ath, atl, ath_date, pct_rank, m: [[yyyymm, avg], ...]}}

- monthly means (rounded 2dp), sorted ascending; idempotent rebuilds.
- pct_rank = percentile rank of latest vs full monthly history (0-100).
- chg_pct = latest vs previous month (null when <2 months).
- No synthesis: labels with <1 month are skipped; gaps left as gaps.

The browser cannot load the 22MB full file; this ~2MB cache carries
everything the desk charts need. Catalog breadth (356 ids) vs cached
labels is reported in meta for the coverage note.
"""
import json
import os

import pandas as pd

SRC = os.path.join("scripts", "fearnpulse_rates_full.csv")
CATALOG = os.path.join("data", "derived", "fearnleys_catalog.csv")
OUT = os.path.join("data", "derived", "fearnleys_series_monthly.json")


def main():
    print(f"Loading {SRC}...")
    df = pd.read_csv(SRC, usecols=["label", "rate_type", "rate_subtype",
                                   "route", "unit", "date", "rate"])
    df["label"] = df["label"].astype(str).str.strip()
    df["date"] = df["date"].astype(str).str.strip()
    df["rate"] = pd.to_numeric(df["rate"], errors="coerce")
    df = df.dropna(subset=["label", "rate"])
    df = df[df["label"] != ""]
    df["ym"] = df["date"].str.slice(0, 7)

    meta_rows = df.drop_duplicates("label").set_index("label")
    monthly = df.groupby(["label", "ym"])["rate"].mean().round(2)

    labels = {}
    for label, grp in monthly.groupby(level=0):
        pts = sorted((ym, float(v)) for ym, v in grp.droplevel(0).items())
        vals = [v for _, v in pts]
        meta = meta_rows.loc[label]
        latest, prev = vals[-1], (vals[-2] if len(vals) > 1 else None)
        lo, hi = min(vals), max(vals)
        below = sum(1 for v in vals if v < latest)
        labels[str(label)] = {
            "type": str(meta["rate_type"]),
            "subtype": str(meta["rate_subtype"]),
            "route": str(meta["route"]),
            "unit": str(meta["unit"]),
            "n": len(pts),
            "first": pts[0][0],
            "last": pts[-1][0],
            "latest": latest,
            "prev": prev,
            "chg_pct": round((latest - prev) / abs(prev) * 100, 2) if prev else None,
            "ath": hi,
            "atl": lo,
            "ath_date": pts[vals.index(hi)][0],
            "pct_rank": round(below / len(vals) * 100, 1),
            "m": [[ym, v] for ym, v in pts],
        }

    try:
        cat = pd.read_csv(CATALOG, usecols=["id"])
        catalog_ids = int(len(cat))
    except Exception:
        catalog_ids = None

    payload = {
        "meta": {
            "source": SRC,
            "labels_cached": len(labels),
            "catalog_ids": catalog_ids,
            "monthly_points": int(sum(v["n"] for v in labels.values())),
        },
        "labels": labels,
    }
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(payload, f, separators=(",", ":"))
    kb = round(os.path.getsize(OUT) / 1024, 1)
    print(f"Generated {OUT}: {len(labels)} labels ({kb} KB).")


if __name__ == "__main__":
    main()
