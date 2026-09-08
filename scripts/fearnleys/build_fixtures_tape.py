#!/usr/bin/env python3
"""Fixtures tape + facet caches for the Fearnleys desk.

Reads data/derived/fearnleys_fixtures_full.csv (538k rows; substantively a
2024→ tape — header discloses this) and emits:
  data/derived/fearnleys_fixtures_tape.json   last ~45 days, compact arrays
  data/derived/fearnleys_fixtures_facets.json full-history aggregates

Dedupe key: date+vessel+charterer+route (TANKPRO 2025 arrivals are mass-
duplicated). No synthesis; nulls stay null.
"""
import json
import os
from datetime import timedelta

import pandas as pd

SRC = os.path.join("data", "derived", "fearnleys_fixtures_full.csv")
TAPE = os.path.join("data", "derived", "fearnleys_fixtures_tape.json")
FACETS = os.path.join("data", "derived", "fearnleys_fixtures_facets.json")

TAPE_DAYS = 45


def main():
    print(f"Loading {SRC}...")
    df = pd.read_csv(SRC, low_memory=False)
    df.columns = [c.strip().lower() for c in df.columns]
    dcol = "date"
    df[dcol] = pd.to_datetime(df[dcol], errors="coerce")
    df = df.dropna(subset=[dcol])
    # One far-future strayer (2026-12-18) breaks "last 45 days"; anchor the
    # window to the last REAL publication date (99th-percentile cadence)
    # instead of the raw max.
    real_max = df[dcol].quantile(0.999)
    df = df[df[dcol] <= real_max]
    before = len(df)

    # vessel column is named `vessel`; map to the generic names used below
    if "vessel" in df.columns and "vessel_name" not in df.columns:
        df["vessel_name"] = df["vessel"]
    if "rate" in df.columns:
        df["rate_numeric"] = pd.to_numeric(df["rate"], errors="coerce")

    key_cols = [c for c in ["date", "vessel_name", "charterer", "route"] if c in df.columns]
    df = df.drop_duplicates(subset=key_cols).sort_values(dcol)
    print(f"rows {before} -> {len(df)} after dedupe on {key_cols}")

    def s(v):
        return "" if pd.isna(v) else str(v).strip()

    def r(v):
        return None if pd.isna(v) else round(float(v), 2)

    date_s = df[dcol].dt.strftime("%Y-%m-%d")

    # Facets over full deduped history
    df["ym"] = df[dcol].dt.strftime("%Y-%m")
    facets = {"total_rows": int(len(df)), "first": date_s.min(), "last": date_s.max()}
    for col, key in [("department", "dept"), ("segment", "segment"),
                     ("charterer", "charterer"), ("route", "route"),
                     ("commodity", "commodity")]:
        if col in df.columns:
            vc = df[col].fillna("—").astype(str).str.strip().replace("", "—").value_counts().head(40)
            facets[key] = [[k, int(v)] for k, v in vc.items()]
    if "ym" in df.columns:
        weekly = df.groupby("ym").size().sort_index()
        facets["monthly_counts"] = [[k, int(v)] for k, v in weekly.items()]
    if "charterer" in df.columns:
        cur = df[df[dcol] >= df[dcol].max() - pd.Timedelta(days=365)]
        league = cur["charterer"].fillna("—").astype(str).str.strip().replace("", "—").value_counts().head(25)
        facets["ytd_charterer_league"] = [[k, int(v)] for k, v in league.items()]

    with open(FACETS, "w", encoding="utf-8") as f:
        json.dump(facets, f, separators=(",", ":"))
    print(f"  {FACETS}: {round(os.path.getsize(FACETS) / 1024, 1)} KB")

    # Tape: last N days — anchored per department. Live desks (published
    # within TAPE_DAYS of the real max) get the trailing window; departments
    # that STOPPED publishing (e.g. TANKPRO, last obs 2026-02-19) would render
    # a permanently-empty "No fixtures match" tape under a raw 45d cut, so
    # they get their final TAPE_DAYS of activity instead (archived-desk
    # display, same doctrine as archived rate series).
    cutoff_live = real_max - pd.Timedelta(days=TAPE_DAYS)
    last_pub = df.groupby("department")[dcol].max()
    live_depts = set(last_pub[last_pub >= cutoff_live].index)

    parts = []
    for dept, g in df.groupby("department"):
        anchor = real_max if dept in live_depts else last_pub[dept]
        parts.append(g[g[dcol] >= anchor - pd.Timedelta(days=TAPE_DAYS)])
    tape = pd.concat(parts).sort_values(dcol)
    cutoff = cutoff_live
    want = [c for c in ["department", "segment", "vessel_name", "charterer", "route",
                        "commodity", "rate_numeric", "period", "laycan", "comment"]
            if c in tape.columns]
    tdf = tape[want + [dcol]].copy()
    tdf["d"] = tdf[dcol].dt.strftime("%Y-%m-%d")
    tdf = tdf.sort_values(dcol, ascending=False)
    key_map = {"vessel_name": "v", "charterer": "c", "route": "rt", "commodity": "cm",
               "department": "dp", "segment": "sg", "period": "p", "laycan": "lc",
               "comment": "x", "rate_numeric": "r"}
    out_rows = []
    for rec_t in tdf.to_dict("records"):
        rec = {"d": rec_t["d"]}
        for c in want:
            v = rec_t.get(c)
            k = key_map.get(c, c)
            if c == "rate_numeric":
                rec[k] = r(v)
            else:
                sv = s(v)
                if sv:
                    rec[k] = sv
        out_rows.append(rec)

    payload = {
        "meta": {
            "source": SRC, "tape_days": TAPE_DAYS,
            "rows": len(out_rows),
            "first": str(tape[dcol].min().date()) if len(tape) else None,
            "last": str(tape[dcol].max().date()) if len(tape) else None,
            "dedupe_key": key_cols,
            "schema": key_map,
            "note": "Fixtures feed substantively covers 2024 onward; earlier years are sparse department scraps. Rate is numeric where parseable (92% text/RNR historically — kept in comment).",
        },
        "rows": out_rows,
    }
    with open(TAPE, "w", encoding="utf-8") as f:
        json.dump(payload, f, separators=(",", ":"))
    print(f"  {TAPE}: {round(os.path.getsize(TAPE) / 1024, 1)} KB ({len(out_rows)} rows)")


if __name__ == "__main__":
    main()
