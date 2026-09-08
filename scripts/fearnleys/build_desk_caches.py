#!/usr/bin/env python3
"""Fearnleys desk caches (Wave-3): tenor, NB prices, asset curves, gas extra.

Reads repo-owned derived CSVs, emits four lazy-loaded section caches:
  data/derived/fearnleys_desk_tenor.json    (long tenor rows from time_charter_rates.csv)
  data/derived/fearnleys_nb_prices.json     (NB monthly by family from vessel_valuations.csv)
  data/derived/fearnleys_asset_curves.json  (age-ladder history + scrap floor per class)
  data/derived/fearnleys_gas_extra.json     (lng/lpg charter + spot glue)

No synthesis: every number is a real CSV row; gaps stay null. All outputs
sorted + round(2) for byte-identical idempotent rebuilds.
"""
import json
import os

import pandas as pd

TC = os.path.join("data", "derived", "time_charter_rates.csv")
VAL = os.path.join("data", "derived", "vessel_valuations.csv")
SCRAP = os.path.join("data", "derived", "scrappage_prices.csv")
LNG = os.path.join("data", "derived", "lng_charter_rates.csv")
LPG_TC = os.path.join("data", "derived", "lpg_charter_rates.csv")
LPG_SPOT = os.path.join("data", "derived", "lpg_spot_rates.csv")

OUT = os.path.join("data", "derived")

TENOR_COLS = {
    "capesize_4_6m_avg": ("Capesize", "4-6M"), "capesize_1y_avg": ("Capesize", "1Y"),
    "capesize_2y_avg": ("Capesize", "2Y"),
    "panamax_4_6m_avg": ("Panamax", "4-6M"), "panamax_1y_avg": ("Panamax", "1Y"),
    "panamax_2y_avg": ("Panamax", "2Y"),
    "supramax_1y_avg": ("Supramax", "1Y"), "handysize_1y_avg": ("Handysize", "1Y"),
    "vlcc_1y": ("VLCC", "1Y"), "vlcc_2y": ("VLCC", "2Y"), "vlcc_3y": ("VLCC", "3Y"),
    "suezmax_1y": ("Suezmax", "1Y"), "suezmax_2y": ("Suezmax", "2Y"),
    "aframax_1y": ("Aframax", "1Y"),
    "mr_1y_tc": ("MR", "1Y"), "mr_2y_tc": ("MR", "2Y"), "mr_3y_tc": ("MR", "3Y"),
}

FAMILIES = {
    "bulk": ["Capesize", "Panamax", "Kamsarmax", "Post-Panamax", "Supramax",
             "Ultramax", "Handysize", "Newcastlemax"],
    "tanker": ["VLCC", "Suezmax", "Aframax", "LR2", "LR1", "MR", "Handy", "Product"],
    "gas": ["LNG", "LPG"],
}


def dump(path, payload):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, separators=(",", ":"))
    print(f"  {path}: {round(os.path.getsize(path) / 1024, 1)} KB")


def build_tenor():
    df = pd.read_csv(TC)
    df["date"] = df["date"].astype(str).str.strip()
    df = df.sort_values("date")
    rows = []
    for col, (cls, tenor) in TENOR_COLS.items():
        if col not in df.columns:
            continue
        sub = df[["date", col]].dropna()
        for _, r in sub.iterrows():
            v = r[col]
            if pd.isna(v) or float(v) == 0:
                continue
            rows.append([r["date"], cls, tenor, round(float(v), 2)])
    payload = {"meta": {"source": TC, "rows": len(rows)}, "rows": rows}
    dump(os.path.join(OUT, "fearnleys_desk_tenor.json"), payload)
    return len(rows)


def build_nb_prices():
    df = pd.read_csv(VAL, usecols=["date", "category", "tenor_type", "vessel_class", "valuation_usd_m"])
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    nb = df[(df["category"].astype(str).str.upper() == "NEWBUILDING") &
            (df["tenor_type"].astype(str).str.upper() == "PRICES")]
    nb = nb[nb["valuation_usd_m"].notna() & (nb["valuation_usd_m"] > 0)]
    nb["ym"] = nb["date"].dt.strftime("%Y-%m")
    fam_of = {v: f for f, vs in FAMILIES.items() for v in vs}
    nb["family"] = nb["vessel_class"].map(fam_of).fillna("other")
    g = nb.groupby(["family", "vessel_class", "ym"])["valuation_usd_m"].mean().round(2)
    series = {}
    for (fam, cls), grp in g.groupby(level=[0, 1]):
        key = f"{fam}|{cls}"
        series[key] = sorted([[ym, float(v)] for (f, c, ym), v in grp.items()])
    payload = {"meta": {"source": VAL, "series": len(series),
                        "source_note": "Monthly means of real NB valuation rows; unit USD M."}, "series": series}
    dump(os.path.join(OUT, "fearnleys_nb_prices.json"), payload)
    return len(series)


def build_asset_curves():
    df = pd.read_csv(VAL, usecols=["date", "category", "tenor_type", "vessel_class", "valuation_usd_m"])
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    df = df[df["valuation_usd_m"].notna() & (df["valuation_usd_m"] > 0)]
    df["ym"] = df["date"].dt.strftime("%Y-%m")
    classes = {}
    for cls, grp in df.groupby("vessel_class"):
        series = {}
        for tenor, tg in grp.groupby("tenor_type"):
            m = tg.groupby("ym")["valuation_usd_m"].mean().round(2).sort_index()
            series[str(tenor)] = [[ym, float(v)] for ym, v in m.items()]
        if series:
            classes[str(cls)] = series
    scrap = {}
    if os.path.exists(SCRAP):
        sc = pd.read_csv(SCRAP)
        date_col = "date" if "date" in sc.columns else sc.columns[0]
        sc["ym"] = pd.to_datetime(sc[date_col], errors="coerce").dt.strftime("%Y-%m")
        val_cols = [c for c in sc.columns if c not in (date_col, "ym") and sc[c].dtype != object]
        for c in val_cols:
            m = sc[["ym", c]].dropna().groupby("ym")[c].mean().round(2).sort_index()
            scrap[str(c)] = [[ym, float(v)] for ym, v in m.items()]
    payload = {
        "meta": {"source": VAL, "classes": len(classes), "scrap_series": len(scrap),
                 "note": "Monthly means of real rows; tenor keys as published (resale/5y/10y/15y/NB)."},
        "classes": classes, "scrap": scrap,
    }
    dump(os.path.join(OUT, "fearnleys_asset_curves.json"), payload)
    return len(classes)


def build_gas_extra():
    def monthly(path, value_col, rename=None):
        if not os.path.exists(path):
            return {}
        df = pd.read_csv(path)
        dcol = "date" if "date" in df.columns else df.columns[0]
        df["ym"] = pd.to_datetime(df[dcol], errors="coerce").dt.strftime("%Y-%m")
        out = {}
        for c in df.columns:
            if c in (dcol, "ym") or df[c].dtype == object:
                continue
            m = df[["ym", c]].dropna().groupby("ym")[c].mean().round(2).sort_index()
            if len(m):
                key = (rename or {}).get(c, c)
                out[key] = [[ym, float(v)] for ym, v in m.items()]
        return out

    payload = {
        "lng_charter": monthly(LNG, None),
        "lpg_tc": monthly(LPG_TC, None),
        "lpg_spot": monthly(LPG_SPOT, None),
    }
    dump(os.path.join(OUT, "fearnleys_gas_extra.json"), payload)
    return sum(len(v) for v in payload.values())


if __name__ == "__main__":
    print("Building Fearnleys desk caches...")
    n1 = build_tenor()
    n2 = build_nb_prices()
    n3 = build_asset_curves()
    n4 = build_gas_extra()
    print(f"Done: tenor_rows={n1} nb_series={n2} asset_classes={n3} gas_series={n4}")
