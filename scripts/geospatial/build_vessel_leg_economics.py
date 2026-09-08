#!/usr/bin/env python3
"""Latest voyage-leg economics per IMO for the Tracking tab.

Reads data/geospatial/vessel_voyage_tracks_master.csv (real recorded legs:
arrival/departure dates, transit_days, distance_nm) and emits
data/derived/vessel_leg_economics.json keyed by IMO string:

  {imo: {transit_days, distance_nm, avg_speed_kn, leg_port, leg_date}}

avg_speed_kn is DERIVED (distance_nm / transit_days / 24) and omitted when
either input is missing/non-positive. Latest leg = max arrival_date per IMO.
No synthesis: IMOs without a usable leg are absent from the output, and the
frontend treats absence as 'no leg data'.

Idempotent: same input rows -> byte-identical output (sorted keys).
"""
import json
import os

import pandas as pd

MASTER = os.path.join("data", "geospatial", "vessel_voyage_tracks_master.csv")
OUT = os.path.join("data", "derived", "vessel_leg_economics.json")


def main():
    print(f"Loading {MASTER}...")
    df = pd.read_csv(
        MASTER,
        usecols=["imo_number", "portname", "arrival_date", "transit_days", "distance_nm"],
    )
    df["imo_number"] = df["imo_number"].astype(str).str.strip()
    df["arrival_date"] = pd.to_datetime(df["arrival_date"], errors="coerce")
    df["transit_days"] = pd.to_numeric(df["transit_days"], errors="coerce")
    df["distance_nm"] = pd.to_numeric(df["distance_nm"], errors="coerce")
    df = df.dropna(subset=["imo_number"])
    df = df[df["imo_number"] != ""].sort_values(["imo_number", "arrival_date"])

    out = {}
    for imo, g in df.groupby("imo_number"):
        leg = g.iloc[-1]
        entry = {}
        td = leg["transit_days"]
        dn = leg["distance_nm"]
        if pd.notna(td) and float(td) > 0:
            entry["transit_days"] = round(float(td), 1)
        if pd.notna(dn) and float(dn) > 0:
            entry["distance_nm"] = round(float(dn), 1)
        if "transit_days" in entry and "distance_nm" in entry:
            entry["avg_speed_kn"] = round(entry["distance_nm"] / entry["transit_days"] / 24, 1)
        port = str(leg["portname"]).strip() if pd.notna(leg["portname"]) else ""
        if port and port.lower() != "nan":
            entry["leg_port"] = port
        if pd.notna(leg["arrival_date"]):
            entry["leg_date"] = leg["arrival_date"].strftime("%Y-%m-%d")
        if entry:
            out[str(imo)] = entry

    # Flat IMO -> leg map (no meta wrapper: the frontend looks up DATA.voyageLegs[imo]
    # directly; provenance lives in this script's docstring + workflow logs).
    # Idempotent: same input rows -> byte-identical output (sorted keys).
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(out, f, sort_keys=True, separators=(",", ":"))
    print(f"Generated {OUT}: {len(out)} IMOs.")


if __name__ == "__main__":
    main()
