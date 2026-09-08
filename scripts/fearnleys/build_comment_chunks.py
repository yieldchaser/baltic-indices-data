#!/usr/bin/env python3
"""Chunked broker-comment archive for lazy loading in the Fearnleys desk.

data/derived/fearnleys_broker_comments.csv (11.7k rows, ~6MB) is too heavy
for the initial page load, so the desk ships the latest 40 via
fearnleys_summary.json and lazy-loads these per-desk chunks on demand:

  data/derived/fearnleys_comments_{tanker,dry,gas,snp}.json

Records are compact [{d: date, t: type, n: name, x: text}], newest first,
byte-identical re-runs (sorted keys, fixed order). No text is altered.
"""
import json
import os

import pandas as pd

SRC = os.path.join("data", "derived", "fearnleys_broker_comments.csv")
OUT_DIR = os.path.join("data", "derived")

TANKER = {"CROSS MED", "WAFR/UKC", "WAFR/USG", "BITR-1", "BITR-2", "BITR-3",
          "BOT/WEST", "MEG/EAST", "CEYHAN/USG", "BLSEA/MED",
          "Suezmax Weekly Comment", "Aframax Weekly Comment",
          "VLCC Weekly Comment", "Tank Activity"}
DRY = {"Panamax Weekly Comment", "Capesize Weekly Comment",
       "Supramax Weekly Comment", "Chartering Weekly Comment",
       "Dry Bulk Activity", "Container Activity"}
GAS = {"LNG Market Report", "Gas Market Weekly Comment - Eastern Market",
       "Gas Market Weekly Comment - Western Market", "Gas Market Report",
       "Gas Market Weekly Comment - MEG", "Gas Market Weekly Comment - FE",
       "Gas Market Weekly Comment - Americas", "Daily BLPG Report",
       "LNG Activity", "LPG Activity"}

DESKS = [("tanker", TANKER), ("dry", DRY), ("gas", GAS)]


def main():
    print(f"Loading {SRC}...")
    df = pd.read_csv(SRC, usecols=["date", "comment_type", "comment_name", "text"])
    df["date"] = df["date"].astype(str).str.strip()
    df["text"] = df["text"].fillna("").astype(str)
    df = df[(df["date"] != "") & (df["text"] != "")]
    df = df.sort_values("date", ascending=False)

    assigned = set()
    for desk, types in DESKS:
        sub = df[df["comment_type"].isin(types)]
        assigned |= set(sub.index)
        recs = [{"d": r.date, "t": r.comment_type, "n": str(r.comment_name or ""),
                 "x": r.text} for r in sub.itertuples()]
        path = os.path.join(OUT_DIR, f"fearnleys_comments_{desk}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(recs, f, separators=(",", ":"))
        print(f"  {desk}: {len(recs)} comments -> {path}")

    rest = df.loc[~df.index.isin(assigned)]
    recs = [{"d": r.date, "t": r.comment_type, "n": str(r.comment_name or ""),
             "x": r.text} for r in rest.itertuples()]
    path = os.path.join(OUT_DIR, "fearnleys_comments_snp.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(recs, f, separators=(",", ":"))
    print(f"  snp+rest: {len(recs)} comments -> {path}")
    print(f"Total: {len(df)} comments chunked.")


if __name__ == "__main__":
    main()
