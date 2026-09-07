#!/usr/bin/env python3
"""
Build Vessel Trajectories Cache for Tracking Terminal
Extracts multi-year historical voyage trajectories for active lineup vessels
into a high-speed JSON lookup for the Leaflet tracking engine.
"""

import json
import os
import pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
LINEUPS_PATH = os.path.join(ROOT, 'data', 'geospatial', 'port_lineups_active.csv')
VECTORS_PATH = os.path.join(ROOT, 'data', 'geospatial', 'ui_voyage_vectors.csv')
OUT_PATH = os.path.join(ROOT, 'data', 'geospatial', 'vessel_trajectories_active.json')

def main():
    if not os.path.exists(LINEUPS_PATH) or not os.path.exists(VECTORS_PATH):
        print(f"Error: Missing input files in {ROOT}/data/geospatial/")
        return

    lineups = pd.read_csv(LINEUPS_PATH)
    vectors = pd.read_csv(VECTORS_PATH)

    active_imos = set(lineups['imo_number'].dropna())
    sub = vectors[vectors['imo_number'].isin(active_imos)]

    out = {}
    for _, row in sub.iterrows():
        imo = row['imo_number']
        try:
            traj = json.loads(row['trajectory_sequence_json'])
            out[imo] = traj
        except Exception:
            pass

    with open(OUT_PATH, 'w', encoding='utf-8') as f:
        json.dump(out, f, separators=(',', ':'))

    print(f"Successfully compiled {OUT_PATH}: {len(out)} active vessels, {os.path.getsize(OUT_PATH)/1024:.1f} KB.")

if __name__ == '__main__':
    main()
