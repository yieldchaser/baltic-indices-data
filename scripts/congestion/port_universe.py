#!/usr/bin/env python3
"""
Canonical Port Universe — Wave-1 single source of hub truth.

The hub list itself lives ONLY in scripts/compute_port_stress_matrix.py
(PORT_METADATA: 50 port-asset series across 41 physical UN/LOCODE hubs).
This module re-exports it (zero duplication) so that
scripts/congestion/build_port_stress_cache.py and tests validate against the
same source the matrix is built from.

Count reconciliation (the "41 vs 50" question):
  - 50 = monitored port-asset SERIES (16 Dry Bulk + 11 Tankers + 11 LPG + 12 LNG).
  - 41 = physical UN/LOCODE HUBS (9 ports repeat across asset classes:
         NLRTM, CNQDG, CNNGB, USHOU, USCRP, AUDAM, AUGLT, QARLF, NGBON).
  - build_port_stress_cache groups the matrix CSV by (locode, asset_class),
    so port_stress_summary.json carries 50 hubs entries; metadata.total_hubs
    and summary.total_monitored are therefore 50 when the input is complete.
  - Frontend (index.html) holds NO hardcoded hub list: it renders
    summary.hubs and falls back to `sm.total_monitored || 50` for the HUD
    total, which matches the 50-series canonical count. No index.html edits
    were needed for Wave-1.
  - scripts/geospatial/build_geospatial_tracker.py keeps its own 46-entry
    PORT_COORDINATES on purpose (voyage-reconstruction scope: the 41 stress
    hubs plus 5 dry-bulk-only waypoints IDKMT, INPRT, KRKAN, TRCKL, USSWP).
    It is a different scope, not a competing truth.

Usage:
    from scripts.congestion.port_universe import PORT_METADATA, SERIES_COUNT, PHYSICAL_HUBS
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

from compute_port_stress_matrix import (  # noqa: E402 — canonical source, defined there
    CANONICAL_PHYSICAL_HUBS,
    CANONICAL_SERIES_COUNT,
    PORT_METADATA,
)

SERIES_COUNT = CANONICAL_SERIES_COUNT  # 50 port-asset series
PHYSICAL_HUB_COUNT = CANONICAL_PHYSICAL_HUBS  # 41 physical UN/LOCODE hubs
PHYSICAL_HUBS = sorted({e["locode"] for e in PORT_METADATA})

BY_ASSET_CLASS = {}
for _e in PORT_METADATA:
    BY_ASSET_CLASS.setdefault(_e["asset_class"], []).append(_e)


def validate_hub_counts(n_series: int, n_physical: int) -> None:
    """Raise ValueError when observed counts drift from the canonical truth."""
    if n_series != SERIES_COUNT:
        raise ValueError(f"series count {n_series} != canonical {SERIES_COUNT}")
    if n_physical != PHYSICAL_HUB_COUNT:
        raise ValueError(f"physical hub count {n_physical} != canonical {PHYSICAL_HUB_COUNT}")
