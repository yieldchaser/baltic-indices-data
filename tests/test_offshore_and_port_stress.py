#!/usr/bin/env python3
"""
Test Suite: Offshore & OSV Terminal and Global Port Stress Matrix
================================================================
Validates data integrity, pre-aggregated caches, and frontend UI hooks.
"""

import json
import re
from pathlib import Path
import pytest

ROOT = Path(__file__).resolve().parent.parent
OFFSHORE_CACHE = ROOT / "data" / "derived" / "offshore_summary.json"
PORT_STRESS_CACHE = ROOT / "data" / "derived" / "port_stress_summary.json"
INDEX_HTML = ROOT / "index.html"


def test_offshore_summary_cache_exists_and_valid():
    """Verify offshore_summary.json exists and contains complete 97-month data across 4 categories."""
    assert OFFSHORE_CACHE.exists(), f"Missing cache: {OFFSHORE_CACHE}"
    with open(OFFSHORE_CACHE, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Check metadata
    assert "metadata" in data
    assert data["metadata"]["total_monthly_records"] >= 370
    assert data["metadata"]["total_reports"] >= 90

    # Check 4 categories
    assert "categories" in data
    cats = data["categories"]
    assert "large_ahts" in cats
    assert "med_ahts" in cats
    assert "large_psv" in cats
    assert "med_psv" in cats

    # Check continuous monthly series
    for k in ["large_ahts", "med_ahts", "large_psv", "med_psv"]:
        cat_data = cats[k]
        assert cat_data["latest_dayrate_gbp"] > 0
        assert len(cat_data["series"]) >= 90
        # Verify first point has date and rate
        assert "date" in cat_data["series"][0]
        assert "avg_dayrate" in cat_data["series"][0]

    # Check ledger
    assert len(data["all_ledger"]) >= 370
    assert len(data["reports"]) >= 90


def test_port_stress_summary_cache_exists_and_valid():
    """Verify port_stress_summary.json exists and contains 41+ ports with 5Y seasonal envelopes."""
    assert PORT_STRESS_CACHE.exists(), f"Missing cache: {PORT_STRESS_CACHE}"
    with open(PORT_STRESS_CACHE, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Check summary metrics
    assert "summary" in data
    assert data["summary"]["total_monitored"] >= 41
    assert "by_asset_class" in data["summary"]
    for ac in ["Dry Bulk", "Tankers", "LPG", "LNG"]:
        assert ac in data["summary"]["by_asset_class"]
        assert data["summary"]["by_asset_class"][ac] >= 10

    # Check hubs list
    assert "hubs" in data
    assert len(data["hubs"]) >= 41

    # Check ports series map
    assert "ports_series" in data
    assert len(data["ports_series"]) >= 41

    # Verify a key port (e.g., AUPHE Port Hedland) has weekly series
    p_keys = [k.lower() for k in data["ports_series"].keys()]
    assert any("auphe" in k for k in p_keys)
    sample_key = next(k for k in data["ports_series"].keys() if "auphe" in k.lower())
    series = data["ports_series"][sample_key]["series"]
    assert len(series) >= 200
    assert "c" in series[0]  # live calls
    assert "mean" in series[0]
    assert "min" in series[0]
    assert "max" in series[0]


def test_zero_emojis_in_index_html():
    """Ensure strict compliance with zero-emoji policy."""
    assert INDEX_HTML.exists()
    with open(INDEX_HTML, "r", encoding="utf-8") as f:
        content = f.read()

    real_emoji_pattern = re.compile(
        "["
        "\U0001F600-\U0001F64F"  # emoticons
        "\U0001F300-\U0001F5FF"  # symbols & pictographs
        "\U0001F680-\U0001F6FF"  # transport & map symbols
        "\U0001F900-\U0001F9FF"  # supplemental symbols
        "\U0001FA70-\U0001FAFF"  # symbols extended
        "\U0001F1E6-\U0001F1FF"  # regional flags
        "]",
        flags=re.UNICODE,
    )
    matches = real_emoji_pattern.findall(content)
    assert len(matches) == 0, f"Found {len(matches)} prohibited emojis in index.html: {matches[:5]}"


def test_frontend_offshore_and_port_stress_elements_present():
    """Verify index.html contains the necessary DOM hooks, tabs, canvas IDs, controllers, and tooltips."""
    with open(INDEX_HTML, "r", encoding="utf-8") as f:
        html = f.read()

    # Offshore Tab hooks
    assert 'data-tab="offshore"' in html
    assert 'id="tab-offshore"' in html
    assert 'id="offshoreMainChart"' in html
    assert 'id="offshoreLedgerTableBody"' in html
    assert 'id="offshoreReportsGrid"' in html

    # Port Stress hooks in Tracking tab
    assert 'id="trackingSubviewStress"' in html
    assert 'id="portStressTableBody"' in html
    assert 'id="portStressEnvelopeChart"' in html

    # Data fetch promises in loadAllData
    assert 'offshoreSummaryPromise' in html
    assert 'portStressSummaryPromise' in html

    # Runtime controllers
    assert 'function renderOffshoreTab' in html
    assert 'function renderOffshoreMainChart' in html
    assert 'function renderOffshoreLedgerTable' in html
    assert 'function renderOffshoreReports' in html
    assert 'function renderPortStressSubView' in html
    assert 'function renderPortStressTable' in html
    assert 'function renderPortStressEnvelopeChart' in html

    # Unified rich tooltips
    assert "type === 'offshore-kpi-card'" in html
    assert "type === 'offshore-ledger-row'" in html
    assert "type === 'port-stress-cell'" in html
    assert "type === 'port-stress-summary-hud'" in html

    # 5Y range buttons across sections
    assert 'id="btnOffshoreRange5y"' in html
    assert 'data-stress-range="5y"' in html
    assert 'data-range="5y"' in html

    # Offshore controls have tooltips
    assert 'id="btnOffshoreSegAll"' in html
    assert 'btnOffshoreSegAll" onclick="setOffshoreSegment(\'all\')" data-tooltip=' in html
    assert 'btnOffshoreMetricRates" onclick="setOffshoreMetric(\'rates\')" data-tooltip=' in html
    assert 'btnOffshoreRangeAll" onclick="setOffshoreRange(\'all\')" data-tooltip=' in html
    assert 'id="offshoreSearchInput"' in html
    assert 'offshoreSearchInput" class="tracking-input" placeholder="Filter month or vessel..." oninput="onOffshoreSearchInput()" style="width:160px;" data-tooltip=' in html

    # Seabreeze reports: single PDF button, no redundant Web button
    assert 'data-tooltip="Download official Seabreeze monthly market intelligence report (PDF)."' in html
    assert '<a href="${rep.card_url}" target="_blank" rel="noopener" class="offshore-pdf-btn" style="background:#111820; border-color:var(--border);">\n          Web' not in html

    # Fearnleys Asset Valuations & S&P Deal Ledger tooltips
    assert 'id="btnAssetCape"' in html
    assert 'btnAssetCape" onclick="switchFearnAssetSeg(\'capesize\')" data-tooltip=' in html
    assert 'id="btnSnpAll"' in html
    assert 'btnSnpAll" onclick="setSnpFilter(\'all\')" data-tooltip=' in html
    assert 'id="fearnSnpSearchInput"' in html
    assert 'id="fearnSnpSearchInput" placeholder="Search by vessel, shipyard, or buyer..." oninput="filterSnpDeals(this.value)" style="max-width:320px;" data-tooltip=' in html


