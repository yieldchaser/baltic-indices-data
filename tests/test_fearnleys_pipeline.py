"""
tests/test_fearnleys_pipeline.py
Automated test suite verifying the Fearnleys Hasura commercial intelligence integration:
  - Cache JSON integrity and schema completeness
  - 56-Year 1Y TC benchmark statistics and multi-decade monthly time series
  - 50-Year secondhand asset valuation cycles & newbuilding replacement parity
  - Commercial fixture analytics, sector volumes, and top charterers league table
  - Verified secondhand S&P deal ledger
  - Non-purging database preservation (Alibra & Intermodal OCR intact)
  - Zero emojis in index.html
  - Frontend DOM element wiring and dynamic calculated tooltip handlers
"""

import json
import os
import re
import pytest

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DERIVED_DIR = os.path.join(BASE_DIR, "data", "derived")
SUMMARY_JSON = os.path.join(DERIVED_DIR, "fearnleys_summary.json")
HTML_PATH = os.path.join(BASE_DIR, "index.html")


def test_fearnleys_summary_cache_exists_and_valid():
    """Confirms pre-aggregated cache exists, is valid JSON, and has required top-level keys."""
    assert os.path.exists(SUMMARY_JSON), f"Missing cache file: {SUMMARY_JSON}"
    file_size_kb = os.path.getsize(SUMMARY_JSON) / 1024.0
    assert file_size_kb > 100, f"Cache file too small ({file_size_kb:.1f} KB)"
    assert file_size_kb < 1500, f"Cache file too large ({file_size_kb:.1f} KB)"

    with open(SUMMARY_JSON, "r", encoding="utf-8") as f:
        data = json.load(f)

    expected_keys = [
        "generated_at",
        "provenance",
        "tce_benchmarks_56y",
        "asset_valuations_50y",
        "fixtures_analytics",
        "snp_deals",
        "broker_sentiment",
    ]
    for key in expected_keys:
        assert key in data, f"Missing key in summary cache: {key}"

    assert "rate_records" in data["provenance"]
    assert data["provenance"]["rate_records"] >= 300000
    assert data["provenance"]["fixture_records"] >= 500000
    assert data["provenance"]["snp_records"] >= 2500


def test_56y_tce_benchmarks_completeness():
    """Verifies 56-year 1Y TC rates across Capesize, Panamax, Supramax, Handysize, Tankers."""
    with open(SUMMARY_JSON, "r", encoding="utf-8") as f:
        data = json.load(f)

    tce = data["tce_benchmarks_56y"]
    stats = tce["stats"]
    required_segs = ["capesize", "panamax", "supramax", "handysize", "vlcc", "suezmax", "aframax"]

    for seg in required_segs:
        assert seg in stats, f"Missing TCE segment in stats: {seg}"
        s = stats[seg]
        assert s["current"] > 0, f"{seg} current rate invalid: {s['current']}"
        assert s["ath"] >= s["current"], f"{seg} ATH ({s['ath']}) lower than current ({s['current']})"
        assert s["atl"] > 0, f"{seg} ATL invalid: {s['atl']}"
        assert 0.0 <= s["pct_56y"] <= 100.0, f"{seg} 56Y percentile invalid: {s['pct_56y']}"
        assert s["start_year"] <= 2023, f"{seg} start year too late: {s['start_year']}"

    # Verify multi-decade timeline
    timeline = tce["monthly_series"]
    assert len(timeline) >= 400, f"Too few monthly timeline entries: {len(timeline)}"
    first_year = int(timeline[0]["date"][:4])
    assert first_year <= 1975, f"Earliest date should be <=1975, got {first_year}"


def test_50y_asset_valuations_completeness():
    """Verifies 50-year asset valuations across dry bulk and tanker age tiers."""
    with open(SUMMARY_JSON, "r", encoding="utf-8") as f:
        data = json.load(f)

    assets = data["asset_valuations_50y"]
    required_segs = ["capesize", "kamsarmax", "ultramax", "vlcc", "suezmax", "aframax", "mr"]

    for seg in required_segs:
        assert seg in assets, f"Missing asset segment: {seg}"
        stats = assets[seg]["stats"]
        assert "5y" in stats, f"Missing 5Y valuation for {seg}"
        assert stats["5y"]["current"] > 0, f"Invalid 5Y value for {seg}: {stats['5y']['current']}"
        assert len(assets[seg]["history"]) >= 50, f"Too few historical asset points for {seg}"


def test_fixture_analytics_and_league_table():
    """Verifies commercial fixture analytics and top charterers league table."""
    with open(SUMMARY_JSON, "r", encoding="utf-8") as f:
        data = json.load(f)

    fix = data["fixtures_analytics"]
    assert fix["total_fixtures"] >= 530000, f"Total fixtures count too low: {fix['total_fixtures']}"
    assert "department_totals" in fix
    assert fix["department_totals"].get("BULK", 0) >= 400000

    # League table checks
    league = fix["top_charterers"]
    assert len(league) >= 20, f"League table too short: {len(league)}"
    charterer_names = [c["charterer"] for c in league]
    assert "UNIPEC" in charterer_names, "UNIPEC must be in top charterers"
    assert "SHELL" in charterer_names, "SHELL must be in top charterers"

    # Recent fixtures checks
    recent = fix["recent_fixtures"]
    assert len(recent) >= 50, f"Too few recent fixtures: {len(recent)}"
    sample = recent[0]
    assert "vessel" in sample
    assert "charterer" in sample
    assert "route" in sample


def test_snp_deals_ledger():
    """Verifies verified secondhand vessel sale & purchase ledger."""
    with open(SUMMARY_JSON, "r", encoding="utf-8") as f:
        data = json.load(f)

    snp = data["snp_deals"]
    assert snp["total_deals"] >= 2500, f"Too few S&P deals: {snp['total_deals']}"
    assert snp["total_volume_usd_m"] >= 40000, f"Total turnover too low: {snp['total_volume_usd_m']}"

    deals = snp["recent_deals"]
    assert len(deals) >= 100, f"Too few recent deals: {len(deals)}"
    for d in deals[:10]:
        assert d["vessel"], "Deal vessel name cannot be empty"
        assert d["price_usd_m"] is not None and d["price_usd_m"] > 0, "Deal price must be positive"


def test_database_preservation_policy():
    """CRITICAL: Verifies existing Alibra and Intermodal OCR files remain 100% intact."""
    alibra_csv = os.path.join(DERIVED_DIR, "time_charter_rates.csv")
    alibra_matrix = os.path.join(DERIVED_DIR, "alibra_tce_matrix.json")
    intermodal_csv = os.path.join(DERIVED_DIR, "intermodal_tc_rates.csv")

    assert os.path.exists(alibra_csv), "CRITICAL REGRESSION: time_charter_rates.csv was purged!"
    assert os.path.getsize(alibra_csv) > 1000, "time_charter_rates.csv is corrupted or truncated"

    assert os.path.exists(alibra_matrix), "CRITICAL REGRESSION: alibra_tce_matrix.json was purged!"
    assert os.path.getsize(alibra_matrix) > 1000, "alibra_tce_matrix.json is corrupted or truncated"

    assert os.path.exists(intermodal_csv), "CRITICAL REGRESSION: intermodal_tc_rates.csv was purged!"
    assert os.path.getsize(intermodal_csv) > 100, "intermodal_tc_rates.csv is corrupted or truncated"


def test_zero_emojis_in_index_html():
    """CRITICAL: Verifies zero emojis anywhere in index.html (strict rule compliance)."""
    with open(HTML_PATH, "r", encoding="utf-8") as f:
        text = f.read()

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

    matches = real_emoji_pattern.findall(text)
    assert len(matches) == 0, f"Found {len(matches)} prohibited emojis in index.html: {matches[:5]}"


def test_frontend_fearnleys_elements_present():
    """Verifies that all new DOM containers, buttons, and dynamic tooltips are present in index.html."""
    with open(HTML_PATH, "r", encoding="utf-8") as f:
        html = f.read()

    required_strings = [
        'id="fearnleysTceRibbonCard"',
        'id="fearnleysTceRibbonGrid"',
        'id="fearnleysTce56yChart"',
        'id="fearnTceSegToggle"',
        'id="fearnTceRangeToggle"',
        'id="fearnTceScaleToggle"',
        'id="fearnleysFixturesContainer"',
        'id="fearnFixtureVolumeChart"',
        'id="fearnCharterersTable"',
        'id="fearnRecentFixturesTable"',
        'id="fearnleysAssetValuationContainer"',
        'id="fearnAssetCycleChart"',
        'id="fearnleysSnpLedgerContainer"',
        'id="fearnSnpTable"',
        'data-tt-type="fearnleys-56y-tce"',
        'data-tt-type="fearnleys-asset-value"',
        'data-tt-type="fearnleys-fixture-kpi"',
        'data-tt-type="fearnleys-snp-deal"',
        'initFearnleysTerminal',
        'FEARNLEYS_CONTROLLER',
        'renderFearnleysTce56yChart',
        'switchFearnTceSeg',
        'switchFearnTceRange',
        'switchFearnTceScale',
        'DATA.fearnleysSummary',
        'fetch(\'data/derived/fearnleys_summary.json\')',
    ]

    for req in required_strings:
        assert req in html, f"Missing required string in index.html: {req}"


def test_56y_chart_data_span():
    """Verifies that 56-year dataset actually spans from 1970 to 2026."""
    with open(SUMMARY_JSON, "r", encoding="utf-8") as f:
        data = json.load(f)

    series = data["tce_benchmarks_56y"]["monthly_series"]
    assert len(series) >= 680, f"Expected ~681 months, got {len(series)}"
    assert series[0]["date"] == "1970-01", f"First monthly date must be 1970-01, got {series[0]['date']}"
    assert series[0]["panamax"] is not None and series[0]["panamax"] > 0, "Panamax must have 1970 data"
    assert series[-1]["date"] >= "2026-08", f"Latest monthly date should reach 2026, got {series[-1]['date']}"

