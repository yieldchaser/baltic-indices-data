#!/usr/bin/env python3
"""Seasonality merge: quarterly + monthly + heatmaps render from one tab.

- Single Seasonality button/panel; old tab ids gone.
- One dispatch triple; QDDataGrid called once; matrices unified.
- USDA bunker loader parses real rows (was zero); dead usdaCostSpreads key gone.
"""
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
HTML = (ROOT / "index.html").read_text(encoding="utf-8")


def test_seasonality_merge_markers():
    assert 'data-tab="seasonality"' in HTML
    assert 'id="tab-seasonality"' in HTML
    for dead in ['data-tab="quarterly"', 'data-tab="monthly-dash"',
                 'data-tab="heatmaps"', 'id="tab-quarterly"',
                 'id="tab-monthly-dash"', 'id="tab-heatmaps"',
                 'usdaCostSpreads']:
        assert dead not in HTML, f"leftover: {dead}"
    assert "if (tabId === 'seasonality')" in HTML
    assert "if (currentTab === 'seasonality')" in HTML
    assert HTML.count("function () { renderQDDataGrid(productKey); },") == 1
    assert "function _renderWinRateMatrix(" in HTML
    assert "renderQuarterlyWinRateMatrix(productKey);" in HTML  # now in batch
    for section in ["SECTION: Quarterly", "SECTION: Monthly", "SECTION: Heatmaps"]:
        assert section in HTML, section
