"""
Unit Test Suite for Decision 4 Source Wiring.
Verifies manifest coverage, SQLite spine tables, row counts,
multi-hop traversal joins, and pristine directory boundaries.
"""

import os
import sys
import json
import sqlite3
import subprocess
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SOURCES_JSON = REPO_ROOT / "knowledge" / "manifests" / "sources.json"
SPINE_DB = REPO_ROOT / "data" / "derived" / "maritime_knowledge_spine.db"

class TestSourceWiring(unittest.TestCase):

    def test_01_pristine_boundaries(self):
        """Verify knowledge/trees/, knowledge/derived/, and VERIFICATION_LOG.md have 0 modifications."""
        res = subprocess.run(
            ["git", "status", "--porcelain", "knowledge/trees", "knowledge/derived", "docs/VERIFICATION_LOG.md"],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True
        )
        self.assertEqual(res.returncode, 0)
        self.assertEqual(
            res.stdout.strip(), "",
            f"Protected boundaries modified:\n{res.stdout}"
        )

    def test_02_sources_manifest_coverage(self):
        """Verify sources.json has all uncovered source categories additively."""
        self.assertTrue(SOURCES_JSON.exists(), f"sources.json missing at {SOURCES_JSON}")
        data = json.loads(SOURCES_JSON.read_text(encoding="utf-8"))
        
        # Check counts
        counts = data.get("counts", {})
        required_keys = ["futures", "indices", "cftc_statements", "etf", "flows", "congestion", "drewry_ais", "fearnleys"]
        for k in required_keys:
            self.assertIn(k, counts, f"Key '{k}' missing from sources.json counts")
            self.assertTrue(len(counts[k]) > 0, f"No subcategories in counts for '{k}'")

        # Check paths
        paths = data.get("paths", {})
        for k in required_keys:
            self.assertIn(k, paths, f"Key '{k}' missing from sources.json paths")

    def test_03_spine_tables_and_row_counts(self):
        """Verify SQLite knowledge spine has all 13 dimension and fact tables with non-zero counts."""
        self.assertTrue(SPINE_DB.exists(), f"Spine DB missing at {SPINE_DB}")
        conn = sqlite3.connect(SPINE_DB)
        cur = conn.cursor()

        expected_tables = {
            "dim_tree_nodes": 40000,
            "fact_ingested_assets": 20000,
            "dim_companies": 100,
            "fact_port_stress": 10000,
            "fact_fixtures": 100000,
            "fact_fearnleys_snp": 2000,
            "fact_bunker_prices": 50000,
            "fact_sgx_curves": 100000,
            "fact_capital_link_indices": 30000,
            "fact_cftc_etf_ledgers": 100,
            "fact_etf_holdings": 1000,
            "fact_usda_grain_flows": 3000,
            "fact_portwatch_congestion": 30000,
        }

        for table, min_rows in expected_tables.items():
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            cnt = cur.fetchone()[0]
            self.assertGreaterEqual(cnt, min_rows, f"Table {table} has {cnt} rows, expected >= {min_rows}")

        conn.close()

    def test_04_sgx_etf_multihop_join(self):
        """Verify multi-hop join between ETF holdings and SGX forward curves."""
        conn = sqlite3.connect(SPINE_DB)
        cur = conn.cursor()
        query = """
        SELECT h.fund, h.contract_name, s.commodity_family, s.price
        FROM fact_etf_holdings h
        JOIN fact_sgx_curves s ON s.quote_date = h.quote_date AND s.commodity_family LIKE '%Capesize%'
        WHERE h.fund = 'BDRY'
        LIMIT 5;
        """
        cur.execute(query)
        rows = cur.fetchall()
        self.assertGreater(len(rows), 0, "No joined rows between ETF holdings and SGX curves")
        conn.close()

    def test_05_capital_link_snp_multihop_join(self):
        """Verify multi-hop join between Capital Link indices and Fearnleys S&P deals."""
        conn = sqlite3.connect(SPINE_DB)
        cur = conn.cursor()
        query = """
        SELECT c.index_code, c.close_val, s.vessel_name, s.price_usd
        FROM fact_capital_link_indices c
        JOIN fact_fearnleys_snp s ON SUBSTR(s.created_at, 1, 10) = c.quote_date
        WHERE c.index_code = 'CLDBI'
        LIMIT 5;
        """
        cur.execute(query)
        rows = cur.fetchall()
        self.assertGreater(len(rows), 0, "No joined rows between Capital Link indices and S&P deals")
        conn.close()

if __name__ == "__main__":
    unittest.main()
