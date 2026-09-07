"""
Unit Test Suite for LightRAG Graph Layer & Multi-Hop Query Engine.
Verifies additive-only constraint, relational integrity, storage formats,
and multi-hop traversal across 3+ hops (Q1, Q2, Q3, Q19).
"""

import os
import sys
import io
import json
import sqlite3
import subprocess
import unittest
from pathlib import Path

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

import networkx as nx

REPO_ROOT = Path(__file__).resolve().parent.parent
SPINE_DB = REPO_ROOT / "data" / "derived" / "maritime_knowledge_spine.db"
GRAPH_DIR = REPO_ROOT / "data" / "derived" / "lightrag_graph"
GRAPHML_FILE = GRAPH_DIR / "graph_chunk_entity_relation.graphml"
SUMMARY_FILE = GRAPH_DIR / "graph_summary.json"

# Add repo root to sys.path
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.graph.query_graph import MaritimeGraphQueryEngine

class TestGraphLayer(unittest.TestCase):

    def setUp(self):
        self.engine = MaritimeGraphQueryEngine()

    def test_01_zero_modification_to_trees_and_derived(self):
        """Verify that knowledge/trees/ and knowledge/derived/ have 0 modified or untracked files."""
        res = subprocess.run(
            ["git", "status", "--porcelain", "knowledge/trees", "knowledge/derived"],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True
        )
        self.assertEqual(res.returncode, 0)
        # Both directories must remain 100% clean
        self.assertEqual(
            res.stdout.strip(),
            "",
            f"Expected knowledge/trees and knowledge/derived to be pristine, but found changes:\n{res.stdout}"
        )

    def test_02_relational_spine_integrity(self):
        """Verify that SQLite spine contains required dimension and fact tables with valid row counts."""
        self.assertTrue(SPINE_DB.exists(), f"Spine DB missing at {SPINE_DB}")
        conn = sqlite3.connect(f"file:{SPINE_DB}?mode=ro", uri=True)
        cur = conn.cursor()

        # Check dim_tree_nodes
        cur.execute("SELECT count(*) FROM dim_tree_nodes")
        tree_count = cur.fetchone()[0]
        self.assertGreaterEqual(tree_count, 30000, f"Expected >= 30,000 tree nodes, found {tree_count}")

        # Check fact_ingested_assets
        cur.execute("SELECT count(*) FROM fact_ingested_assets")
        asset_count = cur.fetchone()[0]
        self.assertGreaterEqual(asset_count, 10000, f"Expected >= 10,000 ingested assets, found {asset_count}")

        # Check fact_fixtures
        cur.execute("SELECT count(*) FROM fact_fixtures")
        fix_count = cur.fetchone()[0]
        self.assertGreaterEqual(fix_count, 10000, f"Expected >= 10,000 fixtures, found {fix_count}")

        # Check fact_bunker_prices
        cur.execute("SELECT count(*) FROM fact_bunker_prices")
        bunker_count = cur.fetchone()[0]
        self.assertGreaterEqual(bunker_count, 10000, f"Expected >= 10,000 bunker prices, found {bunker_count}")

        # Check fact_sgx_curves
        cur.execute("SELECT count(*) FROM fact_sgx_curves")
        sgx_count = cur.fetchone()[0]
        self.assertGreaterEqual(sgx_count, 10000, f"Expected >= 10,000 SGX curve points, found {sgx_count}")

        conn.close()

    def test_03_lightrag_storage_artifacts(self):
        """Verify that all LightRAG storage files exist and are non-empty."""
        self.assertTrue(GRAPHML_FILE.exists(), f"Missing graphml at {GRAPHML_FILE}")
        self.assertGreater(GRAPHML_FILE.stat().st_size, 1000)

        self.assertTrue(SUMMARY_FILE.exists(), f"Missing summary at {SUMMARY_FILE}")
        with open(SUMMARY_FILE, "r", encoding="utf-8") as f:
            summary = json.load(f)
        self.assertIn("total_graph_nodes", summary)
        self.assertIn("total_graph_edges", summary)
        self.assertGreaterEqual(summary["total_graph_nodes"], 20)
        self.assertGreaterEqual(summary["total_graph_edges"], 50)

        for vdb_file in ["vdb_entities.json", "vdb_relationships.json", "vdb_chunks.json"]:
            p = GRAPH_DIR / vdb_file
            self.assertTrue(p.exists(), f"Missing {vdb_file} at {p}")
            self.assertGreater(p.stat().st_size, 500)

    def test_04_networkx_graph_properties(self):
        """Verify that NetworkX graph loads cleanly and contains core maritime hubs."""
        g = self.engine.graph
        self.assertIsInstance(g, nx.Graph)
        self.assertGreater(g.number_of_nodes(), 20)
        self.assertGreater(g.number_of_edges(), 50)

        # Ensure major vessel classes exist as hubs
        for expected_hub in ["Capesize", "Panamax", "Supramax", "Handysize"]:
            self.assertIn(expected_hub, g.nodes, f"Expected hub '{expected_hub}' in graph")
            degree = g.degree(expected_hub)
            self.assertGreater(degree, 5, f"Hub '{expected_hub}' degree too low: {degree}")

    def test_05_entity_source_id_relational_link(self):
        """Verify that graph entity chunks map to dim_tree_nodes(node_id) in SQLite spine."""
        kv_chunks_path = GRAPH_DIR / "kv_store_text_chunks.json"
        self.assertTrue(kv_chunks_path.exists(), f"Missing {kv_chunks_path}")
        with open(kv_chunks_path, "r", encoding="utf-8") as f:
            kv_data = json.load(f)

        self.assertGreater(len(kv_data), 0)

        # Sample source IDs from chunk entries
        sample_source_ids = []
        for chunk_id, entry in list(kv_data.items())[:100]:
            src = entry.get("source_id")
            if src and src != "UNKNOWN":
                sample_source_ids.append(src)

        self.assertGreater(len(sample_source_ids), 0)

        conn = sqlite3.connect(f"file:{SPINE_DB}?mode=ro", uri=True)
        cur = conn.cursor()
        placeholders = ",".join(["?"] * len(sample_source_ids))
        cur.execute(f"SELECT count(*) FROM dim_tree_nodes WHERE node_id IN ({placeholders})", sample_source_ids)
        matched_count = cur.fetchone()[0]
        conn.close()

        self.assertGreater(
            matched_count, 0,
            f"Expected sampled source_ids to join with dim_tree_nodes, matched {matched_count}/{len(sample_source_ids)}"
        )

    def test_06_multihop_chain_q1(self):
        """Verify 4-hop Q1 resolution (Capesize fixtures -> Baltic C5TC -> 5yr valuations)."""
        res = self.engine.execute_chain_q1()
        self.assertEqual(res["question_id"], "Q1")
        self.assertEqual(res["total_hops"], 4)
        self.assertEqual(len(res["hops"]), 4)

        hop4 = res["hops"][3]
        val = hop4["valuation"]
        self.assertEqual(val["vessel_class"], "Capesize")
        self.assertEqual(val["price_5y_usd_m"], 67.50)
        self.assertEqual(val["implied_1y_charter_yield_pct"], 20.01)

    def test_07_multihop_chain_q2(self):
        """Verify 3-hop Q2 resolution (SGX FEF spread vs Baltic Capesize basket)."""
        res = self.engine.execute_chain_q2()
        self.assertEqual(res["question_id"], "Q2")
        self.assertEqual(res["total_hops"], 3)
        self.assertEqual(len(res["hops"]), 3)
        self.assertIn("front_back_spread_usd", res["hops"][1])

    def test_08_multihop_chain_q3(self):
        """Verify 4-hop Q3 resolution (Kamsarmax ex-ECSA fixture -> Singapore bunker net TCE)."""
        res = self.engine.execute_chain_q3()
        self.assertEqual(res["question_id"], "Q3")
        self.assertEqual(res["total_hops"], 4)
        self.assertEqual(len(res["hops"]), 4)

        hop4 = res["hops"][3]
        self.assertIn("implied_net_tce_per_day", hop4)
        self.assertGreater(hop4["implied_net_tce_per_day"], 0)

    def test_09_multihop_chain_q19(self):
        """Verify 4-hop Q19 resolution (Fixture -> Bunker Net TCE -> Asset Yield -> SGX FFA)."""
        res = self.engine.execute_chain_q19()
        self.assertEqual(res["question_id"], "Q19")
        self.assertEqual(res["total_hops"], 4)
        self.assertEqual(len(res["hops"]), 4)

        # Hop 1: Fixture
        self.assertIn("fixture", res["hops"][0])
        # Hop 2: Bunker Net TCE
        self.assertEqual(res["hops"][1]["computed_net_tce_usd_day"], 27500.0)
        # Hop 3: Secondhand valuation
        self.assertEqual(res["hops"][2]["implied_1y_charter_yield_pct"], 20.01)
        self.assertEqual(res["hops"][2]["secondhand_5y_price_usd_m"], 67.50)
        # Hop 4: SGX curve confirmation
        self.assertIn("sgx_contracts", res["hops"][3])

    def test_10_generic_query(self):
        """Verify generic multi-hop query returns graph traversal and tree links."""
        res = self.engine.query("Capesize iron ore")
        self.assertIn("matched_graph_entities", res)
        self.assertIn("Capesize", res["matched_graph_entities"])
        self.assertIn("Iron Ore", res["matched_graph_entities"])
        self.assertIn("graph_traversal", res)
        self.assertIn("connected_tree_nodes", res)

if __name__ == "__main__":
    unittest.main()
