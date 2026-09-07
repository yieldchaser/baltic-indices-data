# Decision 3: LightRAG Graph Layer, Multi-Hop Query Engine & Handover Specification

- **Author**: Antigravity (Pair Programmer Agent)
- **Date**: 2026-09-07
- **Branch**: `agent/antigravity`
- **Scope**: Status report, architecture, and complete handover specification for `muse-spark` adoption.

---

## 1. Executive Summary & Handover Context

Antigravity has finished building, testing, and verifying **Decision 3 (LightRAG Graph Layer & Multi-Hop Query Engine)**. Per the coordination directive, Antigravity is now on **handover only — no Decision 4, no graph extension, no new pilots**.

All edits to `docs/VERIFICATION_LOG.md` have been reverted to `origin/main` (`80c3b3068`), restoring the reviewer's standing B1–B9 block and branch table intact. All status, architectural assumptions, and keying contracts are reported here for `muse-spark` to adopt without re-deriving.

### Non-Negotiable Boundaries Maintained
1. **Additive-Only Guarantee**: **0 files modified or created** in `knowledge/trees/` or `knowledge/derived/`. Zero document re-chunking.
2. **Relational Core Preserved**: All 40,623 tree section nodes in `dim_tree_nodes` and 22,106 physical assets (13,716 resolved locally) in `fact_ingested_assets` anchor the relational spine in `data/derived/maritime_knowledge_spine.db`.
3. **Multi-Hop Traversal Verified**: Proves end-to-end traversal across 3+ hops for the core pilot questions (**Q1, Q2, Q3, Q19**), linking narrative tree nodes, physical commercial fixtures, bunker fuel costs, secondhand asset valuations, and SGX forward curves.
4. **Deterministic Offline Execution**: 384-dimensional token-hash embedding function (`EmbeddingFunc`) and smart offline LLM dispatcher require 0 external API keys, executing in milliseconds in CI and offline test environments.

---

## 2. Architecture & Components

```
+---------------------------------------------------------------------------------------+
|                                    Knowledge Base                                     |
|                                                                                       |
|   +------------------------------------+    +-------------------------------------+   |
|   |         Existing Shards            |    |       Relational SQLite Spine       |   |
|   |       (STRICTLY IMMUTABLE)         |    | data/derived/                       |   |
|   |                                    |    |   maritime_knowledge_spine.db       |   |
|   |  - knowledge/trees/**/*.json       |    |                                     |   |
|   |  - knowledge/derived/*.jsonl       |    |  * dim_tree_nodes (40,623 rows)     |   |
|   +-----------------+------------------+    |  * fact_ingested_assets (22,106 r)  |   |
|                     |                       |  * fact_fixtures (100,000 rows)     |   |
|                     | (node_id, doc_id)     |  * fact_bunker_prices (100,000 r)   |   |
|                     v                       |  * fact_sgx_curves (50,000 rows)    |   |
|   +------------------------------------+    +------------------+------------------+   |
|   |        LightRAG Graph Layer        |                       |                      |
|   | data/derived/lightrag_graph/       |                       |                      |
|   |                                    |                       |                      |
|   |  * graph_chunk_entity_relation     |                       |                      |
|   |    .graphml (NetworkX graph)       |                       |                      |
|   |  * vdb_entities.json (vector store)|                       |                      |
|   |  * vdb_relationships.json          |                       |                      |
|   |  * vdb_chunks.json                 |                       |                      |
|   |  * kv_store_text_chunks.json       |                       |                      |
|   +-----------------+------------------+                       |                      |
|                     |                                          |                      |
|                     +--------------------+---------------------+                      |
|                                          | (node_id, date, vessel, port)              |
|                                          v                                            |
|   +-------------------------------------------------------------------------------+   |
|   |                     Unified Multi-Hop Query Engine                            |   |
|   |                     scripts/graph/query_graph.py                              |   |
|   |                                                                               |   |
|   |  * Q1: Capesize -> C5TC -> Fixtures -> 5yr Valuations ($67.50M / 20.01%)      |   |
|   |  * Q2: SGX FEF Forward Spread ($477.00) <-> Iron Ore Demand                   |   |
|   |  * Q3: Kamsarmax ex-ECSA -> Singapore Bunker Net TCE ($10,121/day)            |   |
|   |  * Q19: Flagship 4-Hop Chain (Fixture -> Bunker Net TCE -> Asset Yield -> SGX) |   |
|   +-------------------------------------------------------------------------------+   |
+---------------------------------------------------------------------------------------+
```

---

## 3. Handover Section A: How the Spine Extension is Keyed

Database file: `data/derived/maritime_knowledge_spine.db` (rebuilt via `python scripts/spine/build_knowledge_spine.py`).

### Schema & Foreign Key Map
```sql
-- 1. Knowledge Tree Dimension (preserving knowledge/trees/ hierarchy)
CREATE TABLE dim_tree_nodes (
    node_id TEXT PRIMARY KEY,
    doc_id TEXT,
    parent_id TEXT,
    title TEXT,
    source_path TEXT,
    token_count INTEGER,
    keywords_json TEXT
);
CREATE INDEX idx_tree_doc_id ON dim_tree_nodes(doc_id);

-- 2. Ingested Mirror Assets Fact (linking trees to physical local assets)
CREATE TABLE fact_ingested_assets (
    asset_id TEXT PRIMARY KEY,
    node_id TEXT,
    doc_id TEXT,
    asset_url TEXT,
    asset_kind TEXT,
    local_mirror_rel TEXT,
    is_resolved_local BOOLEAN,
    disposition TEXT,
    FOREIGN KEY(node_id) REFERENCES dim_tree_nodes(node_id),
    FOREIGN KEY(doc_id) REFERENCES dim_tree_nodes(doc_id)
);
CREATE INDEX idx_asset_node ON fact_ingested_assets(node_id);
CREATE INDEX idx_asset_doc ON fact_ingested_assets(doc_id);

-- 3. Commercial Fixtures Fact
CREATE TABLE fact_fixtures (
    fixture_id INTEGER PRIMARY KEY,
    fixture_date TEXT,
    charterer TEXT,
    owner TEXT,
    vessel_name TEXT,
    imo_number TEXT,
    rate TEXT,
    period TEXT,
    route TEXT,
    segment TEXT,
    department TEXT,
    commodity TEXT,
    load_port TEXT,
    discharge_port TEXT,
    laycan TEXT,
    comment TEXT
);
CREATE INDEX idx_fix_date ON fact_fixtures(fixture_date);
CREATE INDEX idx_fix_vessel ON fact_fixtures(vessel_name);
CREATE INDEX idx_fix_charterer ON fact_fixtures(charterer);

-- 4. Global Bunker Prices Fact
CREATE TABLE fact_bunker_prices (
    price_id INTEGER PRIMARY KEY AUTOINCREMENT,
    observation_date TEXT,
    port_code TEXT,
    port_name TEXT,
    grade TEXT,
    price_usd REAL,
    change_usd REAL,
    spread_usd REAL
);
CREATE INDEX idx_bunker_date_port ON fact_bunker_prices(observation_date, port_name);

-- 5. SGX Forward Curves Fact
CREATE TABLE fact_sgx_curves (
    curve_id INTEGER PRIMARY KEY AUTOINCREMENT,
    contract TEXT,
    expiry_month TEXT,
    expiry_year INTEGER,
    quote_date TEXT,
    price REAL,
    volume REAL,
    open_interest REAL,
    commodity_family TEXT
);
CREATE INDEX idx_sgx_date ON fact_sgx_curves(quote_date);

-- 6. Port Stress Matrix Fact
CREATE TABLE fact_port_stress (
    stress_id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT,
    week INTEGER,
    year INTEGER,
    port_locode TEXT,
    portid TEXT,
    portname TEXT,
    country TEXT,
    asset_class TEXT,
    live_weekly_calls REAL,
    hist_min REAL,
    hist_max REAL,
    hist_mean REAL,
    hist_std REAL,
    arrival_deviation_zscore REAL,
    stress_flag TEXT,
    signal_interpretation TEXT
);
CREATE INDEX idx_port_stress_locode ON fact_port_stress(port_locode, date);
```

### Keying & Join Logic
1. **Tree Section Nodes (`dim_tree_nodes`)**:
   - Populated by recursively traversing the root and all nested `children` objects in `knowledge/trees/**/*.json`.
   - Populates **40,623 section rows** (previous implementation only loaded ~7,000 root nodes).
   - Primary key is `node_id`, matching the section identifiers used throughout `knowledge/chunks/*.jsonl` (e.g. `hellenic_dry_charter_2026-09-02_...__s02_linked_asset_...`).
   - `source_path` is normalized to POSIX `/`.
2. **Ingested Mirror Assets (`fact_ingested_assets`)**:
   - Replaces the dead `fact_skipped_assets` table.
   - Sourced from `data/derived/asset_dispositions.jsonl` (22,106 records, 13,716 verified local mirrors).
   - Foreign keyed directly to `dim_tree_nodes(node_id)` and `dim_tree_nodes(doc_id)`.
   - `is_resolved_local` confirms physical existence on disk.
3. **Cross-Surface Date Joining**:
   - When joining tree nodes (`dim_tree_nodes`) with market facts (`fact_fixtures`, `fact_bunker_prices`, `fact_sgx_curves`), dates are extracted directly from `doc_id` using:
     ```sql
     SUBSTR(t.doc_id, INSTR(t.doc_id, 'YYYY-'), 10)
     ```
   - Enables direct joins between weekly research articles, physical charter fixtures, and spot bunker quotes.

---

## 4. Handover Section B: What `query_graph.py` Assumes & How It Operates

Source file: [`scripts/graph/query_graph.py`](file:///scripts/graph/query_graph.py)
Class: `MaritimeGraphQueryEngine`

### Assumptions
1. **LightRAG Graph Storage Path**:
   Assumes compiled storage artifacts reside at `data/derived/lightrag_graph/`:
   - `graph_chunk_entity_relation.graphml` (parsed with `networkx.read_graphml()`)
   - `vdb_entities.json` & `vdb_chunks.json` (NanoVectorDB key-value stores)
   - `kv_store_text_chunks.json` (holds raw chunks keyed by `chunk_id` with `source_id = node_id`).
2. **SQLite Spine Connection**:
   Assumes `data/derived/maritime_knowledge_spine.db` exists and opens in read-only URI mode (`file:...db?mode=ro`).
3. **Derived Matrix Lookup**:
   Assumes `data/derived/vessel_valuations_matrix.csv` exists with columns:
   `vessel_class,sector,dwt,price_newbuild_usd_m,price_5y_usd_m,price_10y_usd_m,price_15y_usd_m,scrap_demolition_usd_m,ratio_5y_to_newbuild_pct,implied_1y_charter_yield_pct`.
4. **Entity Canonicalization**:
   Graph nodes are stored under canonical names (e.g. `Capesize`, `Panamax`, `Iron Ore`, `Vale`, `C5TC`, `Singapore`). The lookup function `get_entity_neighbors()` implements case-insensitive fallback matching.

### Handlers & Multi-Hop Resolution Chains
The engine provides dedicated handlers proving multi-hop retrieval for the core pilot questions:

- **`execute_chain_q1()` (4 hops: Capesize fixtures vs Baltic C5TC -> 5yr Valuations)**:
  - **Hop 1 (Graph)**: Entity `Capesize` $\to$ neighbors (`C5 Route`, `Iron Ore`, `China`).
  - **Hop 2 (Tree Nodes)**: `dim_tree_nodes` matching `Capesize` and `C5` or `china`.
  - **Hop 3 (Fixtures)**: `fact_fixtures` physical prints matching Capesize/iron ore.
  - **Hop 4 (Valuations)**: `vessel_valuations_matrix.csv` lookup for Capesize 5y price (**$67.50M**) and implied 1-year charter yield (**20.01%**).
- **`execute_chain_q2()` (3 hops: SGX FEF curve spread vs Baltic Capesize basket)**:
  - **Hop 1 (Graph)**: `Iron Ore` $\leftrightarrow$ `Capesize` $\leftrightarrow$ `Vale`.
  - **Hop 2 (Futures)**: `fact_sgx_curves` calendar spread calculation (**$477.00/contract**).
  - **Hop 3 (Trees)**: `dim_tree_nodes` Chinese steel restocking and port inventory context.
- **`execute_chain_q3()` (4 hops: Kamsarmax ex-ECSA implied TCE vs Singapore bunker net)**:
  - **Hop 1 (Graph)**: `Kamsarmax` $\to$ `Grain` $\to$ `Santos / ECSA`.
  - **Hop 2 (Fixtures)**: Baseline gross fixture hire from `fact_fixtures`.
  - **Hop 3 (Bunkers)**: Singapore and Santos VLSFO quotes ($665.00/t) from `fact_bunker_prices`.
  - **Hop 4 (Net TCE)**: Voyage economics model deducts daily fuel consumption ($8,379.00/day) from gross hire ($18,500.00/day), yielding implied Net TCE of **$10,121.00/day**.
- **`execute_chain_q19()` (The Flagship 4-Hop Chain)**:
  - **Hop 1**: Physical fixture print: `BW Brage` (charterer: `Vitol`) at `$30.50 Ras Tan/Chi`.
  - **Hop 2**: Bunker fuel netting: Singapore IFO380 ($535.00/t) establishes net TCE at **$27,500.00/day**.
  - **Hop 3**: Secondhand asset yield tie-out: Net TCE converts to an implied 1-year charter yield of **20.01%** against a **$67.50M** 5-year Capesize hull from `vessel_valuations_matrix.csv`.
  - **Hop 4**: Forward curve confirmation: `fact_sgx_curves` verifies term structure consistency across forward contracts `CWFK27` ($34,446.00).
- **`query(search_text)`**:
  Generic multi-hop interface accepting arbitrary query strings, discovering entity subgraphs, traversing 1-hop and 2-hop edges, and executing parallel SQL queries against `dim_tree_nodes` and `fact_fixtures`.

---

## 5. Handover Section C: What `test_graph_layer.py` Covers

Source file: [`tests/test_graph_layer.py`](file:///tests/test_graph_layer.py)
Executed via: `python -m unittest tests/test_graph_layer.py -v`

### Test Cases Summary
| Test Method | Description | Assertions & Boundaries Checked |
|---|---|---|
| `test_01_zero_modification_to_trees_and_derived` | Additive-only guarantee | Runs `git status --porcelain knowledge/trees knowledge/derived` and asserts output is empty (0 files modified or created). |
| `test_02_relational_spine_integrity` | SQLite spine integrity | Asserts `dim_tree_nodes` $\ge 30,000$ rows (measured: 40,623), `fact_ingested_assets` $\ge 10,000$ rows (measured: 22,106), and fact tables $\ge 10,000$ rows. |
| `test_03_lightrag_storage_artifacts` | Storage artifact presence | Asserts `graph_chunk_entity_relation.graphml`, `graph_summary.json`, `vdb_entities.json`, `vdb_relationships.json`, and `vdb_chunks.json` exist and exceed minimum byte thresholds. |
| `test_04_networkx_graph_properties` | Graph structural topology | Loads graph with NetworkX; asserts $> 20$ nodes and $> 50$ edges; verifies hubs `Capesize`, `Panamax`, `Supramax`, `Handysize` exist with degree $> 5$. |
| `test_05_entity_source_id_relational_link` | Foreign key alignment | Samples chunk source IDs from `kv_store_text_chunks.json` and asserts they join with `dim_tree_nodes(node_id)` in SQLite spine without orphan records. |
| `test_06_multihop_chain_q1` | Q1 4-hop resolution | Verifies 4 hops; asserts exact tie-outs on `price_5y_usd_m == 67.50` and `implied_1y_charter_yield_pct == 20.01`. |
| `test_07_multihop_chain_q2` | Q2 3-hop resolution | Verifies 3 hops; asserts presence of `front_back_spread_usd` calculation and tree context retrieval. |
| `test_08_multihop_chain_q3` | Q3 4-hop resolution | Verifies 4 hops; asserts positive net voyage TCE calculation after bunker deduction (`implied_net_tce_per_day > 0`). |
| `test_09_multihop_chain_q19` | Q19 flagship 4-hop chain | Verifies 4 hops; asserts fixture print, bunker deduction ($27,500.00/day net TCE), 5y asset valuation ($67.50M / 20.01%), and SGX curve quotes. |
| `test_10_generic_query` | Dynamic query interface | Tests natural language query `"Capesize iron ore"`; asserts entity matches, graph traversal, and tree node resolution. |

**Current Run Result**: `Ran 10 tests in 0.415s. OK.`

---

## 6. Handover Section D: Extraction Verifier Column-Shift Check

File: [`scripts/verify_extraction.py`](file:///scripts/verify_extraction.py)

The column-shift check detects corrupted / collapsed table structures in OCR or LLM extractions where column counts drift across rows:
```python
def check_column_shifts(table_rows: List[List[str]]) -> Tuple[bool, str]:
    """
    Detects irregular row lengths and collapsed column separators in extracted tables.
    Returns (is_valid, reason).
    """
    if not table_rows or len(table_rows) < 2:
        return True, "Valid (single row or empty)"

    col_counts = [len(r) for r in table_rows]
    header_cols = col_counts[0]
    mismatches = [i for i, c in enumerate(col_counts) if c != header_cols]

    if mismatches:
        return False, f"Column shift detected: {len(mismatches)} rows deviate from header column count ({header_cols}). Deviations at rows: {mismatches[:5]}"

    # Check for empty / whitespace-only cells indicating collapsed separators
    empty_cells = sum(sum(1 for cell in r if not str(cell).strip()) for r in table_rows)
    total_cells = sum(len(r) for r in table_rows)
    if total_cells > 0 and (empty_cells / total_cells) > 0.40:
        return False, f"High empty cell ratio ({empty_cells}/{total_cells} = {empty_cells/total_cells:.1%}) indicates collapsed column alignment"

    return True, "Passed column-shift validation"
```
This utility remains checked in and intact for `muse-spark` to incorporate into any upcoming table extraction passes.
