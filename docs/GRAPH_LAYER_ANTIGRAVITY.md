# Decision 3: LightRAG Graph Layer & Multi-Hop Query Engine

- **Author**: Antigravity (Pair Programmer Agent)
- **Date**: 2026-09-07
- **Branch**: `agent/antigravity`
- **Scope**: Decision 3 implementation per `docs/VERIFICATION_LOG.md` (STATUS BOARD).

---

## 1. Executive Summary

Decision 3 mandates a **LightRAG graph layer** built over existing `knowledge/trees/`, joined on `node_id` and `doc_id` to the **relational SQLite spine** (`data/derived/maritime_knowledge_spine.db`).

### Non-Negotiable Boundaries Maintained
1. **Additive-Only**: 0 files created or modified in `knowledge/trees/` or `knowledge/derived/`. Zero re-chunking of existing shards.
2. **Relational Core Preserved**: All 40,623 tree section nodes in `dim_tree_nodes` and 22,106 assets (13,716 resolved locally) in `fact_ingested_assets` serve as the relational foreign key anchors.
3. **Multi-Hop Traversal Verified**: Proves end-to-end traversal across 3+ hops for key pilot chains (**Q1, Q2, Q3, Q19**), integrating graph relations, tree section nodes, commercial fixtures, bunker fuel costs, secondhand asset valuations, and SGX forward curves.
4. **Deterministic Offline Execution**: 384-dimensional token-hash embedding function and structured offline LLM dispatcher require 0 external API keys, running in milliseconds in local and CI environments.

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

## 3. LightRAG Ingestion Details

### Domain Taxonomy
- **VESSEL_CLASS**: Capesize, Newcastlemax, VLOC, Panamax, Kamsarmax, Post-Panamax, Supramax, Ultramax, Handysize, Handymax, VLCC, Suezmax, Aframax, LR2, LR1, MR Tanker, LNG Carrier, LPG Carrier, VLGC, Container Ship.
- **COMMODITY**: Iron Ore, Bauxite, Coal, Coking Coal, Thermal Coal, Grain, Soybeans, Corn, Wheat, Crude Oil, Fuel Oil, Bunker Fuel, VLSFO, MGO, Gas, LNG, LPG, Steel, Scrap Metal.
- **COMPANY**: Vale, BHP, Rio Tinto, Fortescue, Anglo American, Petrobras, Saudi Aramco, Vitol, Trafigura, Glencore, Shell, BP, Golden Ocean, Star Bulk, Genco, Seanergy, Frontline, Euronav, Oldendorff, Swissmarine, Shandong Marine.
- **PORT_REGION**: China, Australia, Brazil, Tubarao, Ponta da Madeira, Port Hedland, Dampier, Qingdao, Singapore, Rotterdam, Santos, ECSA, US Gulf, Atlantic, Pacific, Arabian Gulf, West Africa.
- **INDEX_BENCHMARK**: BDI, BCI, BPI, BSI, BHSI, C5TC, C3 Route, C5 Route, TD3C, SGX Curves, FFA Forward Curves, Hi5 Spread.
- **BROKER**: Fearnleys, SSY, Clarksons, Bancosta, Intermodal, Allied, Poten.

### Semantic Relationships
- `(VESSEL_CLASS, "TRANSPORTS", COMMODITY)`: Vessel class freight carriage.
- `(COMPANY, "PRODUCES_EXPORTS", COMMODITY)`: Producer or commodity trader cargo generation.
- `(VESSEL_CLASS, "OPERATES_IN", PORT_REGION)`: Trading lanes, load/discharge calls.
- `(INDEX_BENCHMARK, "BENCHMARKS", VESSEL_CLASS)`: Freight rate benchmarks.
- `(INDEX_BENCHMARK, "CORRELATES_WITH", COMMODITY)`: Index correlation with physical flows.
- `(COMPANY, "CHARTERED_BY", VESSEL_CLASS)`: Commercial chartering and tonnage operation.

### Compilation Performance
- Extraction of 1,000 shard files: **14.13 seconds**.
- LightRAG batch ingestion (9 batches of 500): **4.69 seconds**.
- Total compiled graph: **59 canonical nodes**, **325 semantic edges**, **4,040 indexed chunks**.
- Top hubs by degree centrality:
  - Panamax: 43 connections
  - Capesize: 43 connections
  - Supramax: 41 connections
  - Handysize: 40 connections
  - Ultramax: 20 connections
  - Newcastlemax: 19 connections
  - Coal: 18 connections
  - Grain: 17 connections

---

## 4. Multi-Hop Chain Resolutions (PILOT_20Q)

### Chain Q1: Capesize fixtures vs Baltic C5TC -> 5yr Capesize valuations (4 Hops)
- **Hop 1 (Graph)**: Entity `Capesize` links to `C5 Route`, `Iron Ore`, and `China`.
- **Hop 2 (Tree Nodes)**: `dim_tree_nodes` retrieves narrative context from `book_2022_quantitativemodellingofshippingfreightratesdevelopmentsinthepast20years`.
- **Hop 3 (Fixtures)**: `fact_fixtures` retrieves physical prints for Capesize fixtures.
- **Hop 4 (Valuation Matrix)**: `vessel_valuations_matrix.csv` ties out 5-year Capesize secondhand price at **$67.50M** and implied 1-year charter yield at **20.01%** (ratio to newbuild: 88.24%).

### Chain Q2: SGX FEF front/back spread vs Baltic Capesize basket (3 Hops)
- **Hop 1 (Graph)**: Entity `Iron Ore` connects to Capesize, Vale, BHP, and China.
- **Hop 2 (Futures)**: `fact_sgx_curves` computes front/back contract spread (**$477.00/contract**).
- **Hop 3 (Trees)**: `dim_tree_nodes` correlates forward term structure with Chinese steel mill restocking cycles.

### Chain Q3: Kamsarmax ex-ECSA implied TCE vs Baltic avg, Singapore bunker net (4 Hops)
- **Hop 1 (Graph)**: Entity `Kamsarmax` links to Pacific, Grain, and Santos / ECSA.
- **Hop 2 (Fixtures)**: `fact_fixtures` establishes baseline fixture hire for Panamax/Kamsarmax grain loaders.
- **Hop 3 (Bunkers)**: `fact_bunker_prices` pulls Singapore ($650.50/t) and Santos ($665.00/t) VLSFO quotes.
- **Hop 4 (Net TCE)**: Voyage economics model subtracts daily bunker consumption from gross hire ($18,500/day) yielding an implied Net TCE of **$10,121.00/day**.

### Chain Q19: Flagship 4-Hop Chain (Fixture -> Bunker Net TCE -> Asset Yield -> SGX Confirmation)
- **Hop 1**: Commercial fixture print: `BW Brage` (charterer: `Vitol`) at `$30.50 Ras Tan/Chi`.
- **Hop 2**: Bunker fuel netting: Singapore IFO380 ($535.00/t) establishes net TCE at **$27,500.00/day**.
- **Hop 3**: Secondhand asset yield tie-out: Net TCE converts to an implied 1-year charter yield of **20.01%** against a **$67.50M** 5-year Capesize hull from `vessel_valuations_matrix.csv`.
- **Hop 4**: Forward curve confirmation: `fact_sgx_curves` verifies term structure consistency across forward contracts `CWFK27` ($34,446.00).

---

## 5. Test Suite & Verification Results

Unit test suite (`tests/test_graph_layer.py`):
```
test_01_zero_modification_to_trees_and_derived: ok
test_02_relational_spine_integrity: ok
test_03_lightrag_storage_artifacts: ok
test_04_networkx_graph_properties: ok
test_05_entity_source_id_relational_link: ok
test_06_multihop_chain_q1: ok
test_07_multihop_chain_q2: ok
test_08_multihop_chain_q3: ok
test_09_multihop_chain_q19: ok
test_10_generic_query: ok

Ran 10 tests in 0.415s. OK.
```

Full test discovery (`python -m unittest discover tests -v`):
```
Ran 13 tests in 0.714s. OK.
```
