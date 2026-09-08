# Shipping Repository Dataset Inventory & Data Health Documentation

This document outlines the complete dataset inventory, publishing frequencies, primary data sources, and data health status for all **42 CSV datasets** tracked in the repository as of **September 2026**.

> [!WARNING]
> **Shallow CSVs until backfill runs land (E2E audit, no-break):** `drewry_wci_historical.csv` **139 rows** (2024-01-04→2026-08-26 provisional), `brazil_comexstat_exports.csv` **92 rows** (2024+ recent slice), `us_eia_weekly_crude_exports.csv` **500 rows** (real 2017+ kept as-is without `EIA_API_KEY`), `usda_fas_outstanding_export_sales.csv` **10k rows** (tail 2006 — FAS DESC 60k lands on Thu 15 UTC runs), `fbx_historical.csv` **108 rows** (Mar-2026+ slice; full 2017→present backfill is a follow-up). How to trigger: `gh workflow run poten_drewry_weekly.yml -f backfill_2011=true` (Wayback 2011→present, assessed only, gaps null); `gh workflow run upstream_commodity_flows.yml -f comexstat_full=1` (full 1997→live, multi-hour paced) or `COMEXSTAT_FULL_HISTORY=1 python scripts/scrapers/fetch_comexstat_brazil.py`; `gh workflow run usda_weekly.yml` (Thu 15 UTC FAS DESC 60k). Pre-2024 WCI values are never synthesized.

---

## 1. Freight & Shipping Indices (`data/indices/`)

| File Name | Commodity / Vessel Class | Start Date | Frequency | Data Source | Status |
|:---|:---|:---:|:---:|:---|:---:|
| [`bdiy_historical.csv`](file:///c:/Users/Dell/Github/Shipping/data/indices/bdiy_historical.csv) | Baltic Dry Index (BDI) | Jan 1985 | Daily | Baltic Exchange | Active |
| [`cape_historical.csv`](file:///c:/Users/Dell/Github/Shipping/data/indices/cape_historical.csv) | Baltic Capesize Index | Oct 2008 | Daily | Baltic Exchange | Active |
| [`panama_historical.csv`](file:///c:/Users/Dell/Github/Shipping/data/indices/panama_historical.csv) | Baltic Panamax Index | Oct 2008 | Daily | Baltic Exchange | Active |
| [`suprama_historical.csv`](file:///c:/Users/Dell/Github/Shipping/data/indices/suprama_historical.csv) | Baltic Supramax Index | Oct 2008 | Daily | Baltic Exchange | Active |
| [`handysize_historical.csv`](file:///c:/Users/Dell/Github/Shipping/data/indices/handysize_historical.csv) | Baltic Handysize Index | Oct 2008 | Daily | Baltic Exchange | Active |
| [`dirtytanker_historical.csv`](file:///c:/Users/Dell/Github/Shipping/data/indices/dirtytanker_historical.csv) | Baltic Dirty Tanker Index (BDTI) | Dec 2007 | Daily | Baltic Exchange | Active |
| [`cleantanker_historical.csv`](file:///c:/Users/Dell/Github/Shipping/data/indices/cleantanker_historical.csv) | Baltic Clean Tanker Index (BCTI) | Jan 2008 | Daily | Baltic Exchange | Active |
| [`blng_historical.csv`](file:///c:/Users/Dell/Github/Shipping/data/indices/blng_historical.csv) | Baltic LNG Freight Index | Mar 2026 | Daily | Baltic Exchange | Active |
| [`blpg_historical.csv`](file:///c:/Users/Dell/Github/Shipping/data/indices/blpg_historical.csv) | Baltic LPG Freight Index | Mar 2026 | Daily | Baltic Exchange | Active |
| [`fbx_historical.csv`](file:///c:/Users/Dell/Github/Shipping/data/indices/fbx_historical.csv) | Freightos Baltic Container Index | Mar 2026 | Daily | Freightos / Baltic | Active |
| [`bai_historical.csv`](file:///c:/Users/Dell/Github/Shipping/data/indices/bai_historical.csv) | Baltic Air Freight Index | Jan 2018 | Weekly | TAC Index / Baltic | Lagged (Weekly) |
| [`blpg_fearnleys_historical.csv`](file:///c:/Users/Dell/Github/Shipping/data/indices/blpg_fearnleys_historical.csv) | Legacy Fearnleys BLPG Index | Jan 2019 | Discontinued | Fearnleys | Legacy (Superseded by `blpg_historical.csv`) |

---

## 2. Derived Intelligence & Sector Benchmarks (`data/derived/`)

| File Name | Content Description | Start Date | Frequency | Data Source | Status |
|:---|:---|:---:|:---:|:---|:---:|
| [`time_charter_rates.csv`](file:///c:/Users/Dell/Github/Shipping/data/derived/time_charter_rates.csv) | Merged Alibra/Fearnleys 1Y & 2Y TC Rates | Jan 2000 | Weekly | Alibra & Fearnleys | Active |
| [`time_charter_rates_fearnleys.csv`](file:///c:/Users/Dell/Github/Shipping/data/derived/time_charter_rates_fearnleys.csv) | Pure Fearnleys Hasura GraphQL TC Rates | Jan 2000 | Weekly | Fearnleys Hasura API | Active |
| [`vessel_valuations.csv`](file:///c:/Users/Dell/Github/Shipping/data/derived/vessel_valuations.csv) | 10Y Asset Values & Demolition Prices | Dec 1970 | Weekly | Clarksons / Fearnleys | Active |
| [`scrappage_prices.csv`](file:///c:/Users/Dell/Github/Shipping/data/derived/scrappage_prices.csv) | Demolition / Scrap Prices ($/LDT) via AnyDoc OCR | Jul 2021 | Weekly | Hellenic / Intermodal | Active (~377 rows) |
| [`iron_ore_restocking.csv`](file:///c:/Users/Dell/Github/Shipping/data/derived/iron_ore_restocking.csv) | CFR 62% Iron Ore & Qingdao Port Stock | Jul 2018 | Weekly | Mysteel / S&P Global | Active |
| [`lng_charter_rates.csv`](file:///c:/Users/Dell/Github/Shipping/data/derived/lng_charter_rates.csv) | LNG Carrier Spot & Time Charter Rates | Jan 2017 | Weekly | Spark Commodities | Active |
| [`lpg_charter_rates.csv`](file:///c:/Users/Dell/Github/Shipping/data/derived/lpg_charter_rates.csv) | VLGC LPG Time Charter Rates | Jul 2019 | Weekly | Fearnleys / Clarksons | Active |
| [`lpg_spot_rates.csv`](file:///c:/Users/Dell/Github/Shipping/data/derived/lpg_spot_rates.csv) | Ras Tanura to Chiba LPG Freight Rates | Jan 2004 | Weekly | Fearnleys | Active |
| [`tanker_forward_curves.csv`](file:///c:/Users/Dell/Github/Shipping/data/derived/tanker_forward_curves.csv) | Tanker FFA 22-Month Forward Term Structure | Aug 2026 | Weekly / Daily | Alibra Poller | Active |
| [`tanker_forward_curves_history.csv`](file:///c:/Users/Dell/Github/Shipping/data/derived/tanker_forward_curves_history.csv) | Persistent Tanker Forward Curve History | Aug 2026 | Weekly / Daily | Alibra Poller | Active |
| [`alibra_tce_matrix.json`](file:///c:/Users/Dell/Github/Shipping/data/derived/alibra_tce_matrix.json) | Live Period TCE Matrix & WoW Deltas | Aug 2026 | Weekly / Daily | Alibra Poller | Active |
| [`fearnleys_catalog.csv`](file:///c:/Users/Dell/Github/Shipping/data/derived/fearnleys_catalog.csv) | Hasura GraphQL Route Catalog | N/A | Static | Fearnleys GraphQL Schema | Static Metadata |
| [`fearnleys_series_monthly.json`](file:///c:/Users/Dell/Github/Shipping/data/derived/fearnleys_series_monthly.json) | Per-label monthly means + ATH/ATL/percentile for all 294 rate series (32,085 pts) backing the Fearnleys desk browser | Full history → live | Weekly (Wed/Thu, `fearnleys_weekly.yml`) | `scripts/fearnleys/build_series_cache.py` from `fearnpulse_rates_full.csv` | Active (646 KB; 294/356 catalog ids have rows) |
| [`fearnleys_comments_tanker.json`, `_dry`, `_gas`, `_snp`](file:///c:/Users/Dell/Github/Shipping/data/derived/fearnleys_comments_tanker.json) | Broker-comment archive chunked per desk for lazy load: tanker 8,775 / dry 1,525 / gas 1,088 / snp+rest 321 (11,709 non-blank of 11,714 CSV rows) | Sep 2018 → live | On sync (`data_expansion.yml`) | `scripts/fearnleys/build_comment_chunks.py` | Active (~5 MB total) |
| [`vessel_leg_economics.json`](file:///c:/Users/Dell/Github/Shipping/data/derived/vessel_leg_economics.json) | Latest voyage leg per IMO (transit_days, distance_nm, derived avg kn; omitted where unrecorded) — 2,657 IMOs, 100% lineup overlap | Snapshot | Mon–Thu (`data_expansion.yml`) | `scripts/geospatial/build_vessel_leg_economics.py` | Active |

---

## 3. ETF Holdings, Flows & Backtests (`data/etf/`)

| File Name | Content Description | Start Date | Frequency | Data Source | Status |
|:---|:---|:---:|:---:|:---|:---:|
| [`bdry_liquidity.csv`](file:///c:/Users/Dell/Github/Shipping/data/etf/bdry_liquidity.csv) | BDRY ETF AUM, Shares & Safe Liquidity | Mar 2018 | Daily | Breakwave / Yahoo Finance | Active |
| [`bwet_liquidity.csv`](file:///c:/Users/Dell/Github/Shipping/data/etf/bwet_liquidity.csv) | BWET ETF AUM, Shares & Safe Liquidity | May 2023 | Daily | Breakwave / Yahoo Finance | Active |
| [`BDRY_flows.csv`](file:///c:/Users/Dell/Github/Shipping/data/etf/BDRY_flows.csv) | BDRY Capital Net Flows ($M) | Mar 2018 | Daily | Breakwave Advisors | Active |
| [`BWET_flows.csv`](file:///c:/Users/Dell/Github/Shipping/data/etf/BWET_flows.csv) | BWET Capital Net Flows ($M) | May 2023 | Daily | Breakwave Advisors | Active |
| [`bdry_holdings_history.csv`](file:///c:/Users/Dell/Github/Shipping/data/etf/bdry_holdings_history.csv) | BDRY Daily Contract Basket History | Jun 2026 | Daily | Breakwave Advisors | Active |
| [`bwet_holdings_history.csv`](file:///c:/Users/Dell/Github/Shipping/data/etf/bwet_holdings_history.csv) | BWET Daily Contract Basket History | Jun 2026 | Daily | Breakwave Advisors | Active |
| [`bdry_daily_dollar_decomposition.csv`](file:///c:/Users/Dell/Github/Shipping/data/etf/bdry_daily_dollar_decomposition.csv) | BDRY Daily Variation Margin & PnL Attribution | Jun 2026 | Daily | Breakwave Advisors | Active |
| [`bwet_daily_dollar_decomposition.csv`](file:///c:/Users/Dell/Github/Shipping/data/etf/bwet_daily_dollar_decomposition.csv) | BWET Daily Variation Margin & PnL Attribution | Jun 2026 | Daily | Breakwave Advisors | Active |
| [`bdry_daily_return_backtest.csv`](file:///c:/Users/Dell/Github/Shipping/data/etf/bdry_daily_return_backtest.csv) | BDRY Bottom-Up vs Actual NAV Backtest | Jun 2026 | Daily | Breakwave Advisors | Active |
| [`bwet_daily_return_backtest.csv`](file:///c:/Users/Dell/Github/Shipping/data/etf/bwet_daily_return_backtest.csv) | BWET Bottom-Up vs Actual NAV Backtest | Jun 2026 | Daily | Breakwave Advisors | Active |
| [`BDRY_Daily.csv`](file:///c:/Users/Dell/Github/Shipping/data/etf/BDRY_Daily.csv) | Legacy BDRY P/D History | Mar 2018 | Daily | Legacy Amplify Feed | Synced from `bdry_liquidity.csv` |
| [`BWET_Daily.csv`](file:///c:/Users/Dell/Github/Shipping/data/etf/BWET_Daily.csv) | Legacy BWET P/D History | May 2023 | Daily | Legacy Amplify Feed | Synced from `bwet_liquidity.csv` |
| [`bdry_holdings.csv`](file:///c:/Users/Dell/Github/Shipping/data/etf/bdry_holdings.csv) | Current BDRY Portfolio Snapshot | N/A | Daily | Breakwave Advisors | Static Snapshot |
| [`bwet_holdings.csv`](file:///c:/Users/Dell/Github/Shipping/data/etf/bwet_holdings.csv) | Current BWET Portfolio Snapshot | N/A | Daily | Breakwave Advisors | Static Snapshot |

---

## 4. Freight Futures / FFAs (`data/futures/`)

| File Name | Content Description | Start Date | Frequency | Data Source | Status |
|:---|:---|:---:|:---:|:---|:---:|
| [`bdryff_history.csv`](file:///c:/Users/Dell/Github/Shipping/data/futures/bdryff_history.csv) | BDRYFF Dry Bulk Forward Curve History | Feb 2010 | Daily | Breakwave / SGX | Active |
| [`bwetff_history.csv`](file:///c:/Users/Dell/Github/Shipping/data/futures/bwetff_history.csv) | BWETFF Tanker Forward Curve History | Dec 2016 | Daily | Breakwave / SGX | Active |
| [`sgx_cape_futures.csv`](file:///c:/Users/Dell/Github/Shipping/data/futures/sgx_cape_futures.csv) | SGX Capesize FFA Forward Curve | Dec 2024 | Daily | Singapore Exchange (SGX) | Active |
| [`sgx_panamax_futures.csv`](file:///c:/Users/Dell/Github/Shipping/data/futures/sgx_panamax_futures.csv) | SGX Panamax FFA Forward Curve | Dec 2024 | Daily | Singapore Exchange (SGX) | Active |
| [`sgx_supramax_futures.csv`](file:///c:/Users/Dell/Github/Shipping/data/futures/sgx_supramax_futures.csv) | SGX Supramax FFA Forward Curve | Aug 2024 | Daily | Singapore Exchange (SGX) | Active |
| [`sgx_handysize_futures.csv`](file:///c:/Users/Dell/Github/Shipping/data/futures/sgx_handysize_futures.csv) | SGX Handysize FFA Forward Curve | Dec 2024 | Daily | Singapore Exchange (SGX) | Active |
| [`sgx_cape_futures_history.csv`](file:///c:/Users/Dell/Github/Shipping/data/futures/sgx_cape_futures_history.csv) | SGX Capesize FFA **Full Contract Lives** — prices only on cleared sessions (~7 per typical life; SGX redacts lookback prices), volume + open-interest full-depth back to 2022 even where prices are zeroed | 2022 (activity) / Mar 2026+ daily prices | Daily (`--rebuild` + Mon–Thu CI refresh) | Singapore Exchange (SGX) via `expansion_sgx_history_backfill.py` | Active (~119k rows; frontend **Contract Archive** shows each contract's traded-day count — lazy per-vessel-class load, session-cached) |
| [`sgx_panamax_futures_history.csv`](file:///c:/Users/Dell/Github/Shipping/data/futures/sgx_panamax_futures_history.csv) | SGX Panamax FFA Full Contract Lives | 2022 | Daily (`--rebuild` + Mon–Thu CI refresh) | Singapore Exchange (SGX) via `expansion_sgx_history_backfill.py` | Active (~37k rows) |
| [`sgx_supramax_futures_history.csv`](file:///c:/Users/Dell/Github/Shipping/data/futures/sgx_supramax_futures_history.csv) | SGX Supramax FFA Full Contract Lives | 2022 | Daily (`--rebuild` + Mon–Thu CI refresh) | Singapore Exchange (SGX) via `expansion_sgx_history_backfill.py` | Active (~122k rows) |
| [`sgx_handysize_futures_history.csv`](file:///c:/Users/Dell/Github/Shipping/data/futures/sgx_handysize_futures_history.csv) | SGX Handysize FFA Full Contract Lives | 2022 | Daily (`--rebuild` + Mon–Thu CI refresh) | Singapore Exchange (SGX) via `expansion_sgx_history_backfill.py` | Active (~46k rows) |

---

## 5. Expansion Collectors (`data/congestion/`, `data/macro/`, `data/bunkers/`)

Mon–Thu 05:00 UTC via `.github/workflows/data_expansion.yml`. All collectors are
idempotent upserts, retry x3 with backoff, and fail gracefully without corrupting
existing data.

| File Name | Content Description | Coverage | Frequency | Data Source | Status |
|:---|:---|:---:|:---:|:---|:---:|
| [`data/congestion/chokepoint_transits_daily.csv`](file:///c:/Users/Dell/Github/Shipping/data/congestion/chokepoint_transits_daily.csv) | Daily transit counts across 28 maritime chokepoints (Suez, Panama, Bosporus, Malacca, ...) by vessel class | 2019-01-01 → live | Daily (incremental) | IMF PortWatch ArcGIS (`Daily_Chokepoints_Data`) via `expansion_portwatch.py` | Active (~78k rows; upstream lags ~5 days) |
| [`data/congestion/chokepoint_geo_summary.json`](file:///c:/Users/Dell/Github/Shipping/data/congestion/chokepoint_geo_summary.json) | Per-chokepoint frontend cache: monthly + 90d-daily series (total/tanker/bulk/container/**general-cargo/Ro-Ro**/capacity), 7/30d avgs, baseline deltas, rerouting telemetry | 2019-01-01 → live | Mon–Thu (`data_expansion.yml`) | `scripts/geospatial/build_chokepoint_cache.py` | Active (28 cps, 563 KB) |
| [`data/congestion/port_calls_daily.csv`](file:///c:/Users/Dell/Github/Shipping/data/congestion/port_calls_daily.csv) | Daily port call volumes for curated major ports by segment | 2019-01-01 → live (chunked incremental; pre-Build C stalled at 2020-10-30, now backfilling) | Daily (incremental) | IMF PortWatch ArcGIS (`Daily_Ports_Data`) via `expansion_portwatch.py` | Active (curated set) |
| [`data/macro/commodities_monthly.csv`](file:///c:/Users/Dell/Github/Shipping/data/macro/commodities_monthly.csv) | World Bank Pink Sheet monthly commodity prices — iron ore, coal, crude, natgas, LNG, grains, metals + CMO indices. Core cargo-demand inputs for dry bulk & tanker analysis; rendered on the Signals tab as **Cargo Demand Drivers**. Series the Pink Sheet no longer publishes (all-empty columns, e.g. `coal_newcastle` after the 2026 WB series restructure) are auto-dropped from the schema on each refresh | Jan 1960 → live (monthly) | Monthly (~4th of month, prior-month data) | World Bank CMO xlsx via `expansion_worldbank_pinksheet.py` | Active (current through Jul 2026) |
| [`data/bunkers/bunker_prices_daily.csv`](file:///c:/Users/Dell/Github/Shipping/data/bunkers/bunker_prices_daily.csv) | Bunker fuel prices ($/mt): VLSFO / MGO / IFO380 across global average, regional averages and 8 major hubs (Singapore, Rotterdam, Fujairah, Houston, ...) | Live snapshots accumulate | Daily (snapshot append) | Ship & Bunker tabbed price tables via `expansion_bunker_prices.py` | Active |

> [!NOTE]
> **Retired expansion targets** (removed 2026-08-22 after source access was lost):
> OPEC MOMR appendix (Cloudflare IP-block on all opec.org routes), GMS weekly
> demolition rates (moved behind the Ship Recycling Portal login — the dashboard's
> $/LDT needs are served by `data/derived/scrappage_prices.csv` from Hellenic OCR),
> Intermodal fleet/orderbook PDFs (form-gated), and macro rates/FX (`rates_fx.csv`
> had no consumer in this shipping-focused repo).

### 5.1 Knowledge Pipeline Artifacts Consumed by the Frontend

| File | Producer | Consumer | Notes |
|:---|:---|:---|:---|
| `knowledge/chunks/index.json` | `process_knowledge.py::write_chunk_index()` (emitted after every derived rebuild) | Q&A panel shard discovery + `generate_brief.py` | Small stat-only manifest (`file/stem/year/bytes` per `.jsonl` shard). Fixes the Jan-1 year-rollover bug where hardcoded shard lists silently missed the new year's files. Frontend falls back to its static list when the manifest is absent (e.g. stale local checkout). |
| `knowledge/chunks/search/index.json` + `search/{stem}.idx.json` | `scripts/search_index_build.py` via `build_derived()` post-pass | Q&A fast-path retrieval (B1) | BM25-ready per-shard posting indexes: vocab + per-doc top-40 terms, `38.6 MB` total across 77 shards vs ~141 MB raw text. Browser ranks candidates from these first and downloads only hit shards; falls back to legacy full scan if unavailable. `i` = ordinal among parsed lines (aligned with the frontend's parsed-row arrays); per-candidate `chunk_id` verification guards against staleness. |
| `knowledge/chunks/*.jsonl` | `process_knowledge.py` | Q&A BM25 retrieval | New/reprocessed documents get sentence-aware chunk boundaries and full Breakwave bullet sentences; existing corpus is untouched until a natural re-process (no `COMPILER_VERSION` bump, avoiding a mass re-OCR run). Chunks now carry `source_url` provenance. Archived bot-challenge pages are labelled `is_error_page` and excluded from signals/derived data. Image-asset chunks may carry a `[structured table]` markdown block (B2 geometry-based table recovery) above the raw OCR text. |
| `knowledge/derived/breakwave_signals.json` | `build_derived()` | Signals tab fast path | Kept in the Pages deploy (62 KB) so production uses the relative-path load instead of the 88 MB `signals.jsonl` raw.githubusercontent fallback. |
| `knowledge/manifests/derived_cache.json`, `knowledge/derived/.wiki_*_cache.json` | `build_derived()` / `build_wiki.py` (B4) | none (local speed caches) | Content-addressed incremental-build caches, **gitignored/local-only** (~305 MB). Cold run = full rebuild (CI behavior unchanged); warm run measured 3.75× faster with byte-identical outputs. `KNOWLEDGE_FULL_DERIVED=1` bypasses. |

---

## 6. Agricultural, Container & Multi-Broker Intelligence (`data/commodities/`, `data/indices/`, `data/derived/`)

Ingested via scheduled workflows (`.github/workflows/usda_weekly.yml`, `.github/workflows/poten_drewry_weekly.yml`, `.github/workflows/broker_reports_weekly.yml`).

| File Name | Content Description | Coverage | Frequency | Data Source | Status |
|:---|:---|:---:|:---:|:---|:---:|
| [`data/indices/drewry_wci_historical.csv`](file:///c:/Users/Dell/Github/Shipping/data/indices/drewry_wci_historical.csv) | Drewry World Container Index (WCI) — Composite 40ft spot rate ($/FEU) & East-West routes (Shanghai-Rotterdam, Genoa, LA, NY) | 2011–live (Wayback-assessed, gaps=null, see note) | Weekly (Thu) | Drewry Supply Chain Advisors via `fetch_drewry_wci.py` + `backfill_drewry_wayback.py` (Wayback CDX) | Active |
| [`data/commodities/usda_fas_outstanding_export_sales.csv`](file:///c:/Users/Dell/Github/Shipping/data/commodities/usda_fas_outstanding_export_sales.csv) | USDA FAS Weekly Export Sales — Outstanding commitments and accumulated exports by commodity (Corn, Soybeans, Wheat) and destination country | ~60k most-recent rows (date DESC + offset pagination) | Weekly (Thu) | USDA Foreign Agricultural Service Open API (`885i-uek7`) via `fetch_usda_fas_exports.py` | Active |
| [`data/commodities/panama_canal_draft_and_slots.csv`](file:///c:/Users/Dell/Github/Shipping/data/commodities/panama_canal_draft_and_slots.csv) | Panama Canal Authority (ACP) Advisories — Maximum allowable draft limits (TFW ft), Gatun Lake water levels, and transit booking slots across El Niño drought periods | 2022–live | Periodic / Weekly | Panama Canal Authority (ACP) via `fetch_panama_canal_advisories.py` | Active |
| [`data/commodities/usda_us_vs_brazil_landed_costs.csv`](file:///c:/Users/Dell/Github/Shipping/data/commodities/usda_us_vs_brazil_landed_costs.csv) | USDA Landed Soybean Transportation Costs ($/MT) to Shanghai — Multi-modal logistics breakdown (Truck, Rail/Barge, Ocean) comparing US Midwest vs Brazilian Cerrado | 2017–live | Quarterly | USDA AgTransport Socrata Open API via `fetch_usda_grains.py` | Active |
| [`data/commodities/usda_grain_vessel_loading_queues.csv`](file:///c:/Users/Dell/Github/Shipping/data/commodities/usda_grain_vessel_loading_queues.csv) | USDA Grain Vessel Loading Queues — Weekly counts of bulk carriers In-Port, Loaded (Past 7 Days), and Due (Next 10 Days) at US Gulf and PNW terminals | 2020–live | Weekly (Thu) | USDA AgTransport Socrata Open API via `fetch_usda_grains.py` | Active |
| [`data/derived/usda_grain_vessel_rates_japan.csv`](file:///c:/Users/Dell/Github/Shipping/data/derived/usda_grain_vessel_rates_japan.csv) | USDA Bulk Grain Ocean Freight Rates ($/MT) to Japan — US Gulf vs Pacific Northwest (PNW) export rates and spatial freight spread | 2017–live | Weekly | USDA AgTransport via `fetch_usda_grains.py` | Active |
| [`data/derived/usda_us_vs_brazil_cost_spreads.csv`](file:///c:/Users/Dell/Github/Shipping/data/derived/usda_us_vs_brazil_cost_spreads.csv) | Landed Cost Spreads ($/MT) — US vs Brazil landed soybean cost differential to China | 2017–live | Quarterly | USDA AgTransport via `fetch_usda_grains.py` | Active |
| [`data/derived/usda_bunker_fuel_daily.csv`](file:///c:/Users/Dell/Github/Shipping/data/derived/usda_bunker_fuel_daily.csv) | USDA Bunker Fuel Daily Spot Prices ($/MT) — VLSFO 0.5%, MGO, IFO 180cSt, IFO 380cSt | 2019–live | Daily | USDA AgTransport via `fetch_usda_grains.py` | Active |
| [`data/derived/intermodal_tc_rates.csv`](file:///c:/Users/Dell/Github/Shipping/data/derived/intermodal_tc_rates.csv) | Intermodal Shipbrokers Weekly Period TC Rates ($/day) — Fills MR, LR1, Handysize, and 3Y period charter gaps | 2025–live | Weekly (Fri) | Intermodal Research via `update_intermodal_tc_rates.py` | Active |

> [!NOTE]
> **Container depth (Build F, no-break):** `data/indices/drewry_wci_historical.csv`
> now extends to **2011 via the Wayback Machine** (`backfill_drewry_wayback.py`
> + `fetch_drewry_wci.py::backfill_drewry_wayback()` using the CDX API over
> `drewry.co.uk` WCI pages, 2011→present). Only Drewry-assessed snapshots are
> kept — **no pre-2024 values are synthesized**; missing weeks stay absent and
> the frontend renders them as gaps (`spanGaps:true`). Upsert is idempotent
> (candidate in `/tmp`, then dedup by `date` + sort, canonical header
> `date,composite_index,...` preserved). The old Build C synthetic/canonical
> placeholder (`generate_canonical_wci_history()`, Red-Sea-spike shape) is
> removed.
> **FBX:** Freightos Baltic Index (FBX) upstream history starts **2017 via
> Freightos** (local `fbx_historical.csv` is currently a Mar-2026+ slice —
> full 2017→present backfill is a follow-up). **SCFI opportunity:** the
> Shanghai Containerized Freight Index (SSE-SCFI, 2009+) is the natural next
> container benchmark to ingest.

---

## 7. Upstream Physical Commodity Flows, Logistics & Environmental Regimes (`data/commodities/`, `data/congestion/`, `data/derived/`)

Ingested weekly/monthly via `.github/workflows/upstream_commodity_flows.yml` and specialized scrapers in `scripts/scrapers/`.

| File Name | Content Description | Coverage | Frequency | Data Source | Status |
|:---|:---|:---:|:---:|:---|:---:|
| [`data/commodities/brazil_comexstat_exports.csv`](file:///c:/Users/Dell/Github/Shipping/data/commodities/brazil_comexstat_exports.csv) | Brazilian seaborne exports: Iron Ore (`NCM 2601`), Crude Oil (`NCM 2709`), Soybeans (`NCM 1201`), Raw Sugar (`NCM 1701`) | 1997–live (year-by-year, Build C) | Monthly | Brazilian MDIC / SECEX ComexStat API (`api-comexstat.mdic.gov.br`) | Active |
| [`data/commodities/australia_ppa_iron_ore.csv`](file:///c:/Users/Dell/Github/Shipping/data/commodities/australia_ppa_iron_ore.csv) | Pilbara Ports Authority iron ore throughput: Port Hedland (15 mos, `live_ppa_archive` via Wayback cargo-stats-by-destination PDFs) & Port of Dampier (2024-07→2026-05 continuous, `live_ppa_dampier` via PPA Dampier FY cargo-statistics PDFs) (Mt, MoM%, YoY%) | 2024–live | Monthly | Pilbara Ports Authority (PPA) Shipping Statistics | Active |
| [`data/commodities/major_miners_quarterly_shipments.csv`](file:///c:/Users/Dell/Github/Shipping/data/commodities/major_miners_quarterly_shipments.csv) | Big 4 Iron Ore Miners: Vale, Rio Tinto, BHP, Fortescue (Production Mt, Shipments Mt, C1 Cash Cost $/t, Annual Guidance) | 2024–live | Quarterly | Mining Company Operations Reports & Production Releases | Active |
| [`data/commodities/us_eia_weekly_crude_exports.csv`](file:///c:/Users/Dell/Github/Shipping/data/commodities/us_eia_weekly_crude_exports.csv) | US Gulf Coast (PADD 3) & Total US weekly crude and petroleum exports (`WCREXUS2`, kbpd, 4W MA) | 1991–live with key, else existing 2017+ kept as-is (no synthetic fallback, Build C) | Weekly (Wed) | US Energy Information Administration (EIA Weekly Petroleum Status Report, `EIA_API_KEY` required) | Active |
| [`data/congestion/portwatch_port_congestion.csv`](file:///c:/Users/Dell/Github/Shipping/data/congestion/portwatch_port_congestion.csv) | Core12 measured port activity (Qingdao, Ningbo, Port Hedland, Newcastle, Singapore, Rotterdam, Houston, Tubarao, Santos, Rizhao, Hay Point, Qinhuangdao + All): daily port calls by class + dry-bulk/tanker import/export tonnages (kt), real IMF PortWatch AIS observations only — no waiting-time/anchored-ship synthesis (withdrawn 2026-08-25) | 2019–live | Daily | IMF PortWatch ArcGIS `Daily_Ports_Data` FeatureServer (`fetch_portwatch_port_activity.py`) | Active |
| [`data/derived/eu_ets_carbon_daily.csv`](file:///c:/Users/Dell/Github/Shipping/data/derived/eu_ets_carbon_daily.csv) | EU ETS EUA spot carbon allowance (€/t CO2), Singapore/Rotterdam/Houston Hi-5 bunker fuel spreads ($/MT), and daily scrubber savings ($/day) | 2024–live | Daily | European Energy Exchange (EEX) / Ship & Bunker | Active |
| [`data/commodities/newcastle_coal_exports.csv`](file:///c:/Users/Dell/Github/Shipping/data/commodities/newcastle_coal_exports.csv) | Port of Newcastle monthly coal export throughput only (Mt, Thermal/Met, Vessels Loaded) — Dalrymple Bay (DBCT) and Gladstone are not yet in this feed | 2018–live | Monthly | Port of Newcastle / TfNSW opendata (`fetch_newcastle_coal.py`) | Active |
| [`data/commodities/australia_req_commodity_exports.csv`](file:///c:/Users/Dell/Github/Shipping/data/commodities/australia_req_commodity_exports.csv) | Australia DISR Resources and Energy Quarterly: Iron Ore, Metallurgical Coal, Thermal Coal, LNG, Bauxite/Alumina export volumes (Mt) and values (AUD Bn) | 2024–live | Quarterly | Australian Department of Industry, Science and Resources (DISR REQ) | Active |
| [`data/derived/ton_mile_utilization_matrix.csv`](file:///c:/Users/Dell/Github/Shipping/data/derived/ton_mile_utilization_matrix.csv) | Capesize-only global ton-mile absorption (diagnostic, Bn Ton-NM) and modeled fleet utilization ($U\%$) under spatial trade routing and port congestion constraints | 2024–live | Monthly | Quantitative Ton-Mile Distance & Elasticity Engine (`generate_ton_mile_matrix.py`) | Active |

---

## 🛠️ Automated Health Check Commands

To verify dataset freshness and staleness across all datasets at any time:
```bash
python scripts/check_data_health.py
```

Spike / flatline guard (flag-only, never fabricates or deletes `data/`):
```bash
python scripts/check_data_spike_health.py
```
Flags WoW >30% jumps, 3-sigma breaks vs prior 252, >15 flatline repeats and empty rows into `knowledge/manifests/spike_queue.jsonl` (one JSON object per line, always exits 0).

Rich-tooltip overhaul (`getCalculatedTooltip` in `index.html`): `concept-brazil-exports`, `concept-ppa-throughput`, `concept-eia-exports`, `concept-port-congestion`, `concept-carbon-ets`, `concept-ton-mile-sim`.
