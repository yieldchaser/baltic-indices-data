# Pilot 20Q — muse-spark (read-only retrieval-need probe)

- Author: muse-spark (coder subagent)
- Date: 2026-09-06
- Branch: `agent/muse-spark`, worktree `C:\Users\Dell\Github\shipping-muse-spark` (no commits)
- Parent: `docs/INVENTORY_MUSE_SPARK.md` §11 (the 20 questions); method companion to §§3/6/10-P2
- Scope: **read-only pilot. No extraction, no shard writes, no scope commitment.**

## 0. Explicit non-commitment

**This file measures retrieval need, not batch scope.** It answers, per question,
whether today's committed retrieval surfaces can serve the question (single-hop),
need joins across surfaces (multi-hop), or are stopped by a missing/unwired leg or
by extraction-quality failure. It does **not** commit to any extraction batch, any
graph-vendor choice, any re-OCR scope, or any P0-queue reversal. Verdict labels:

- `single-hop` — answerable from one retrieval surface today.
- `multi-hop` — needs ≥2 surfaces; hop chain listed with concrete ids/files.
- `blocked-unwired` — a necessary leg is absent from, or unwired to, the retrieval
  surfaces (named). "Unwired" includes files present on disk but outside
  `knowledge/` manifests/UI wiring, because the pilot probes the retrieval layer.
- `blocked-ocr-quality` — a necessary leg is present but extraction-quality-dead
  (suspect asset named; includes the JS-render Poten case, flagged as such).

## 1. Method (all figures measured in-worktree at HEAD 68ae74fd0)

- Grepped `knowledge/chunks/*.jsonl` (81 files probed; full-repo sweeps where noted)
  for per-question keywords; resolved `chunk_id`/`doc_id`/`section_id`(=node_id).
- Read tree shard `knowledge/trees/hellenic/dry_charter/2026/2026-09-02_*.json`
  directly (root + `children` node_ids) to confirm the layer-over reference shape.
- Read `knowledge/manifests/{sources,coverage_report,documents}.jsonl|json`,
  `knowledge/derived/{signals,themes,topic_evidence,section_index}.jsonl`,
  `timelines.json`, `breakwave_signals.json`.
- Row-looked 25 `data/` CSVs (columns, n, first/last dates) via throwaway scripts
  under `C:\Users\Dell\AppData\Local\Temp\opencode` (`csv_windows.py`,
  `chunk_probes*.py`, `manifest_probe*.py`, `text_probe*.py`, `final_nums.py`).
- Quoted chunk `text` verbatim for latest-week docs to test OCR readability.
- Checked `index.html` `fetch(` targets (8) and process env for vision vendor keys
  (names only — NONE found; vision stays BLOCKED per INGESTED_IMAGE_AUDIT §7).
- Marked `uncertain` wherever the evidence is thin instead of guessing.

Corpus snapshot reused from INVENTORY: 8,850 docs / 101,967 chunks / 31,228 sections;
latest-week anchors — hellenic dry/tanker 2026-09-02, valuations/demolition/
shipbuilding 2026-09-01, iron_ore 2026-09-04, baltic 2026-10-04 (stubs, see Q1),
poten 2026-08-24, broker Intermodal/Bancosta 2026-09-02, SGX 2026-09-04,
CapitalLink master 2026-09-03, Baltic indices CSVs 2026-08-10 (stale ~3wk).

## 2. Per-question verdicts

### Q1 — Capesize fixtures vs Baltic C5TC → 5yr Capesize valuations — `multi-hop`
- Hop 1 (hellenic estimates, OCR-noisy): doc
  `hellenic_dry_charter_2026-09-02_..._september_2_2026`, node
  `...__s02_linked_asset_..._02092026dry_a472ff592510_jpg`
  (chunk `..._002`, len 749). `__s01_main` is a 29-char stub
  ("Linked asset: 02092026dry.jpg"); all numbers live in the image OCR, which
  carries glyph garble (`A L| B R A`, `(Sipdpr)`, `(38% dint)`, `SMAXJULTRA`,
  `PANAIKMAX`). NOTE: hellenic dry_charter holds TC *estimates*, not named
  fixtures — the named-fixture leg must come from hop 2.
- Hop 2 (named fixtures): `broker_reports_broker_report_2026-09-02_intermodal...`
  (doc `..._week_35_2026_broker_s_insi`, 25 chunks; e.g. chunk `..._insi_009`
  `GW Elite 82,177 ... $18,500 Swissmarine` + `Baltic Indices` table).
- Hop 3 (C5TC): 2026 baltic chunks are stubs
  (`baltic_dry_2026-10-04..._001`, len 34, "04 Sept 2026 / Bulk report - Week 36");
   only 8 `C5TC` hits in baltic_dry.jsonl (72 repo-wide incl. breakwave/broker), newest full text
  `baltic_dry_2019-06-21..._001` ("C5TC market opened Monday at $14,203...").
  Current-week C5TC must come from `data/indices/cape_historical.csv`
  (4,312 rows, 2008-10-06→2026-08-10 — ~3wk stale) or the broker Baltic table.
- Hop 4 (5yr value): `hellenic_vessel_valuations_2026-09-01...` `__s01_main`
  (chunk `..._001`, S&P narrative "Bulker values remain stable") — but its
  `__s02` matrix-image OCR is garbled beyond use (`at wes ee a soc eos...`,
  chunk `..._003`); clean age curve instead in
  `data/derived/vessel_valuations_matrix.csv` (9 rows; Capesize 5y **$67.50m**,
  10y $48.00m, 15y $31.50m, scrap $10.80m, yield 20.01%).
- 4 hops across tree node ids + 2 CSVs. OCR caveat on hops 1/4-image, both with
  clean fallbacks (broker text, matrix CSV).

### Q2 — SGX FEF front/back spread vs Baltic capesize basket — `multi-hop`
- Hop 1 (SGX, unwired-to-knowledge but on disk):
  `data/futures/sgx_iron_ore_fef_history.csv` (95,494 rows, 2018-01-19→2026-09-04;
  40 contracts on last date) + `sgx_iron_ore_fef.csv` snapshot. `sources.json`
  omits SGX; no SGX `fetch(` in index.html (8 targets checked).
- Hop 2 (basket): baltic 2026 stubs (Q1) → `data/indices/cape_historical.csv`
  (→2026-08-10) or broker Intermodal BCI/BDI section
  (chunk `..._insi_009/010`: `BCI BPI BSI`, `Average T/C Rates`).
- Spread + direction both computable; join key is week. Thin: Baltic-index CSV
  staleness (~3wk) noted, not blocking.

### Q3 — Kamsarmax ex-ECSA implied TCE vs Baltic avg, Singapore bunker net — `multi-hop`
- Hop 1 (fixture/route): `hellenic_dry_charter_202*.jsonl` carries 26 `kamsarmax`
  hits (e.g. doc `hellenic_dry_charter_2021-07-14..._001`) and 68 ECSA/Brazil hits
  in the 2026 file (doc `hellenic_dry_charter_2026-01-07..._001`).
  `TCE` = 0 hits in hellenic 2026 chunks — implied-TCE must be *computed*.
- Hop 2 (Baltic avg): `data/indices/panama_historical.csv` (4,312 rows →08-10)
  or broker text (Q2).
- Hop 3 (bunkers): `data/bunkers/bunker_prices_daily.csv` (378 rows,
  2026-08-22→09-03; ports incl `singapore`, `rotterdam`; grades VLSFO/MGO/IFO380)
  + `data/bunkers/bunker_master_historical.csv` (482,024 rows,
  obs 2018-02-12→2026-09-04; Singapore present).

### Q4 — Ultramax 5/10/15yr move this month + fixture support — `multi-hop`
- Hop 1 (curve): `ultramax` = 7 hits in
  `knowledge/chunks/hellenic_vessel_valuations_2026.jsonl`
  (docs 2026-07-09/15/21, e.g. chunk
  `hellenic_vessel_valuations_2026-07-09..._001`); month-move needs the weekly
  series across those node ids. Clean levels in matrix CSV: Ultramax
  5y **$31.50m** / 10y **$24.00m** / 15y **$16.50m** (verified row).
  `data/derived/vessel_valuations.csv` (20,499 rows, 1970-12-01→2026-08-05)
  gives the monthly history.
- Hop 2 (fixture support/contradict): hellenic dry OCR image (SMAX/ULTRA row,
  Q1 node) + broker S&P/fixture text (Q1 hop 2).

### Q5 — CFTC grains positioning → Panamax demand vs USDA flows — `blocked-unwired`
- Positioning leg ABSENT: `data/cftc_statements/parsed/` holds only BDRY/BWET
  fund ledgers (`bdry_monthly_cftc_ledger.csv` cols = fund NAV/share/PNL —
  `fund,period_ended,...,realized_futures_pnl_dollars,...`, no COT positioning)
  + `statement_text_audit.csv` (138 rows). Repo-wide `CFTC` chunk hits are false
  positives (book prose). No grains (or any commodity) COT in knowledge or data.
- Flow leg present: `data/derived/usda_grain_vessel_rates_japan.csv` (368 rows,
  Jan-1996→Aug-2026) BUT `usda_grain_freight_spreads.csv` is EMPTY (0 cols,
  0 rows); `usda_us_vs_brazil_cost_spreads.csv` ends 12/31/2025.
- Blocked on: **CFTC grains/COT speculative-positioning source (unwired & absent)**.

### Q6 — BDRY 10-Q futures-roll disclosure vs SGX/indices since quarter-end — `blocked-unwired`
- 10-Q leg unwired: `docs/BDRY-BWET_Form10-Q_March-31-2026.pdf` (66pp) exists on
  disk but ZERO `docs/` `source_path` rows in `documents.jsonl` (verified); the
  128 BDRY-mention docs are breakwave PDFs *mentioning* BDRY
  (e.g. `breakwave_drybulk_2021-10-26`), not the filing. Roll-cost prose shows
  only 2 weak breakwave_insights hits (docs 2026-06-05/06-29), not the disclosure.
- Validation leg present-but-unwired: SGX histories (Q2) + indices (Q10).
- Blocked on: **`docs/` 10-Q/factsheet/prospectus filings + SGX futures (both
  unwired to knowledge/manifests/UI)**. Unblock path exists: P1 Sample B already
  validated the 10-Q p6 investments-table pattern (9×4, tie-out exact).

### Q7 — Week-high demolition $/ldt vs oldest Capesize valuation — `multi-hop`
- Hop 1 (demolition): doc `hellenic_demolition_2026-09-01_..._week_35_2026`:
  `__s01_main` stub (86 chars) + `__s02` linked-PDF **native text, clean**
  (chunk `..._002`, len 1411: `$/LT Ldt` table `$425 $465 $480 $271`...,
  `WEEKLY TREND: UPWARDS`, yearly-demolition chart text).
  Corroboration: `data/derived/scrappage_prices.csv` (379 rows,
  2021-07-03→2026-09-01) + broker Intermodal `Indicative Demolition Prices ($/ldt)`
  (chunk `..._insi_021`). Caveat: `data/demolition/shipandbunker_demolition_fixtures.csv`
  (465 rows) has NO price columns (`year,week,sale_date,vessel_name,vessel_type,
  build_date,seller,source`) — "which fixture set the high" at fixture grain is
  `uncertain`.
- Hop 2 (oldest Capesize value): valuations `__s01_main` chunk `..._001` —
  oldest bulker sold that week is Capesize Jian Fa (2004, $18.5m vs VV $18.8m);
  matrix CSV gives 15y Capesize $31.50m + scrap $10.80m for the scrap-compare leg.

### Q8 — Poten latest tanker opinion vs Baltic dirty VLCC — `blocked-ocr-quality`
- Poten leg quality-dead: latest doc `poten_tankers_2026-08-24_..._will_he_or_won_t_he`
  = 9 chunks, `__s01` metadata only (len 170) + `__s02` **nav boilerplate**
  (chunks `..._002`–`..._009`, e.g. `..._002` len 2096: "About Us What We Do
  Services ... LNG Contract Intelligence Service ..."). JS-rendered body missing
  (INVENTORY §7e re-confirmed in-worktree). 68 `VLCC` hits in
  `poten_tankers_2026.jsonl` predate it (e.g. doc `poten_tankers_2026-01-09_..._show_me_the_barrels`).
- Baltic leg servable: `baltic_tanker_2026.jsonl` 31 VLCC/TD3C/dirty hits
  (docs 2026-05-15/22/29, though 2026 bodies are stubs — Q1 pattern) +
  `data/indices/dirtytanker_historical.csv` (4,499 rows →08-10) + broker
  Intermodal tanker section (VLCC/Suezmax/Aframax TCE + TD3/TD6/TD9 tables,
  chunks `..._insi_005`–`..._008`).
- Suspect asset: **Poten latest doc nodes
  `poten_tankers_2026-08-24...__s01/__s02` (+ mirror `reports/poten/`)** —
  grouped under this verdict as extraction-quality failure (JS-render, not OCR;
  stated precisely). Fallback (older Poten + broker + CSV) noted but does not
  answer "latest opinion".

### Q9 — OSV dayrate bracket moved most + hellenic match — `multi-hop`
- Hop 1 (single-CSV answer): `data/derived/seabrokers_osv_dayrates.csv`
  (371 rows, 2018-05-01→2026-08-01, 4 categories). Latest month 2026-08-01:
  **AHTS >22,000 BHP +499.19% YoY (£96,015)** > AHTS <22k +292.05% >
  PSVS <900m² +279.99% > PSVS >900m² +261.94% (computed).
- Hop 2 (hellenic match — thin/expected-negative): `OSV|PSV|AHTS|offshore` = 476
  repo-wide hits but concentrated in baltic_gas/tanker; hellenic's six wired
  categories (sources.json) contain no OSV market — the join likely proves a
  negative. Still a cross-node hop (proving the negative needs the search).
  No OSV fixture/valuation catalog is wired anywhere.

### Q10 — CLDBI WoW vs BDI + constituent gap — `multi-hop`
- Hop 1: `data/indices/capital_link_drybulk_cldbi.csv` (5,230 rows,
  2005-01-03→2026-09-03, OHLCV) + `capital_link_indices_master.csv`
  (5,245 rows, all 7 indices →09-03).
- Hop 2: `data/indices/bdiy_historical.csv` (10,492 rows, 1985-01-04→2026-08-10).
- Hop 3 (weighting): BDIY weighting (40% Cape / 30% Panamax / 30% Supramax) is in
  clean text — hellenic_shipbuilding latest doc chunk
  `hellenic_shipbuilding_2026-09-01..._004` (verbatim BDIY-vs-BDRYFF note).
  **Thin/uncertain:** CLDBI constituent weights are NOT in the CSVs (OHLCV only);
  XLSX constituent sheets unverified → "which constituent explains the gap" is
  answerable only directionally (sector sub-indices CLCI/CLTI/…) without wired
  holdings.

### Q11 — 10yr MR value vs Poten clean + TC-rate CSVs — `multi-hop`
- Hop 1 (paper value, clean): matrix CSV **MR Product Tanker 10y $33.00m**
  (5y $43.00m, 15y $23.50m, verified row) + hellenic tanker OCR
  (doc `hellenic_tanker_charter_2026-09-02...`, node `...__s02_linked_asset...
  _02092026tank_f27d5ab1d16b_jpg`, chunks `..._002/_003`: MR IMO3 1YR 28,000…
  with glyph noise but numerals intact) + S&P comp (MR2 PM Regent 2018, $45.5m,
  valuations `..._001`).
- Hop 2 (TC support): `data/derived/time_charter_rates.csv` (2,084 rows,
  2000-01-05→2026-09-02; `mr_1y/2y/3y/5y` cols) +
  `intermodal_tc_rates.csv` (46 rows, 2025-03-07→2026-08-21, `mr_1y_tc`) +
  `time_charter_rates_fearnleys.csv` (1,599 rows →09-02).
- Hop 3 (Poten clean — degraded): 6 `MR|clean` hits in `poten_tankers_2026.jsonl`
  sit inside the 2026-08-24 boilerplate ("Clean Fuels & Chemicals Advisory" nav
  strings) — corroborative leg only, flagged, not load-bearing.

### Q12 — Grain-port congestion + Panamax spike + PortWatch — `multi-hop`
- Hop 1 (congestion datapoint): `data/congestion/portwatch_port_congestion.csv`
  (36,361 rows, 2019-01-01→2026-08-28, **13 ports incl Santos, Tubarao,
  Houston (US-TX), Qingdao** — grain-port coverage verified) +
  `port_calls_daily.csv` (40,370 rows) + `chokepoint_transits_daily.csv`.
  `congest|portwatch|port call` = 1,085 chunk hits (mostly baltic_container).
- Hop 2 (Panamax spike): `panamax` = 533 hits in `hellenic_dry_charter_202*.jsonl`
  (6 files) + broker Intermodal Panamax/Kamsarmax section
  (chunk `..._insi_011`: "North Pacific grain volumes increased...").
- Spike-vs-congestion timing join spans CSV rows + weekly node ids — graph-shaped.

### Q13 — M65F discount to FEF vs hellenic iron-ore commentary — `multi-hop`
- Hop 1 (discount, computable): `data/futures/sgx_iron_ore_m65f_history.csv`
  (61,552 rows, 2018-12-03→2026-09-04; 28 contracts last date) ×
  `sgx_iron_ore_fef_history.csv` (95,494 rows; 40 contracts last date).
  SGX unwired (Q2) but row-complete.
- Hop 2 (commentary, dual quality): doc
  `hellenic_iron_ore_2026-09-04_..._september_4_2026` — `__s01_main` clean native
  text (chunk `..._001`, len 1213: DCE I2701 727, Qingdao spot, 35-port stocks
  **143.91mt**); `__s02` image OCR garbled (`lOPI62`, `704 a`, `10195`,
  `lOsi6S`, chunks `..._002`–`..._005`); `__s03` linked-PDF text CLEAN
  (chunks `..._006/_007`: IOPI65 842, IOSI65 117.05, SGX 62% Aug-26 99.60,
  C5 17.06). `M65F|FEF|62%|65%|58%` = 1,009 hits in the 2026 file.
- Use PDF-leg numbers, not the image OCR, for the track.

### Q14 — broker_reports vs hellenic same-fixture rate match — `multi-hop`
- Hop 1 (broker): latest broker doc `broker_reports_broker_report_2026-09-02_
  bancosta...` + Intermodal doc (25 chunks: fixtures, forward curves
  chunks `..._039`–`..._041`, S&P table chunk `..._insi_014`, orders
  `..._insi_014/015/018/019`). `fixture|charter` = 338 hits in
  `broker_reports_broker_report_2026.jsonl`; `vessel|fixture|charter` = 1,392.
- Hop 2 (hellenic): concrete vessel-level join verified —
  **HANDY DEVBULK SINEM**: hellenic valuations 2026-09-01 `__s01_main`
  ("sold to Turkish buyers for USD 14.8 mil, VV Value USD 15.0 mil") vs broker
  Intermodal S&P table ("HANDY DEVBULK SINEM 38,009 2013 ... around $15.0m").
  Same hull, $0.2m delta — the cross-node entity-resolution case in miniature.
  (Caveat: hellenic weeklies carry S&P sales + TC estimates, not voyage fixtures
  per se — "fixture first reported in hellenic" is `uncertain` at fixture grain.)

### Q15 — Singapore vs Rotterdam bunker spread + route flip — `multi-hop`
- Hop 1 (spread, exact): `bunker_prices_daily.csv` last date 2026-09-03 —
  **Singapore VLSFO 856.0 vs Rotterdam VLSFO 682.0 ($174/mt)** (verified rows);
  window 2026-08-22→09-03 only (short — `uncertain` for regime claims) +
  `bunker_master_historical.csv` depth (→09-04) + `eu_ets_carbon_daily.csv`
  Hi5 spreads (2,990 rows →09-03).
- Hop 2 (routes): C5 route levels only in old full-text baltic
  (`baltic_dry_2019-06-21..._001`: "West Australia to China C5 ... $8.00 ...
  $7.40"); current routes via broker text. Broker bunker table itself has an OCR
  suspect (`Ro:erdam` for Rotterdam, chunks `..._insi_043/_044`) — use CSVs for
  the spread, not the broker OCR.

### Q16 — Shipbuilding order → future Capesize supply vs 5yr curve — `multi-hop`
- Hop 1 (orders): hellenic_shipbuilding latest doc is a misfiled Breakwave dry
  report (doc `hellenic_shipbuilding_2026-09-01_..._9_1_2026`, chunks `..._002`–
  `..._005`: BDIY-vs-BDRYFF, fundamentals — NO orders). Real orders in broker
  Intermodal (chunks `..._insi_014/015/018/019`: **16 orders / 55 firm + 13
  optional; Shandong Marine 2+4×325k dwt @ $150m, 2030, vs Seanergy/Enesel 210k**).
  `hellenic_shipbuilding_202*.jsonl` order-hits only 4 (thin category).
- Hop 2 (supply): `data/derived/fleet_orderbook_matrix.csv` (13 rows; Capesize
  orderbook **6.09%** vs Newcastlemax **13.33%** — "most future supply"
  computable) + `coverage_report.json` topics carry shipbuilding evidence
  (but broker_reports/poten absent from topic source_counts — verified).
- Hop 3 (curve): matrix CSV Capesize 5y $67.50m (Q1).

### Q17 — CFTC crude positioning vs dirty-tanker lead/lag — `blocked-unwired`
- Positioning leg ABSENT (same gap as Q5, crude flavor): no COT in
  `data/cftc_statements/parsed/` (BDRY/BWET ledgers only) or knowledge.
- Rate leg present: `dirtytanker_historical.csv` (→08-10) + broker tanker section
  (Q8) + `tanker_forward_curves_history.csv`.
- Blocked on: **CFTC crude speculative-positioning (COT) source**.

### Q18 — ETF disclosure change (factsheet vs 10-Q) + curve justification — `blocked-unwired`
- Both disclosure legs unwired: `docs/Amplify_BDRY_FactSheet.pdf`,
  `docs/Amplify_BWET_FactSheet.pdf`, both prospectuses, and the 10-Q sit on disk
  (7 PDFs in `docs/`) with zero ledger rows (Q6). P1 Sample B proves the 10-Q p6
  table is extractable (9 contracts Apr–Jun 2026, tie-out exact) — but it is not
  in any retrieval surface.
- Holdings/curve legs present-but-unwired: `data/etf/bdry_holdings_history.csv`
  (897 rows, 2026-06-21→09-04; e.g. C5TCM FFA weights 21.21/14.02/14.76%) +
  `bwet_holdings_history.csv` (638 rows) + SGX freight histories
  (cape 183,377 rows; panamax 66,053) + broker forward curves (Q14).
- Blocked on: **ETF disclosure set (factsheets/10-Q/prospectus) + holdings/EDGAR
  wiring — unwired to knowledge/manifests/UI**.

### Q19 — fixture→TCE→yield-vs-5yr→SGX chain — `multi-hop`
- Flagship 4-hop chain (all legs evidenced above, none single-sourced):
  broker/hellenic fixture-or-estimate node (Q1/Q14)
  → `bunker_{prices_daily,master}` TCE net (Q3/Q15; 2026-09-03 Sing/Rott legs)
  → `vessel_valuations_matrix.csv` `implied_1y_charter_yield_pct`
  (Capesize **20.01%**, Kamsarmax 17.63%, Ultramax 17.96% — verified rows) vs 5y
  price → SGX FEF/M65F/cape histories confirm/reject (Q2/Q13).
- Needs entity resolution (vessel/route/week) across tree node ids + CSV weeks.

### Q20 — which single unwired source changes Q19 most — `single-hop`
- Analytic synthesis over manifest-level evidence (one reasoning surface — no
  data join required):
  - Drewry AIS: `reports/drewry/ais_manifest.csv` = **274 rows** (filename/year/
    week/vessel_class/size/sha) but **0 local PDFs** (glob-verified); WCI md only
    (`reports/drewry/2026/2026-*.drewry_wci.md` ×4) + 539 opinion md. An
    independent AIS rate/volume leg for the TCE/yield chain is manifest-only.
  - SGX: row-complete CSVs (Q2/Q13) but absent from `sources.json` `paths` and
    from index.html fetches — wiring-only gap.
  - CapitalLink: XLSX (7) + converted CSVs (master →09-03) present, same
    wiring-only gap; constituent weights unverified (Q10).
  - SNP catalog: **absent entirely** — `fearnleys_catalog.csv` (356 rows) is a
    TC-*rate* catalog (`rate_type TC`, routes incl Capesize/Panamax), not an SNP
    fixture catalog; `shipandbunker_demolition_fixtures.csv` lacks prices (Q7).
- Judgment (uncertain by construction — it is a counterfactual): **Drewry AIS**
  changes Q19 most, because SGX/CapitalLink gaps are wiring-only (data already
  row-lookable) while AIS numbers are not on disk at all; SNP-catalog absence
  bites Q14 harder than Q19. Missing exactly: the 274 manifested AIS PDFs
  (route/class/week volumes + rates) behind `ais_manifest.csv`.

## 3. Roll-up

| Verdict | Count | Questions |
| --- | --- | --- |
| single-hop | 1 (5%) | Q20 |
| multi-hop | 14 (70%) | Q1, Q2, Q3, Q4, Q7, Q9, Q10, Q11, Q12, Q13, Q14, Q15, Q16, Q19 |
| blocked-unwired | 4 (20%) | Q5 (CFTC grains COT), Q6 (10-Q/factsheets+SGX), Q17 (CFTC crude COT), Q18 (ETF disclosures) |
| blocked-ocr-quality | 1 (5%) | Q8 (Poten latest body-missing; JS-render, stated) |
| **Total** | **20** | |

Cross-cutting quality notes (not verdicts, but load-bearing for the graph call):
- 2026 baltic chunks are stubs (33–35 chars; cookie-wall per INVENTORY §7d) —
  current-week Baltic legs route via CSVs or broker text, never via baltic 2026 nodes.
- Latest-week hellenic numbers live in linked-image OCR with glyph noise
  (dry estimates `..._02092026dry_a472ff592510_jpg`; valuations matrix
  `..._010920264ffg_3f4fc98878b7_jpg` = unreadable; tanker estimates partly
  usable; broker `Ro:erdam`) — but clean fallbacks exist (PDF-text legs,
  matrix/scrappage/bunker CSVs). Vision BLOCKED (env check: no vendor keys).
- Poten latest (2026-08-24) is nav-boilerplate-only (JS-render miss), contaminating
  Q8 (blocking) and the Poten legs of Q11 (non-blocking).
- Coverage gaps re-verified: `sources.json` paths omit drewry/SGX/CapitalLink/
  EDGAR/grain/SNP/TC-matrix; `coverage_report.json` topics omit broker_reports +
  poten (+books in topics); `topic_evidence.jsonl` = 10 topics × 250 (no broker/
  poten); `usda_grain_freight_spreads.csv` empty; indices CSVs stale to 08-10.

## 4. Graph-layer recommendation (graded by the numbers)

**Recommendation: layer-over-trees with cross-node edges** (per the binding
INVENTORY §6 constraint — consume existing `node_id`/`doc_id` as reference, never
replace tree shards or overwrite `knowledge/derived/`).

Grading:
- 14/20 (70%) need multi-hop joins across distinct tree node ids *plus* CSV row
  windows (fixtures↔estimates↔valuations↔bunkers↔SGX↔orderbook). A lighter
  single-source setup serves only 1/20 (5%, Q20 meta). The counts reject the
  lighter option.
- The load-bearing join type is cross-node entity + week resolution (Q14's
  DEVBULK SINEM $14.8m↔$15.0m hull match; Q19's 4-hop chain; Q12's spike-vs-
  congestion timing) — i.e. edges between existing node ids and week keys, plus
  CSV-week attachments. That is precisely a layer over trees, not a re-chunking.
- The 5 blocked questions (25%) are NOT fixable by any graph layer: 4 need source
  wiring (CFTC COT, 10-Q/factsheets/EDGAR, SGX/CapitalLink UI+manifest wiring,
  SNP-catalog build) and 1 needs extraction repair (Poten JS-render; vision
  re-OCR for the image-OCR suspects stays BLOCKED on keys). Sequence wiring +
  repairs before/with the layer; do not let the layer mask the blocks.
- Non-commitment restated: this grades *retrieval need* (70/5/20/5). It does not
  select LightRAG vs GraphRAG vs Neo4j vs Graphiti, does not scope any batch, and
  does not authorize reprocessing — those remain user calls (cf. INVENTORY §10 P2,
  INGESTED_IMAGE_AUDIT §7).
