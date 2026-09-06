# P1 Calibration — muse-spark

- Author: muse-spark (coder subagent)
- Date: 2026-09-06
- Branch: `agent/muse-spark`, worktree `C:\Users\Dell\Github\shipping-muse-spark`
- Context: see `docs/INVENTORY_MUSE_SPARK.md` (§7 format classes, §3 skipped-queue, §10 plan P1). This doc is the P1 sample-validation companion: one text+table sample, one dense multi-table sample, one chart-heavy assessment.
- Scope: review baseline §5 criteria 4–6 — sample-validated before batch, extractor≠verifier, redo loop logged.

## 0. Harness reuse statement (no duplication)

Sibling worktree `C:\Users\Dell\Github\shipping-antigravity` (read-only) provides:

- `scripts/harness/verify_extraction.py` — `ExtractionVerifier.verify_table()` checks: `empty_table`, `missing_headers`, `col_count_mismatch`, `row_count_mismatch` (tolerance ±1), `row_length_mismatch` (column alignment), `header_bleed` (repeated header block inside rows), `narrative_bleed` (prose embedded as a single cell), plus `snp` column-shift invariants (`column_shifted_numeric_vessel`, `column_shifted_text_in_dwt`); `demolition` schema hook is a no-op. Telemetry + JSONL audit log.
- `scripts/harness/calibrate_sample.py` — pymupdf `find_tables` loop with header fallback (blank first row → try second), independent verify pass, and a redo loop on `header_bleed` (strip + re-verify). Its SSY/Allied/Fearnleys sample paths do not exist in this repo, so only the *pattern* was reused, pointed at our P1 samples.
- `scripts/spine/build_knowledge_spine.py` (skimmed) — SQLite spine over `knowledge/trees/` node ids, P0 skipped-assets queue, fixtures/bunkers/SGX facts. Not executed; noted as the downstream consumer of calibrated tables. Additive-only constraint from the inventory (§6) respected: no shard writes in this task.

Reuse method (M3, post-merge location): Pass 2 now imports `ExtractionVerifier`
from the committed shared harness `calibration/p1/verify_table.py`
(`sys.path.insert(0, "<repo>/calibration/p1")`) — no committed file imports
from `shipping-antigravity` anywhere. Pre-merge, Pass 2 imported the sibling
harness by cross-worktree `sys.path`; results were cross-checked at merge
time and are identical on the P1 fixtures (same pass/fail, same issue check
names; rerun evidence in `calibration/p1/README.md`). M4/B6 contribution: the
`expected_rows` (tolerance ±1) / `expected_cols` (exact) assertions plus POSIX
normalization of `source_file` are implemented in `verify_table.py`, so
per-template expected counts are enforced by shared code — the proposed
location both branches import from after merge. One deliberate determinism
fix vs the sibling: the repeated-header diagnostic is sorted.

## Pass separation (extractor≠verifier)

- **Pass 1 — extractor** (no harness import): `calibration/p1/p1_pass1.py` (native libs only: pymupdf 1.28.2, pdfplumber, PIL 12.1.1, regex text-grouping). Output: `calibration/p1/p1_pass1_extract.json`. (M2 fixtures: scripts scrubbed to POSIX repo-relative paths; outputs verbatim from the 2026-09-06 run; rerun reproduces them exactly — see `calibration/p1/README.md`.)
- **Pass 2 — verifier** (harness only, no re-extraction): `calibration/p1/p1_pass2.py` imports `ExtractionVerifier` from the committed `calibration/p1/verify_table.py`, runs `verify_table` with `expected_rows/cols`, plus explicit bleed-containment and arithmetic tie-out checks. Output: `calibration/p1/p1_pass2_verify.json`; audit trail: `calibration/p1/p1_verification_audit_log.jsonl` (3 entries: 1 FAIL + 2 PASS).

## Sample A (text+table) — PASS (after 1 redo)

- Source: `reports/hellenic/demolition/pdfs/2021-07-03_best-oasis-weekly-recycling-market-report-02-july-2021_weekly-ship-recycling-report_137b264ac3ac.pdf` — 7 pages, all with text layer (p1 ~1598 chars).
- Target: demolition sales table, **PDF p6** (1-based).
- Tool: pdfplumber matrix (primary) + pymupdf `find_tables` geometry (boundary cross-check). Both engines agree on the raw grid.
- Measured (raw): **9 rows × 10 cols**; bbox `[17.65, 145.36, 577.68, 410.14]` inside page rect `595.32 × 841.92` (full-width band, mid-page — matches the sales-table position).
- Manual source-page count: **5 vessels** (Moon Spring, Global M, Aston I, Maya VN, Kutch Bay).
- Pass 1a (naive: row0 = header, rows1–8 = data): 8 rows × 10 cols → harness **FAIL**, `row_count_mismatch` (expected 5, got 8). Rows 1–3 are ruling-split header fragments (`Year of` / `Build`); cols 3–4 are permanently empty ruling artifacts.
- Redo R-A1: folded fragment rows into the header, moved the `Year of Build` label onto data-bearing col 2, dropped the 3 fragment rows. Re-extract: **5 rows × 10 cols**.
- Pass 2 on redo: harness **PASS, 0 issues** (`expected_rows=5, expected_cols=10`; all rows length 10).
- Bleed check: PASS — no `GREECE` (p7 contact block) and no `Domestic prices` (p2 headline block) in extracted rows.
- Final headers: `Vessel Name | Type of Vessel | Year of Build | '' | '' | Country of Build | LDT | Term of Sale | Location of Delivery | Sale Price/LT (USD)` (2 dead cols preserved as measured, not silently dropped).

## Sample B (dense multi-table) — PASS (after 1 redo)

- Source: `docs/BDRY-BWET_Form10-Q_March-31-2026.pdf` — **66 pages** confirmed.
- Target (one table only): **PDF p6**, `Combined Schedules of Investments — BREAKWAVE DRY BULK SHIPPING ETF futures contracts, March 31, 2026 (Unaudited)`: 9 contracts (3 Capesize + 3 Panamax + 3 Supramax), Apr–Jun 2026 expiries. Subtotal line kept out of the grid, used as tie-out instead.
- Tool: pdfplumber `extract_text` + wrapped-line joining (a row completes when a line ends with `N%`) + regex column split `(description (N contracts) | unrealized | notional | %)`. p6 has 61 text lines; block = lines 16–36.
- First attempt (visual `find_tables`, either engine/strategy): **13 fragmented 1-row × 13-col tables** on p6 — unusable as a statement table (no ruling lines between rows). Documented as the failed first pass; redo switched to text-line grouping (this page's ruling structure defeats line-based finders — calibration finding, not a harness failure).
- Measured vs verified: **9 rows × 4 cols**, 9/9 candidate rows parsed, 0 unparsed → harness **PASS, 0 issues** (`expected_rows=9, expected_cols=4`; all rows length 4).
- Page attribution: PDF p6; boundary = text lines 16–36 of 61.
- Bleed check: PASS — money-market block (Invesco/MONEY MARKET, lines 7–13) and tanker futures block (West Africa/Middle East Gulf, lines 38–59) both absent from extracted rows.
- Arithmetic tie-out (extra, outside harness): Σ unrealized = **−2,157,385** and Σ notional = **43,916,630**, both exactly equal to the printed subtotal line `$ (2,157,385) $ 43,916,630 100%`.

## Sample C (chart-heavy) — RESELECTED from true-skipped (vision pass sandbox-blocked; project path open per W1 below)

X1 correction: the survey below (first 5 image-type entries in ledger order,
~~struck as invalid~~) sampled **ingested** assets, not skipped ones —
`data/derived/asset_dispositions.jsonl` shows all 5 with
`disposition=ingested` and tree-shard `linked_image_asset` node ids (the
`node_id` itself is proof of ingestion; the old local-resolution code selected
ingested assets by construction). Do not use rows 1–5 as skipped-queue
evidence.

| # | Parent doc (truncated) | Asset file | Format | Dimensions (W×H) | Bytes |
|---|---|---|---|---|---|
| ~~1~~ | ~~`breakwave_insights_…_2020_06_06_…_the_drama_continues…_op`~~ | ~~`…_img_map-minas-gerais-brazil_fc088b057bd4.jpg`~~ | ~~GIF-in-.jpg~~ | ~~1600×1147~~ | ~~362,069~~ |
| ~~2~~ | ~~same parent~~ | ~~`…_img_img-1960_8a20a313afb5.jpg`~~ | ~~JPEG~~ | ~~1125×1530~~ | ~~210,372~~ |
| ~~3~~ | ~~`breakwave_insights_…_2020_06_09_…_chasing_the_rally…`~~ | ~~`…_img_image-asset_b5a625fe3458.jpeg`~~ | ~~JPEG~~ | ~~2500×1874~~ | ~~1,241,793~~ |
| ~~4~~ | ~~same parent~~ | ~~`…_img_arrow_d2d09ee0d34a.jpg`~~ | ~~JPEG~~ | ~~480×339~~ | ~~25,841~~ |
| ~~5~~ | ~~same parent~~ | ~~`…_img_image-asset_06e250205128.jpeg`~~ | ~~JPEG~~ | ~~746×468~~ | ~~29,716~~ |

Disposition totals (D1–D4, from `asset_dispositions.jsonl`, 22,106 records =
ledger discovered): ingested **13,591** (node_id non-null on all 13,591 —
gate-enforced; all mirrors on disk) vs skipped **8,424**
(8,341 `unresolvable_external` + 48 `non_content_link` + 35 `duplicate_path`) vs
**failed 91** (86 `PDFSyntaxError` + 4 `unknown_extraction_failure`
(`reason_unknown=true`) + 1 `unresolvable_relative_ref`). Failed assets with a
local mirror present: **90/91** — every extraction failure resolved locally
(the only mirror-less failure is the `/s/congestions.png` resolve-miss);
skipped assets with a local mirror: **35/8,424 — exactly the 35
`duplicate_path` dups** (33 img + 1 link + 1 pdf; all 35 resolve and exist on
disk); every other skipped asset carries `local_mirror_rel=null` by
construction (external/non-content never resolve). Matrix
(`skip_cause_matrix.json`) is aggregated FROM the dispositions in the same run
(single derivation path); three-way gate ingested 13,591 / skipped 8,424 /
failed 91 vs ledger, zero per-doc mismatches.

D4 correction: the M1 reselection below (first 5 unique dup pairs, ~~struck as
superseded~~) sampled the wrong target — dup mirrors are byte-identical to
their ingested twins, so no unextracted content lives there. Sample C is
re-pointed at the ingested set + the 91 failures, where unextracted /
mis-extracted content lives: 3 ingested images with suspect OCR numerics
(S1–S3 from `docs/INGESTED_IMAGE_AUDIT_MUSE_SPARK.md` §6, committed tree
shards) + 2 failed PDF assets of the `PDFSyntaxError` class (real local files
confirmed on disk; both are HTML error pages saved under `.pdf` names —
`F1` magic `<!DOCTYPE html>`, `F2` IE-conditional bot-challenge page — which
is why section extraction raised).

| # | Parent doc (truncated) | Asset file | Format | Dimensions (W×H) | Bytes |
|---|---|---|---|---|---|
| ~~1~~ | ~~`…_2021_11_10_drybulk_freight_rates_have_been_hammered…`~~ | ~~`…_img_ch328329_45f94b2d443a.png`~~ | ~~PNG~~ | ~~493×456~~ | ~~13,577~~ |
| ~~2~~ | ~~same parent~~ | ~~`…_img_ch428329_c4f0f1d367e7.png`~~ | ~~PNG~~ | ~~476×473~~ | ~~18,278~~ |
| ~~3~~ | ~~`…_2022_01_14_recent_developments_in_indian_coal…`~~ | ~~`…_img_chart328929_80a1b052f03e.jpg`~~ | ~~JPEG~~ | ~~674×332~~ | ~~24,933~~ |
| ~~4~~ | ~~`…_2022_04_11_large_number_of_coal_mine_deaths…`~~ | ~~`…_img_chart2282429_8c9bdb6b9c8a.jpg`~~ | ~~JPEG~~ | ~~679×216~~ | ~~31,423~~ |
| ~~5~~ | ~~`…_2022_07_21_signal_dry_bulk_weekly_report`~~ | ~~`…_img_unnamed284629_5be730eb0f80.png`~~ | ~~PNG~~ | ~~2000×800~~ | ~~181,930~~ |

M1 outcome (a) — reselected Sample C from true-skipped local assets: the only
true-skipped images on disk are the `duplicate_path` dups (27 unique
doc+mirror pairs, all breakwave_insights; the 2 `unresolvable_external` img
records have no mirror). First 5 unique pairs in ledger order (each dup's
ingested twin in the same doc carries the cited node) are struck above —
superseded by the D4 set below.

D4 Sample C (current): unextracted / mis-extracted content.

| # | Kind | Parent doc | Asset file (local mirror) | Format | Size | Disposition evidence |
|---|---|---|---|---|---|---|
| C1 | ingested, suspect OCR | `…_2020_06_06_…_the_drama_continues…` (Vale iron-ore table, audit S1) | `…_img_img-1960_8a20a313afb5.jpg` | JPEG | 1125×1530, 210,372 | `disposition=ingested`, node `…_img_img_1960_8a20a313afb5_jpg`; suspect `34.438` amid comma-thousands siblings |
| C2 | ingested, suspect OCR | `…_2020_06_12_…_highest_capesize_index…` (Q3 capesize chart, audit S2) | `…_img_q3capesize_8e5616c79c4c.jpg` | JPEG | 734×394, 24,293 | `disposition=ingested`, node `…_img_q3capesize_8e5616c79c4c_jpg`; suspects `15366,000` vs `15200.035` vs `9407.00` + `Avorage` |
| C3 | ingested, suspect OCR | `…_2020_11_23_…_vaccines_investor_optimism…` (dollar-index chart, audit S3) | `…_img_ulf80_7abfec11426d.jpg` | JPEG | 451×283, 16,720 | `disposition=ingested`, node `…_img_ulf80_7abfec11426d_jpg`; suspects `100,00` amid dot-decimals, `97,000`/`$6,000` amid dot-3-decimals |
| F1 | failed extraction | `breakwave_insights_…_2022_05_30_…_russias_seaborne_oil_flows_changing_patterns` | `reports/breakwave/pdfs/2022-05-30_…_the-future-of-saudi-price-discrimina_0d4f8cd656a6.pdf` | `.pdf` name, HTML body | 121,703 | `disposition=failed`, `reason=PDFSyntaxError`, mirror on disk |
| F2 | failed extraction | `breakwave_insights_…_2022_12_09_…_china_reopening_boosts_sentiment…` | `reports/breakwave/pdfs/2022-12-09_…_china-commodity-trade-data-november-_1bb3ed9ffda4.pdf` | `.pdf` name, HTML body | 5,174 | `disposition=failed`, `reason=PDFSyntaxError`, mirror on disk |

- Two-stage vision pass executable here? **No — sandbox-blocked (W1 correction, verified 2026-09-06).** Environment check (`Get-ChildItem Env:` + named lookups, names only, no values read): `REDUCTO_API_KEY`, `LLAMA_CLOUD_API_KEY`, `ANTHROPIC_API_KEY`, `OPENAI_API_KEY` (and bare `REDUCTO`/`LLAMA`/`ANTHROPIC`/`OPENAI`) all **ABSENT in this sandbox** — a sandbox-only fact, not a project block. The project-level path exists: CI secrets `NIM_API_KEY` + `OLLAMA_BASE_URL`/`OLLAMA_API_KEY`/`OLLAMA_MODEL` are wired into the knowledge pipeline (`.github/workflows/daily_knowledge_update.yml:66-79`, `.github/workflows/process_knowledge.yml:90-99`; `OPENROUTER_API_KEY`/`GROQ_API_KEY` project secrets are wired to the brief workflow, `.github/workflows/daily_brief.yml:64-65`, consumed by `scripts/generate_brief.py:94-108`), and `scripts/process_knowledge.py:28-38` already runs an NIM/Ollama client (`_call_ollama_once`, `:1519-1561`; `_call_nim_once`, `:1607-1614`) whose payloads are text-only (`"messages": [{"role": "user", "content": prompt}]`, `:1524` and `:1612`; zero `image_url`/multipart matches in either client, `scripts/generate_brief.py` included — its only `vision` hit is the word "revisions"). Native libs (PIL/pymupdf) can stage assets — survey, dedupe, dimension/format triage — but chart datapoint reading needs a vision model that is not provisioned in this sandbox.
- Exact unblock requirement (W1, spec only — **NOT authorization to run a batch**): add a multimodal call path (`image_url`/multipart) to the existing NIM/Ollama client + a CI run against a vision-capable model + written approval of per-image egress budget (scope footnote below) before any batch run.
- Deferred chart protocol (spec, not execution): **stage 1 — axis/scale-first**: per image, record chart type, x/y axis labels, units, scale (linear/log), legend entries, and source-text cross-reference; a stage-1 record with unreadable axes FAILS closed and blocks stage 2. **Stage 2 — datapoints-second**: transcribe series values with (x, y, unit) triples, each triple citing its stage-1 axis record; totals/endpoint values tied back to the parent document's prose numbers where stated. Both stages carry `parent_doc_id` + image `node_id` attribution end-to-end.

## Redo-loop log

| ID | Sample | Trigger (verifier FAIL) | Fix (re-extraction) | Re-verify |
|---|---|---|---|---|
| R-A1 | A hellenic p6 | `row_count_mismatch`: 8 vs manual 5 (header-fragment rows counted as data) | header fold + fragment-row strip + label-to-data-column repair → 5×10 | PASS, 0 issues |
| R-B1 | B 10-Q p6 | visual finder yields 13× (1×13) fragments, no coherent statement table | text-line grouping + regex column split → 9×4 | PASS, 0 issues + arithmetic tie-out exact |

## Verdicts

| Sample | Verdict | Harness result |
|---|---|---|
| A hellenic demolition p6 (5×10) | **PASS** | 1 FAIL → redo → PASS, 0 issues; bleed clean |
| B 10-Q p6 BDRY futures (9×4) | **PASS** | PASS, 0 issues; bleed clean; subtotal tie-out exact |
| C chart set, D4: 3 ingested suspect-OCR images (S1–S3) + 2 failed PDFSyntaxError PDFs (F1–F2) | **ASSESSED / vision sandbox-blocked (W1 project path recorded above; no batch authorized)** | n/a (no extraction claimed); readiness verdict + deferred protocol recorded |

Harness session summary: 3 tables inspected, 2 passed, 1 failed (the intentional first-attempt FAIL), i.e. both final tables PASS.

## Scope footnote (image counts, measured in-worktree)

Reviewer budgets **21,528** reports/ page images (breakwave **14,633** + hellenic **6,895**). Repo-wide `jpg+png` is **21,532** — the **4 extra live outside `reports/`**. Both figures are consistent; the difference is scope (reports-only vs repo-wide), re-measured here via `git ls-files` counts (reports-only 21,528; breakwave 14,633; hellenic 6,895; repo-wide 21,532).
