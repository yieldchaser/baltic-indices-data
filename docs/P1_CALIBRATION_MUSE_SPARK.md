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

Reuse method: `sys.path.insert` import of `ExtractionVerifier` from the sibling harness path at verify time (Pass 2). Nothing was copied into this branch — `git status` shows only this new doc as the repo change.

## Pass separation (extractor≠verifier)

- **Pass 1 — extractor** (no harness import): `C:\Users\Dell\AppData\Local\Temp\opencode\p1_pass1.py` (native libs only: pymupdf 1.28.2, pdfplumber, PIL 12.1.1, regex text-grouping). Output: `p1_pass1_extract.json`.
- **Pass 2 — verifier** (harness only, no re-extraction): `C:\Users\Dell\AppData\Local\Temp\opencode\p1_pass2.py` imports the sibling `ExtractionVerifier`, runs `verify_table` with `expected_rows/cols`, plus explicit bleed-containment and arithmetic tie-out checks. Output: `p1_pass2_verify.json`; audit trail: `p1_verification_audit_log.jsonl` (3 entries: 1 FAIL + 2 PASS).

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

## Sample C (chart-heavy) — ASSESSED, not forced (vision pass BLOCKED)

Surveyed the first 5 image-type `linked_assets_skipped` queue entries in ledger order (parent attribution via `knowledge/manifests/documents.jsonl` + `linked_image_asset` sections in `knowledge/trees/` shards; dimensions via PIL; all 5 resolve to local disk):

| # | Parent doc (truncated) | Asset file | Format | Dimensions (W×H) | Bytes |
|---|---|---|---|---|---|
| 1 | `breakwave_insights_…_2020_06_06_…_the_drama_continues…_op` | `…_img_map-minas-gerais-brazil_fc088b057bd4.jpg` | GIF-in-.jpg | 1600×1147 | 362,069 |
| 2 | same parent | `…_img_img-1960_8a20a313afb5.jpg` | JPEG | 1125×1530 | 210,372 |
| 3 | `breakwave_insights_…_2020_06_09_…_chasing_the_rally…` | `…_img_image-asset_b5a625fe3458.jpeg` | JPEG | 2500×1874 | 1,241,793 |
| 4 | same parent | `…_img_arrow_d2d09ee0d34a.jpg` | JPEG | 480×339 | 25,841 |
| 5 | same parent | `…_img_image-asset_06e250205128.jpeg` | JPEG | 746×468 | 29,716 |

- Two-stage vision pass executable here? **No — BLOCKED.** Environment check (`Get-ChildItem Env:` + named lookups, names only, no values read): `REDUCTO_API_KEY`, `LLAMA_CLOUD_API_KEY`, `ANTHROPIC_API_KEY`, `OPENAI_API_KEY` (and bare `REDUCTO`/`LLAMA`/`ANTHROPIC`/`OPENAI`) all **ABSENT**. Native libs (PIL/pymupdf) can stage assets — survey, dedupe, dimension/format triage — but chart datapoint reading needs a vision model that is not provisioned in this sandbox.
- Exact unblock requirement: provision **one** of (a) an Anthropic/OpenAI API key for direct two-stage vision, or (b) a Reducto/LlamaCloud key for managed async parse; plus written approval of per-image egress budget (scope footnote below) before any batch run.
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
| C chart survey (5 images) | **ASSESSED / vision BLOCKED** | n/a (no extraction claimed); readiness verdict + deferred protocol recorded |

Harness session summary: 3 tables inspected, 2 passed, 1 failed (the intentional first-attempt FAIL), i.e. both final tables PASS.

## Scope footnote (image counts, measured in-worktree)

Reviewer budgets **21,528** reports/ page images (breakwave **14,633** + hellenic **6,895**). Repo-wide `jpg+png` is **21,532** — the **4 extra live outside `reports/`**. Both figures are consistent; the difference is scope (reports-only vs repo-wide), re-measured here via `git ls-files` counts (reports-only 21,528; breakwave 14,633; hellenic 6,895; repo-wide 21,532).
