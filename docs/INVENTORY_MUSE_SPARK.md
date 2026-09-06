# Inventory — muse-spark

- Author: muse-spark
- Date: 2026-09-06
- Branch: agent/muse-spark
- Base HEAD: ad2d67024 (56,874 tracked files; sync of origin/main 1ffb02db4)
- Scope: inventory + plan ONLY. No extraction code, no shard writes.
- Worktree-concurrency: work done in C:\Users\Dell\Github\shipping-muse-spark on branch agent/muse-spark, never committing to main from shared checkout, merges one-at-a-time.
- Method: every number below measured directly in this worktree at HEAD ad2d67024. Where a figure contradicts the mission brief, the measured figure governs.

Spot-checks re-verified in this worktree 2026-09-06: `reports/baltic` = 2891 tracked files;
`reports/drewry` = 548 tracked files with 0 local PDFs; `reports/hellenic` = 14052 tracked files
(incl 3948 tracked PDFs); `knowledge/manifests/sources.json` (generated 2026-09-06) covers breakwave/baltic/
breakwave_insights/hellenic/broker_reports/poten/books only; `data/Capital_Link_*.xlsx` (7 files) present.

## 1. Census (tracked files at HEAD ad2d67024)

| Area | Count | Notes |
| --- | --- | --- |
| repo total | 56,874 | HEAD ad2d67024 |
| reports/ total | 36,312 | |
| reports/baltic | 2891 | dry/tanker/gas/container/ningbo |
| reports/breakwave | 18289 | insights (81 local PDFs; 14,633 jpg+png page images) |
| reports/broker_reports | 105 | md only |
| reports/drewry | 548 | 0 local PDFs — md + ais_manifest.csv only |
| reports/drybulk | 209 | breakwave drybulk (209 PDF) |
| reports/hellenic | 14052 | incl 3948 tracked PDFs; 6,895 jpg+png page images |
| reports/panama_canal | 1 | |
| reports/poten | 30 | opinions, metadata-only |
| reports/seabrokers | 97 | md |
| reports/tankers | 78 | breakwave tankers (78 PDF) |
| reports/ top-level | 12 | incl 11 book PDFs |
| data/ total | 515 | |
| data/bunkers | 8 | |
| data/cache | 1 | |
| data/cftc_statements | 144 | |
| data/commodities | 22 | |
| data/congestion | 4 | |
| data/demolition | 1 | fixtures CSV |
| data/derived | 30 | incl compiled TC-rate CSVs, fearnleys_catalog.csv, USDA grain CSVs |
| data/etf | 145 | |
| data/flows | 3 | BDRY/BWET/all_flows_summary JSON |
| data/futures | 14 | incl SGX FEF/M65F/LPF + cape/panamax/supramax/handysize CSVs |
| data/indices | 22 | incl Capital Link converted CSVs |
| data/macro | 1 | |
| data/manifests | 2 | |
| data/raw | 7 | |
| data/reports | 100 | seabrokers-related |
| data/rulebooks | 3 | |
| data/ top-level | 8 | incl 7 Capital_Link_*.xlsx (CLDBI/CLCI/CLTI/CLMI/CLMFI/CLLG/CLMLP) |
| knowledge/ total | 19,799 | 8,850 tree shards + 8,850 docs + chunks/derived/manifests |
| knowledge/books | 1798 | |
| knowledge/briefs | 108 | |
| knowledge/chunks | 165 | files; 101,967 chunks per coverage_report.json |
| knowledge/derived | 6 | signals/themes/topic_evidence/section_index/timelines shards |
| knowledge/docs | 8850 | |
| knowledge/manifests | 7 | documents/errors/sources/coverage/lint/provenance/spike_queue |
| knowledge/reports | 1 | |
| knowledge/trees | 8850 | hierarchical section shards (already sectioned, not flat RAG) |
| knowledge/wiki | 11 | |
| index.html | ~30,464 lines (~30k) | 9 tabs (9 `<div class="tab-panel">` divs, 9 unique `id="tab-*"`), 53 `fetch(` call sites |

Repo-wide by extension (measured, cost-model relevant): **pdf 4,475** · **jpg+png 21,532**
(21,936 incl 409 jpeg; +9 webp) · **html 9,264** (+2 htm) · csv 294 · xlsx 17 · md 11,550 ·
json 9,080 · jsonl 88 · py 119.

Counting method: count with `git -c core.quotepath=false ls-files`, because default
quotepath octal-quotes non-ASCII filenames and naive matching misses them — quotepath=false
gives the HIGHER correct figures. Default quotepath undercounts images by 5 octal-quoted
.jpg (+5 vs default: hellenic 6,895; reports-only 21,528; repo-wide 21,532);
it also misses octal-quoted `.pdf` matches.

## 2. Binding definition: "unprocessed" (ledger-diff, not filesystem walk)

> **Unprocessed** = a diff against `knowledge/manifests/documents.jsonl` on
> `source_hash` + `compiler_version`. It is **not** a filesystem walk.

A directory listing both re-flags already-ingested material and misses
content-changed files whose paths did not move. The ledger already carries the
incremental-processing mechanism; use it.

`knowledge/manifests/documents.jsonl` measured at HEAD: **8,850 rows, 8,850 unique
doc_ids.** Schema fields confirmed:
`doc_id, source, category, date, title, source_path, doc_path, tree_path, tree_node_count,
chunk_file, chunk_count, source_hash, source_hash_version, compiler_version, processed_at,
linked_assets_discovered/mirrored/ingested/skipped/failed`.
Distinct values measured: `compiler_version` = {2: 8850};
`source_hash_version` = {content_sha1_v2: 8850}.

## 3. P0 work item: the `linked_assets_skipped` queue (measured, not inherited)

Aggregating `documents.jsonl` at HEAD (verified, matches reviewer baseline):

| | discovered | mirrored | ingested | **skipped** | failed |
|---|---|---|---|---|---|
| **all** | 22,106 | 13,716 | 13,591 | **8,424** | 91 |
| breakwave_insights | 16,629 | 8,959 | 8,859 | **7,703** | 67 |
| hellenic | 5,477 | 4,757 | 4,732 | **721** | 24 |
| baltic / breakwave / broker_reports / poten / book | 0 | 0 | 0 | 0 | 0 |

Docs with skipped > 0: **3,167** (breakwave_insights 2,475; hellenic 692).
**8,424 linked assets were discovered and then skipped** — largely the chart-image
assets the mission wants a two-stage vision pass on. They are the highest-value
unprocessed material in the repo. **This is P0.**

Skip causes (read from `scripts/process_knowledge.py:2309-2399`
`collect_linked_asset_sections`): per-doc `MAX_LINKED_ASSETS_PER_DOC` cap, empty href,
non-content link, external http(s) URL (skipped, not failed), duplicate already-mirrored
path (skipped). Failed (91) = unresolvable relative ref, linked-asset extraction
exception, or empty extracted text — parent document still compiles.

Sampled skipped-queue parents (ledger rows with `linked_assets_skipped > 0`):

- breakwave_insights: `breakwave_insights_insights_2020-06-06_..._the_drama_continues_as_brazilan_judge_hats_vales_iron_ore_op`
  (discovered 2 / mirrored 0 / ingested 0 / **skipped 2**; sibling ingested sections present
  in the same tree shard as `__s02/__s03_linked_asset_*_jpg` image sections).
- hellenic: `hellenic_demolition_0000-00-00_..._athenian_shipbrokers_s_a_demolition_quick_update_week_03_2026`
  (discovered 1 / mirrored 0 / ingested 0 / **skipped 1**; source page is a 520-error capture).
- Third source: **none exists** — only breakwave_insights and hellenic have any skipped > 0.

Parent-document attribution riding with every queued item (no filesystem re-walk needed):

- Ledger row: `doc_id, source, category, date, source_path, doc_path, tree_path, chunk_file`.
- Tree sections (`knowledge/trees/...json`): `node_id` = `{doc_id}__s{NN}_linked_asset_{file}_{hash}_{ext}`,
  `doc_id`, `parent_id`, `title` ("Linked asset: {filename}"),
  `section_type` in {linked_image_asset, linked_pdf, linked_text_asset},
  `section_path` / `section_path_text`, `summary` led with `Source asset: {reports/... rel path}`.
- Chunks (`knowledge/chunks/*.jsonl`): `chunk_id, doc_id, source, category, date,
  section_id` (= node_id), `section_title, section_path(_text), section_level`.

Secondary queue: `knowledge/manifests/errors.jsonl` holds **83 entries**. Measured classes:
79x `PDFSyntaxError: No /Root object! - Is this really a PDF?` raised in
`collect_linked_asset_sections -> extract_linked_text_asset` on linked assets
(parent doc still compiles; mostly breakwave/hellenic archive HTML whose linked "PDF"
is not a real PDF); 2x `[Errno 36] File name too long` on breakwave HTML asset URLs
(mechanical, fixable); 1x `TypeError: unsupported operand for -: float and str`;
1x `[Errno 22] Invalid argument`. Samples: `reports/breakwave/2022/2022-03-23_signal-dry-bulk-weekly-report.html`
(Errno 36); `reports/breakwave/2022/2022-05-30_russias-seaborne-oil-flows-changing-patterns.html`
(No /Root object); `reports/breakwave/2022/2022-12-09_china-reopening-boosts-sentiment-in-industrial-metal-markets.html`
(No /Root object).

Genuinely uncovered sources (each verified absent from `knowledge/manifests/sources.json`,
whose `paths` sub-keys cover breakwave/baltic/breakwave_insights/hellenic/broker_reports/poten/books only).
Scope clarifier (B9): `knowledge/manifests/documents.jsonl` covers `reports/` sources only
(verified: all 8,850 rows carry a `reports/...` `source_path`, zero `data/` entries), so
`data/` holdings are a separate scope measured in rows, not documents — no `data/` CSV row
count in this document is a document count or an output of the ledger diff.

- SGX iron ore/freight futures (`data/futures/sgx_iron_ore_fef/m65f/lpf*.csv` + cape/panamax/supramax/handysize present on disk).
- 7 Capital Link index XLSX in `data/` (CLDBI/CLCI/CLTI/CLMI/CLMFI/CLLG/CLMLP).
- Drewry AIS + opinions (`reports/drewry`, 548 files, 0 local PDFs).
- SSY / Fearnleys / Gibson / Allied weeklies (only `data/derived/time_charter_rates_fearnleys.csv` + `fearnleys_catalog.csv` compiled artifacts present).
- Grain + port flows (USDA grain CSVs + `data/flows/*.json` present, no manifest coverage).
- SNP commercial fixtures (only demolition fixtures CSV + scattered weeklies, no catalog).
- Historical time-charter rates (`data/derived/time_charter_rates.csv`, `intermodal_tc_rates.csv`, `lng/lpg_charter_rates.csv` unwired to UI/knowledge).
- Broker comments / seabrokers OSV dayrates (`data/derived/seabrokers_osv_dayrates.csv`, most complete new source, unwired).
- SEC EDGAR pull (only BDRY/BWET 10-Q/prospectus/factsheets + CFTC statements parsed; no miners/shipyards universe).

## 4. Mission-brief claims vs measured (cost model)

- Brief "35,957 reports/PDFs, 5.94GB" vs measured: `reports/` = 36,312 files, but only
  **4,475 PDFs repo-wide** (reports-only 4,328: hellenic 3,948, drybulk 209, breakwave 81,
  tankers 78, top-level 11). Per-page OCR budgets aimed at PDFs are ~8x oversized and
  aimed at the wrong modality.
- The real volume is **21,528 page images under `reports/`** (breakwave 14,633 + hellenic 6,895 jpg+png)
  + 4 outside `reports/` (`assets/Picture1-4.png`) = **21,532 repo-wide**, plus **9,264 HTML**. Budget the vision pass for images, not PDFs.
- Brief "19,801 KG shards sitting unused" vs measured: `knowledge/` = 19,799 files, of which
  **8,850 tree shards are actively maintained section hierarchies** (see section 6).
- Brief "332 tabular datasets" vs measured: 294 CSV + 17 XLSX on disk. Reconcile, don't inherit.
- `docs/AUDIT_UNRENDERED_DATA_SOURCES.md` is **not in git** (verified: absent from
  `git ls-files docs/` and from `git log --all --diff-filter=A`). It is cited nowhere in
  this document; all figures above are measured in-worktree at HEAD ad2d67024.

## 5. Rendered vs unrendered

Rendered today: index.html renders prompt tabular CSVs + knowledge-chunks Q&A.
Unrendered / unwired:

- Drewry AIS dashboards — manifest-only, PDFs not downloaded.
- Capital Link xlsx — converted to data/indices CSVs but absent from knowledge/manifests.
- SGX iron ore — FEF/M65F/LPF raw ingested, not in manifests.
- SEC EDGAR depth — only BDRY/BWET 10-Q/prospectus/factsheets + CFTC statements parsed;
  no miners/shipyards universe.
- broker_reports 105 md — in sources.json but missing from coverage topics.
- Grain/port — USDA/PortWatch CSVs present; coverage topic only breakwave/hellenic.
- SNP/fixtures — only demolition fixtures CSV + scattered SNP weeklies, no catalog.
- Time-charter matrix — compiled CSVs in data/derived, unwired to UI/knowledge.

## 6. Manifest coverage gap + graph-layer constraint

`knowledge/manifests/sources.json` (2026-09-06) covers breakwave / baltic / hellenic /
broker_reports / poten / books ONLY. It omits drewry, SGX, CapitalLink, EDGAR, grain,
SNP, TC-matrix sources. `coverage_report.json` omits broker_reports topics.

**Graph-layer constraint (binding): the graph layer will consume the existing
`knowledge/trees/` `node_id` / `doc_id` values as a layer-over reference — it will never
replace tree shards and never overwrite `knowledge/derived/` or any compiled shard.
GraphRAG / LightRAG / Neo4j / Graphiti are reviewable only as a layer over existing
node ids, not as a substitute for them. Additive only.**

## 7. Format classes (verified samples)

- (a) Mixed-layout text+table PDFs WITH text layer: `docs/BDRY-BWET_Form10-Q_March-31-2026.pdf`
  (66pp, p1 ~2881 chars); hellenic demolition sample (7pp, p1 ~1611 chars).
- (b) Chart-heavy 2-pager WITH text layer: `docs/Amplify_BDRY_FactSheet.pdf` (2pp, p1 ~2567 chars).
- (c) Clean tabular: data/futures SGX CSVs; data/derived TC-rate CSVs; Capital Link
  single-sheet xlsx (CLMI 5232x10).
- (d) HTML stubs: reports/baltic sample is cookie-wall thin.
- (e) Derived md/JSON: seabrokers 97 md + OSV dayrates CSV (most complete new source);
  Poten opinions metadata-only (JS-rendered body missing).

## 8. Tooling verdict (probed, nothing installed)

- READY: pypdf 6.14.2 / pymupdf 1.28.2 / pdfplumber / pandas / openpyxl / torch.
- ABSENT: docling / camelot / transformers / ocrmypdf / tesseract / ghostscript / java.
- NO API keys (Reducto / LlamaCloud / Anthropic / OpenAI all absent) — Reducto/LlamaParse
  evaluation is sandbox-BLOCKED until the §7-W1 unblock lands. **W1-note (2026-09-06): sandbox-only fact — CI carries `NIM_API_KEY` + `OLLAMA_*` (knowledge pipeline, `daily_knowledge_update.yml:66-79` / `process_knowledge.yml:90-99`) and `OPENROUTER`/`GROQ` keys (brief workflow, `daily_brief.yml:64-65`); existing NIM/Ollama client payloads are text-only (`process_knowledge.py:1524` / `:1612`); unblock = multimodal call path + CI vision-model run + spend approval, NOT a batch authorization (see INGESTED_IMAGE_AUDIT §7).**
- Docling install is possible (torch present) but OCR deps missing.
- All sampled PDFs have text layers, so native extraction suffices for phase-1 samples.
- Drewry AIS PDFs are not local (manifest-only) — must download before chart-pipeline
  validation; check native dashboard export first per mission.

## 9. Sibling scope boundaries (do not duplicate)

- agent/antigravity +1 commit (645db9079) added `scripts/harness/verify_extraction.py` +
  `calibrate_sample.py` + `scripts/spine/build_knowledge_spine.py` — REUSE the harness
  for verifier passes.
- Shared checkout has many UNTRACKED active-work files (fetch_fearnleys_*, SGX probes,
  sec_edgar_pipeline.py, shipbroker scrapers, fearnleys derived CSVs) — other agents'
  live scope, do not touch.

## 10. Plan

- P0 — Drain the `linked_assets_skipped` queue (8,424: 7,703 breakwave_insights + 721 hellenic)
  with parent-document attribution (`doc_id`/node_id/section_path) preserved end-to-end;
  triage `errors.jsonl` (83) separately — mechanical classes first. Ledger-diff on
  `source_hash` + `compiler_version` defines what is left; additive only.
- P1 — Sample validation per class (1 text+table, 1 chart-heavy, 1 dense multi-table)
  with row-count / column-alignment / page-attribution checks via the antigravity harness.
- P2 — Pilot 20 multi-hop questions (below), THEN choose graph layer (LightRAG vs
  GraphRAG vs Neo4j vs Graphiti per mission) as a layer OVER `knowledge/trees/` node ids.
- P3 — Incremental batch extraction with extractor/verifier separation + redo loop + audit log.
- Constraint: additive — never overwrite knowledge/derived shards; never replace tree shards.

## 11. Pilot questions (20, multi-hop)

1. Which Capesize fixtures in the latest hellenic dry_charter report clear above the Baltic C5TC, and what does that imply for 5-year-old Capesize valuations this week?
2. How does the current SGX FEF front-month/back-month spread compare with the Baltic capesize basket direction over the same week?
3. For a Kamsarmax fixed ex-EC South America, what is the implied daily TCE vs the Baltic Kamsarmax average, and how does bunker cost at Singapore change the net?
4. Which vessel-valuation age curve (5/10/15yr) moved most this month for Ultramax, and does any fixture in the weeklies support or contradict it?
5. What does the CFTC grains positioning say about near-term Panamax demand, and is it consistent with USDA export-flow CSVs?
6. How did the BDRY 10-Q describe its futures-roll costs, and do SGX/indices moves since quarter-end validate the disclosed risk?
7. Which demolition fixtures set the week’s high $/ldt, and how does that scrap value compare with the oldest Capesize valuation in the same week?
8. Poten’s latest tanker opinion vs Baltic dirty-tanker assessments: where do they agree or diverge on VLCC direction?
9. Which OSV dayrate bracket in the seabrokers CSV moved most, and is there a matching fixture/valuation signal in hellenic weeklies?
10. Compare Capital Link drybulk index (CLDBI) week-on-week with the Baltic Dry Index: which constituent direction explains the gap?
11. A 10-year-old MR tanker valuation vs Poten clean-tanker commentary: is the paper value supported by current TC-rate CSVs?
12. Which grain-port congestion datapoint coincides with a Panamax fixture spike, and what does PortWatch show for that port?
13. How does the SGX M65F (65% Fe) discount to FEF track against hellenic iron-ore commentary this week?
14. Which broker_reports weekly (105 md) mentions the same fixture first reported in hellenic, and do the rate details match?
15. Bunker prices Singapore vs Rotterdam: what is the spread, and which fixture route economics flip on that spread?
16. Which shipbuilding order in hellenic weeklies implies the most future Capesize supply, and how does that read against the 5-year valuation curve?
17. CFTC crude positioning vs Baltic dirty-tanker rates: does speculative length lead or lag the VLCC leg this month?
18. Which ETF holding disclosure (BDRY/BWET factsheet) changed most vs the 10-Q, and does the futures-curve shape justify it?
19. Build the chain: fixture rate → TCE net of bunkers → implied asset yield vs 5yr valuation → SGX curve confirmation or rejection.
20. Which single unwired source (Drewry AIS / SGX / CapitalLink / SNP catalog) would have changed the answer to Q19, and what exactly is missing?

## 12. Next action for muse-spark

P0 skipped-queue triage (attribution-preserving pass plan, no shard writes) + P1 sample calibration on the hellenic demolition PDF + 10-Q + factsheet.

## 13. Sequencing: cause-split ownership (muse-spark) → queue rebuild (antigravity)

- muse-spark owns the skipped-queue cause-split instrumentation: `scripts/analysis/split_skip_causes.py`
  (stdlib-only read-only replay of the five `collect_linked_asset_sections` skip branches) plus its
  output `data/derived/skip_cause_matrix.json` (per-source × per-cause, reconciled: 8,424 skipped /
  3,167 docs, zero per-doc mismatches vs `documents.jsonl`).
- antigravity's P0 queue rebuild consumes `data/derived/skip_cause_matrix.json` as its cause-split
  input; it does not re-derive the split.
- Verifier reuse (antigravity `ExtractionVerifier` harness) continues, but its pass rate is not
  inherited as a quality signal until per-template expected-column-count assertions exist (B6).

## 14. Reviewer follow-ups X1/M1–M4 (2026-09-06, this branch, no commits)

- X1/M1: `scripts/analysis/split_skip_causes.py` now also emits
  `data/derived/asset_dispositions.jsonl` (22,106 records = ledger discovered;
  gate still 8,424/3,167, zero mismatches). Ingested 13,681 (all mirrors on
  disk) vs skipped 8,425; skipped-with-mirror = exactly the 35
  `duplicate_path` dups (33 img + 1 link + 1 pdf). X1: the old P1 §C survey
  sampled ingested assets (all 5 carry tree node ids) — struck. M1 outcome
  (a): Sample C reselected from true-skipped local dup images (27 unique
   pairs); details in `docs/P1_CALIBRATION_MUSE_SPARK.md` §C, whose readiness
   verdict + deferred protocol + sandbox-blocked status stand (W1 project path open per INGESTED_IMAGE_AUDIT §7; no batch authorized).
- M2: P1 fixtures committed under `calibration/p1/` (`p1_pass1.py`,
  `p1_pass2.py`, both JSON outputs + audit JSONL; paths scrubbed to POSIX
  repo-relative; reruns reproduce the recorded counts exactly).
- M3/M4: `calibration/p1/verify_table.py` is the proposed shared-harness
  location (expected-rows/cols assertions + POSIX normalization implemented
  there; no committed file imports from `shipping-antigravity`; sibling
  results cross-checked pre-merge).

## 15. Reviewer PASS caveats close-out (2026-09-06, this branch, no commits)

- N2 (non-blocking, closed, no regen): all 22,106 `asset_dispositions.jsonl`
  records carry `href` (0 missing; 8,341 `unresolvable_external` all with
  external URLs, e.g. reuters/valemuestrasamples) so external-vs-other stays
  auditable; gate still green — `split_skip_causes.py` untouched.
- N3 (closed): `skip_cause_matrix.json` now records `docs_replayed` 8416 +
  `docs_excluded` 434 (`docs_excluded_by_source`: book 12 / poten 30 /
  broker_reports 105 / breakwave 287; `docs_excluded_breakwave_by_category`:
  drybulk 209 / tankers 78 — verified against `documents.jsonl` sources;
  `breakwave` 287 = 209+78) + `exclusion_reason` inline
  (LINKED_ASSET_SOURCES = baltic/breakwave_insights/hellenic; `adapt_baltic`
  never calls the collector so baltic replays zeros). Verified 8416+434=8850.
- N1 (deferred, `process_knowledge.py` untouched): instrumented run deferred
  until `process_knowledge.py` is next touched.
- New evidence (no scope commitment): ingested-image profile + 3 OCR
  suspects in `docs/INGESTED_IMAGE_AUDIT_MUSE_SPARK.md`; reprocessing scope
   awaits user call; vision sandbox-blocked (no API-key names in sandbox env; project-level path per INGESTED_IMAGE_AUDIT §7 W1 — CI NIM/Ollama secrets + multimodal call-path addition, NOT a batch authorization). Reviewer V1–V4
  follow-ups closed 2026-09-06 in the same file (§8 + §V3 appendix, no commits).

## 16. Reviewer PASS-WITH-CHANGES close-out D1–D4 (2026-09-06, this branch, no commits)

- D1: `asset_dispositions.jsonl` gains `failed` as a fourth disposition
  (`reason` = errors.jsonl exception class for the parent doc, else
  `unknown_extraction_failure` with `reason_unknown=true`; the no-exception
  resolve-miss branch uses `unresolvable_relative_ref`). `ingested` now
  requires non-null `node_id` — gate-enforced, 0 violations.
- D2: the old 97 null-node vs 91 ledger-failed gap (net six) is resolved as
  replay artifacts, fixed not documented-around: 7 CNBC-linked html text
  assets were truly ingested but missed by the `Source asset:`-prefix-only
  lookup (their summaries carry raw page text) — now matched via the
  `Linked asset: {filename}` title fallback; the remaining delta is the
  mirror-less `/s/congestions.png` resolve-miss, now `failed`. Result:
  ingested 13,591 + skipped 8,424 + failed 91 (86 `PDFSyntaxError` + 4
  unknown + 1 unresolvable) = 22,106, three-way gate zero mismatches.
- D3: `skip_cause_matrix.json` is now aggregated FROM the dispositions in the
  same run (single derivation path) plus new `failed_by_reason` /
  ledger-failed cross-check fields; all N3 fields retained.
- D4: P1 §C re-pointed at ingested + failures (3 suspect-OCR images S1–S3 +
   2 `PDFSyntaxError` PDFs, files confirmed on disk); old rows struck,
   protocol + sandbox-blocked status stand (W1, as above).
- E1/E2 (D1–D4 PASS entry, evidence/spec only, no commits): 7 inferential `PDFSyntaxError` attributions flagged via new `attribution` field (`split_skip_causes.py` regen, gate green, deterministic) + single OOXML recovery candidate identified, not recovered — see `docs/INGESTED_IMAGE_AUDIT_MUSE_SPARK.md` §9.

## 17. Pilot 20Q pointer (2026-09-06, this branch, no commits)

- Read-only pilot over Q1–Q20 (§11): `docs/PILOT_20Q_MUSE_SPARK.md` — roll-up single-hop 1 / multi-hop 14 / blocked-unwired 4 / blocked-ocr-quality 1; recommends layer-over-trees with cross-node edges; retrieval-need only, no batch scope committed.
