# Inventory — muse-spark

- Author: muse-spark
- Date: 2026-09-06
- Branch: agent/muse-spark
- Base HEAD: 187ee53ba (56,873 tracked files)
- Scope: inventory + plan ONLY. No extraction code, no shard writes.
- Worktree-concurrency: work done in C:\Users\Dell\Github\shipping-muse-spark on branch agent/muse-spark, never committing to main from shared checkout, merges one-at-a-time.

Spot-checks re-verified in this worktree 2026-09-06: `reports/baltic` = 2891 tracked files;
`reports/drewry` = 548 tracked files with 0 local PDFs; `reports/hellenic` = 14058 tracked files;
`knowledge/manifests/sources.json` (generated 2026-09-06) covers breakwave/baltic/hellenic/
broker_reports/poten/books only; `data/Capital_Link_*.xlsx` (7 files) present.

## 1. Census (tracked files at HEAD)

| Area | Count | Notes |
| --- | --- | --- |
| reports/ total | 36,319 | |
| reports/baltic | 2891 | dry/tanker/gas/container/ningbo |
| reports/breakwave | 18289 | insights |
| reports/broker_reports | 105 | md only |
| reports/drewry | 548 | 0 local PDFs — md + ais_manifest.csv only |
| reports/drybulk | 209 | breakwave drybulk |
| reports/hellenic | 14058 | incl 3947 tracked PDFs |
| reports/panama_canal | 1 | |
| reports/poten | 30 | opinions, metadata-only |
| reports/seabrokers | 97 | md |
| reports/tankers | 78 | breakwave tankers |
| data/ total | 515 | |
| data/bunkers | 8 | |
| data/cache | 1 | |
| data/cftc_statements | 144 | |
| data/commodities | 22 | |
| data/congestion | 4 | |
| data/demolition | 1 | fixtures CSV |
| data/derived | 30 | incl compiled TC-rate CSVs |
| data/etf | 145 | |
| data/flows | 3 | |
| data/futures | 14 | incl SGX FEF/M65F/LPF raw |
| data/indices | 22 | incl Capital Link converted CSVs |
| data/macro | 1 | |
| data/manifests | 2 | |
| data/raw | 7 | |
| data/reports | 100 | seabrokers-related |
| data/rulebooks | 3 | |
| data/ top-level | 8 | incl 7 Capital_Link_*.xlsx (~227KB each, single-sheet IndexArchiveValue) |
| knowledge/ total | ~19,797 | |
| knowledge/books | 1798 | |
| knowledge/briefs | 108 | |
| knowledge/chunks | 165 | |
| knowledge/derived | 6 | |
| knowledge/docs | 8850 | |
| knowledge/manifests | 7 | |
| knowledge/reports | 1 | |
| knowledge/trees | 8850 | |
| knowledge/wiki | 11 | |
| index.html | ~30,464 lines (~30k) | 9 tabs (9 `<div class="tab-panel">` divs, 9 unique `id="tab-*"`), 53 `fetch(` call sites |

## 2. Sibling audit note (corroborating, not canonical)

`docs/AUDIT_UNRENDERED_DATA_SOURCES.md` is NOT on main — it is uncommitted sibling work
in the shared checkout. It claims 56,864 files / 7.35GB / 332 tabular / 35,957 reports /
19,801 shards, consistent with HEAD count 56,873 within counting-method tolerance.
Treat as corroborating, not canonical.

## 3. Rendered vs unrendered

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

## 4. Manifest coverage gap

`knowledge/manifests/sources.json` (2026-09-06) covers breakwave / baltic / hellenic /
broker_reports / poten / books ONLY. It omits drewry, SGX, CapitalLink, EDGAR, grain,
SNP, TC-matrix sources. `coverage_report.json` omits broker_reports topics.

## 5. Format classes (verified samples)

- (a) Mixed-layout text+table PDFs WITH text layer: `docs/BDRY-BWET_Form10-Q_March-31-2026.pdf`
  (66pp, p1 ~2881 chars); hellenic demolition sample (7pp, p1 ~1611 chars).
- (b) Chart-heavy 2-pager WITH text layer: `docs/Amplify_BDRY_FactSheet.pdf` (2pp, p1 ~2567 chars).
- (c) Clean tabular: data/futures SGX CSVs; data/derived TC-rate CSVs; Capital Link
  single-sheet xlsx (CLMI 5232x10).
- (d) HTML stubs: reports/baltic sample is cookie-wall thin.
- (e) Derived md/JSON: seabrokers 97 md + OSV dayrates CSV (most complete new source);
  Poten opinions metadata-only (JS-rendered body missing).

## 6. Tooling verdict (probed, nothing installed)

- READY: pypdf 6.14.2 / pymupdf 1.28.2 / pdfplumber / pandas / openpyxl / torch.
- ABSENT: docling / camelot / transformers / ocrmypdf / tesseract / ghostscript / java.
- NO API keys (Reducto / LlamaCloud / Anthropic / OpenAI all absent) — Reducto/LlamaParse
  evaluation is BLOCKED until keys are provisioned.
- Docling install is possible (torch present) but OCR deps missing.
- All sampled PDFs have text layers, so native extraction suffices for phase-1 samples.
- Drewry AIS PDFs are not local (manifest-only) — must download before chart-pipeline
  validation; check native dashboard export first per mission.

## 7. Sibling scope boundaries (do not duplicate)

- agent/antigravity +1 commit (645db9079) added `scripts/harness/verify_extraction.py` +
  `calibrate_sample.py` + `scripts/spine/build_knowledge_spine.py` — REUSE the harness
  for verifier passes.
- Shared checkout has many UNTRACKED active-work files (fetch_fearnleys_*, SGX probes,
  sec_edgar_pipeline.py, shipbroker scrapers, fearnleys derived CSVs) — other agents'
  live scope, do not touch.

## 8. Plan

- P0 — Reconcile manifests: add missing sources to sources.json/coverage, additive only.
- P1 — Sample validation per class (1 text+table, 1 chart-heavy, 1 dense multi-table)
  with row-count / column-alignment / page-attribution checks via the antigravity harness.
- P2 — Pilot 20 multi-hop questions (below), THEN choose graph layer (LightRAG vs
  GraphRAG vs Neo4j vs Graphiti per mission).
- P3 — Incremental batch extraction with extractor/verifier separation + redo loop + audit log.
- Constraint: additive — never overwrite knowledge/derived shards.

## 9. Pilot questions (20, multi-hop)

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

## 10. Next action for muse-spark

P0 manifest reconciliation + P1 sample calibration on the hellenic demolition PDF + 10-Q + factsheet.
