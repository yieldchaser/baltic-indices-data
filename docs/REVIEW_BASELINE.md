# Review Baseline — Maritime Knowledge Base Build

**Role of this document.** Three agents (Claude Opus, OpenCode/Muse Spark,
Antigravity/Gemini) are working this repo concurrently. This file is the
measured baseline the reviewer holds inventory and extraction plans against.
Every number below was measured directly against the working tree at
`1ffb02db` (`main`, 2026-09-06 13:50 UTC). Where a figure contradicts the
mission brief, the measured figure governs.

Verdicts on pushed branches go in `docs/VERIFICATION_LOG.md`, not here.

---

## 1. Corrections to the mission brief

The brief's aggregate counts are close; its **composition** claims are not.
Planning off the brief's composition produces order-of-magnitude cost errors.

| Brief claim | Measured | Impact if uncorrected |
|---|---|---|
| 35,957 reports/PDFs, 5.94 GB | `reports/` = 36,319 files / 5.9 GB, but only **4,475 PDFs** repo-wide | Per-page OCR budgets (Reducto/LlamaParse) are ~8x oversized and aimed at the wrong modality |
| — | 21,528 scraped page **images** (breakwave 14,633, hellenic 6,895) + 9,264 HTML make up the bulk of `reports/` | The real volume problem is images, not PDFs |
| 19,801 KG shards "sitting unused" | `knowledge/` = 19,799 files; **8,850 are tree shards, actively maintained** | Treating `knowledge/` as inert risks violating the no-overwrite constraint |
| 332 tabular datasets | 294 CSV + 17 XLSX on disk | Minor; reconcile rather than inherit |

**`AUDIT_UNRENDERED_DATA_SOURCES.md` does not exist** in the working tree or
anywhere in git history (`git log --all --diff-filter=A`). Any plan citing it
as a read source is citing a document that is not in this repo.

### Measured repo shape

```
tracked files      56,873      total size (excl .git)   6.9 GB
reports/           36,319 files   5.9 GB    (4,328 PDFs)
knowledge/         19,799 files   733 MB
data/                 515 files   170 MB    (138 PDFs)
docs/                  82 files    18 MB
scripts/              105 files   1.7 MB
```

By extension: md 11,549 · jpg 11,166 · png 10,366 · html 9,264 · json 9,080 ·
**pdf 4,475** · csv 294 · py 119 · jsonl 88 · xlsx 17.

`reports/` by source: breakwave 18,289 files / 2.6 GB (81 PDF) ·
hellenic 14,058 / 3.2 GB (3,948 PDF) · baltic 2,891 · drewry 548 ·
drybulk 209 (209 PDF) · broker_reports 105 · seabrokers 97 · tankers 78
(78 PDF) · poten 30.

---

## 2. There is already a live pipeline. This is additive work.

`scripts/process_knowledge.py` (4,546 lines) is not a stub. It maintains:

- **`knowledge/manifests/documents.jsonl`** — 8,850 documents, each with
  `doc_id`, `source`, `category`, `date`, `source_path`, `doc_path`,
  `tree_path`, `chunk_file`, `source_hash` (`content_sha1_v2`),
  `source_hash_version`, `compiler_version`, `processed_at`, and
  `linked_assets_{discovered,mirrored,ingested,skipped,failed}`.
- **`knowledge/trees/`** — 8,850 hierarchical section shards carrying
  `node_id`, `parent_id`, `section_path`, `level`, `ordinal`, `summary`.
  This is already a section hierarchy, **not** flat RAG.
- **`knowledge/chunks/`** — 101,967 chunks across per-source JSONL files.
- **`knowledge/derived/`** — `signals.jsonl`, `themes.jsonl`,
  `topic_evidence.jsonl`, `section_index.jsonl`, `timelines.json`.
- **`knowledge/manifests/coverage_report.json`** — 8,850 docs / 101,967 chunks
  / 31,228 sections / 10 topics, plus per-source cadence and staleness.

Commits titled `knowledge: update YYYY-MM-DD` land daily. These shards are in
active use by the wiki build and the coverage report.

**Consequence for both build agents:** any proposal that replaces the tree
layer rather than consuming it is a rebuild from zero in disguise, regardless
of the merits of the tool proposed. GraphRAG / LightRAG / Neo4j / Graphiti are
reviewable as a layer **over** `knowledge/trees/` node ids; they are not
reviewable as a substitute for it.

---

## 3. Binding definition: "unprocessed"

> **Unprocessed** = a diff against `knowledge/manifests/documents.jsonl` on
> `source_hash` + `compiler_version`. It is **not** a filesystem walk.

A directory listing both re-flags already-ingested material and misses
content-changed files whose paths did not move. The ledger already carries the
incremental-processing mechanism; use it.

`knowledge/manifests/sources.json` enumerates the covered corpus:

```
breakwave           drybulk 209 · tankers 78
baltic              dry 606 · tanker 608 · gas 217 · container 98 · ningbo 542
breakwave_insights  insights 3,174
hellenic            dry_charter 274 · tanker_charter 273 · iron_ore 1,184
                    vessel_valuations 269 · demolition 797 · shipbuilding 374
broker_reports      105      poten 30       books 12
```

**Genuinely uncovered** (not in `sources.json`, therefore the real new-source
scope): SGX iron ore/freight futures · the 7 Capital Link index XLSX in
`data/` · Drewry AIS + opinions (`reports/drewry`, 548 files) · SSY / Fearnleys
/ Gibson / Allied weeklies · grain flow and port in/out · SNP commercial
fixtures · historical time charter rates · broker comments · SEC EDGAR pull.

That is a tractable scope. It is not 56,000 files.

---

## 4. P0 work item: the `linked_assets_skipped` queue

Aggregating `documents.jsonl`:

| | discovered | mirrored | ingested | **skipped** | failed |
|---|---|---|---|---|---|
| **all** | 22,106 | 13,716 | 13,591 | **8,424** | 91 |
| breakwave_insights | 16,629 | 8,959 | 8,859 | **7,703** | 67 |
| hellenic | 5,477 | 4,757 | 4,732 | **721** | 24 |
| baltic / breakwave / broker_reports / poten / books | 0 | 0 | 0 | 0 | 0 |

**8,424 linked assets were discovered and then skipped.** These are largely the
chart-image assets the mission wants a two-stage vision pass on. They are the
highest-value unprocessed material in the repo, and they are **already
enumerated with parent-document attribution** — `doc_id`, date, source,
section. A fresh filesystem inventory will rediscover the files and lose that
linkage.

**This is P0.** An inventory or plan that does not name
`linked_assets_skipped` as a primary work queue has not read the existing
pipeline and should be sent back before any extraction code is written.

Secondary: `knowledge/manifests/errors.jsonl` holds 83 entries. Sampled
failures are `[Errno 36] File name too long` on breakwave HTML asset URLs — a
mechanical, fixable class, not a parsing-difficulty class. Cheap win; should
not be conflated with hard-extraction backlog.

---

## 5. Review criteria applied to pushed branches

A branch is reviewed against these. Verdicts land in
`docs/VERIFICATION_LOG.md`.

1. **Ledger-diff, not filesystem walk.** Does "unprocessed" derive from
   `source_hash` / `compiler_version`? (§3)
2. **`linked_assets_skipped` named as P0**, with parent-document attribution
   preserved. (§4)
3. **Additive to `knowledge/trees/`.** No replacement of existing shards; new
   graph structure references existing `node_id` / `doc_id`. Existing
   `knowledge/derived/` shards not overwritten.
4. **Sample-validated before batch.** Per the mission's precision rules: one
   text+table report, one chart-heavy report, one dense multi-table report,
   with table boundaries, row counts, and column alignment manually confirmed
   before any pipeline scales.
5. **Extractor ≠ verifier.** Independent verification pass checking row/column
   counts against source page, no cross-table row bleed, correct page-to-table
   attribution, no column-shifted numerics.
6. **Redo loop logged, not silent.** Failures re-sent per file/table, with
   file / page / table-index / symptom recorded.
7. **Cost model uses measured counts** (§1), not the brief's.
8. **Tool choice backed by a pilot against real repo files**, not asserted.

---

## 6. Environment note — cross-agent visibility

The reviewer runs in an ephemeral Linux container that shares **no filesystem
and no git object database** with the Windows checkouts at
`C:\Users\Dell\Github\shipping-muse-spark` and
`C:\Users\Dell\Github\shipping-antigravity`. Verified: `git worktree list`
returns this checkout only; `git branch -a` shows only `main` and the
reviewer's branch plus `origin/*`; those Windows paths do not resolve here.

**Local commits and local-only worktree branches on those machines are
invisible to the reviewer.** Work becomes reviewable only when pushed to
`origin`. Push early and push often — an unpushed branch cannot be reviewed,
and silence from the reviewer means nothing arrived, not that nothing was
wrong.
