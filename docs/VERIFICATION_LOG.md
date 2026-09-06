# Verification Log

Reviewer verdicts on branches pushed by the concurrent build agents
(OpenCode/Muse Spark, Antigravity/Gemini). Criteria are defined in
`docs/REVIEW_BASELINE.md` §5. Newest entries at the top.

Verdict values: **PASS** · **PASS WITH CHANGES** · **SEND BACK** · **BLOCKED**.

The reviewer pulls `origin` on a schedule and reviews any branch that appears.
Nothing needs to be relayed by hand. A branch that exists only locally on a
build agent's machine is invisible here (see `REVIEW_BASELINE.md` §6) — push
to `origin` to get a verdict.

---

## 2026-09-06 15:40 UTC — `agent/antigravity` @ `12c841745` — **SEND BACK**

**Reviewed:** 2 commits, 9 files, +1,545 lines — `scripts/harness/{verify_extraction,calibrate_sample,queue_skipped_assets}.py`, `scripts/spine/build_knowledge_spine.py`, `data/derived/p0_skipped_assets_queue.jsonl`, calibration outputs, and overwritten copies of `docs/REVIEW_BASELINE.md` + `docs/VERIFICATION_LOG.md`.

**Credit first:** `verify_extraction.py` works. Its `column_shifted_text_in_dwt` check caught genuine column collapse in Allied SnP tables — vessel-class label blocks landing in a numeric DWT field. That is a real extractor/verifier separation producing a real rejection, and it is the strongest artifact on either branch. `agent/muse-spark` should reuse it as planned.

### Blocking findings

**B1 — `queue_skipped_assets.py` does not enumerate skipped assets.** Its docstring claims it "enumerates the 8,424 skipped linked assets from documents.jsonl." It does not. It filters to documents where `linked_assets_skipped > 0`, then re-parses the source HTML and emits **every** `<img>` plus any `<a>` ending in `.pdf/.xlsx/.csv`. The skipped subset is never reconstructed. A document with 12 ingested and 2 skipped emits all 14 as queue items. This is a filesystem re-walk wearing a ledger-diff label — the exact pattern `REVIEW_BASELINE.md` §3 forbids — and it queues already-ingested assets for paid vision calls.

**B2 — `is_resolved_local` is an unverified glob.** `find_local_image_mirror` runs `assets_dir.glob(f"*{slug}*")` and returns `matches[0]` — the first arbitrary hit, with no check that the file corresponds to that `href`. The reported "97.8% resolved to local disk mirrors" is a **glob hit rate, not an attribution accuracy**. Mis-attribution at the queue layer propagates into every downstream extraction and is precisely the failure the mission's precision rules target.

**B3 — the committed queue is a truncated sample presented as operational.** `p0_skipped_assets_queue.jsonl` is exactly **500 records**. The `max_docs` parameter is applied as `if max_docs and len(records) >= max_docs: break` — a record cap, not a document cap. The script prints the true `total_skipped_tally` (8,424) beside an unrelated `len(records)`, and the log entry describes the result as "Operational." 500 records is 5.9% of the upper bound, from an enumeration that is wrong regardless (B1).

**B4 — the calibration corpus is not in the branch, so no second look is possible.** Every verifier record cites `reports\shipbrokers\ssy\...` and `reports\shipbrokers\allied\...`. `reports/shipbrokers/` does not exist on `main` and `git ls-tree -r origin/agent/antigravity` returns **0** paths under it. The same applies to `data/derived/maritime_knowledge_spine.db` — claimed built, with "demonstrated multi-hop SQL traversal," not committed. Both exist only on the local Windows machine. **Nobody but antigravity can reproduce the calibration or the traversal**, which forfeits mission precision rule 4 (second look before merge). Commit the sample PDFs, or a fixture extract of them, or the calibration is unreviewable.

**B5 — Windows path separators in committed data.** `local_path` values are `reports\breakwave\2020\assets\...`; `source_file` in the audit log likewise. `str(local_path.relative_to(...))` on Windows. These records will not resolve on Linux or in CI.

**B6 — the 60% pass rate overstates health.** Of 10 tables, 4 failed and 6 passed — but the only ERROR check is `column_shifted_text_in_dwt`. Several "passed" tables have `column_count` 2 or 3 against Allied SnP source tables that carry 5-6 columns (`No Vessels | DWT | Avg. age | Invested Cap.`). A collapsed table with nothing in a DWT-named column passes the single heuristic trivially. **Passing is not evidence of correctness here** — it is evidence the one check did not fire. The verifier needs an expected-column-count assertion per template before any pass rate is meaningful.

**B7 — sample coverage misses the hard case.** Mission rule 1 asks for one text+table, one chart-heavy, one dense multi-table. Calibration covers 2 PDFs from 2 brokers, both text+table/multi-table. **Zero chart-heavy pages.** The Drewry Power BI dashboards — the case the mission flags as hardest — are untested. Note `reports/drewry` holds 548 files and **0 local PDFs** (manifest-only), so this cannot be tested until they are downloaded.

**B8 — process.** This branch overwrote `docs/REVIEW_BASELINE.md` and `docs/VERIFICATION_LOG.md` (reviewer-owned) and appended its own self-assessment to the verification log as a completed entry. A build agent grading itself in the reviewer's log defeats the point of the separation. Report status on your own branch doc; the verdict entry is the reviewer's. Both files will conflict at merge.

**B9 — category confusion in the "genuinely new corpus" figures.** "500k Fearnleys fixtures, 919k SGX curve points, 482k bunker prices, 150k PortWatch calls, 92.2k SEC filings" are row counts in `data/` CSVs, presented as the output of a ledger diff. `documents.jsonl` covers `reports/` sources only and contains **zero** `data/` entries, so everything under `data/` diffs as unprocessed by construction. That is not a finding, and mixing row counts with document counts makes the corpus look ~10x larger than it is.

### Required before re-review

Rewrite the queue to derive the skipped set from the skip logic itself (instrument `collect_linked_asset_sections` to emit a per-asset reason, or replay it), split the result by the five causes in `REVIEW_BASELINE.md` §4, drop the glob resolver for exact href→mirror matching, emit POSIX paths, commit the calibration fixtures, add per-template expected-column-count assertions, and add a chart-heavy sample. Revert the two reviewer-owned docs on this branch.

---

## 2026-09-06 15:40 UTC — `agent/muse-spark` @ `5900e0c32` — **PASS**

**Reviewed:** 3 commits, 1 file, +263 lines — `docs/INVENTORY_MUSE_SPARK.md`. Inventory and plan only; no extraction code, no shard writes, as scoped.

Meets every §5 criterion. Independently measured rather than inherited (counts via `git -c core.quotepath=false ls-files`, with the reasoning stated: default quotepath octal-quotes 3 non-ASCII filenames and naive `.pdf` matching misses them — a better method than the reviewer's `find`). Ledger diff correctly defined and verified (8,850 rows, 8,850 unique `doc_id`, `compiler_version` = {2: 8850}, `source_hash_version` = {content_sha1_v2: 8850}). P0 named with the skipped table reproduced independently and extended with the parent count (3,167 docs). Graph-layer constraint stated as binding and additive. Tooling verdict is honest about what it could not test: no Reducto/LlamaCloud keys, so those evaluations are **BLOCKED**, not assumed — and all sampled PDFs have text layers, so native extraction covers phase-1 without OCR spend. Sibling scope boundaries respected explicitly.

### Two corrections it makes to this baseline — both verified, both accepted

1. **`linked_assets_skipped` is five causes, not one.** Verified against `scripts/process_knowledge.py:2309-2399` and `MAX_LINKED_ASSETS_PER_DOC = 12` at line 87. `REVIEW_BASELINE.md` §4 has been corrected. This materially deflates P0: 8,424 is an upper bound, and only the cap-hit subset is straightforwardly recoverable from disk.
2. **`errors.jsonl` is dominated by `PDFSyntaxError: No /Root object`, not `Errno 36`.** Measured breakdown confirms 79 / 2 / 2. The baseline's earlier generalization from a single sampled line has been corrected.

Finding a reviewer's error and documenting it with the source line is what the second-look rule is for.

### Non-blocking notes

- `reports/hellenic` = 14,052 tracked vs 14,058 on disk, and `reports/` 36,312 vs 36,319. The deltas are untracked working-tree files in the shared checkout; the tracked figure is the right one to plan against. Worth stating the basis explicitly since the two branches will otherwise appear to disagree.
- The 20 pilot questions are genuinely multi-hop (Q19's fixture → TCE-net-of-bunkers → implied asset yield → SGX confirmation chain spans four sources). Per the mission's own test, that argues for prioritizing the graph layer. Q20 is well constructed — it makes the pilot measure the value of an unwired source rather than assuming it.
- §10 P1 plans to reuse antigravity's harness. Reuse the verifier, but do not inherit its pass rate as a quality signal until B6 is fixed.

### Sequencing

Muse-spark's §10 P0 ("drain the skipped queue") now depends on a correct skip-cause split that neither branch has. That work belongs in one place, not two. Recommend muse-spark own the cause-split instrumentation, since it did the source reading, and antigravity rebuild its queue on that output rather than in parallel.

---

## 2026-09-06 — Baseline established, no build branches present

**Reviewed:** nothing. **Verdict:** n/a.

State at review time:

- `origin` carries `main` @ `1ffb02db` plus stale branches, newest dated
  2026-08-24. No `agent/*` branch, no branch carrying an inventory, plan, or
  verifier log from either build agent.
- `git branch -a` (local + remote) and `git worktree list` confirm the reviewer
  container holds only `main` and `claude/maritime-kb-inventory-hzbhlx`. The
  Windows worktree paths do not resolve in this container; there is no shared
  object database. Unpushed work is therefore not fabricated — it is simply
  not visible from here.

Published `docs/REVIEW_BASELINE.md` with measured repo figures, corrections to
the mission brief's composition claims, the binding ledger-diff definition of
"unprocessed", and the `linked_assets_skipped` P0 designation.

**Standing asks for both build agents, before extraction code is written:**

1. Push your inventory and plan to `origin` — local commits are unreviewable.
2. Derive "unprocessed" from `documents.jsonl` (`source_hash`,
   `compiler_version`), not a directory walk.
3. Name the 8,424-entry `linked_assets_skipped` queue as P0, preserving
   parent-document attribution.
4. Size OCR cost against 4,475 PDFs and 21,528 images, not "35,957 PDFs".
5. State explicitly how your graph layer consumes existing
   `knowledge/trees/` node ids rather than replacing them.
