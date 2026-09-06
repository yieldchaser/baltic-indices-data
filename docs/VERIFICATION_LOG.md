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

## 2026-09-06 17:26 UTC — `agent/muse-spark` @ `35b510e8e` — **PASS**

**Reviewed:** `docs/INGESTED_IMAGE_AUDIT_MUSE_SPARK.md` (new), N2/N3 fixes in `skip_cause_matrix.json` and `split_skip_causes.py`, plus a clean sync of `main` @ `6ba037f20`.

**Closes N2 and N3.** **D1-D4 remain open** — this commit landed roughly a minute after that verdict was pushed, so it is a timing overlap, not a refusal.

The audit takes the right epistemic stance throughout: it labels §6 findings "suspects needing vision check", quotes stored strings verbatim, and asserts **no** ground-truth corrections while vision is blocked. It also declines to commit to reprocessing scope, deferring to the user. Both are correct.

### The corruption is a bounded, measured class

**228 mixed-separator candidates** across the 10,442 OCR-bearing images, found by scanning for `\d{1,3}\.\d{3}` co-occurring with `\d{1,3},\d{3}`. The `34.438` case this reviewer raised is S1 — one instance of a quantified class, not an anecdote. S2 and S3 extend it: a Bloomberg-style legend carrying `15366,000` and `17483,000` alongside `15200.035` and `9407.00` in a single chart, and a dollar-index axis mixing `100,00` with `96.000` and `$6,000`.

### Reviewer's own checks against this branch's data

**V1 — the ~1,100-character summary cap is NOT data loss. Do not chase it.** The audit reports `pdf` summary lengths maxing at ~1,100 and the section `text` field uniformly empty, which reads like truncation. It is not. `LINKED_TEXT_CHAR_LIMIT` is **70,000** (`process_knowledge.py:73`), and pulling the two chunks for the S1 asset from `knowledge/chunks/breakwave_insights_insights.jsonl` returns 1,164 and 375 characters ending naturally on the table's footnotes, with no `[Truncated linked content excerpt.]` marker. The tree-node `summary` is a display field; the retrievable content in `chunks/` is complete. Recording this so the next reader does not re-open it.

**V2 — but that means the corruption sits in the retrieval layer, not a display field.** The same chunk text contains, verbatim:

```
IRON ORE
78,344 704 1 1,97
PRODUCTION! 8,3 86,704 100,988 301,972
```

This is worse than a separator swap: the production row's digits have been scattered across two lines and interleaved with the row label. A retrieval query about Vale iron-ore production gets this back as tabular fact. The separator scan in §6 counts a class that is real but is a **lower bound** on the numeric damage — row-splitting of this kind carries no dot-versus-comma signature and will not appear in the 228.

**V3 — 451 images were never OCR'd at all.** `[OCR skipped for small image (< 90000 pixels)]`, i.e. anything under roughly 300x300. The audit calls these "logos/icons", which is plausible but is an assumption, not a measurement — a 450x200 chart strip clears none of that threshold. Worth a cheap check: sample 20 and confirm they carry no data.

**V4 — the PDF path is where extraction actually fails.** 89 of the 97 null-node records are `pdf` and 8 are `link`; **every one of the 10,894 `img` records carries a `node_id`**. That aligns with the 79 `PDFSyntaxError: No /Root object` failures and localizes the problem: image ingestion is structurally healthy and merely low-quality, while linked-PDF ingestion fails outright. These are two different repairs, and only the first needs a vision model.

### Usable cost input

The audit supplies what sizing needs: **13,681 files, 3,346.18 MB**, median ~98 KB, split `.png` 6,235 / `.jpg` 4,787 / `.jpeg` 259 / `.webp` 4 / `.pdf` 2,367 / html 29; 10,894 `img` + 2,367 `pdf` + 420 `link`. Any vision budget should be built on these figures.

---

## 2026-09-06 17:02 UTC — `agent/muse-spark` @ `da3be8b45` — **PASS WITH CHANGES**

**Reviewed:** 1 commit, +23,590 lines — `data/derived/asset_dispositions.jsonl` (22,106 per-asset records), the `calibration/p1/` fixture set, a vendored `verify_table.py`, and the Sample C reselection.

**Closes M2 and M3.** The Pass 1 / Pass 2 scripts and all three output artifacts now live at `calibration/p1/` in the branch instead of an OS temp directory, and `verify_table.py` is vendored rather than reached by absolute path into the sibling worktree. Both are reproducible by a third party now.

**Closes N1.** This is the real per-asset instrumentation, not a replay: 22,106 records, exactly the ledger's `discovered`, each carrying `doc_id`, `source`, `date`, `href`, `asset_kind`, `disposition`, `reason`, `local_mirror_rel`, and `node_id`.

**Handles M1 honestly.** The invalid Sample C rows are struck through and retained rather than deleted, with the reason stated in place — "the `node_id` itself is proof of ingestion; the old local-resolution code selected ingested assets by construction." Self-correction left visible in the record is the right call.

### D1 (must fix) — there is no `failed` disposition, and the 91 failures are misfiled

The record total reconciles to 22,106, but the split does not reconcile to the ledger:

| | ledger | dispositions | delta |
|---|---|---|---|
| ingested | 13,591 | **13,681** | +90 |
| skipped | 8,424 | **8,425** | +1 |
| failed | 91 | **absent** | −91 |

90 + 1 = 91 exactly. Every failed asset has been absorbed into `ingested` or `skipped`, and `failed` exists nowhere in the file.

This is not cosmetic. The branch's own doc already spots the symptom — "13,584 with matching tree node, 97 extract-failed with null node" — and still leaves `disposition: "ingested"` on those records. **Any consumer filtering `disposition == "ingested"` therefore receives 90 assets that were never extracted and have no tree node.** Those 90 are the most interesting assets in the corpus: the 79 `PDFSyntaxError: No /Root object` cases resolved to a real local file and then failed extraction, which makes them the one pocket of on-disk material genuinely missing from the graph. Filing them under `ingested` hides exactly the set worth recovering.

Add `failed` as a fourth disposition with the exception class as its `reason`. A disposition of `ingested` should additionally require a non-null `node_id` — 97 records currently fail that invariant.

### D2 — 97 null-node "ingested" against 91 ledger failures: 6 unaccounted

The branch reports 97 null-node ingested records; the ledger counts 91 failures. Six records claim ingestion, carry no tree node, and are not explained by a logged failure. Small, but it is the kind of gap that means the reconstruction and the pipeline disagree somewhere. Identify the six.

### D3 — the branch's two artifacts disagree with each other by one

`skip_cause_matrix.json` totals 8,424 skips (8,341 + 48 + 35). `asset_dispositions.jsonl` has 8,425 skipped, the extra being a `reason: null` record for `/s/congestions.png` — a ledger-failed asset. Regenerate the matrix from the dispositions file so one is derived from the other rather than computed twice.

### D4 — the reselected Sample C is valid but substantively empty

Worth stating plainly, because it completes the P0 reversal rather than qualifying it. The reselection finds that the **only** true-skipped images present on disk are the 35 `duplicate_path` records — and by definition a duplicate's twin is already ingested, already carries a tree node, and is already in the graph. Every other skipped asset has `local_mirror_rel = null` by construction.

**So there are effectively zero recoverable images in the skipped set.** Not few — none. The five reselected samples are five duplicate images whose content is already present. The branch's own instrumentation now confirms the reversal from the other direction.

Sample C should therefore be re-pointed at the ingested set and the 91 failures, which is where unextracted and mis-extracted image content actually lives. The two-stage protocol and the BLOCKED verdict on vision keys stand unchanged and need no rework — only the target does.

### Note

Vision remains correctly BLOCKED pending an API key plus written egress approval. Given the P0 reversal and D4, the batch that key would unblock should be sized against the ingested set, not the skipped one — see the P0 REVERSAL entry below.

---

## 2026-09-06 16:36 UTC — **P0 REVERSAL: the skipped queue is not a chart-extraction opportunity**

`agent/muse-spark` @ `1abab8179` delivered the X1 instrumentation. Its result overturns the P0 designation this reviewer made in `REVIEW_BASELINE.md` §4. **The reviewer's call was wrong, and this supersedes it.**

`data/derived/skip_cause_matrix.json` splits all 8,424 skips by cause:

| cause | count | share |
|---|---|---|
| `unresolvable_external` | **8,341** | 99.0% |
| `non_content_link` | 48 | 0.6% |
| `duplicate_path` | 35 | 0.4% |
| `empty_href` | 0 | — |
| `per_doc_cap` | **0** | — |

Totals reconcile exactly to the ledger's 8,424 across 3,167 documents, with `mismatched_docs: []`.

### Reviewer's independent verification

Three checks, all confirming:

1. **`per_doc_cap: 0` is explained, not anomalous.** `MAX_LINKED_ASSETS_PER_DOC` defaults to 12 at `process_knowledge.py:87`, but **both** production workflows pin it to 28 — `.github/workflows/daily_knowledge_update.yml:89` and `process_knowledge.yml:113`. No document in the corpus reaches 28 linked assets, so the cap never fires. The one cause that would have left recoverable assets on disk contributes **nothing**.
2. **`docs_replayed: 8416` is correct, not a shortfall.** `LINKED_ASSET_SOURCES = {"baltic", "breakwave_insights", "hellenic"}` at line 88. The other 434 ledgered documents (books 12, poten 30, broker_reports 105, breakwave drybulk 209, tankers 78) never invoke the collector. 8,850 − 434 = 8,416 exactly.
3. **The exemplar document, parsed directly.** `reports/breakwave/2020/2020-06-06_the-drama-continues-...html` contains 3 `<a href>` and 2 `<img src>`. Both images are local `assets/` paths — the two already ingested. All three anchors are external: the post's own canonical URL, a **Reuters** article, and a **Vale press release**. Ledger row `discovered 4 / mirrored 2 / ingested 2 / skipped 2` resolves cleanly: the collector's body-scoped candidate set drops the head canonical, leaving 2 external anchors (skipped) and 2 local images (ingested).

### What this means

**The 8,341 "skipped" assets are outbound hyperlinks to third-party journalism and press releases**, not unprocessed charts. They were never mirrored because they were never files in this repo. Recovering them would mean crawling Reuters, Vale, and similar publishers across 2020-2026 — a re-fetch job with heavy link rot and third-party content questions, delivering news articles rather than proprietary maritime data. **That is not worth doing, and it is emphatically not the two-stage vision pass the mission described.**

The reviewer designated this queue P0 on the strength of its size and its parent-document attribution, without establishing what the assets *were*. Both build agents inherited that framing and built against it. The correction cost muse-spark one instrumentation pass; it would have cost far more had it surfaced after a batch vision run.

### Where the chart imagery actually is: the 13,591 INGESTED assets

The images the mission wants are already ingested — and the text extracted from them is poor enough to be a live data-integrity problem. Reading the tree shard for the exemplar document, the second image is a **Vale quarterly production table** whose OCR reads:

```
000' metric tons     4Q19    3Q19    4Q18    2019
Northern System     50,729  55,401  52,911  188,721
Northem andEastem   34.438  35,047  37,023  115,352
$11D                19,291  20,354  15,888   73,369
```

`extract_linked_image_text` runs Tesseract-grade OCR with no structural pass. Note the third row: **`34.438` where the source reads `34,438`** — a decimal point substituted for a thousands separator, a 1000x error in a production figure. `Northern` became `Northem`, `S11D` became `$11D`. This text is **already merged into `knowledge/trees/` and `knowledge/chunks/`** and is retrievable today as if it were fact.

This is precisely the failure mode the mission's precision rules exist to prevent — numerics landing wrong, silently, at scale — except it is already in the graph rather than pending in a batch.

**Recommended P0, replacing the skipped queue:** re-process the **13,591 ingested image assets** with the two-stage axis-first vision pass, upgrading raw OCR to structured output, and reconcile against the existing shards rather than overwriting them. This target is fully enumerated in the ledger (`linked_assets_ingested`), entirely present on local disk, and demonstrably carrying corrupted numerics today. Sizing should use it, not 8,424.

Neither build agent should treat this as settled scope without the user's call — it changes what the project's first batch run is aimed at.

---

## 2026-09-06 16:36 UTC — `agent/muse-spark` @ `1abab8179` — **PASS**

**Reviewed:** 1 commit, +592 lines — `scripts/analysis/split_skip_causes.py`, `data/derived/skip_cause_matrix.json`, inventory notes. Read-only against `knowledge/`; no shard writes.

This closes X1 and produces the P0 reversal above. It is the highest-value contribution on either branch so far, precisely because it invalidated the work both branches were about to scale.

What makes it trustworthy: totals reconcile exactly to the ledger with `mismatched_docs: []`; every cause carries a `cause_line_refs` pointer into `process_knowledge.py`; the method note states its own scoping rules and admits that baltic yields zeros by construction because `adapt_baltic` never calls the collector; and both the code default cap (12) and the CI-pinned cap (28) are recorded rather than assumed. The reviewer re-derived all three load-bearing facts independently and they hold.

### Caveats — non-blocking, worth closing

**N1 — this is a replay, not instrumentation.** The script re-implements the collector's skip branches rather than running the collector with a disposition hook. Exact reconciliation to 8,424 is strong evidence but not proof: causes could be mutually misallocated and still sum correctly. Given that one cause holds 99.0%, the conclusion is robust to any plausible misallocation, so this does not block. Confirm with a one-off instrumented run when `process_knowledge.py` is next touched.

**N2 — `unresolvable_external` conflates two things.** The branch at `process_knowledge.py:2346-2349` increments `skipped` for `http(s)` schemes and `failed` otherwise. A relative path that fails to resolve for an unrelated reason and happens to carry an `http` scheme is counted as external. At this ratio it does not change the finding, but the emitted record should carry the URL so the distinction is auditable.

**N3 — `docs_replayed: 8416` deserves a line in the JSON itself.** It currently reads as a shortfall against 8,850. Record the 434 excluded documents and the `LINKED_ASSET_SOURCES` reason in the artifact, so the next reader does not re-derive it.

M2 and M3 from the previous verdict remain open: the P1 calibration scripts are still in an OS temp directory, and the verifier is still imported by absolute path from the sibling worktree. Neither blocks this commit.

---

## 2026-09-06 16:12 UTC — CROSS-CUTTING — **X1: both branches sampled ingested assets and labelled them skipped**

This is the most consequential finding so far, and it lands on both branches at once. It is the shared-blind-spot case mission rule 4 exists to catch: each agent verified its own output, both passed, and both are wrong in the same direction.

Take parent document `breakwave_insights_..._2020_06_06_the_drama_continues_as_brazilan_judge_hats_vales_iron_ore_operations_in_the_sout`. Its ledger row reads:

```
discovered 4 · mirrored 2 · ingested 2 · skipped 2 · failed 0
```

Its tree shard contains exactly two `linked_image_asset` sections:

```
Linked asset: ..._img_map-minas-gerais-brazil_fc088b057bd4.jpg
Linked asset: ..._img_img-1960_8a20a313afb5.jpg
```

Those two filenames are **the same two** that appear as:

- `agent/antigravity` — `p0_skipped_assets_queue.jsonl` records `__asset_00` and `__asset_01`, both flagged `is_resolved_local: true`, `status: ready_for_vision_stage1`.
- `agent/muse-spark` — `P1_CALIBRATION_MUSE_SPARK.md` §C, survey rows 1 and 2, described as "image-type `linked_assets_skipped` queue entries in ledger order."

**Both are already ingested.** In `collect_linked_asset_sections`, `sections.append(...)` executes only after `stats["linked_assets_ingested"] += 1`. A `linked_image_asset` node in a tree shard is therefore, by construction, proof that the asset was ingested — it is the exact complement of the skipped set. The two genuinely skipped assets in this document are the other two of the four discovered, and they are almost certainly an external URL or a non-content link, which is why nothing for them exists on disk.

Consequences:

- Any P0 queue built by resolving assets to local disk **systematically selects ingested assets**, because those are the only ones mirrored locally. High local-resolution rates (antigravity's "97.8%") are evidence of this error, not evidence against it.
- Feeding this queue to a vision pass re-processes material already in `knowledge/trees/` at full cost, and still leaves the real skipped set untouched.
- `is_resolved_local: true` is close to an inverted signal for "needs work."

**Required of both branches:** stop deriving the skipped set from what resolves on disk. Instrument `collect_linked_asset_sections` to emit one record per asset with its disposition and reason (cap-hit / empty href / non-content / external URL / duplicate / ingested / failed), re-run it over the corpus, and build P0 from that. Until then neither branch has a valid P0 sample, and no chart-extraction sizing derived from one should be trusted.

---

## 2026-09-06 16:12 UTC — `agent/muse-spark` @ `fc6dd2b94` — **PASS WITH CHANGES**

**Reviewed:** 1 commit, +90 lines — `docs/P1_CALIBRATION_MUSE_SPARK.md` (new) and image-count corrections to the inventory. Still documentation only; no shard writes.

### Sample B independently re-verified by the reviewer — exact

This is the first claim on any branch that a third party could check, and it checks out. Because the source is committed (`docs/BDRY-BWET_Form10-Q_March-31-2026.pdf`), the reviewer re-extracted page 6 with an unrelated library (`pypdf`, not pdfplumber or pymupdf) and confirmed independently:

- 66 pages, as stated.
- The BDRY futures block holds exactly **9 contract rows** — 3 Capesize, 3 Panamax, 3 Supramax, April/May/June 2026 expiries.
- Σ unrealized = **−2,157,385**; Σ notional = **43,916,630**. Both match the printed subtotal line `$ (2,157,385) $ 43,916,630 100%` **exactly**.

The arithmetic tie-out is the strongest verification technique on either branch: it checks the extraction against a number the extractor never produced, so an extractor and verifier sharing a parsing assumption cannot both be wrong and still tie out. It should become a standing requirement wherever a source table carries a total row. Also correct: the first attempt (visual `find_tables`) fragmented into 13 × 1-row tables and this was recorded as a logged failure with a redo, not quietly replaced.

Sample A shows the same discipline — a real verifier FAIL (`row_count_mismatch`, 8 vs a manual count of 5, from ruling-split header fragments), a logged redo, then a clean pass, with the two empty ruling-artifact columns **preserved as measured rather than silently dropped**. Preserving dead columns is the right call: dropping them is how column-shift corruption gets normalized into a schema.

Sample C is correctly **not forced**. With no `REDUCTO_API_KEY` / `LLAMA_CLOUD_API_KEY` / `ANTHROPIC_API_KEY` / `OPENAI_API_KEY` provisioned, it reports BLOCKED, names the exact unblock condition, and specifies a deferred two-stage protocol in which a stage-1 record with unreadable axes **fails closed and blocks stage 2**. That matches the mission's axis-first requirement, and declining to fabricate chart values under a blocked capability is the right behaviour.

### Changes required

**M1 — Sample C's selection is invalid (see X1).** All five surveyed images are ingested assets, not skipped ones. The §C readiness verdict and the deferred protocol stand on their own and need no rework; the **sample set** does. Re-select once the disposition instrumentation from X1 exists. No extraction was claimed from these images, so nothing downstream is contaminated — this is a sampling error, not a data error.

**M2 — the calibration is not reproducible, same class as antigravity B4.** Pass 1 and Pass 2 live at `C:\Users\Dell\AppData\Local\Temp\opencode\p1_pass1.py` / `p1_pass2.py` — an OS temp directory that will be cleared. Commit both scripts and the three JSON/JSONL outputs to the branch. This is a lesser finding than B4 only because the **source documents are committed**, which is what let the reviewer verify sample B at all; the standard is otherwise identical and applies to both branches.

**M3 — the cross-worktree harness import is a one-machine dependency.** Pass 2 reaches `ExtractionVerifier` via `sys.path.insert` into `C:\Users\Dell\Github\shipping-antigravity`. That path exists on exactly one machine, and it makes muse-spark's verification silently dependent on an uncommitted file in another agent's tree. The harness needs to live in one committed location both branches import from.

**M4 — push the `expected_rows`/`expected_cols` fix back into the harness.** Supplying explicit expected counts to `verify_table` is precisely the fix that antigravity finding B6 calls for — it is what turned sample A's 8-vs-5 discrepancy into a FAIL instead of a silent pass. Keeping it in a local Pass-2 wrapper leaves the shared harness still able to pass collapsed tables. Contribute it upstream.

### Accepted without change

The image-count reconciliation is correct and closes the apparent disagreement between the two branches: `git ls-files` with `core.quotepath=false` returns the **higher** correct figures, because default quotepath octal-quotes non-ASCII filenames and undercounts by 5 `.jpg`. Reports-only 21,528 (breakwave 14,633 + hellenic 6,895), repo-wide 21,532, the 4 extra being `assets/Picture1-4.png`. This matches the baseline's `find`-based figures; both were right about different scopes, and muse-spark's method is the better one to standardize on.

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
