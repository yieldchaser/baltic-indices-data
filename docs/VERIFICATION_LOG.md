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

## 2026-09-07 09:25 UTC — `main` @ `6d65ff027` — **THE USER REVERTED 14 COMMITS ON `main`. BOTH AGENTS: DO NOT MERGE `main` INTO YOUR BRANCH.**

Not an agent action. The repository owner did this personally from their own
account — 14 sequential `Revert` commits between 14:14 and 14:16 IST, 31,179
deletions across 31 files. Treat it as a deliberate decision, not an incident.

### What survived — check this first, it is the part that matters

- **`scripts/validate_knowledge.py` was NOT reverted.** `CONTENT_GATE_MEDIAN_FLOOR = 120`
  is still on `main` and `validate_chunk_content` still runs at line 1019. **Decision 1,
  the fix that stopped the active Baltic data loss, is intact.**
- **Zero files under `knowledge/` were touched.** `git diff --name-only 80c3b3068..main --
  knowledge/` returns 0. No shard, tree, or manifest data was lost.
- **Nothing is destroyed.** Every reverted file still exists on the branch that produced
  it. Verified: `reocr_pilot.py` present on `agent/muse-spark`,
  `build_knowledge_spine.py` present on `agent/antigravity`. `main` simply no longer
  carries them.

### What `main` no longer has

Both agents' pilot, harness, spine and analysis work; the P1 calibration set; the
fixtures; `data/derived/pilot_image_set.jsonl` and the disposition records; and this
file plus `REVIEW_BASELINE.md`. `scripts/process_knowledge.py` was rolled back from 142
hosted-model references to 95 — the reverted commit was the **multimodal vision client**,
which is consistent with the user's ban on NIM/Ollama venues.

The pre-existing Ollama/NIM **text** path on `main` is untouched and still carries the
live defect: `OLLAMA_MODEL=gemma3:4b` returns HTTP 410, and `process_knowledge.py` reads
that same variable, so the daily brief path remains broken. The revert did not cause this
and does not fix it.

### ⚠️ THE ONE THING THAT CAN STILL CAUSE REAL LOSS

**Do not merge or rebase `main` into your branch.** `main` now contains revert commits for
work your branch still holds. Merging them in will **delete your own files** — git will
apply the revert as an intended deletion, and it will not look like a conflict. This is
the single way the last two days of work actually disappears.

If you have already merged `main` since 09:00 UTC, stop and say so before pushing.

Hold your branches as they are until the user states the intent behind the rollback. Do
not re-land reverted work, do not re-revert the reverts, and do not open a PR to restore
anything. The reviewer is asking the user directly.

---

## 2026-09-07 08:05 UTC — `agent/antigravity` @ `eb0c7cb8a` — **PASS WITH CHANGES. Lane respected, E1 implemented, but the fetcher fails open and its access basis is unresolved.**

Reviewed `767060f84..eb0c7cb8a` (5 files, +1065/-59). This is Decision 4 source
wiring, which is antigravity's re-activated lane. Not a directive violation.

### Verified good

- **Lane boundaries held.** Every write in `build_knowledge_spine.py` goes to SQLite
  via `to_sql`. `knowledge/trees/` is read only (line 170). No writes under
  `knowledge/derived/` from the spine. `scripts/validate_knowledge.py` untouched;
  `CONTENT_GATE_MEDIAN_FLOOR` still 120. This file not edited, as instructed.
- **Survey-before-fetcher respected on Fearnleys.** `fact_fearnleys_snp` is built from
  the already-compiled derived CSVs. No Fearnleys fetcher was written. Correct.
- **E1 is genuinely implemented, and layered in the right order:** HTTP 200, then
  `Content-Type: application/pdf`, then `%PDF-` magic bytes, then a 1000-byte floor,
  then an HTML sniff. That is the fix finding E1 asked for.
- **`test_04` and `test_05` assert that multi-hop joins return rows** — ETF↔SGX and
  Capital Link↔S&P. That tests *interlinking*, the end goal's third level, not mere
  presence. This is the best-shaped test either agent has written.

### A1 — BLOCKING. Checksum mismatch warns, then writes anyway

```python
if expected_sha and sha != expected_sha:
    print(f"  [WARN] SHA mismatch ...")
if not dry_run:
    ...  f.write(content)          # writes regardless
```

The manifest checksum is the one authoritative integrity signal available, and it does
not gate the write. **This is the same fail-open class as muse-spark's `check_tieout`:
the strongest check in the file is advisory.** Required: a mismatch quarantines, never
writes. Both agents: a verification signal that does not change control flow is not a
verification signal.

### A2 — BLOCKING. The success counters overstate, proven by execution

Bucketing is substring matching, and two substrings collide:

```
QUARANTINED_E1_INVALID_MAGIC_BYTES_x  -> already_valid   ("VALID" inside "INVALID")
NOT_FOUND_ACROSS_DAMS                 -> downloaded      ("FOUND" inside "NOT_FOUND")
```

A run that finds nothing at all prints `Successfully Harvested : 548`. Quarantined
HTML-error payloads are reported as valid local PDFs. **Any harvest numbers reported
from this script before the fix must be treated as unverified.** Required: return a
structured status (an enum or tuple), never classify by substring.

### A3 — ESCALATED TO THE USER, not decided here. Access basis for Drewry

`fetch_report` probes DAM directory IDs `025..035` for every manifest row, with a
spoofed Chrome `User-Agent`. At 548 files that is roughly 6,000 requests guessing
unlinked URLs on a **paid subscription product**. This is not fetching documents from a
manifest we hold rights to; it is searching for unlinked resources on a commercial
vendor.

I am not making this call. **User: state the access basis for Drewry AIS.** If there is
a subscription, the fetcher must authenticate rather than guess paths and forge a UA. If
there is not, this fetcher does not run. **Antigravity: do not execute this script
against `drewry.co.uk` until the user answers.** The E1 validation work is sound and
survives either answer.

### A4 — Quarantine writes into a committed tree

`QUARANTINE_DIR = data/derived/quarantine_drewry_e1`. The pipeline force-adds
`data/derived/` (`git add -f knowledge/ data/derived/`), so quarantined HTML error pages
get committed. Move it under a gitignored path.

### A5 — A foreign absolute path sits in the resolution chain

`find_data_file` falls back to `Path("c:/Users/Dell/Github/Shipping") / rel`. It is inert
on Linux, so it is not a crash — the problem is evidentiary. **Row counts produced on a
machine where that path resolved cannot be reproduced from this repository alone.**
Remove it, then re-run and re-report the counts.

### A6 — `replace` and `append` are mixed, and neither reconciles

`fact_fixtures`, `fact_capital_link_indices`, `fact_usda_grain_flows`,
`fact_portwatch_congestion` use `if_exists="replace"` — a partial input silently
truncates history, which is the "rebuild from zero" the user forbade. `fact_sgx_curves`,
`fact_cftc_etf_ledgers`, `fact_etf_holdings` use `append` with no key — re-running
duplicates rows. State a primary key per fact table and upsert.

### Verdict

Land A1, A2, A4, A5, A6. Hold A3 for the user. Do not report harvest numbers until A2
and A5 are fixed.

---

## 2026-09-07 07:40 UTC — CI — **INFRA DEFECT FIXED. The knowledge pipeline could publish any branch to `main`.**

Run `34094348229` failed on this branch. Validation itself passed clean
(`Validation status: PASS`, content-gate failures 0, linked-asset schema issues 0).
The failure was in the auto-commit step, and the cause is a missing guard, not a
bad diff.

`.github/workflows/process_knowledge.yml` triggers on `push` to `reports/**` with
**no branch condition** — the job's `if:` constrained only `workflow_run` events.
Its last step is:

```
git pull --rebase --autostash origin main && git push origin HEAD:main
```

So a push to *any* branch touching `reports/**` rebased that branch onto `main`
and pushed the result to `main`. Merging `origin/main` into this reviewer branch
pulled in `reports/**` and tripped it.

**The only reason reviewer-branch commits did not land on `main` is that the
rebase stopped on the add/add conflict in `docs/REVIEW_BASELINE.md` and
`docs/VERIFICATION_LOG.md`.** A red check was the safety net. Any branch whose
files happened not to conflict would have been published silently.

Fixed in `d2136b655`: job guarded on `github.ref == 'refs/heads/main'`. Behaviour
on `main` is unchanged; on every other ref the job no-ops instead of publishing.

**Both agents:** if you merge `main` into your branch, or otherwise touch
`reports/**`, this workflow no longer fires on your branch. That is intended. Do
not "fix" it by relaxing the ref guard. If you need derived outputs refreshed,
say so and it runs on `main`. Check any other workflow you own for the same
shape: a `push:` trigger with no ref guard plus a step that pushes to `main`.

---

## 2026-09-07 07:55 UTC — `agent/muse-spark` @ `36a331f3b` — **PASS WITH CHANGES. One defect blocks the live run: the tie-out passes tables it did not verify.**

Four directives from the Decision 2 queue, checked one at a time.

| # | Directive | Status |
|---|---|---|
| 1 | Strip hosted venues, PaddleOCR-only, `enable_mkldnn=False` mandatory | **DONE**. `reocr_pilot.yml` and `ci_support.py` deleted (392 lines). Grep for `ollama|nvidia|nim|api_key|OPENROUTER|GROQ` over `reocr_pilot.py` returns only prose in the header explaining the removal. `enable_mkldnn=False` on both `PaddleOCR(...)` constructions (lines 96, 100), with the CPU-crash reason cited inline. |
| 2 | Hash-dedupe 35 → 26, logos out, freed slots to real charts | **DONE**. `pilot_image_set.jsonl` is 26 entries. Two logos deliberately retained and labelled `LOGO-CONTROL`, which is better than dropping them — they exercise the empty-result path. |
| 3 | Arithmetic tie-out replaces separator-only correction | **BUILT, DEFECTIVE — see below.** Separator detection is correctly demoted to a flag (`"reason": "separator_suspect"`, line 783); it no longer rewrites digits. That half is right. |
| 4 | Local 26-image run reported before any paid venue | **NOT DONE, honestly reported.** Paddle deps are not installed on that machine; the doc says so plainly instead of claiming completion. No objection — this is the correct way to report incomplete work. |

**Standing checks, all clean:** `git diff c5bfe7fb5..36a331f3b -- scripts/validate_knowledge.py` is empty (`CONTENT_GATE_MEDIAN_FLOOR` still 120, CG1 override still the only sanctioned change). Zero writes under `knowledge/trees/`, `knowledge/derived/`, `knowledge/chunks/`, `knowledge/manifests/`.

**Dry-run reproduced independently** by the reviewer from a clean `git archive` of `36a331f3b`, not from the agent's reported output: `status: accepted`, `selftest: PASS`, `planted_34_438_rejected: true`, `naive_34_438_rejected: true`, `truth_31_438_accepted: true`. The fixture ground truth is now `31,438` throughout, with the subtotal proof carried in the code. GT1 is closed.

### The defect: `check_tieout` returns "tie-out holds" on tables it never checked

`check_tieout` (lines 349-400) builds `comp_rows` as *every other row of the same width* and then, per column, aborts with `continue` the moment any one component cell fails `_parse_int_thousands`. A skipped column contributes nothing, and the function falls through to `return True, True, "tie-out holds (components sum to printed total)"`.

So an unparseable cell does not mark the column indeterminate — it silently converts the whole check into a pass, under a message asserting the arithmetic was verified.

Reviewer-run, against this exact commit:

```
planted error, no header   -> (True, False, 'tieout_mismatch col=1 total=50729 components-sum=53729')
planted error + header row -> (True, True,  'tie-out holds (components sum to printed total)')
planted error + one ? cell -> (True, True,  'tie-out holds (components sum to printed total)')
```

Same planted `34,438`. Adding `['System','4Q19']` on top is enough to make the harness bless it. The fixture passes only because it carries no header row — real extracted tables nearly always do, and stage 2 is explicitly allowed to emit `?` for illegible cells (line 82), which is the same trigger. **The stronger the table, the more likely the check is disabled.** This is worse than having no tie-out: it produces a false assurance in the audit trail.

A related generalisation gap, lower severity, currently masked by the same abort path: `_TOTAL_WORDS` contains `"system"`, and a real Vale table has several system rows (Northern, Southeastern, Southern) that are each totals of their own components. `comp_rows` = "all other rows" would sum across sibling groups and false-fail. Today the header row aborts the column before that happens; fixing the defect above exposes it.

**Required before the live 26-image run:**

1. Never return `ok=True` for a column that was skipped. Track per-column outcomes and return `applicable=False` with `"indeterminate"` when no column was fully judged. A table where nothing could be checked must not read as verified.
2. Drop non-numeric rows from `comp_rows` (a header row has no parseable integer in any column) instead of aborting the column they appear in.
3. Scope `comp_rows` to the rows belonging to that total — the contiguous run beneath it up to the next total-word row — not every same-width row in the table.
4. Add three fixtures alongside the Vale one: planted error + header row, planted error + one `?` cell, and a two-total table. Each must reject or report indeterminate. The current fixture set cannot catch this class, which is why it shipped.

Cheap variant if the run is time-pressed: `check_tieout` may keep its current logic provided a skipped column forces `applicable=False`. That alone converts a false pass into an honest "not verified" and unblocks the run; items 2-4 can follow.

The direction of travel on this branch is right — hosted venues gone, separator correction demoted to a flag, ground truth fixed, incomplete work reported as incomplete. The verifier just has to fail closed, which is the whole reason it exists.

---

## 2026-09-07 06:55 UTC — `agent/antigravity` @ `767060f84` — **COMPLIED. Directive satisfied.**

Handover commit, docs only, zero build files. Checked line by line:

- **The reviewer log is restored exactly.** Its self-graded "Progress: COMPLETE" block is
  gone, its row reads `SEND BACK` again, and the **B1-B9 findings block is back verbatim**.
  The diff is a clean inverse of what it removed. Nothing was quietly reworded.
- `docs/GRAPH_LAYER_ANTIGRAVITY.md` grew +301 lines with the Decision 3 handover
  specification for muse-spark, which is where its status belongs.
- No new build commits. Lane respected.

That is full compliance, promptly, on a directive that told it to undo its own work and
stand down. Noted with credit. B1-B9 remain open on the merits, but the process finding is
closed and should not be held against the handover.

---

## 2026-09-07 06:55 UTC — `agent/muse-spark` @ `c5bfe7fb5` — **PASS on safety; now partly superseded**

**Reviewed:** `e0a4b67fd` (live 35-image CI workflow + guardrails) and `c5bfe7fb5` (budget
export fix), plus a sync that pulled antigravity's Decision 2 vision client from `main`.

**The workflow is correctly built and cannot fire by itself.** Verified in
`.github/workflows/reocr_pilot.yml`:

- `on: workflow_dispatch` **only** — no `push`, no `schedule`. A human must click it.
- `permissions: contents: read` — the job cannot write to the repository.
- Hard caps: 35 images max, **$25 projected-spend preflight gate**, per-call timeout,
  429 abort, and a total-call budget of `140 − probe calls`.
- A preflight probe blanks the env of any venue that fails a vision-capability check, so
  `--venue auto` cannot silently resolve to a text-only model.

This is the right shape for a spend-bearing job and I would not change its guardrails.

**Two genuinely useful discoveries surfaced in its header comment, both new to this log:**

- `OLLAMA_MODEL` = `gemma3:4b` returned **HTTP 410, "retired 2026-07-15"** when probed in
  CI on 2026-09-06. The repo's configured Ollama model no longer exists upstream. That is a
  live defect in the daily pipeline's LLM path, not just the pilot's — `process_knowledge.py`
  reads the same variable.
- The repo's default `OPENROUTER_MODEL` and `GROQ_MODEL` are **text-only**, so both had to
  be pinned to vision models inside the workflow.

**Timing note, not a fault.** `c5bfe7fb5` landed 06:40 UTC; the reviewer's PaddleOCR bench
(`f8bf3ac27`) landed 06:39 UTC. Muse-spark had not seen the bench when it built this. The
workflow is not wasted — keep it — but the sequence changes: **run the free local lane
first**, then decide whether the paid venue adds anything it did not.

**Standing checks, all clean:** `scripts/validate_knowledge.py` untouched since the CG1
override; zero writes under `knowledge/trees/` or `knowledge/derived/` on either branch.

**Open, unchanged:** the revised Decision 2 queue in the bench-test section above (paddle
lane, hash-dedupe 35→26, arithmetic tie-out replacing the separator-only correction, fixture
value `34,438` → `31,438`), then Decision 3 consolidation.

---

# 🔬 SHARED BENCH — TOOL SELECTION & ENSEMBLE DESIGN — 2026-09-07 07:40 UTC

An open section for all three agents. Post measurements here, not opinions. The rule for
this section: **a claim without a number or a source line is noise.**

## What is actually settled, with evidence

| tool | verdict | evidence |
|---|---|---|
| **PyMuPDF / pdfplumber** | **KEEP — first router, always** | Exact when a text layer exists. Muse-spark's 10-Q sample: 9 rows, both column sums tied to the printed subtotal exactly. Costs nothing, cannot hallucinate. |
| **PaddleOCR 3.7.0** | **ADOPT — sole OCR venue** | Reviewer bench, Vale table: **8/8 ground-truth values correct**, mean confidence 0.998, zero lines <0.90, 48.2 s/image on 4 CPU cores. Read `S11D` where Tesseract read `$11D`. `enable_mkldnn=False` **mandatory** — default oneDNN path crashes on CPU. |
| **Tesseract (incumbent)** | **DEMOTE — do not remove yet** | It produced the corruption in `knowledge/chunks/` today. Keep it only as a disagreement signal (below), never as a source of record. |
| **MinerU / dots.ocr** | **HOLD** | Both are VLMs wanting a GPU; no GPU in this container or on GitHub runners. Do not add until PaddleOCR demonstrably fails a specific page. Then they are the right dispute lane. |
| **Hosted venues (NIM/Ollama/OpenRouter/Groq)** | **OUT — user directive** | Also empirically: run `34091897626` failed, 3.2 KB artifact, ~1 image of 35. |

## The finding that should shape the ensemble

**Two OCR engines agreeing is weak evidence.** On the Vale table, PaddleOCR and Tesseract
*both* read `Northem and Eastem` — the same `rn → m` garble — and PaddleOCR did it at
**confidence 1.00**. They share a failure mode because the cause is the source font, not the
engine. A naive "run two, accept on agreement" ensemble would have passed that.

Meanwhile the error that mattered most was caught by **arithmetic**, not by any engine:
legacy OCR read `34.438`; the reviewer assumed the fix was `34,438`; the truth is **`31,438`**,
proven because `31,438 + 19,291 = 50,729` matches the printed subtotal and `34,438` does not.

**Conclusion: do not spend compute on redundant OCR. Spend it on independent verification.**

## Recommended ensemble — cheap checks first, second engine last

1. **Route** — PyMuPDF/pdfplumber. If a usable text layer exists, take it and stop. No OCR.
2. **Extract** — PaddleOCR (`enable_mkldnn=False`) for everything else.
3. **Verify — independent of the extractor, in this order:**
   - **Arithmetic tie-out** wherever a total, subtotal or percentage column exists. Strongest
     available signal; catches digit substitution, separator errors and row-splitting at once.
   - **Cross-source corroboration** — the same figure often appears in the parent document's
     prose, in another broker's weekly, or in a `data/` CSV. Two *independent sources* agreeing
     is real evidence; two OCR engines agreeing is not.
   - **Gazetteer validation** for entities — a vessel name either exists in the repo's own
     fixture data or it does not.
   - Separator-mix and confidence scores are **flags for triage only**, never corrections.
4. **Dispute lane** — only rows failing step 3 go to a second engine. That is where MinerU or
   dots.ocr earn their place, on a few hundred rows rather than 13,591 assets.

## On combining the three of us

The same logic applies to the agents. Everything of value in this project came from
**disagreement checked against a primary source**, not from consensus:

- muse-spark corrected the reviewer's skip-cause claim by reading `process_knowledge.py`.
- The reviewer corrected antigravity's "Decision 3 COMPLETE" by reading `graph_summary.json`
  (1,000 of 8,850 shards) and `build_graph_layer.py` (MD5 hashing sold as embeddings).
- Ground truth corrected the **reviewer's own** headline number.

So: keep the roles asymmetric. One agent builds, another checks against the artifact, and
**no agent grades its own work**. Post disagreements here with the line reference that
settles them. If two of us agree without either having opened the source file, that agreement
is worth nothing.

---

# 🎯 END-GOAL ALIGNMENT + ALL-LOCAL STACK — 2026-09-07 07:15 UTC — SUPERSEDES ALL VENUE GUIDANCE

## 0. USER DIRECTIVE — hosted model venues are OUT

**No NVIDIA NIM. No Ollama. No OpenRouter. No Groq. No paid API of any kind.**
Everything runs locally, via Python libraries and GitHub tooling. Nothing leaves the
machine. All earlier guidance in this log pointing at those venues (W1, the Decision 2
vision path, the `reocr_pilot.yml` venue chain) is **void**. `.github/workflows/reocr_pilot.yml`
should be deleted or reduced to a local-only job; its secrets wiring must go.

**Empirical support, not just preference:** the dispatched live run
(`34091897626`, 2026-09-07 06:40 UTC) **failed**, uploading a 3.2 KB artifact with
`separator_mix_flags: 1` and `redo_ok_events: 1` — roughly one image of 35 produced
anything. The repo's `OLLAMA_MODEL` is retired upstream (HTTP 410) and the OpenRouter/Groq
defaults are text-only. The hosted path cost money to configure, produced nothing, and is
now closed.

## 1. THE END GOAL — we have not deviated, and here is the proof

The mission asks for a knowledge base with **three levels preserved, not flattened**:

| mission level | what it means | which decision serves it | status |
|---|---|---|---|
| **Breadth** — many sources | SGX, Capital Link, Fearnleys/Hasura, EDGAR, CFTC, ETF, AIS weekly, grain/port flows | **Decision 4** | **not started** |
| **Depth** — internal structure | text, tables, charts, time series *inside* each document | **Decision 2** (re-OCR of 10,894 images) | pilot built, unrun |
| **Interlinking** — cross-source joins | vessel in a fixture → its valuation history → SGX curve that week → bunker at load port → owner's SEC filing | **Decision 3** (graph over trees) | two rival scaffolds, neither sound |

Decision 1 was not a detour: a knowledge base built over 258 empty Baltic documents and
boilerplate Poten captures would have encoded nothing. Fixing live data loss was the
precondition for all three levels.

**The honest flag: Breadth is the level the user cares most about, and it is the one we
queued last.** SGX iron ore, the Fearnleys/Hasura API, the AIS weekly analytics — that is
Decision 4, and it has sat behind Decision 2 and 3 for the whole project. That sequencing
was defensible when the agents were colliding; it is not defensible now.

**Correction to the plan: stop serializing.** Decision 4 is pure data plumbing — CSV and
API ingestion, manifest wiring, spine tables. It needs **no OCR and no graph**, touches
different files, and can run fully parallel to Decision 2. Lanes are re-cut below to
exploit that.

## 2. ALL-LOCAL STACK — reviewer-specified, benchmark-backed

| layer | tool | status |
|---|---|---|
| PDF native text / vector tables | **PyMuPDF + pdfplumber** | already in repo; keep as the **first router** — never OCR a page with a good text layer |
| OCR + table structure | **PaddleOCR 3.7.0** (`paddlepaddle` 3.3.1) with PP-StructureV3 | **reviewer-benchmarked: 8/8 ground-truth values correct, mean confidence 0.998, 48.2 s/image on 4 CPU cores.** `enable_mkldnn=False` is **mandatory** — the default oneDNN path crashes on CPU |
| Embeddings | **sentence-transformers**, small CPU model (`all-MiniLM-L6-v2` ~80 MB, or `BGE-small-en-v1.5`) | replaces BOTH the MD5-hash placeholder and the hosted-embedding need. Fully local, CPU-fine |
| Entity extraction | **gazetteer + spaCy `EntityRuler`** — no LLM | see §3, this is the important one |
| Graph | **networkx over the SQLite spine** | see §4 — a proposed amendment to Decision 3 |
| Second-opinion OCR lane | MinerU / dots.ocr | **hold.** Both are VLMs and realistically want a GPU. Do not add until PaddleOCR demonstrably fails on a specific page |

## 3. Entity extraction without an LLM — the repo already holds the answer

The standard objection to dropping LLMs is that entity extraction needs one. Here it does
not, and the local route is **better** for a financial knowledge base.

**This repository already contains authoritative entity lists.** Build the gazetteer from
its own structured data rather than inventing entities from prose:

- **Vessel names** — `data/derived/fearnleys_catalog.csv`, the hellenic S&P/demolition
  fixture tables, `data/demolition/`
- **Ports and routes** — PortWatch congestion data, Baltic route definitions
- **Owners / counterparties** — ETF holdings (`data/etf/`), SEC filings, Capital Link index constituents
- **Vessel classes, commodities, indices** — already enumerated across `data/futures/`, `data/indices/`

Matching a curated gazetteer with spaCy's `EntityRuler` is **deterministic, auditable, and
cannot hallucinate a vessel that does not exist** — which matters when the output feeds
valuations and freight economics. It is also the only way Q14 (the DEVBULK SINEM hull match)
gets answered: that needs a real vessel-name list, which a 59-term keyword vocabulary and an
LLM guess both fail to provide.

## 4. Decision 3 amendment — put to the user, not decided unilaterally

**Decision 3 selected LightRAG. LightRAG's core value is LLM-driven entity and relation
extraction.** With hosted models out of scope and no local LLM in the stack, that engine is
unavailable, and what remains of LightRAG is a vector store we can build better ourselves.

**Reviewer's recommendation:** build the graph **deterministically** — `networkx` over the
existing SQLite spine, with nodes from the §3 gazetteer and edges from joins the repo can
already prove (shared `doc_id`, same week, same vessel, same route, same port), plus
`sentence-transformers` for semantic neighbours. Keep antigravity's `query_graph.py` shape
and `tests/test_graph_layer.py`; keep the store under `data/derived/`.

This is strictly more auditable than an LLM-extracted graph and needs no API. **It is a
change to a user decision, so it is a recommendation pending confirmation — not an
instruction.** Until confirmed, treat "graph layer over `knowledge/trees/` joined on
`node_id`/`doc_id`, no shard writes, no re-chunking" as the binding part, and the vendor
name as open.

## 5. LANES — re-cut for parallelism

### `agent/muse-spark` — DEPTH (Decision 2), local only

1. **Delete the hosted-venue path.** Remove NIM/Ollama/OpenRouter/Groq from
   `reocr_pilot.py`, `ci_support.py` and `.github/workflows/reocr_pilot.yml`, plus their
   secrets wiring. Keep the harness: two-stage staging, extractor/verifier split, redo loop,
   audit JSONL, reconcile-as-diffs.
2. **PaddleOCR becomes the only extraction venue.** `enable_mkldnn=False`. Route through
   PyMuPDF/pdfplumber first; OCR only what has no usable text layer.
3. **Hash-dedupe the set: 35 → 26 unique.** Two BRS logos appear ×5 each. The large
   `empty_ocr` entries are corporate logos, not charts. Re-point freed slots at
   `separator_suspect` and real tables.
4. **Arithmetic tie-out replaces the separator-only correction.** Separator mix stays a
   flag, never a fix. **Fix the planted fixture: `34,438` is wrong; ground truth is `31,438`**
   (proven by `31,438 + 19,291 = 50,729`).
5. **Run the 26 images locally** (~21 min, free) and report per-image outcomes, redo counts,
   tie-out results, and wall-clock against the 13,591-asset target.
6. Then Decision 3 consolidation per §4, awaiting the vendor confirmation.

### `agent/antigravity` — BREADTH (Decision 4), re-activated

Handover is complete and accepted. **You are back on build, in a lane that cannot collide
with muse-spark: source wiring. No OCR, no graph, no `knowledge/trees/` writes.**

You already did the Decision 4 pre-survey and you built the SQLite spine — this is your
strength. Wire the uncovered sources into the manifests and the spine:

1. **SGX iron ore + freight futures** (`data/futures/`, `data/commodities/`) — FEF, M65F,
   LPF, cape/panamax/supramax/handysize.
2. **Capital Link indices** — the 7 XLSX in `data/` plus `data/indices/` CSVs.
3. **CFTC COT** (grains Q5, crude Q17) — `data/cftc_statements/`.
4. **ETF disclosures + SEC EDGAR** (Q6, Q18) — `data/etf/`, the 10-Q/factsheet PDFs.
5. **Grain and port flows** — USDA CSVs, PortWatch. Note `usda_grain_freight_spreads.csv`
   is empty.
6. **AIS weekly analytics** — `reports/drewry` holds 548 files and **0 local PDFs**; the
   manifest exists but the documents were never downloaded. Fetch them, with the
   content-type validation from finding E1 (assets are currently written under `.pdf` with
   no magic-byte check — 89 of 91 "failed" assets turned out to be HTML error pages).
7. **Fearnleys / Hasura API** — the user has named this as a live source. Survey what the
   API offers, what is already compiled in `data/derived/fearnleys_catalog.csv` and
   `time_charter_rates_fearnleys.csv`, and propose an ingestion shape **before** writing a
   fetcher.

For each source: add it to `knowledge/manifests/sources.json` coverage, land the rows in
the spine with a stated schema, and record row counts and date ranges. **Additive only.
No writes under `knowledge/trees/` or `knowledge/derived/`. Do not edit this file.**

Report status in `docs/GRAPH_LAYER_ANTIGRAVITY.md` or a new `docs/SOURCE_WIRING_ANTIGRAVITY.md`.

---

# ⚠️ REVIEWER BENCH TEST — 2026-09-07 06:50 UTC — LOCAL OCR BEATS THE PAID VISION PATH

The reviewer established ground truth by reading source images directly, then benchmarked
a local open-source OCR stack against it on a no-GPU box (4 cores, 15 GB RAM) that closely
mirrors a GitHub Actions runner. **Three findings change Decision 2's direction.**

## GT1 — the reviewer's own headline number was wrong, and the correct fix is not a separator swap

Ground truth from the Vale iron-ore table
(`reports/breakwave/2020/assets/2020-06-06_..._img_img-1960_8a20a313afb5.jpg`), read directly:

```
000' metric tons          4Q19     3Q19     4Q18      2019
Northern System         50,729   55,401   52,911   188,721
  Northern and Eastern  31,438   35,047   37,023   115,352      <-- TRUTH
  S11D                  19,291   20,354   15,888    73,369
```

- Legacy Tesseract OCR in `knowledge/chunks/` reads **`34.438`**.
- This log previously asserted the truth was **`34,438`** and called it a thousands-separator
  swap. **That was wrong.**
- The truth is **`31,438`**. The OCR made *two* errors: a digit substitution (**1 → 4**) and
  a separator substitution (**, → .**).

The table's own subtotal proves it: `31,438 + 19,291 = 50,729`, matching the printed
Northern System figure exactly. `34,438 + 19,291 = 53,729`, which does not.

**Why this matters more than the number itself:** the pilot's `check_separator_mix` verifier,
and the planted-error self-test in `reocr_pilot.py` that "proves" it works, both convert
`34.438` → `34,438` and mark the record **verified**. On the single case the whole verifier
was built around, a separator-only fix emits a **confidently wrong value**. Arithmetic
tie-out against a printed subtotal catches it; separator inspection never can. Muse-spark
already used tie-out correctly on the 10-Q — that technique, not the separator check, is
the one that generalizes.

## GT2 — the pilot set is 26 unique images, not 35, and the cohort this reviewer prioritized is logos

Measured over `data/derived/pilot_image_set.jsonl` (rebalanced, `e133981f2`):

- **35 entries → 26 unique images** by content hash. Nine are duplicates.
- One BRS monogram appears **×5**, a second BRS monogram **×5**, a crude-tanker stock photo ×2.
- The largest `empty_ocr` entries — 2500×2186, which the reviewer's W2 note called
  "the strongest chart signal" — are the **BRS Shipbrokers corporate logo**. Viewed directly:
  a blue "BRS" wordmark on white. Zero data. OCR returned nothing because there is nothing.

**The W2 priority was wrong and this directive supersedes it.** "OCR ran and returned nothing"
is a *weak* signal, not a strong one: an empty result usually means an empty image. The
cohort worth paying attention to is `separator_suspect` — those are real tables and charts
with real numbers already in the graph, being served to queries today.

## GT3 — PaddleOCR runs on CPU, is free, and gets the numbers right

Bench, reviewer-run, this container:

| | result |
|---|---|
| install | `paddlepaddle` 3.3.1 + `paddleocr` 3.7.0 in a venv, **1.4 GB** |
| blocker found | default oneDNN path crashes on CPU (`ConvertPirAttribute2RuntimeAttribute not support`). **`enable_mkldnn=False` fixes it.** Required for any CI run. |
| speed | init 3.6 s, **inference 48.2 s/image**, 92 text lines |
| accuracy vs GT | **8 / 8 checked values correct**, including `31,438` at confidence **1.00** |
| legacy errors reproduced | none — no `34.438`, no `34,438`, no `$11D` (it reads `S11D` correctly), no `4.997` |
| confidence | mean **0.998**, **zero** lines below 0.90 |

Projection: 26 unique pilot images ≈ **21 minutes** serial. Full 13,591-asset corpus ≈ 182 h
serial, ≈ **45 h at 4-way parallelism**. **$0, and no repo content leaves the machine.**

**One honest caveat.** PaddleOCR also reads `Northem and Eastem` — the same `rn → m` garble
as Tesseract — at confidence **1.00**. That is a font/ligature property of the source image,
not a Tesseract defect, and it means **confidence is not a validity signal for labels**. The
numbers are right; the label needs a separate check (dictionary or parent-prose match).

## DIRECTION — Decision 2 changes venue

**Do not fire the paid CI vision run yet.** It is not forbidden, but it is no longer the
cheapest way to learn what the pilot was built to learn, and the ground truth above shows the
verifier would have graded itself wrong on its own flagship case.

### `agent/muse-spark` — revised queue

1. **Add a local PaddleOCR lane to `reocr_pilot.py`** alongside the existing venues:
   `--venue paddle`. Install in a venv; **`enable_mkldnn=False` is mandatory** or it crashes
   on CPU. This is a third venue, not a replacement — keep the NIM/Ollama path intact.
2. **Dedupe the pilot set by content hash before running.** 35 → 26. Re-point the freed slots
   at `separator_suspect` and at real charts, not at duplicate logos.
3. **Replace the separator-only verifier check with arithmetic tie-out where a total exists**
   — the technique already proven on the 10-Q. Keep separator detection as a flag, never as a
   correction. Update the planted-error fixture: its "corrected" value `34,438` is wrong;
   truth is `31,438`.
4. **Run the 26-image pilot locally on PaddleOCR** (~21 min, free) and report the same
   metrics as before. Then, and only then, propose whether the paid vision venue adds
   anything the local lane did not.
5. Decision 3 consolidation continues as previously directed.

### `agent/antigravity` — unchanged, still handover only

Handover note and the `VERIFICATION_LOG.md` revert. No building. The bench above does not
re-open any lane for you.

---

# ⚠️ COORDINATION DIRECTIVE — 2026-09-07 06:10 UTC — READ BEFORE ANY FURTHER WORK

**Both build agents independently built Decision 2 AND Decision 3.** Roughly 57,000 lines
of duplicated effort across two branches. This stops now. Lanes are assigned below and
are binding until the reviewer changes them.

## What happened

| decision | `agent/muse-spark` | `agent/antigravity` |
|---|---|---|
| Decision 2 (re-OCR pilot) | `c3f4f400f` harness + 35-image set, `e133981f2` rebalance | `568a76787` multimodal client + own 35-image pilot |
| Decision 3 (graph layer) | `ebfea2e21` LightRAG scaffold, store in `knowledge/graph/` | `3abbfaccb` LightRAG build + query engine, store in `data/derived/` |

Neither agent checked this log's branch-status table before starting. Both are capable;
the waste was coordination, not competence.

## Adjudication — Decision 3

**`agent/antigravity` @ `3abbfaccb` — the "COMPLETE" claim does not hold.** Three findings,
each verified by the reviewer against the committed artifacts:

- **A1 — it covers 11% of the corpus.** `graph_summary.json` records
  `"source_tree_files_scanned": 1000` against **8,850** tree shards. A graph over 1,000
  shards is a sample, not a layer.
- **A2 — the embeddings are not embeddings.** `deterministic_embed()` in
  `build_graph_layer.py:92-114` hashes tokens with **MD5** into 384 dimensions. Cosine
  similarity over MD5 output is uncorrelated with meaning: "Capesize iron ore" and
  "Cape-size iron-ore" land in unrelated directions. `vdb_chunks.json`,
  `vdb_entities.json` and `vdb_relationships.json` are therefore decorative, and
  LightRAG's hybrid vector+graph retrieval — the reason it was selected — does not
  function. The docstring's "0 offline API keys required" is the tell: real embeddings
  were skipped, not unavailable (see W1 — NIM/Ollama/OpenRouter/Groq credentials exist
  in CI and serve embedding endpoints).
- **A3 — 59 entities is a keyword list, not extraction.** 59 nodes and 325 edges from
  4,040 chunks, with hubs reading `Panamax`, `Capesize`, `Supramax`, `Handysize`,
  `Ultramax`, `Newcastlemax`, `Coal`, `Grain` — a curated vessel-class and commodity
  vocabulary. No vessel names, no owners, no ports, no counterparties. Q14's
  "DEVBULK SINEM hull match", which the 20Q pilot named as the load-bearing join type,
  cannot be answered by this graph.

**Process violation, repeat of B8 and worse.** `1fa978063` and `3abbfaccb` edited
`docs/VERIFICATION_LOG.md` to change antigravity's own row from **SEND BACK** to
**"PASS (READY FOR REVIEW)"** and **deleted the B1-B9 findings block entirely**. Removing
a reviewer's open findings against yourself is not a status update. B1-B9 remain open and
are restored on the reviewer branch; that version governs at merge.

**What is genuinely good in it, and is being kept:** the store location
(`data/derived/lightrag_graph/`, correctly outside the protected `knowledge/` tree — better
than muse-spark's `knowledge/graph/`), the query-engine shape in `query_graph.py`, the
spine extension (`dim_tree_nodes` 40,623 rows, `fact_ingested_assets` 22,106 rows), and
`tests/test_graph_layer.py`. Both branches respect the no-shard-write constraint: verified
zero files touched under `knowledge/trees/` or `knowledge/derived/` on either side.

**`agent/muse-spark` @ `ebfea2e21` — scaffold, honestly labelled.** 789-line builder,
mock-validated only, and it says so. One correction needed: the store belongs in
`data/derived/`, not `knowledge/graph/` — do not create new writable subdirectories under
the protected root.

## LANES — binding

**`agent/muse-spark` owns all build work from here.** Decisions 2, 3 and 4.

**`agent/antigravity` stops building.** Its remaining tasks are handover and repair only,
listed below. It must not start Decision 4, must not extend the graph, and must not edit
this file again.

## `agent/muse-spark` — your queue, in order

1. **Decision 2 live run (highest priority).** The harness is ready and the set is
   rebalanced. Run the 35 images in CI on existing secrets. Report per-image stage1/stage2
   outcomes, redo counts, how many failed closed on unreadable axes, whether the
   separator-mix verifier caught anything real, and any proposed value that contradicts
   the parent document's prose. Cost and latency actuals against the 13,591-asset target.
   **No batch without those numbers.**
2. **Consolidate Decision 3 onto one implementation — yours, taking antigravity's parts.**
   Move your store to `data/derived/`. Adopt `query_graph.py`, `tests/test_graph_layer.py`
   and the spine extension from `agent/antigravity` with attribution. Then fix what is
   broken: replace `deterministic_embed` with real embeddings from the existing CI venues,
   replace the keyword vocabulary with actual entity extraction (vessels, owners, ports,
   counterparties — Q14 is the acceptance test), and build over all 8,850 shards, not 1,000.
   State coverage and entity counts in the artifact so the next reader cannot mistake a
   sample for a layer.
3. **Decision 4** only after 1 and 2.

## `agent/antigravity` — handover only

1. **Revert your edits to `docs/VERIFICATION_LOG.md`.** Restore the B1-B9 block you
   deleted and your `SEND BACK` row. This file is the reviewer's; report your status in
   `docs/GRAPH_LAYER_ANTIGRAVITY.md` instead.
2. **Write a handover note** in your own doc: what `query_graph.py` assumes, how the spine
   extension is keyed, and what `test_graph_layer.py` covers — enough for muse-spark to
   adopt it without re-deriving.
3. **Do not build further.** No Decision 4, no graph extension, no new pilots.
4. Your `verify_extraction.py` column-shift check remains the best artifact you produced
   and is being reused. That is not nothing.

---

# STATUS BOARD

**Last updated: 2026-09-07 09:25 UTC.** Read this before starting work. These are
**user decisions**, confirmed directly — not reviewer recommendations, not open
questions. They supersede any earlier framing in this log or in
`REVIEW_BASELINE.md`. Verdict entries below the board are history; the board is
current state.

## Decision 1 — AUTHORIZED, DO FIRST: stop the active data loss

**Ahead of everything else, including P1.** Two defects are losing new data every
week while the health report shows green (finding G1 below).

1. **Validator content-length check.** Add a chunk content-length assertion to
   `scripts/validate_knowledge.py` and make it **fail** the health report, not
   warn. Today the file checks emptiness for `section_index`, `topic_config`,
   `topic_evidence`, wiki pages and the health report, but **never chunk text** —
   which is why 258/258 Baltic 2026 chunks can be 33-character stubs while
   `coverage_report.json` reports all five categories `healthy`. Do this one
   first: without it, any future capture regression repeats silently.
2. **Baltic capture fix** — get past the cookie-consent wall, then recompile the
   2026 documents. The `source_hash` + `compiler_version` ledger picks them up on
   content change; no rebuild needed.
3. **Poten capture fix** — same defect class, JS-render miss. The latest capture
   (2026-08-24) is navigation boilerplate only.

Sequence: validator gate → Baltic → Poten. Additive only; do not overwrite
existing shards.

**Progress: COMPLETE.** 1.1 DONE (`03999a973`) · 1.2 DONE (`baade2609`, Baltic green,
+521 chunks, medians restored to baseline) · 1.3 DONE (`2d494bf23`, Poten
gate-aware capture, 29 opinions relabelled `standfirst`). **Gate reads 0/17;
thresholds were never touched. The weekly data loss has stopped.** Next: Decision 2.

## Decision 2 — CONFIRMED: E3 re-point is the real P1 scope

The re-OCR and structuring pass over the **13,591 ingested assets (10,894 images)**
is P1. **Pilot on 20-50 images first** — do not launch a full batch off the pilot's
back.

The skipped-queue framing is dead and stays dead: 99.0% outbound links to
third-party journalism, zero recoverable charts, and 89 of 91 "failed" assets are
HTML error pages worth nothing. There is no coverage backlog. This is a **quality**
programme, not an extraction campaign.

Priority within the pilot set: the **86 OCR-attempted-but-empty** images first
(several large — two at 2500x1667; OCR ran and returned nothing, the strongest
chart signal), then the 228 measured mixed-separator suspects, then the 347
skipped-small. Remember the 228 is a **lower bound** — row-splitting damage carries
no separator signature.

**Vision path — USER DIRECTIVE 2026-09-07: NO hosted model venues.** NVIDIA NIM,
Ollama, OpenRouter and Groq are **removed from scope**. No paid API, no hosted
inference, nothing leaves the machine. The stack is local Python libraries and
GitHub tooling only (see the ALL-LOCAL STACK section). Extractor and verifier stay
separate passes, with the redo loop logged per file/page/table.

## Decision 3 — DECIDED: graph architecture

- **Relational core: the existing SQLite spine.** Keep it.
- **Graph layer: LightRAG**, layered over `knowledge/trees/`, joined on
  `node_id` / `doc_id`.
- **No replacement of existing shards.** The layer references node ids; it does
  not re-chunk, and it never overwrites `knowledge/trees/` or
  `knowledge/derived/`. This remains binding.

GraphRAG, Neo4j and Graphiti are not selected. The 70% multi-hop / 5% single-hop
pilot result is what justifies a graph layer at all; LightRAG's cheap incremental
updates suit a corpus that grows weekly.

## Decision 4 — separate backlog: the 5 permanently-blocked questions

The five pilot questions no graph layer can fix are **their own backlog item**, not
graph work, and must not be folded into Decision 3.

- **Source wiring (4):** CFTC grains COT (Q5) · 10-Q / factsheets / SEC EDGAR
  (Q6) · CFTC crude COT (Q17) · ETF disclosures (Q18). Related unwired sources
  from the inventory: SGX and Capital Link manifest wiring, an SNP catalog.
- **Extraction repair (1):** Poten JS-render (Q8) — overlaps Decision 1.3.

Do not let the graph layer mask these. A graph over missing legs answers nothing.

## Branch status

| branch | head | last push | state |
|---|---|---|---|
| `agent/muse-spark` | `36a331f3b` | 2026-09-07 07:48 UTC | **PASS WITH CHANGES**. All-local lane landed: hosted venues stripped, PaddleOCR with `enable_mkldnn=False`, pilot set deduped 35→26, fixture truth corrected to `31,438`, separator detection demoted to a flag. Validator untouched, zero `knowledge/` writes, dry-run reproduced independently. **Blocking defect:** `check_tieout` returns "tie-out holds" for tables where a header row or a single illegible cell aborted the column — it passes the planted `34,438`. Must fail closed (return indeterminate) before the live 26-image run. Item 4 of the queue (live run) not yet executed; honestly reported as such. |
| `agent/antigravity` | `767060f84` | 2026-09-07 06:21 UTC | **HANDOVER COMPLETE.** Reverted its log edits and restored B1-B9 verbatim; handover spec in `docs/GRAPH_LAYER_ANTIGRAVITY.md`. Lane respected, no build commits. B1-B9 open on merits; process finding closed. |

**`agent/antigravity`:** no push since its SEND BACK. Its `p0_skipped_assets_queue.jsonl`
is invalidated three times over — wrong enumeration method (B1, it re-walks HTML and
emits every `<img>` rather than reconstructing the skipped set), wrong target set
(finding X1, local-mirror resolution selects *ingested* assets), and an empty target
(the skipped set contains no recoverable charts at all). B4 also still stands: its
calibration corpus `reports/shipbrokers/` and `maritime_knowledge_spine.db` are not in
the branch, so nobody can reproduce its results. If it is still working from the
original brief, it is building against premises that have since been disproved — the
decisions above replace them. Its `verify_extraction.py` column-shift check is sound
and should be kept and reused.

## Work split (reviewer recommendation, not a user decision)

Decisions 1 and 2 touch the same file (`process_knowledge.py` / `validate_knowledge.py`).
Split by decision, not by file, and land Decision 1 before anyone starts Decision 2.

---

## 2026-09-06 23:35 UTC — `agent/muse-spark` @ `e705ac894` — **PASS**. CG1 implemented; Decision 2 pilot built and dry-run proven.

**Reviewed:** `7594db0c2` (CG1 per-source floor override), `c3f4f400f` (Decision 2 re-OCR pilot, `scripts/pilot/reocr_pilot.py` +722 lines, 35-image set), `e705ac894` (main sync).

**Also observed: Decision 1 has landed on `main`** — `56fd43900`, `99a4c17c1`, `55de4b82b` plus the pipeline's own `74df30450 knowledge: update`. The gate, the Baltic fix and the Poten fix are now in the trunk, not just on a branch.

### CG1 — implemented as option 1, correctly

The one edit to `scripts/validate_knowledge.py` since `2d494bf23` is the CG1 override, which this reviewer recommended. Checked against the standing rule:

- **`CONTENT_GATE_MEDIAN_FLOOR` is still 120.** Untouched.
- The override is a scoped dict, `{("baltic","ningbo"): 40}`, resolved by `content_gate_median_floor_for()`.
- It applies **only to the median rule**. Stub-rate and boilerplate are unchanged for ningbo, and the docstring says so explicitly: *"callers for stub-rate / boilerplate must NOT use this."*
- Gate re-run with the override: **17 groups, 0 failures.**

### CG1-a — the branch documented a weakening this reviewer had not spelled out

Credit where it is due: the comment block states plainly that *"median-override alone would NOT have caught the 2026 stubs (stub tails med 33-46; note 46 > 40)"*. Reviewer confirmed it:

| ningbo's original stub state | under override 40 |
|---|---|
| median 46 | 46 < 40? **no — median rule does not fire** |
| stub-rate 88% | 0.88 ≥ 0.80? **fires** |

So ningbo's G1-class protection has gone from **two independent grounds to one**, and that one has an **8-percentage-point margin**. Concretely, a future ningbo regression producing ~75% stubs at a median of ~45 would now pass **both** rules and go undetected.

That is the accepted cost of not false-firing on a source whose natural median is 74, and it was the trade this reviewer recommended — but the residual belongs on the record rather than in a code comment alone. If ningbo capture quality is ever in doubt, the check is a manual median comparison against its 74-character historical baseline, not the gate.

### Decision 2 pilot — conforms to every board constraint

Checked item by item against the STATUS BOARD:

| requirement | status |
|---|---|
| Pilot 20-50 images, no full batch | **35 images** in `data/derived/pilot_image_set.jsonl` |
| Existing NIM/Ollama client, no new vendor | Same env names as `process_knowledge.py` (`OLLAMA_BASE_URL/API_KEY/MODEL`, `NIM_API_KEY/MODEL/BASE_URL`), plus `OPENROUTER_*` and `GROQ_*` — **all four CI secrets, no new vendor or key** |
| Two-stage, axis-first, stage 1 fails closed | `STAGE1_PROMPT` declares structure/scale; `STAGE2_PROMPT` opens *"The declared scale is:"*, consuming stage 1's output; `verify_stage1` gates before stage 2 runs |
| Extractor and verifier separate | Distinct `verify_stage1` / `verify_stage2` functions; verification never re-calls the model |
| Redo loop logged | `STAGE1_REDO_PROMPT` / `STAGE2_REDO_PROMPT`, with per-attempt JSONL in `audit.jsonl` |
| Additive only | *"never writes to `knowledge/`"*; output is a reconcile **diff** |
| Rate limiting / retry | `_gate()`, `_backoff()`, `MAX_RETRIES`, retry-after parsing |

Two touches worth naming:

**The verifier is self-tested against a planted error.** `MockVenue` returns a stage-2 answer containing `34.438` — the exact Vale mis-separated value from finding S1 — then a corrected one. The dry-run audit shows the loop working end to end:

```
stage1 attempt 0  ok=true
stage2 attempt 0  ok=false  ["separator_mix suspect: dot-thousands and comma-thousands/decimal mix"]
stage2 attempt 1  ok=true
```

final value `34,438`. A harness that proves it catches the specific error class the project exists to fix is materially better evidence than one that merely runs.

**It closes P-a by policy.** Every reconcile diff ends: *"proposal only — apply via pipeline recompile, never hand-edit shards."* That is the hand-edit from the previous verdict turned into a standing rule.

### D2-a — the cohort mix inverts the board's priority order

The board ordered the pilot set: 86 OCR-attempted-but-empty **first**, then the 228 separator suspects, then the 347 skipped-small. The committed set is:

| cohort | count |
|---|---|
| `small_skip` | **18** |
| `empty_ocr` | 12 |
| `empty_ocr_large` | 2 |
| `separator_suspect` | 3 |

The largest allocation goes to the **lowest**-priority cohort. There is a real argument for it — the 347-small group is where V3 graded 16 of 20 "cannot confirm without vision", so it carries the most uncertainty per image, and a pilot's job is to learn rather than to harvest. But the board stated an order, this inverts it, and the deviation is not argued anywhere in the commit.

Not blocking: 14 of 35 (40%) still go to the top-priority cohort, so it is under-weighted rather than ignored. Either re-balance toward `empty_ocr`, or state the learning-value rationale so the deviation is a decision rather than a drift.

### D2-b — the limitation that matters most

**Nothing here has been run against a real vision model.** The proof is a dry-run against `MockVenue` with canned responses. That validates the harness — control flow, staging, verification, redo, audit, diff generation — and validates nothing about extraction accuracy, because no image has been read.

That is the correct place to stop given the sandbox has no keys, and the branch does not overclaim. But it means the pilot's actual finding is still ahead of it: **the 35-image run has to happen in CI**, where the credentials live, before any conclusion about re-OCR quality, cost, or batch readiness is available. The current state is "harness ready", not "pilot complete".

---

## 2026-09-06 22:15 UTC — `agent/muse-spark` @ `2d494bf23` — **PASS. Decision 1 is complete; the gate reads 0/17.**

**Reviewed:** Decision 1.3 — HubSpot-gate-aware Poten capture, 29 refetched opinions, scoped recompile.

### Gate clean, thresholds untouched

Reviewer re-ran its own gate implementation against the branch's chunks:

```
AFTER 2d494bf23   groups=17   FAILURES=0
```

`poten/tankers`: tail median **504**, boilerplate markers **0/50**, newest chunk real analysis ("Venezuela's production and exports are recovering…"). `git diff baade2609..2d494bf23 -- scripts/validate_knowledge.py` is again **zero lines** — three consecutive commits have cleared the gate by fixing data, never by moving a constant.

### The deletion is justified — checked before objecting

The diff removes three files and drops the ledger 8,850 → 8,849, which on an additive-only constraint demands an explanation. Reviewer read the deleted file before judging it. `reports/poten/2026/poten_2026-08-24_will_he_or_won_t_he.md` was 787 lines of **site navigation** — "About Us / What We Do / Services / LNG Market Outlook", the company blurb, and 2024 training-course listings — with frontmatter falsely claiming `completeness: "full_text"`. Zero article content.

The branch's own account holds up on every point I could check:

- The `full_text` label came from a `len >= 400` heuristic firing on ~13 KB of harvested chrome. **It was the only document in `reports/poten/` marked `full_text`;** the other 29 were honestly marked `metadata`. The liar was the anomaly, not the norm.
- The **date was a crawl artifact**. `2026-08-24` was when the crawler ran, not a publication date; the real piece per the live listing is 27 Feb 2026 by Raza Zoya. So there was never a 2026-08-24 Poten opinion to preserve.
- The URL is dead (soft-404 via canonical/og:url homepage match), the listing dek is ~110 chars (below their 400 floor), and there are zero archive snapshots.

Deleting a phantom record is not a loss of corpus. A tombstone would normally be my ask, but you do not tombstone a document that never existed on that date; the inventory entry carries the explanation, which is the right place for it.

### What makes this the strongest commit of the three

- **The 29 survivors got honest labels, not just content.** `completeness: "metadata"` became `"standfirst"`, and the body disclaimer became: *"Public summary layer (title, author, date, standfirst). The complete analysis sits behind a registration form on poten.com; only the openly published summary is archived here."* That names the gate, claims only what it has, and recovered author attribution as a bonus. Compare the frontmatter it replaced, which asserted full text over a nav dump.
- **The root cause was found, not the symptom.** Two defects: a whole-page soup fallback that harvested chrome, and a v1 disclaimer string sitting in every metadata doc's *body*, which compiled into `_001` chunks and produced exactly the 20/50 boilerplate tail the gate caught. The fix removes the fallback and moves extraction to `div.entry-content` → `<article>` → quarantine.
- **A preventive control was added unprompted:** a quarantine gate before *any* write — soft-404 detection, standfirst-date identity, length floor. That is the same class of fix as the E1 scraper defect (assets written under `.pdf` with no content-type validation) and closes it for this scraper.
- **Out-of-scope side writes were reverted** — 8 Alibra/iron-ore/scrappage re-emissions caught and backed out. That is the discipline the additive constraint actually requires, applied without being asked.

### Two notes, neither blocking

**P-a — one shard was hand-edited.** An orphaned 11-character fragment (`..._02-13_..._003`, "the article") survived recompile because pipeline compaction only evicts on `doc_id` change, and was removed by a single-line shard edit. The reasoning is sound and the result reconciles 58/58 against the ledger, but it is the first hand-edit of a shard in this project. Worth a compaction fix later so the tool can do it; not worth blocking on.

**P-b — Poten is now missing its newest post.** "A New Headache For OPEC" (4 Sep 2026) was observed on listing page 1 and deliberately **not** ingested, correctly scoped out as a new `doc_id`. Right call for this task; it means the source is current only to 2026-08-21 until a normal ingest runs.

### Decision 1 status: DONE

1.1 validator gate · 1.2 Baltic capture · 1.3 Poten capture — all landed, all verified, gate at **0/17**, thresholds never touched, no data lost. The active weekly data loss identified in G1 has stopped. C1 is fully resolved: a merge of this branch no longer skips the commit step, so there is no corpus pause.

---

## 2026-09-06 21:24 UTC — **Ningbo watch item resolved — and it inverts into a false-positive risk**

The prior entry flagged `baltic/ningbo` recovering to a median of only 216 as a possible partial capture. Reviewer measured it against ningbo's own history rather than against its siblings, which is the comparison that actually settles it:

| series | n | median | p25 | p75 | max |
|---|---|---|---|---|---|
| `baltic/ningbo` **historical** (all years) | 961 | **74** | 74 | 358 | 358 |
| `baltic/ningbo` **2026 recovered** | 138 | **216** | 74 | 358 | 505 |
| `baltic/dry` historical | 2,152 | 995 | 917 | 1,169 | 2,797 |
| `baltic/dry` 2026 recovered | 256 | 1,064 | 952 | 1,219 | 1,574 |

**Not a partial capture — the opposite.** Ningbo's long-run median is **74 characters**, so the recovered 2026 data at 216 is *richer* than its own historical norm, and its maximum rose from 358 to 505. The NCFI is a containerised freight index that genuinely publishes terse notes; a representative recovered chunk is:

> `Surcharges not included in the total ocean freight reported:\nNCFI_overview`

That is 74 characters and is a complete, legitimate record. Dry behaves the same way — recovered 1,064 against a historical 995 — so both groups recovered to slightly above their own baselines. The watch item is closed.

### CG1 — but this surfaces a real false-positive risk in the gate

`baltic/ningbo`'s natural historical median (**74**) sits **below the gate's 120 floor**. It passes today only because the trailing-50 window currently lands on the richer 2026 captures. If the NCFI reverts to its terser historical format — which is its normal behaviour across 961 chunks of history — **the gate will fire on healthy data**.

This is the inverse of the failure the gate was built to catch, and it matters more than it sounds: a gate that cries wolf on a legitimately terse source is a gate someone eventually silences by lowering the global floor, which would re-open G1 for every other source. The floor must not become the release valve for one source's natural brevity.

Two clean options, neither urgent:

1. **A per-source floor override** for `baltic/ningbo` (~40 would clear its historical p25 of 74 with margin), leaving the global 120 intact for everyone else.
2. **Leave it and document it** — record in the `CONTENT_GATE_*` comment block that ningbo is expected to hover near the floor, so a future firing there is triaged as "check whether NCFI reverted to short-form" rather than treated as a threshold problem.

Reviewer prefers (1): it is explicit, it survives staff turnover better than a comment, and it keeps the global floor untouchable. Either way, the standing rule holds — **if ningbo fires, the fix is a per-source override or a capture check, never a change to `CONTENT_GATE_MEDIAN_FLOOR`.**

Not authorized here; recorded for whoever picks up Decision 1.3.

---

## 2026-09-06 21:00 UTC — `agent/muse-spark` @ `baade2609` — **PASS**. G1 is fixed; C1 resolved by option 1.

**Reviewed:** Decision 1.2 — static-first Baltic capture past the consent DOM, 325 refetched 2026 documents, scoped recompile. Writes under `knowledge/`, which the recompile authorization covers.

### The gate went green, and the green was earned

Reviewer re-ran its own independent implementation of the gate against the branch's chunk files:

```
BEFORE 03999a973   groups=17  FAILURES=6   (5x baltic + poten)
AFTER  baade2609   groups=17  FAILURES=1   (poten/tankers only)
```

`poten/tankers` is Decision 1.3 and correctly still fires on the boilerplate rule. Every Baltic group passes.

**The thresholds were not touched.** `git diff 03999a973..baade2609 -- scripts/validate_knowledge.py` returns **zero changed lines**. No `CONTENT_GATE_*` constant moved. The gate was satisfied by fixing the data, which is the only acceptable way to clear it.

### The recovered content is real

| group | 2026 chunks | median chars |
|---|---|---|
| `baltic/dry` | 51 → **256** | 33 → **1,064** |
| `baltic/tanker` | 51 → **260** | 35 → **1,131** |
| `baltic/gas` | 51 → **128** | 32 → **918** |
| `baltic/container` | 51 → **64** | 38 → **971** |
| `baltic/ningbo` | 54 → **138** | 46 → **216** |

The recovered medians land right on the pre-2026 baseline (`baltic_dry` historical median 995), which is the strongest available signal that this is the same kind of document as before rather than padding. The newest chunk reads as genuine market commentary:

> "The market opened September on a steady-to-firmer note, with sentiment largely positional across both basins. Across the Continent and Mediterranean, activity remained limited, although rates began to show signs of firmness towards the end of the week…"

Nine months of Baltic Exchange weeklies are retrievable again.

### Nothing was destroyed — checked specifically

The diff shows 13-15 line *deletions* from each **pre-2026** Baltic chunk file, which looks alarming on a fix that should only add. It is not data loss: every removed line is a **2026-dated chunk that was filed in the wrong file** (`baltic_dry_2026-01-09_...` living in `baltic_dry.jsonl`), relocated to its correct `_2026.jsonl`. A filing correction, not a deletion.

Aggregate movement:

- Baltic chunks **6,177 → 6,698 (+521)**; no category lost, every one grew.
- `knowledge/derived/section_index.jsonl` 31,228 → 31,743 (+515), tracking the new chunks.
- `knowledge/derived/themes.jsonl` **8,850 → 8,850** and `topic_evidence.jsonl` **2,500 → 2,500** — unchanged.
- Document count unchanged at 8,850, confirming this is a recompile of existing documents against better source captures, not an ingest of new ones. Additive and reconciling, as the constraint requires.

### C1 is resolved — by the best of the three options

The open question was whether landing the gate before the capture fix would halt all knowledge commits. It no longer arises for Baltic: the fix arrived with the gate still passing on all other sources, so **there is no corpus pause**. That is option 1 from the C1 entry, taken without being told to.

One caveat: `poten/tankers` still fires, so a merge of this branch today would still skip the commit step until Decision 1.3 lands. The exposure is now one source and one rule rather than six groups, but C1's underlying question stays live until Poten is fixed or the commit step is made unconditional.

### Watch item

`baltic/ningbo` recovered to a median of **216** — genuine content, comfortably above the 120 floor, but the thinnest passing group by a wide margin (the others sit at 918-1,131) and only 1.8x the floor. Ningbo is a route index and may legitimately publish shorter notes; worth one confirmation that 216 is its natural length rather than a partial capture. If it is partial, the gate will not catch it.

---

## 2026-09-06 19:46 UTC — `agent/muse-spark` @ `03999a973` — **PASS**, with one sequencing consequence for the user

**Reviewed:** `scripts/validate_knowledge.py` +129 lines — `validate_chunk_content()`, the Decision 1.1 content gate. First code change to a live pipeline file on any branch.

### Reviewer re-implemented the gate independently and ran it

Rather than trust the branch's own numbers, the reviewer re-implemented the stated algorithm with its exact constants (`WINDOW=50`, `MIN_SAMPLES=10`, `STUB_CHARS=120`, `MEDIAN_FLOOR=120`, `STUB_RATE=0.80`, boilerplate markers at `0.30`) and ran it over `knowledge/chunks/*.jsonl`. **17 groups checked, 6 fire:**

```
baltic/container  median=38  stub=50/50 (100%)
baltic/dry        median=33  stub=50/50 (100%)
baltic/gas        median=32  stub=50/50 (100%)
baltic/ningbo     median=46  stub=44/50  (88%)
baltic/tanker     median=35  stub=50/50 (100%)
poten/tankers     median=860 stub=0/50   — boilerplate 20/50 (40%)
```

It catches exactly the two G1 targets and nothing else. Margins are wide in both directions: the weakest *passing* group is `hellenic/dry_charter` at median 346 (2.9x the 120 floor) and a 50% stub-rate (30pp under the 0.80 threshold), while `baltic/ningbo` — the weakest *firing* group at 88% stub-rate — fires on median regardless, so it trips on two independent grounds. No group sits near a knife edge.

**The boilerplate rule earns its place.** `poten/tankers` has a median of 860 and a stub-rate of **zero** — both length gates miss it completely. Only the marker rule catches it. A gate built solely on length would have declared the JS-render miss healthy. That is exactly the second failure mode the G1 finding needed covered, and it was covered without being asked.

### Wiring verified — the gate really does fail, not warn

Checked, because a gate that returns 1 into a swallowed exit code is a warning wearing a gate's clothes:

- `validate_chunk_content` failures are summed into `failures`, which drives `return 1`.
- Both workflows invoke it through a pipe to `tee` — which would normally discard the Python exit code — **but both declare `shell: bash` and `set -o pipefail`** (`daily_knowledge_update.yml:103-106`, `process_knowledge.yml:141-147`). The exit code propagates and the job fails. Decision 1.1's "fails, not warns" is satisfied end to end.

### C1 — consequence the user should decide on: this halts all knowledge commits, not just Baltic's

In both workflows, `Commit if changes` / `Commit knowledge artifacts` runs **after** the validate step, and neither carries `if: always()` or a status function — so an `if:` without one implicitly requires `success()`. When the gate fires, **the commit step is skipped entirely**.

The practical effect of merging this before the capture fixes: the daily knowledge pipeline goes red and **stops committing anything at all** — including the 11 healthy groups (breakwave, hellenic, broker_reports and the rest) — until Baltic and Poten are repaired. The corpus stops updating, not just the broken part of it.

This is not a defect in the change. It is the honest behaviour of a real gate, and Decision 1 explicitly ordered the gate first. But it converts "gate first" into "the corpus pauses until the captures are fixed," which is a bigger commitment than the sequence implied, so it belongs in front of the user rather than discovered on tomorrow's red run.

Three ways out, reviewer's preference first:

1. **Land the Baltic capture fix in the same change** so the gate goes green on arrival. Poten can follow — at 40% it fires only on the boilerplate rule and is one source.
2. Accept the pause deliberately, with the red run understood as correct and the fixes prioritized behind it.
3. Keep the gate failing the job while letting the commit proceed (`if: always()` on commit). This preserves daily updates and keeps the alarm visible — but it publishes known-bad shards, which is close to the status quo and should only be chosen knowingly.

**Whichever is chosen, the thresholds must not be loosened to clear the red.** That is the one response that would return the project to where it started, and any future change to `CONTENT_GATE_*` should be treated as a finding in its own right.

### Minor notes, non-blocking

- **N-a — four groups are exempt by size.** `MIN_SAMPLES=10` skips `baltic/baltic` (3 chunks), `hellenic/hellenic` (2), `breakwave/breakwave` (1), `clarksons/shipbuilding` (1). All are stray-category artifacts rather than real sources today, so the exemption is harmless — but a genuinely small source could go dark unseen. Worth a one-line note in the constant's comment.
- **N-b — the boilerplate markers are OR'd via `any()`**, so a legitimate document that happens to contain the phrase "Metadata only" counts toward the rate. Zero corpus-wide false hits today at a 30pp margin; the risk is only that a future source discusses metadata in prose. Requiring two of the three markers would remove it.
- The calibration comment block in the source is unusually good practice: it records the measured medians and stub-rates behind every constant, so the next reader can tell whether a threshold was reasoned or guessed.

---

## 2026-09-06 18:38 UTC — **G1: Baltic 2026 is 100% empty stubs and the pipeline reports it healthy**

Surfaced as a "cross-cutting quality note" in `agent/muse-spark`'s 20Q pilot. It is not a note. It is live, ongoing, silent data loss on the most important price source in the corpus, and it outranks every other item in this log.

Reviewer measured it directly:

| chunk file | stubs (<120 chars) / total |
|---|---|
| `baltic_dry_2026.jsonl` | **51 / 51** |
| `baltic_tanker_2026.jsonl` | **51 / 51** |
| `baltic_gas_2026.jsonl` | **51 / 51** |
| `baltic_container_2026.jsonl` | **51 / 51** |
| `baltic_ningbo_2026.jsonl` | **54 / 54** |
| **total** | **258 / 258 (100.0%)** |

Median chunk length is **33-38 characters**. A representative chunk in full:

```
17 Apr 2026
Bulk report - Week 16
```

That is the entire retrievable content of a Baltic Exchange weekly report. For comparison, pre-2026 `baltic_dry.jsonl` has 2,152 chunks at a median of **995** characters. The cause is the cookie-wall capture noted in the muse-spark inventory §7d — the scraper archives the consent page, and the compiler faithfully turns it into a titled, dated, empty document.

**And every one of them is reported healthy.** `coverage_report.json`:

```
baltic/dry        status=healthy  latest=2026-10-04  docs=598
baltic/tanker     status=healthy  latest=2026-10-04  docs=596
baltic/gas        status=healthy  latest=2026-10-04  docs=217
baltic/container  status=healthy  latest=2026-10-04  docs=98
baltic/ningbo     status=healthy  latest=2026-08-28  docs=510
```

The health machinery grades **cadence and recency, never content**. `validate_knowledge.py` carries emptiness checks for `section_index`, `topic_config`, `topic_evidence`, wiki pages and the health report — and **none for chunk text**. A source can go completely dark and stay green indefinitely, which is exactly what has happened for all of 2026.

**Why this outranks the rest of the log.** Every other finding here concerns historical material that is already captured: OCR quality on ingested images, a skipped set that turned out to be outbound links, 89 failed downloads worth nothing. G1 is different — it is *losing new data every week*, on the Baltic Exchange, while displaying green. A re-OCR batch improves the past; this stops the bleeding in the present.

**Cheapest correct fix, in order:**

1. Add a content-length assertion to `validate_knowledge.py` — flag any source whose recent chunks fall below a floor, and fail the health report rather than pass it. Without this, any future capture regression repeats silently.
2. Fix the Baltic capture to get past the consent wall, then recompile the 2026 documents. The `source_hash` + `compiler_version` ledger will pick them up on content change without a rebuild.

Neither is authorized here; both are small and this reviewer would sequence them ahead of any extraction or graph work.

---

## 2026-09-06 18:38 UTC — `agent/muse-spark` @ `0200b450e` — **PASS**

**Reviewed:** three commits — `7ee0e6f6d` (E1/E2 evidence flags, ZIP candidate, fetch-validation spec), `b4bf31cb3` (`docs/PILOT_20Q_MUSE_SPARK.md`, 367 lines), `0200b450e` (W1-W4 records) — plus two clean syncs of `main`.

**Additive constraint verified.** Reviewer checked each of the three muse-spark commits for writes under `knowledge/`: **zero files in all three**. The `knowledge/` churn in the range diff comes entirely from the `main` sync commits carrying the automated daily update. The no-overwrite constraint holds.

### The pilot answers the mission's own architectural test

| verdict | count |
|---|---|
| multi-hop | **14 (70%)** |
| blocked-unwired | 4 (20%) |
| single-hop | 1 (5%) |
| blocked-ocr-quality | 1 (5%) |

The mission brief's rule was: "If most need multi-hop reasoning across sources, prioritize the graph layer; if not, a lighter setup may be enough." At 70% multi-hop against 5% single-hop, **the graph layer is justified on the project's own stated criterion** — and it is justified by measurement rather than assertion, which is the first time that question has been answered with evidence on either branch.

The recommendation — layer-over-trees with cross-node edges, consuming existing `node_id`/`doc_id`, never replacing shards — is correct and consistent with the binding constraint. The reasoning is right too: the load-bearing join is cross-node entity plus week resolution (the Q14 DEVBULK SINEM $14.8m/$15.0m hull match, Q19's four-hop chain), which is edges over existing nodes, not a re-chunking.

Two things it gets right that are easy to get wrong:

- **It refuses to pick a vendor.** LightRAG vs GraphRAG vs Neo4j vs Graphiti stays a user call. Grading retrieval need is not the same as selecting a tool, and the pilot does not conflate them.
- **It states that the 5 blocked questions are not graph-fixable.** Four need source wiring (CFTC COT, 10-Q/EDGAR, SGX/CapitalLink manifest wiring, an SNP catalog) and one needs extraction repair. "Do not let the layer mask the blocks" is the right instruction — a graph over missing legs answers nothing.

### Secondary findings in the pilot, worth acting on

- **Poten's latest capture (2026-08-24) is navigation boilerplate only** — a JS-render miss. The source is 30 documents and currently contributes nothing current. Same defect class as G1: captured, dated, empty.
- **Current-week hellenic valuations arrive as unreadable image OCR** (`..._010920264ffg_3f4fc98878b7_jpg`), so this week's vessel valuations are not retrievable at all. Clean fallbacks exist via PDF-text legs and the matrix CSVs.
- `usda_grain_freight_spreads.csv` empty; Baltic indices CSVs stale to 2026-08-10, roughly four weeks.

### Standing

W1-W4 are recorded accurately, including the vision reframing, and the pilot correctly cites it as "project path open, NOT a batch authorization." Every document on this branch has now held that line unprompted across six consecutive commits.

---

## 2026-09-06 18:14 UTC — `agent/muse-spark` @ `26d70151c` — **PASS**

**Reviewed:** V1/V2/V4 corrections and the V3 small-image appendix in `docs/INGESTED_IMAGE_AUDIT_MUSE_SPARK.md`.

**V3 answered honestly, and the answer is "assumption not confirmed."** Of 20 sampled sub-90,000-pixel images: **4 confirm-no-data** (three byte-identical Vale logos, one author portrait) and **16 cannot-confirm-without-vision**. Six are wide-strip graphics with names like `tableseason`, `settlements`, `2hsettle` and the `klav*` series, sitting in parent documents that discuss settlement prices and futures curves. The earlier "logos/icons" characterization does not survive contact with the sample. Reporting 16 unresolvable rather than grading them from filenames is the right call.

The population is also corrected: the 451 no-legacy-header images decompose into **347** truly skipped-small, **86** OCR-attempted-but-empty, **6** `[OCR unavailable; install pytesseract]`, and **12** new-format nodes that do carry content. True no-OCR is **439**.

**V4 correction accepted.** This reviewer's "89 pdf + 8 link" came from the superseded pre-D2 null-node accounting. Current is **89 `pdf` + 2 `link`**; seven of the eight were CNBC-linked HTML text assets re-matched as ingested. The branch is right and the reviewer's figure was stale.

### W1 — vision is NOT blocked at the project level. The credentials already exist.

Both branches have reported vision BLOCKED on the basis of an empty local environment. That is true of their sandboxes and false of the project. Reviewer checked the workflows and the pipeline source:

- **CI secrets already configured:** `NIM_API_KEY` (4 references), `OLLAMA_API_KEY` / `OLLAMA_BASE_URL` / `OLLAMA_MODEL` (3 each), `OPENROUTER_API_KEY`, `GROQ_API_KEY`.
- **`process_knowledge.py` already consumes them.** Lines 28-36 read the Ollama and NIM configuration; `ollama_available()` and `nim_available()` gate at 1500 and 1504; a complete client with rate limiting, retries and exponential backoff runs from 1514 to 1611. `daily_knowledge_update.yml:67,73` and `process_knowledge.yml:91,97` pass the secrets into the very script that performs ingestion.

So the missing piece is **a multimodal call path — not a key, a vendor, or infrastructure.** The existing client posts text-only chat payloads with no `image_url` or multipart content handling. NIM and OpenRouter both serve vision-capable models against exactly this interface.

This reframes the unblock request. It is not "provision a Reducto or LlamaCloud key." It is: extend the existing NIM/Ollama client to a vision model and run the pass in CI, where the credentials already live and the rate limiting is already written. That is a much smaller ask, and it may need nothing from the user beyond approval to spend on the existing accounts.

Neither branch should read this as authorization to run a batch. It changes what to ask for, not whether to ask.

### W2 — the 86 empty-OCR images are better vision candidates than the 347 small ones

`[No OCR text detected in linked image.]` means OCR ran and returned nothing. The branch notes some are large — two at 2500x1667, others at 656x330 and 481x289. A large image yielding zero text is a stronger signal of a chart whose labels defeated Tesseract than a 120x100 icon is. Prioritize these 86 above the 347.

### W3 — the 6 `[OCR unavailable; install pytesseract]` nodes are not a live CI defect

Checked before flagging. `pytesseract>=0.3.10` is in `requirements_knowledge.txt`, and both `daily_knowledge_update.yml:33` and `process_knowledge.yml:56` run `apt-get install -y tesseract-ocr tesseract-ocr-eng poppler-utils`. Production CI has OCR. Those six nodes came from an environment without it, not from the scheduled pipeline. Recorded so it is not chased.

### W4 (minor) — dead dependency

`google-generativeai>=0.5.0` sits in `requirements_knowledge.txt` with no importer anywhere under `scripts/`. Harmless, but it misleads anyone surveying which model providers this project actually uses — which is how the vision-key question got framed wrongly in the first place.

### PR #40 mergeability

`origin/main` advanced twice (`6ba037f20` → `1e6762ed1` → `481c224bc`; a daily knowledge update and an alibra poll). GitHub reported `mergeable_state: unknown` mid-recompute. Reviewer ran a local test merge of `origin/main` into this branch: **"Automatic merge went well"**, no conflicts, test merge aborted. The reviewer branch touches only `docs/REVIEW_BASELINE.md` and `docs/VERIFICATION_LOG.md`, which the automated commits never touch.

---

## 2026-09-06 17:50 UTC — `agent/muse-spark` @ `71d576761` — **PASS** (D1-D4 all closed)

**Reviewed:** `asset_dispositions.jsonl` regenerated, `skip_cause_matrix.json` derived from it, `split_skip_causes.py` reworked with a three-way ledger gate, Sample C re-pointed.

**D1 closed, verified exactly.** Reviewer re-tallied the file against the ledger:

| | dispositions | ledger | delta |
|---|---|---|---|
| ingested | 13,591 | 13,591 | **0** |
| skipped | 8,424 | 8,424 | **0** |
| failed | 91 | 91 | **0** |

22,106 records total. **Zero** `ingested` records with a null `node_id` — the invariant is gate-enforced, not merely asserted.

**D2 closed.** The previously unexplained records resolve into a reasoned `failed` breakdown: `PDFSyntaxError` 86, `unknown_extraction_failure` 4, `unresolvable_relative_ref` 1 = 91.

**D3 closed.** The matrix is now derived from the dispositions file; `reconciled_with_ledger: true`, `mismatched_docs: []`.

**D4 closed well.** Sample C is re-pointed at three ingested images with suspect OCR (audit S1-S3) plus two failed PDF-class assets, with disposition evidence cited per row and the superseded rows struck rather than deleted.

### E1 — this falsifies part of the reviewer's own D1 framing

In D1 this reviewer called the failed assets "the most interesting assets in the corpus… the one pocket of on-disk material genuinely missing from the graph." **That was wrong**, and muse-spark's F1/F2 diagnosis is what exposed it: the `.pdf` files are HTML error pages saved under PDF names.

Reviewer checked all 91 independently by reading magic bytes from every mirror on disk:

- **90** of 91 have a mirror present.
- **89** begin `<!DOCTYPE html>` / `<html` — bot-challenge and error pages, not documents.
- **1** begins `PK\x03\x04` — a ZIP container, so an Office file (`.docx`/`.xlsx`) misnamed `.pdf`.
- 0 are real PDFs.

**Recovery value of the entire failed set is one file.** The 86 `PDFSyntaxError` cases are failed downloads carrying no data. What they do reveal is a **scraper defect worth fixing**: linked assets are fetched and written under a `.pdf` extension with no content-type or magic-byte validation, so a challenge page is silently archived as a document and only surfaces 4,000 documents later as a parse exception. Validating the fetch is cheap and prevents recurrence; recovering the existing 89 is pointless.

### E2 — `errors.jsonl` is not a complete failure record

`knowledge/manifests/errors.jsonl` holds **83** entries against **91** ledgered failures — the pipeline under-logs by 8. Muse-spark's method attributes an exception class from the parent document's error entry and falls back to `unknown_extraction_failure` otherwise, which is why it reports 86 `PDFSyntaxError` where the log itself carries 79. Sound approach, but the attribution is inferential for the surplus, and it should say so. The pre-existing gap belongs to `process_knowledge.py`, not to this branch.

### E3 — the strategic finding: there is no coverage backlog

Worth stating now that the measurements have converged. Every large pocket of "unprocessed" material named in the mission brief or by this reviewer has evaporated under measurement:

- 35,957 PDFs → **4,475**.
- 8,424 skipped assets → **99.0% outbound links to third-party journalism**, zero recoverable charts.
- 91 failed assets → **89 error pages, 1 real file**.
- 19,801 "unused" shards → an actively maintained pipeline.

What remains is not a coverage problem but a **quality** one: 10,894 ingested images carrying raw OCR, 228 measured mixed-separator suspects, undetected row-scrambling beyond that, and 451 images never OCR'd at all. The project's first batch should be a re-OCR and structuring pass over material already in the graph — not an extraction campaign against a backlog that does not exist.

That is a materially different project from the mission brief's framing, and it is the user's call to confirm.

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
