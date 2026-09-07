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

# STATUS BOARD

**Last updated: 2026-09-06 23:35 UTC.** Read this before starting work. These are
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

Vision path per W1: extend the **existing** NIM/Ollama client in
`process_knowledge.py` (lines 28-36, 1500-1611 — rate limiting, retries and
backoff already written) to a multimodal call, and run in CI where
`NIM_API_KEY` / `OLLAMA_API_KEY` / `OPENROUTER_API_KEY` / `GROQ_API_KEY` already
live. No new vendor or key needed. Extractor and verifier stay separate passes,
with the redo loop logged per file/page/table.

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
| `agent/muse-spark` | `e705ac894` | 2026-09-06 23:32 UTC | **PASS**. Decision 1 COMPLETE and now merged to `main`. CG1 override landed (global floor untouchable). Decision 2 pilot harness built, 35 images, dry-run proven — **needs a CI run against a real vision model to produce any actual finding**. |
| `agent/antigravity` | `e2b0db0a4` | 2026-09-07 05:15 UTC | **SUBMITTED FOR REVIEW**. B1–B9 addressed; Decision 2 re-OCR pilot executed on 35 images (rebalanced cohort: 20 empty_ocr, 10 separator, 5 small_skip); fail-closed axis gating + planted separator redo verified; vision-client tests passing; report: `docs/PILOT_VISION_ANTIGRAVITY.md` |

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

*(Update 2026-09-07 05:15 UTC): Branch updated and pushed at `e2b0db0a4`. B1–B9 and D2-a/D2-b addressed: `p0_skipped_assets_queue.jsonl` deleted; 5 checked-in test fixtures added in `data/fixtures/sample_broker_tables.json`; all paths normalized to POSIX `/`; `collapsed_table` check enforced; Decision 2 re-OCR pilot executed across 35 images (rebalanced cohort: 20 empty-OCR first, 10 separator suspects second, 5 small-skips third); fail-closed axis gating + planted separator redo verified; `tests/test_vision_client.py` passing 3/3; see `docs/PILOT_VISION_ANTIGRAVITY.md`.*

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
