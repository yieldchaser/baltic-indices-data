# Re-OCR / structuring pilot — muse-spark (Decision 2, PHASE A)

- Author: muse-spark
- Date: 2026-09-07
- Branch: `agent/muse-spark`, worktree `C:\Users\Dell\Github\shipping-muse-spark`, **no commits**
- Scope: venue assessment + pilot harness + set selection. **No live inference ran
  (paddle deps not installed here; install happens at run time). No writes to `knowledge/`.**
- Inputs: `docs/INGESTED_IMAGE_AUDIT_MUSE_SPARK.md` (§5/§6/§10/§V3),
  `data/derived/asset_dispositions.jsonl`, reviewer bench `f8bf3ac27`
  (GT1 Vale 31,438 tie-out proof; GT2 35→26 dedupe + logos-zero-data;
  GT3 paddle CPU bench), user directive 2026-09-07 (supreme: NO hosted venues).

## 1. Venue pivot 2026-09-07: hosted stripped, paddle lane only

Per user directive 2026-09-07 (supreme, NO hosted venues — no
NIM/Ollama/OpenRouter/Groq/paid API):

- STRIPPED from `scripts/pilot/reocr_pilot.py`: all `NIM_API_KEY` /
  `OLLAMA_*` / `OPENROUTER_*` / `GROQ_*` env reads, `image_url`/chat
  payload builders, venue auto/probe/selection, rate-limit/retry/backoff,
  spend guardrails. DELETED `scripts/pilot/ci_support.py` (preflight probe
  + spend gate + token accounting — nothing local left), DELETED
  `.github/workflows/reocr_pilot.yml`, DELETED untracked leftover
  `data/derived/pilot_reocr_out/choice.json` (cancelled-task probe output).
- WHY hosted is dead: `OLLAMA_MODEL` retired upstream HTTP 410 (CI
  2026-09-06), NIM EOL 410, pinned OpenRouter/Groq vision models 404 on
  probe; no paid API authorized. Recorded in the commit-message-ready
  note (§9) + inventory one-liner.
- KEPT skeleton intact: two-stage staging, extractor/verifier split, redo
  loop (`PILOT_REDO_LIMIT`=1), audit JSONL, reconcile-as-diffs (never
  writes to `knowledge/`).
- ONLY venues now: `--venue paddle` (live, local) + `mock`/`--dry-run`
  (fixture self-test, stdlib only, no deps).

Prior sandbox no-venue checks (names only, never values) stand as history:

| Check | Result |
|---|---|
| `NIM_API_KEY` | **absent** (name not in env) |
| `OLLAMA_API_KEY` | **absent** |
| `OPENROUTER_API_KEY` | **absent** |
| `GROQ_API_KEY` | **absent** |
| `OLLAMA_BASE_URL` | **absent/empty** — no config URL, so no TCP target; nothing sent |
| Local Ollama TCP (`127.0.0.1:11434`) | **refused** (`connect_ex` 10061) — no local daemon |
| `transformers` import | **not installed** (no package metadata; a stray `transformers` name shadows nothing usable) |
| pip index reachability | **reachable** (`pip download --no-deps packaging==24.1` succeeded) — a `transformers` install is *feasible* but heavy and was **not** attempted (read-only brief) |
| `PIL` / `requests` | present (PIL 12.1.1, requests 2.32.3) — harness deps satisfied |

**Verdict 2026-09-07:** hosted path dead (above); live pilot runs locally
via `--venue paddle` (§2). No live inference ran in THIS task (paddle deps
not installed; install happens at run time). The dry-run below proves the
full loop (extractor → verifier → redo → reconcile) with the tie-out
fixture mock.

## 2. Harness design: `scripts/pilot/reocr_pilot.py` (paddle lane)

Deps: **stdlib only for mock/dry-run** (argparse, json, re, sys, time,
pathlib, datetime). Live paddle needs at run time (NOT installed here):
`paddlepaddle==3.3.1` + `paddleocr==3.7.0` + `pymupdf`, ~1.4GB venv
(reviewer bench `f8bf3ac27`: CPU 4-core, init 3.6 s, 48.2 s/image, 8/8
values incl. 31,438 at conf 1.00, mean 0.998; 26 images ~21 min serial,
$0, nothing leaves the machine). Lazy imports so mock runs bare.

- **Paddle venue (`--venue paddle`, only live venue).**
  `enable_mkldnn=False` is MANDATORY on CPU — default oneDNN crashes
  (`ConvertPirAttribute2RuntimeAttribute not supported`), reviewer bench
  `f8bf3ac27`; passed explicitly at every `PaddleOCR(...)` construction.
  Images go straight to paddle OCR; PDFs use the text-layer-first router:
  PyMuPDF extracts native text per page, usable iff
  `>=PDF_TEXT_THRESHOLD_CHARS` chars/page (default **200** — above
  header/footer noise, below a real table page; defined in code + here);
  paddle OCR runs ONLY on pages without a usable text layer.
  Deterministic parsing maps OCR text → stage-1 declaration / stage-2
  values (no LLM prompts at run time); `<50` chars or `<3` lines →
  `axes_readable=false` fail-closed (logo/empty path).
- **Two-stage protocol (skeleton kept).**
  - Stage 1 — axis/scale/units *declaration* (chart_type, x/y label+scale+units+ticks,
    table rows/cols/headers, `axes_readable` bool). **Fail-closed:** if
    `axes_readable=false` the image stops there (`unreadable_axes_fail_closed`,
    no stage 2, diff still emitted).
  - Stage 2 — *values against the declared scale* (series points + table_cells +
    units + confidence). Units must match the stage-1 declaration; illegible
    values must be `"?"`, never invented digits.
- **Multimodal payload builders** (same env names the pipeline uses):
  - Ollama chat: `{"model","messages":[{"role":"user","content":prompt,"images":[b64]}]}`
    → `OLLAMA_BASE_URL/api/chat` (`/v1`→`/api` normalization mirrors
    `process_knowledge.py:30-35`; `Authorization: Bearer` only if key set).
  - OpenAI-compatible `image_url`: `content=[{type:text},{type:image_url,
    image_url:{url:data:{mime};base64,…}}]` → `{BASE}/chat/completions` for
    **nim** (`NIM_API_KEY`/`NIM_MODEL`/`NIM_BASE_URL`, `NVIDIA_API_KEY` alias as in
    `generate_brief.py`), **openrouter** (`OPENROUTER_*`), **groq** (`GROQ_*`).
- **Rate-limit + retry/backoff** (compact reimplementation of the
  `process_knowledge.py:1482-1496,1511-1596,1599-1686` behavior, not a copy):
  min-interval gate, `Retry-After` honor, exponential backoff on 429/rate-limit
  else linear, cap + 0.1–0.9 s jitter. Knobs via `PILOT_MIN_INTERVAL_SEC` /
  `PILOT_MAX_RETRIES` / `PILOT_BACKOFF_BASE_SEC` / `PILOT_MAX_BACKOFF_SEC`
  (defaults 1.5/3/1.5/15.0, matching the CI overrides in `process_knowledge.yml`).
- **Extractor/verifier separation:** paddle OCR (or mock fixture) is the
  extractor; deterministic code is the verifier (`verify_stage1` /
  `verify_stage2`): axis-values consistency (stage-2 x ∩ declared ticks),
  units match, row/column counts vs declared table dims,
  **separator-mix FLAG-ONLY** (`flag_separator_mix …`, audit-visible, never
  blocks — bench `f8bf3ac27` GT1: a digit substitution sails through a
  separator swap), **arithmetic tie-out as THE correction path** (components
  sum vs printed total/subtotal row where one exists — the 10-Q technique;
  `tieout_mismatch …` blocks), and cross-check against the existing shard
  OCR text → label **improved** vs **contradicted**.
- **Redo loop:** `REDO_LIMIT` (default 1) extra attempt per stage; every
  attempt appended to the JSONL audit
  (`ts, record, stage, event, attempt, ok, issues, verdict`).
- **Reconcile report:** per-record unified-diff-style proposal
  (`-OCR(existing)` vs `+STAGE1/+STAGE2`) written to `reconcile.diff`.
  **Never writes to `knowledge/`** — proposals apply via pipeline recompile only.
- **Image handling:** images read from repo-relative POSIX `image_rel`;
  paddle reads full resolution directly (no base64/downscale path remains).

Usage:

```bash
python scripts/pilot/reocr_pilot.py --dry-run --out data/derived/pilot_reocr_out
python scripts/pilot/reocr_pilot.py --set data/derived/pilot_image_set.jsonl \
  --venue paddle --max-images 26 --out data/derived/pilot_reocr_out
```

## 3. Pilot set: `data/derived/pilot_image_set.jsonl` (26 unique images)

Bounded set manifest (≤50, POSIX paths, `doc_id` + `node_id` + `image_rel` +
`shard_hint` + reason code + note). All 26 mirrors verified present on disk;
all 26 content hashes (sha256-16) distinct.

**Dedupe 35→26 (2026-09-07, reviewer bench `f8bf3ac27` GT2):** sha256 over
file bytes finds 3 dup families in the 35: BRS monogram v1
(`1280799b6361d81d`, 196,130 B) ×5, v2 (`a71e6d4f37fccf96`, 187,271 B) ×5,
crude-tanker photo (`2bc314176a913bd0`, 27,257 B) ×2 — 9 dup instances,
35−9=26 unique. GT2 viewed the pixels: the 2500×2186 large empties are the
BRS Shipbrokers wordmark (blue on white, zero data); empty-OCR usually
means an empty image. W2's "empty-first" priority is superseded: keep
exactly **2 logo controls** to exercise the empty-result path, drop the
rest, re-point freed slots at `separator_suspect` + real tables/charts
(board priority: `separator_suspect` first, `empty_ocr` only where real
tables/charts).

| Reason | n | Contents |
|---|---|---|
| `empty_ocr_large` | 2 | **both** 2500x1667 OCR-empty JPEGs (2023-08-26 `unnamed284329`, 2024-06-14 `image-asset28729`) — genuine content questions, kept |
| `empty_ocr` | 9 | arrowcoalprices graphic, greek-label png, 2× 2025 big-picture, 2× 2026 hormuz, crude-tanker photo rep (1 of 2, earliest kept) + **2 logo controls**: earliest v1 monogram (2023-07-03) + earliest v2 (2025-09-17), each tagged `LOGO-CONTROL`. Dropped: 2 BRS logos, 4 v1 dups, 4 v2 dups, 1 crude dup (11 drops total with the crude dup) |
| `separator_suspect` | 10 | **S1** Vale iron-ore table, **S2** Q3 capesize chart, **S3** dollar-index chart + **S4–S8** (as before) + **S9/S10** next-2 non-S rows by the same mixed-sep ranking (S9 2025-06-12 oil-surges dot14/com11 min11 tot25; S10 2025-06-19 energy-market dot12/com11 min11 tot23; ranking = mixed class first, min desc, total desc, ledger tiebreak; mirrors verified, hashes unique) |
| `small_skip` | 5 | unchanged (V3#13 tableseason + #2 + #8 + #9/#10 klav pair) |
| **Total** | **26** | 26 unique hashes; within the 20–50 band |

Selection rule (deterministic): content-hash dedupe first (keep earliest
ledger-order rep per hash); logo cap = 2 controls (1 per monogram byte
variant, earliest each); freed slots → separator ranking rerun over
`knowledge/chunks/breakwave_insights_insights.jsonl` (716 mixed candidates
excl. current set), top-2 appended. Every row carries parent `node_id`,
POSIX `image_rel`, `shard_hint`; all 26 mirrors verified on disk at write
time. No dry-run regen needed: dry-run (§4) uses the recorded fixture mock
and never reads the set.

## 4. Dry-run results (tie-out fixture mock, no deps)

```bash
python scripts/pilot/reocr_pilot.py --dry-run --out data/derived/pilot_reocr_out
→ {"mode": "dry-run(mock)", "status": "accepted", "verdict": "improved",
    "audit_events": 3, "mock_calls": ["stage1#0", "stage2#0", "stage2#1"],
    "tieout_proof": {"planted_34_438_rejected": true,
     "naive_34_438_rejected": true, "truth_31_438_accepted": true,
     "truth_proof": "31,438 + 19,291 = 50,729 Northern System 4Q19; 34,438 + 19,291 = 53,729 (fails)"},
    "selftest": "PASS"}
```

Vale GT1 fixture (reviewer bench `f8bf3ac27`; truth 31,438, NOT 34,438 —
legacy OCR made digit 1→4 + separator `,`→`.` errors; proof
31,438+19,291=50,729; naive 34,438+19,291=53,729 fails; 3Q19
35047+20354=55401, 4Q18 37023+15888=52911, 2019 115352+73369=188721 also tie):

1. `stage1#0` → clean declaration (3×5 Vale table) → `ok: true`.
2. `stage2#0` → **planted 34.438** → `ok: false`,
   `issues: ["flag_separator_mix … (flag only…)",
   "tieout_mismatch col=1 total=50729 components-sum=53729
   (34438+19291 != 50729)"]` — rejected by TIE-OUT, flag informational.
3. **Redo** `stage2#1` → truth 31,438 → `ok: true`, no issues →
   status `accepted`, verdict `improved`, diff emitted.
4. Direct proofs (same run): naive `FIXTURE_VALUES_NAIVE` 34,438 →
   `ok: false` (`tieout_mismatch … 34438+19291 != 50729`, no separator
   flag — the old verifier's confidently-wrong answer, now rejected);
   truth 31,438 → `ok: true`.

Separator-mix is flag-only by design; tie-out is the correction path
(10-Q technique). Spot-checks: `verify_stage1({"axes_readable": False})`
→ `axes_unreadable_fail_closed`; `--venue paddle` without paddleocr exits
2 with the dependency note (no pip installs in this task).
Outputs: `data/derived/pilot_reocr_out/{audit.jsonl,results.json,reconcile.diff}`.

## 5. Redo log summary

Dry-run audit (`audit.jsonl`, 3 events): stage1 extract+verify ok (attempt 0);
stage2 extract+verify rejected (attempt 0, `tieout_mismatch` + separator
flag); stage2 extract+verify accepted (attempt 1, tie-out holds). No live
redo log exists — paddle deps not installed here. Live-run audit will follow
the same per-image/per-stage schema (`paddle_ocr`/`pdf_router` load events
+ extract+verify per stage).

## 6. Reconcile diffs (expected form)

Dry-run `reconcile.diff` for the fixture:

```diff
--- a/knowledge/trees/FIXTURE.json
+++ b/proposed (FIXTURE__vale_table__s03)
@@ image FIXTURE/mock.png status=accepted
-OCR(existing): 'Northem andEastem = 34.438 35,047 legacy OCR; Northern System 50,729; S11D 19,291'
+STAGE1(declared): {"chart_type": "table", ... "units": "000' t", ...}
+STAGE2(values): {"values": [{"series": "4Q19", ... "31,438" ...}], ...}
+NOTE: proposal only — apply via pipeline recompile, never hand-edit shards.
```

Live-run diffs will carry the real `shard_hint` tree path and the existing
shard summary text per `node_id` (resolved read-only via
`documents.jsonl → tree_path → node_id` join, as the harness `fill_ocr_from_trees`
does). Labels `improved` vs `contradicted` come from the number-set cross-check.

## 7. Blockers

1. **Paddle deps not installed in this task** (per directive; install happens
   at run time in a venv). Live 26-image paddle run (~21 min serial per bench
   `f8bf3ac27`) is the next step, then report paddle metrics before any paid
   venue is even proposed.
2. No batch authorization beyond this 26-image pilot — the manifest is the
   pilot's input spec, not a queue for the 8,424-skipped backlog.

## 8. Local paddle run (replaces the dead hosted CI path)

- **Hosted CI path DELETED this task** (`.github/workflows/reocr_pilot.yml`
  removed; `ci_support.py` removed): `OLLAMA_MODEL` retired upstream HTTP
  410, NIM EOL 410, pinned vision models 404; no paid API per user directive.
- **Run:** venv with `paddlepaddle==3.3.1 paddleocr==3.7.0 pymupdf`
  (~1.4GB), then
  `python scripts/pilot/reocr_pilot.py --set data/derived/pilot_image_set.jsonl --venue paddle --max-images 26 --out data/derived/pilot_reocr_out`
  (`enable_mkldnn=False` already in code; CPU-only; $0).
- **Cost/time bound:** 26 images, paddle OCR ≤2 attempts × 2 stages logic
  (OCR runs once per image; redo re-parses); ~21 min serial per bench,
  ~45 h full-corpus projection at 4-way parallel (bench `f8bf3ac27`).
- **Outputs:** `data/derived/pilot_reocr_out/{audit.jsonl,results.json,reconcile.diff}`;
  reconcile applied only via pipeline recompile after review.

## 9. Files touched (no commits)

- EDIT `scripts/pilot/reocr_pilot.py` — hosted strip (env/payloads/venue
  auto/probe/rate-limit/guardrails removed) + paddle lane
  (`enable_mkldnn=False` per `f8bf3ac27`, PDF threshold 200, deterministic
  OCR→stage parsing) + tie-out verifier (flag-only separator) + Vale GT1
  fixture (truth 31,438).
- DELETE `scripts/pilot/ci_support.py` — hosted preflight/spend/probe/cost
  only; nothing local left.
- DELETE `.github/workflows/reocr_pilot.yml` — hosted path dead (OLLAMA_MODEL
  retired HTTP 410; no paid API per directive).
- DELETE `data/derived/pilot_reocr_out/choice.json` — untracked leftover
  from the cancelled hosted-probe task.
- REWRITE `data/derived/pilot_image_set.jsonl` — 35→26 unique (hash dedupe;
  2 logo controls; +S9/S10 separator; §3).
- EDIT `docs/PILOT_REOCR_MUSE_SPARK.md` — this report (venue pivot, paddle,
  dedupe table, tie-out, fixture).
- EDIT `docs/INVENTORY_MUSE_SPARK.md` — Decision-2 claim line (1 bullet).
- `data/derived/pilot_reocr_out/{audit.jsonl,results.json,reconcile.diff}` —
  dry-run artifacts (tracked; regenerated by self-test, content per §4).
- Untouched: `process_knowledge.py`, other workflows, requirements,
  `knowledge/` shards.

Commit-message-ready note (do NOT commit per directive):
`pilot: local paddle lane, 35→26 dedupe, tie-out verifier (Decision 2)`

- Hosted venues stripped (NIM/Ollama/OpenRouter/Groq env, payloads,
  auto/probe, retry, spend caps); `ci_support.py`, `reocr_pilot.yml`,
  `choice.json` deleted — hosted path dead (OLLAMA_MODEL retired upstream
  HTTP 410; no paid API per 2026-09-07 directive).
- Paddle lane `--venue paddle` (paddlepaddle 3.3.1 + paddleocr 3.7.0,
  `enable_mkldnn=False` mandatory per bench f8bf3ac27; PDF threshold 200).
- Set 35→26 unique hashes (drop 8 monogram dups + 2 BRS logos + 1 crude dup;
  keep 2 logo controls; +S9/S10 separator).
- Verifier: separator flag-only, tie-out correction; fixture truth 31,438
  (31,438+19,291=50,729); self-test PASS (planted + naive rejected, truth
  accepted).
