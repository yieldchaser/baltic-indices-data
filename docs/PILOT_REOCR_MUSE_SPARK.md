# Re-OCR / structuring pilot — muse-spark (Decision 2, PHASE A)

- Author: muse-spark
- Date: 2026-09-07
- Branch: `agent/muse-spark`, worktree `C:\Users\Dell\Github\shipping-muse-spark`, **no commits**
- Scope: venue assessment + pilot harness + set selection. **No live inference ran
  (no working venue in sandbox). No writes to `knowledge/`.**
- Inputs: `docs/INGESTED_IMAGE_AUDIT_MUSE_SPARK.md` (§5/§6/§10/§V3),
  `data/derived/asset_dispositions.jsonl`, `scripts/process_knowledge.py`
  (client lines read, not copied), `scripts/generate_brief.py` (env names).

## 1. Venue verdict: NO live venue in this sandbox → dry-run only

Time-boxed, read-only checks (names only, never values):

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

**Verdict:** no venue can run a 20–50-image vision pilot from this sandbox.
Live inference was **not** executed. The harness is CI-ready and the dry-run
below proves the full loop (extractor → verifier → redo → reconcile) with a
recorded-fixture mock.

## 2. Harness design: `scripts/pilot/reocr_pilot.py` (NEW, 722 lines)

Deps: **stdlib + requests + PIL only** (argparse, base64, io, json, re, time,
pathlib + requests + PIL).

- **Two-stage protocol.**
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
- **Extractor/verifier separation:** the model is the extractor; deterministic
  code is the verifier (`verify_stage1` / `verify_stage2`):
  axis-values consistency (stage-2 x ∩ declared ticks), units match, row/column
  counts vs declared table dims, numeric-format sanity incl. **separator-mix
  flagging** (dot-thousands `34.438` amid comma-thousands `35,047`), and
  cross-check against the existing shard OCR text → label
  **improved** (overlaps/extends) vs **contradicted** (disjoint number sets).
- **Redo loop:** `PILOT_REDO_LIMIT` (default 1) extra attempt per stage with a
  strengthened prompt citing the rejection reason; every attempt is appended to
  the JSONL audit (`ts, record, stage, event, attempt, ok, issues, verdict`).
- **Reconcile report:** per-record unified-diff-style proposal
  (`-OCR(existing)` vs `+STAGE1/+STAGE2`) written to `reconcile.diff`.
  **Never writes to `knowledge/`** — proposals apply via pipeline recompile only.
- **Image handling:** images read from repo-relative POSIX `image_rel`; oversize
  payloads (>8 MB default) are PIL-downscaled to JPEG before base64.

Usage:

```bash
python scripts/pilot/reocr_pilot.py --dry-run --out data/derived/pilot_reocr_out
python scripts/pilot/reocr_pilot.py --set data/derived/pilot_image_set.jsonl \
  --venue auto --max-images 35 --out data/derived/pilot_reocr_out
```

## 3. Pilot set: `data/derived/pilot_image_set.jsonl` (35 records)

Bounded set manifest (≤50, POSIX paths, `doc_id` + `node_id` + `image_rel` +
`shard_hint` + reason code + note). All 35 mirrors verified present on disk.

**D2-a rebalance (2026-09-07):** the first cut inverted the board's priority
order (18 small_skip vs 14 top-cohort). The new mix restores board order as a
**decision, not drift**: empty_ocr is the primary cohort (OCR ran and returned
nothing — likeliest silent content loss per audit §10/W2), separator_suspect
second (retrieval-layer damage needing vision check), small_skip last and
small by design. small_skip is retained at 5 rather than 0 for uncertainty
learning: V3 grades 16/20 as cannot-confirm-without-vision, so five
hand-picked cannot-confirm cases test whether the <90000px skip threshold is
hiding tables. Separator is expanded 3→8 because the 228 is a lower bound on
retrieval-layer damage (audit §6/V2 — the regex catches only the
mixed-separator class) and suspects are cheap to include at 8 total images.

| Reason | n | Contents |
|---|---|---|
| `empty_ocr_large` | 2 | **both** 2500x1667 OCR-empty JPEGs (2023-08-26 `unnamed284329`, 2024-06-14 `image-asset28729`) |
| `empty_ocr` | 20 | all 10 remaining non-monogram empty classes (arrowcoalprices graphic, 2 BRS logos, greek-label png, 2× 2025 big-picture, 2× 2026 hormuz, 2× crude-tanker photo class = 12 rows incl. the 2 large above → 12 non-monogram rows) + 10 monogram-dup reps (earliest 5 of the 44-instance v1 family + earliest 5 of the 30-instance v2 family, ledger order — both byte-variants now represented; prior cut covered v1 only) |
| `separator_suspect` | 8 | **S1** Vale iron-ore table, **S2** Q3 capesize chart, **S3** dollar-index chart (kept) + **S4–S8** top-5 non-S rows of the 228 mixed-separator scan by separator-mix signal (S4 2025-04-18 fleet-by-class table mix=19, S5 2026-02-06 BDI spot-price summary mix=12, S6/S7/S8 commodities-wrap tables mix=9 each; ranking = mixed-sep class first, then min(#dot3,#com3) desc, then total hits desc, ledger-order tiebreak) |
| `small_skip` | 5 | V3#13 tableseason kept (NAME SAYS TABLE, most suspicious) + V3#2 (`2hsettle`, name says settlement table) + V3#8 (`settlements`, name says settlements table) + V3#9/#10 (Klaveness-strip pair, same outlook-iron-ore doc — tests strip-series recoverability). All five graded cannot-confirm-without-vision; learning value: a hit on any filename-says-table case reopens the skip threshold, a hit on the klav pair reopens the whole outlook series, all-no-data confirms the skip stands. |
| **Total** | **35** | within the 20–50 band; empty cohort (22) is now the majority per board order |

Selection rules (deterministic, from `asset_dispositions.jsonl` ledger order
joined to tree-node summaries): empty = every `[No OCR text detected]`
instance outside the 74-monogram dup family (12 rows, all byte-classes
covered) + earliest-5-per-monogram-byte-variant (10 rows); separator = S1–S3
by fiat + exact rerun of the audit §6 regex scan (187 mixed-sep + 41
dot3+dot2 = 228, reproduced 2026-09-07) ranked as above; small_skip = V3
table-named first (#13, #2, #8) + one strip-series pair (#9/#10). Every row
carries its parent `node_id`, POSIX `image_rel`, and `shard_hint`; all 35
mirrors verified on disk at rebalance time.

Coverage of the 86: all 13 distinct byte-classes represented (11 singleton
classes fully enumerated + both monogram byte-variants with 5 reps each);
expanding to all 86 would burn 74 calls on near-identical pixels and breach
the 50-cap. No dry-run regen needed: the dry-run (§4) ran a recorded-fixture
mock (`FIXTURE/mock.png`, node `FIXTURE__vale_table__s03`) and never read the
set — `audit.jsonl`/`results.json`/`reconcile.diff` reference no set rows, so
no dry-run artifact references a changed row.

## 4. Dry-run results (fixture mock, no network)

```bash
python scripts/pilot/reocr_pilot.py --dry-run --out data/derived/pilot_reocr_out
→ {"mode": "dry-run(mock)", "status": "accepted", "verdict": "improved",
    "audit_events": 3, "mock_calls": ["stage1#0", "stage2#0", "stage2#1"]}
```

One fixture image through both stages with canned responses:

1. `stage1#0` → clean declaration → verifier `ok: true`, no issues.
2. `stage2#0` → **planted separator error** (`34.438` dot-thousands amid
   `35,047`/`7,109` comma siblings) → verifier `ok: false`,
   `issues: ["separator_mix suspect: dot-thousands and comma-thousands/decimal mix"]`.
3. **Redo** `stage2#1` → consistent separators → verifier `ok: true` →
   status `accepted`, verdict `improved`, diff emitted.

The mock proves the verifier catches the exact S1-class separator error and the
redo path recovers. Unit spot-checks also pass: Ollama payload carries
`messages[0].images`, OpenAI-compat payload carries `content[1].image_url`,
`verify_stage1({"axes_readable": False})` → `axes_unreadable_fail_closed`,
live-mode with no venue exits 2 with a no-venue message (probe dir removed).
Outputs: `data/derived/pilot_reocr_out/{audit.jsonl,results.json,reconcile.diff}`.

## 5. Redo log summary

Dry-run audit (`audit.jsonl`, 3 events): stage1 extract+verify ok (attempt 0);
stage2 extract+verify rejected (attempt 0, `separator_mix`); stage2
extract+verify accepted (attempt 1). No live redo log exists — no live calls
were made. Live-run audit will follow the same per-image/per-stage schema.

## 6. Reconcile diffs (expected form)

Dry-run `reconcile.diff` for the fixture:

```diff
--- a/knowledge/trees/FIXTURE.json
+++ b/proposed (FIXTURE__vale_table__s03)
@@ image FIXTURE/mock.png status=accepted
-OCR(existing): 'Northem andEastem = 34.438 35,047 legacy OCR'
+STAGE1(declared): {"chart_type": "table", ... "units": "Mt", ...}
+STAGE2(values): {"values": [{"series": "Output", ... "34,438" ...}], ...}
+NOTE: proposal only — apply via pipeline recompile, never hand-edit shards.
```

Live-run diffs will carry the real `shard_hint` tree path and the existing
shard summary text per `node_id` (resolved read-only via
`documents.jsonl → tree_path → node_id` join, as the harness `fill_ocr_from_trees`
does). Labels `improved` vs `contradicted` come from the number-set cross-check.

## 7. Blockers

1. **No vision venue in sandbox** (this task's outcome, not a surprise — audit §7
   W1 already called it sandbox-only). Live pilot needs the CI path below.
2. No batch authorization beyond this 35-image pilot — the manifest is the
   pilot's input spec, not a queue for the 8,424-skipped backlog.

## 8. CI-run request (exact)

- **Workflow:** `.github/workflows/process_knowledge.yml` (holds the
  `NIM_API_KEY`/`OLLAMA_*` secrets wiring, `:90-99`) — run as a manual
  `workflow_dispatch` job/step on `ubuntu-latest` with the OCR runtime
  (`tesseract-ocr`, `poppler-utils`) already in that workflow; **no workflow
  file changes** in this task.
- **Model (vision-capable, set via existing secrets, no code change):**
  `NIM_MODEL` = a NIM vision model (e.g. `meta/llama-3.2-90b-vision-instruct`)
  with `NIM_API_KEY`/`NIM_BASE_URL`, fallback `OLLAMA_MODEL` = a vision-capable
  Ollama model with `OLLAMA_BASE_URL`/`OLLAMA_API_KEY`. Text-only models will
  fail the stage-1 JSON parse → records land `stage1_rejected`, which is the
  safe, auditable outcome.
- **Command:**
  `python scripts/pilot/reocr_pilot.py --set data/derived/pilot_image_set.jsonl --venue auto --max-images 35 --out data/derived/pilot_reocr_out`
- **Secrets:** `NIM_API_KEY`, `NIM_MODEL`, `NIM_BASE_URL`, `OLLAMA_BASE_URL`,
  `OLLAMA_API_KEY`, `OLLAMA_MODEL` (all already referenced by the workflow).
- **Cost/time bound:** 35 images × ≤(1 + 1 redo) × 2 stages ≈ ≤140 vision calls;
  estimated single-digit USD on NIM pay-per-call at these image sizes; wall-clock
  ≈ 15–40 min at the 1.5 s interval + backoff. Abort if 429-error share > 30%.
- **Outputs:** `data/derived/pilot_reocr_out/{audit.jsonl,results.json,reconcile.diff}`
  attached to the run; reconcile applied only via pipeline recompile after review.

## 9. Files touched (no commits)

- NEW `scripts/pilot/reocr_pilot.py` — pilot harness (stdlib + requests + PIL).
- NEW `data/derived/pilot_image_set.jsonl` — 35-record pilot set manifest
  (D2-a rebalance 2026-09-07: 14/3/18 → 22 empty / 8 separator / 5 small_skip;
  harness untouched, dry-run artifacts unchanged per §3).
- NEW `docs/PILOT_REOCR_MUSE_SPARK.md` — this report.
- EDIT `docs/INVENTORY_MUSE_SPARK.md` §0 — Decision-2 claim line (1 bullet).
- `data/derived/pilot_reocr_out/` — dry-run artifacts (audit/results/diff).
- Untouched: `process_knowledge.py`, workflows, requirements, `knowledge/` shards.
