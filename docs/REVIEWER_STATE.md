# Reviewer State — cold-resume brief

**Purpose.** Everything the reviewer agent (Claude Opus, "big-brain verifier") needs to
resume after a context reset. Facts only. Narrative lives in `docs/VERIFICATION_LOG.md`.

**Last updated:** 2026-09-07 07:50 UTC

---

## 1. Role and boundaries

- **Role:** reviewer / composer. Verifies, adjudicates, directs. **Does not do build work** —
  that goes through the build agents by user instruction.
- **Branch:** `claude/maritime-kb-inventory-hzbhlx`. **PR #40** (draft, docs-only).
- **Owns:** `docs/REVIEW_BASELINE.md`, `docs/VERIFICATION_LOG.md`, this file.
  Build agents must not edit them (antigravity did twice — findings B8 and the
  Decision-3 self-grade; both reverted).
- **Cadence:** hourly self-poll via `send_later` (user asked for token conservation).
  Silent on empty cycles. One poll armed at a time.

## 2. The three agents

| agent | branch | lane |
|---|---|---|
| Claude Opus (this) | `claude/maritime-kb-inventory-hzbhlx` | review, adjudication, ground truth |
| OpenCode / Muse Spark | `agent/muse-spark` | **DEPTH** — Decision 2, local OCR |
| Antigravity / Gemini | `agent/antigravity` | **BREADTH** — Decision 4, source wiring |

Muse-spark needs a user push to act. Antigravity runs on a user-set cron.
Reviewer runs in an ephemeral Linux container sharing **no** filesystem with the user's
Windows worktrees — work is only visible once pushed to `origin`.

## 3. End goal (mission), and decision mapping

A knowledge base preserving **three levels, not flattened into embeddings**:

- **Breadth** (many sources) → **Decision 4** — SGX, Capital Link, Fearnleys/Hasura, EDGAR,
  CFTC, ETF, AIS weekly, grain/port flows. *Not started; user's highest interest.*
- **Depth** (structure inside each source) → **Decision 2** — re-OCR of 13,591 ingested
  assets / 10,894 images. *Pilot built, not yet run locally.*
- **Interlinking** (cross-source joins) → **Decision 3** — graph over `knowledge/trees/`.
  *Two rival scaffolds, neither sound; vendor choice awaiting user.*

Decision 1 (stop live data loss) is **COMPLETE and merged to main**.

## 4. Binding constraints

- **No hosted model venues.** No NVIDIA NIM, Ollama, OpenRouter, Groq, or any paid API.
  All-local Python + GitHub tooling. (User directive 2026-09-07; also empirically —
  run `34091897626` failed, 3.2 KB artifact, ~1 image of 35.)
- **Additive only.** No writes under `knowledge/trees/` or `knowledge/derived/`. No re-chunking.
- **"Unprocessed" = ledger diff** on `knowledge/manifests/documents.jsonl`
  (`source_hash` + `compiler_version`), never a filesystem walk.
- **`CONTENT_GATE_MEDIAN_FLOOR` stays 120.** Only sanctioned validator change is the CG1
  per-source override `{("baltic","ningbo"): 40}`, median rule only.
- **No agent grades its own work.**

## 5. Ground truth established by the reviewer (do not re-derive)

- **Vale iron-ore table** (`reports/breakwave/2020/assets/2020-06-06_..._img_img-1960_8a20a313afb5.jpg`),
  "Northern and Eastern ranges" 4Q19 = **31,438**.
  Legacy OCR reads `34.438`; an earlier reviewer note wrongly asserted `34,438`.
  Proof: `31,438 + 19,291 (S11D) = 50,729` = printed Northern System subtotal.
  `34,438 + 19,291 = 53,729` ≠ subtotal. **A separator-only fix yields a confidently wrong value.**
- **Pilot set** `data/derived/pilot_image_set.jsonl`: 35 entries = **26 unique images**.
  Two BRS Shipbrokers logos appear ×5 each. The large (2500×2186) `empty_ocr` entries are the
  **BRS corporate wordmark**, not charts — reviewer viewed them directly.
- **"OCR returned nothing" is a weak signal, not a strong one** (supersedes the earlier W2 note).

## 6. Reviewer's verification recipes

**Content gate re-implementation** (run against any branch's `knowledge/chunks/*.jsonl`;
expected result **17 groups, 0 failures**):
`WINDOW=50, MIN_SAMPLES=10, STUB_CHARS=120, MEDIAN_FLOOR=120, STUB_RATE=0.80`,
overrides `{("baltic","ningbo"): 40}` (median rule only),
boilerplate markers `("Metadata only","JS-rendered","not retrievable via static fetch")` at `0.30`.
Group by `(source, category)`, order by `(date, chunk_id)`, take trailing 50.

**PaddleOCR bench recipe** (reviewer-verified):
```
python3 -m venv ocrtest && ./ocrtest/bin/pip install paddlepaddle paddleocr   # 1.4 GB
PaddleOCR(use_doc_orientation_classify=False, use_doc_unwarping=False,
          use_textline_orientation=False, lang='en', enable_mkldnn=False)
```
`enable_mkldnn=False` is **mandatory** — default oneDNN path crashes on CPU with
`NotImplementedError: ConvertPirAttribute2RuntimeAttribute not support`.
Measured: init 3.6 s, **48.2 s/image**, 92 lines, **8/8 ground-truth values correct**,
mean confidence 0.998. Full corpus ≈ 45 h at 4-way parallelism, $0.

**Environment:** 4 cores, 15 GB RAM, ~20 GB free, **no GPU** — close to a GitHub Actions runner.
`pip install` into a venv (Debian-managed PyYAML blocks system-wide installs).

## 7. Approved all-local stack

Route with **PyMuPDF/pdfplumber** (never OCR a page with a usable text layer) →
extract once with **PaddleOCR** → verify with **arithmetic tie-out**, **cross-source
corroboration**, **gazetteer validation**. Embeddings: **sentence-transformers**
(`all-MiniLM-L6-v2` / `BGE-small-en-v1.5`). Entities: **gazetteer + spaCy EntityRuler**,
gazetteer built from the repo's own data (vessel names in Fearnleys catalogue and hellenic
fixtures, ports in PortWatch, owners in ETF/SEC, classes in `data/futures`).
MinerU / dots.ocr: **hold** (want a GPU) — reserve as the dispute lane.

**Two OCR engines agreeing is weak evidence** — PaddleOCR and Tesseract produced the same
`rn → m` garble, PaddleOCR at confidence 1.00. Spend compute on verification, not redundancy.

## 8. Open items

- **AWAITING USER:** Decision 3 amendment — LightRAG's value is LLM entity extraction, which
  is unavailable with hosted models out. Reviewer recommends a **deterministic networkx graph
  over the SQLite spine** with a repo-built gazetteer, keeping antigravity's `query_graph.py`
  shape and `tests/test_graph_layer.py`. Neither agent may pick a vendor until confirmed.
- **Live defect:** repo's `OLLAMA_MODEL` (`gemma3:4b`) returns HTTP 410 "retired"; the same
  variable is read by `process_knowledge.py`, so the daily pipeline's LLM path is broken.
  Now moot for the pilot but still live for the brief.
- **Antigravity B1-B9** open on the merits (process finding closed after a clean revert).
- **CG1-a:** ningbo now has single-rule gate protection with an 8pp margin; a regression at
  ~75% stubs and median ~45 would pass undetected. If in doubt, compare against its
  74-char historical median manually. **Never lower the global floor.**
- **E1 scraper defect:** linked assets are written under `.pdf` with no content-type check —
  89 of 91 "failed" assets are HTML error pages. Fix at fetch time.
- **P-a:** shard compaction only evicts on `doc_id` change, so orphaned fragments need a
  hand edit. Worth a tool fix.

## 9. Corrections the reviewer has made to its own findings

Kept visible on purpose — the project's method is disagreement checked against source.

1. `linked_assets_skipped` was called "largely chart images"; it is **99.0% outbound links to
   third-party journalism**. (Caught by muse-spark.)
2. The 91 failed assets were called the best recovery pocket; **89 of 90 on disk are HTML
   error pages**, recovery value ≈ 1 file.
3. `34,438` was asserted as the Vale truth; it is **31,438**. (Caught by reading the image.)
4. Large `empty_ocr` images were called the strongest chart signal; they are **corporate logos**.
