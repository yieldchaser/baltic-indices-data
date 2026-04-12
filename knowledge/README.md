# Shipping Knowledge System Runbook

This document is the operational README for the repo-native knowledge pipeline:

- source scraping into `reports/`
- knowledge compilation into `knowledge/`
- validation and health checks
- online workflow execution and verification

As of `2026-04-11`, the pipeline has been dry-run and validated with:

- `process_knowledge.py --no-llm` -> `processed=0 skipped=7535 errors=0`
- `validate_knowledge.py` -> `Validation status: PASS`


## 1) What Is In Scope

The knowledge system ingests and normalizes:

- Breakwave PDF reports (`drybulk`, `tankers`)
- Baltic HTML reports (`dry`, `tanker`, `gas`, `container`, `ningbo`)
- Breakwave Insights HTML archives (`insights`)
- Hellenic HTML archives (`dry_charter`, `tanker_charter`, `iron_ore`, `vessel_valuations`, `demolition`, `shipbuilding`)
- books (`reports/*.pdf`)

Outputs are written under:

- `knowledge/docs/` (normalized markdown + frontmatter)
- `knowledge/trees/` (section trees)
- `knowledge/chunks/` (retrieval JSONL chunks)
- `knowledge/manifests/` (documents/sources/errors + lint/coverage)
- `knowledge/derived/` (signals/themes/section_index/topic_evidence/timelines)
- `knowledge/wiki/` (topic pages)
- `knowledge/reports/` (health summary)


## 2) Scrapers And Automation

### `report_ingest.yml`

- schedule:
  - `0 8,12,16 * * 1-5` (core windows)
  - `30 9 * * 1-5` (extended window)
- scripts:
  - `scripts/breakwave_scraper.py`
  - `scripts/baltic_scraper.py`
  - `scripts/breakwave_insights_scraper.py`
  - `scripts/hellenic_scraper.py`

### `process_knowledge.yml`

- triggers:
  - on `reports/**` push
  - manual dispatch (`source`, `rebuild`)
- runs:
  - `python scripts/process_knowledge.py ...`
  - `python scripts/validate_knowledge.py`

### `daily_knowledge_update.yml`

- schedule:
  - `30 15 * * *` (daily)
- only processes when `reports/` has files newer than `knowledge/manifests/documents.jsonl`


## 3) Ingestion Coverage Matrix

This is what the compiler currently handles in `scripts/process_knowledge.py`.

### Native report body content

- HTML headings/paragraphs/lists/blockquote: extracted
- HTML tables: extracted via `table_to_text(...)`
- inline `<img>` references: captured as image references
- PDF text pages: extracted with `pdfplumber`

### Linked assets in Hellenic archives

The compiler follows both:

- `<a href="...">`
- `<img src="...">`

Then resolves local assets and ingests:

- `.pdf` -> text extraction (page-limited, truncated safely)
- `.html/.htm` -> section text extraction
- `.txt/.md` -> plain text extraction
- `.csv/.tsv` -> tabular extraction
- `.json` -> parsed/pretty JSON extraction
- `.xls/.xlsx/.xlsm` -> sheet/tabular extraction (`pandas` + `openpyxl`)
- images (`.png/.jpg/.jpeg/.gif/.webp/.svg`) -> image asset section with metadata, SVG text when present, optional OCR notice

Important caveats:

- Remote links are not fetched over network during compile. The compiler resolves assets that exist inside the repo archive.
- If a page links to an external URL, the scraper should mirror that file into `reports/...` for full ingestion.
- OCR for raster images is best-effort. If OCR dependencies are missing, the image is still ingested as a linked image section with metadata/reference.


## 4) LLM Provider Behavior And 429 Protection

LLM calls are controlled in `scripts/process_knowledge.py` with provider chaining:

- `Gemini -> Ollama -> heuristic extraction`

Gemini controls:

- request pacing (`GEMINI_MIN_INTERVAL_SEC`)
- retry/backoff/jitter (`GEMINI_MAX_RETRIES`, `GEMINI_BACKOFF_BASE_SEC`, `GEMINI_MAX_BACKOFF_SEC`)
- retry-after parsing for rate-limit responses
- model override via `GEMINI_MODEL` (default `gemini-2.0-flash`)

Ollama controls:

- base URL / key / model (`OLLAMA_BASE_URL`, `OLLAMA_API_KEY`, `OLLAMA_MODEL`)
- request pacing (`OLLAMA_MIN_INTERVAL_SEC`)
- retry/backoff/jitter (`OLLAMA_MAX_RETRIES`, `OLLAMA_BACKOFF_BASE_SEC`, `OLLAMA_MAX_BACKOFF_SEC`)

Workflow env defaults are set in:

- `.github/workflows/process_knowledge.yml`
- `.github/workflows/daily_knowledge_update.yml`

Why AI Studio may show no new calls:

- run used `--no-llm`
- run skipped all unchanged docs
- `GEMINI_API_KEY` missing in runtime environment
- Gemini failed and fallback provider handled the run


## 5) Online Trigger Runbook

Use this sequence for a clean online run.

1. Trigger `report_ingest.yml` manually:
   - `source=all`
   - `year=auto`
   - Optional historical batch: `start_year=<YYYY>`, `end_year=<YYYY>`
   - Optional remirror: `overwrite=true`
   - `dry_run=false`
2. Wait for reports commit to `main`.
3. Trigger `process_knowledge.yml` manually:
   - `source=all`
   - `rebuild=false` (incremental)
4. Confirm logs include:
   - `[DONE] processed=... skipped=... errors=0`
   - `Validation status: PASS`
5. Confirm workflow pushed updated `knowledge/` artifacts if changes were detected.

Use `rebuild=true` only when intentionally doing a full clean rebuild.
For one-time historical hardening, run scraper backfills first (with `overwrite=true`), then run `process_knowledge.yml` with `rebuild=true`.


## 6) Local Dry-Run Commands

Incremental no-LLM dry run:

```bash
python scripts/process_knowledge.py --no-llm
python scripts/validate_knowledge.py
```

LLM-enabled run:

```bash
python scripts/process_knowledge.py
python scripts/validate_knowledge.py
```

Source-specific runs:

```bash
python scripts/process_knowledge.py --source hellenic --no-llm
python scripts/process_knowledge.py --source breakwave_insights --no-llm
```


## 7) Expected Success Criteria

A healthy run should have:

- `processed + skipped = total source files`
- `errors=0` in processor summary
- validator `PASS`
- `duplicate chunk ids = 0`
- `missing docs/chunks/trees = 0`
- `malformed json/jsonl/tree counts = 0`
- `high-severity health warnings = 0`


## 8) Known Operational Notes

- `knowledge/manifests/errors.jsonl` is an operational log and can contain historical failures even after a clean run.
- Large chunk files can trigger GitHub size warnings (for example large `hellenic_iron_ore.jsonl`).
- Local Windows ACL/file locks can block writes; rerun with proper permissions when needed.


## 9) Dependency Baseline

Knowledge pipeline dependencies are in `requirements_knowledge.txt`, including:

- `pdfplumber`, `beautifulsoup4`, `lxml`, `tiktoken`, `python-frontmatter`, `python-dotenv`, `google-generativeai`
- `pandas`, `openpyxl`, `Pillow` for robust linked spreadsheet/image handling
