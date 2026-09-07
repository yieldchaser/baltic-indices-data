# Ingested-image audit — muse-spark

- Author: muse-spark
- Date: 2026-09-06
- Branch: agent/muse-spark (worktree `C:\Users\Dell\Github\shipping-muse-spark`, no commits)
- Inputs: `data/derived/asset_dispositions.jsonl` + disk + `knowledge/manifests/documents.jsonl` + `knowledge/trees/` shards (read-only; throwaway scripts under `C:\Users\Dell\AppData\Local\Temp\opencode`)
- Scope: evidence only. **Reprocessing (P0 reversal / vision re-OCR) scope awaits the user's call — no scope commitment in this document.**

## 1. Which set is profiled

- Ledger (`documents.jsonl`): discovered 22,106 = mirrored 13,716 + skipped 8,424 + failed 91; ingested **13,591** (breakwave_insights 8,859 + hellenic 4,732).
- Dispositions (`asset_dispositions.jsonl`, 22,106 records = ledger discovered): ingested **13,591** (breakwave_insights 8,859 + hellenic 4,732) + skipped 8,424 (8,341 `unresolvable_external` + 48 `non_content_link` + 35 `duplicate_path`) + failed 91 (verifies: 13,591 + 8,424 + 91 = 22,106).
- Reconciliation: on-disk mirrors 13,716 = disposition-ingested 13,591 + failed-with-mirror 90 (breakwave_insights 66 + hellenic 24) + skipped-`duplicate_path` 35 (breakwave_insights 34 + hellenic 1; all mirrors on disk). Profile below covers the **13,681** on-disk subset = ingested 13,591 + failed-with-mirror 90 (excludes the 35 dups); ledger-ingested = disposition-ingested = 13,591, cited for reference.

## 2. Format split (disk, n=13,681, 0 missing)

By extension (lowercased `local_mirror_rel` suffix, all present on disk):

- `.png` 6,235 · `.jpg` 4,787 · `.jpeg` 259 · `.webp` 4 → image files 11,285
- `.pdf` 2,367
- `.html` 27 · `.htm` 2 → 29

By `asset_kind` (tag-based: `img` = `<img src>`, else `pdf`/`.pdf`/otherwise `link`):

- `img` 10,894 · `pdf` 2,367 · `link` 420 (420 = 391 image files linked via `<a href>` + 29 html)

## 3. Size distribution (disk bytes, n=13,681)

- Total **3,346.18 MB**; median **100,677** (~98 KB); mean 256,467; min 1,905; max 7,910,273
- p10 25,746 · p25 47,362 · p75 383,916 · p90 655,848
- Buckets: `>=300KB` 4,165 · `100-300KB` 2,601 · `50-100KB` 3,201 · `10-50KB` 3,555 · `<10KB` 159
- Per source: breakwave_insights 8,925 files / 1,554.76 MB; hellenic 4,756 files / 1,791.42 MB (hellenic files larger on average)

## 4. Per-source split

- Disposition-ingested: breakwave_insights 8,925 · hellenic 4,756 (total 13,681)
- Ledger-ingested (reference): breakwave_insights 8,859 · hellenic 4,732 (total 13,591)
- Ledger-mirrored (reference): breakwave_insights 8,959 (= 8,925 + 34 dups) · hellenic 4,757 (= 4,756 + 1 dup); failed 67 + 24 = 91

## 5. Tree node_id + OCR-text coverage

- `node_id` present: **13,584 / 13,681 (99.3%)**; without `node_id`: 97 (all `pdf` 89 + `link` 8; breakwave_insights 73 + hellenic 24). All 10,894 `img` records carry a `node_id`.
- Native `text` field on linked-asset sections is uniformly empty (13,584/13,584 len 0) — content lives in `summary`.
- `summary` content length (OCR portion after `OCR text:` for images; text after `Source asset:` header otherwise): n=13,584, median **245**, mean 375.1, min 1, max 1,100, p10 100 / p25 163 / p75 485 / p90 952; buckets `1-99` 1,357 · `100-499` 8,927 · `500-1999` 3,300 · `0` 0. Per kind medians: `img` 209 (mean 282) · `link` 350 (mean 335) · `pdf` 1,039 (mean 826; summaries cap ~1,100 chars).
- CORRECTION (V1, re-measured 2026-09-06): the ~1,100-char cap is on the **summary display field only — NOT data loss**. Extraction is governed by `LINKED_TEXT_CHAR_LIMIT=70000` (`scripts/process_knowledge.py:73`, verified by reading the line). Proof on S1: the tree node summary (1,126 chars, ends mid-OCR at `IRON ORE SALES?`) is a strict prefix of the concatenated chunk texts for the same section (`..._005` 1,164 chars + `..._006` 375 chars = 1,539 chars, complete through the table footnotes, no truncation marker). `chunks/` content is complete; the summary cap implies nothing about data loss. Do not chase further.
- Image OCR specifically (CORRECTED 2026-09-06, exact node-summary join over all 10,894 ingested-`img` node_ids, 0 missing): **10,455 / 10,894 with OCR content** (96.0% = 10,443 legacy non-empty `OCR text:` incl. the 1 pipe-only graphic `...arrowcoalinventories...png`, + 12 new-format `[structured table]`/`[raw ocr]` nodes); **451 with no legacy `OCR text:` header = 347 `[OCR skipped for small image (< 90000 pixels).]` (note trailing period — the audit draft quoted it without) + 104 others, where the 104 = 86 `[No OCR text detected in linked image.]` (OCR attempted, empty result — many NOT small, e.g. 2x 2500x1667 JPEG; see §V3) + 6 `[OCR unavailable; install pytesseract]` (2026 docs) + 12 new-format `[structured table]`/`[raw ocr]` (HAVE OCR content — counted in the 10,455 above, not no-OCR); true no-OCR = 347 + 86 + 6 = 439**. The 228 mixed-separator candidates below come from the 10,443 legacy-`OCR text:` images.

## 6. Corruption suspects (image OCR, vision check required — no ground truth asserted)

Method: regex scan of the 10,443 image OCR strings for `\d{1,3}\.\d{3}` co-occurring with `\d{1,3},\d{3}` (mixed thousands separators in one chart/table) or with `\d+\.\d{2}` (3-decimal vs 2-decimal decimals); 228 mixed-separator candidates repo-wide. **The 228 is a LOWER BOUND on retrieval-layer damage (V2):** the regex only catches the mixed-separator class. A second damage class — **row-splitting / scattered digits** (row labels detached from values, totals exploded across lines, e.g. S1 chunk `IRON ORE / 78,344 704 1 1,97` and `PRODUCTION! 8,3 86,704 100,988 301,972`, confirmed verbatim in `knowledge/chunks/breakwave_insights_insights.jsonl` chunk `..._the_drama_continues_as_brazilan_judge_hats_vales_iron_ore_operations_in_the_sout_005`, same section as the tree node) — passes the separator scan but is equally unreadable without the pixels. Both classes need vision check; no ground-truth correction is asserted for either. Three suspects are recorded below as **suspects needing vision check** — spellings/numbers are quoted exactly as stored; corrections are not asserted.

### S1 — Vale iron-ore table (reviewer Vale-class: dot-vs-comma thousands)

- `doc_id`: `breakwave_insights_insights_2020-06-06_2020_06_06_the_drama_continues_as_brazilan_judge_hats_vales_iron_ore_operations_in_the_sout`
- `node_id`: `breakwave_insights_insights_2020-06-06_2020_06_06_the_drama_continues_as_brazilan_judge_hats_vales_iron_ore_operations_in_the_sout__s03_linked_asset_2020_06_06_the_drama_continues_as_brazilan_judge_hats_vales_iron_ore_operations_in_the_sout_img_img_1960_8a20a313afb5_jpg`
- Mirror: `reports/breakwave/2020/assets/2020-06-06_the-drama-continues-as-brazilan-judge-hats-vales-iron-ore-operations-in-the-sout_img_img-1960_8a20a313afb5.jpg` (JPEG 1125x1530, 210,372 bytes)
- Suspect strings (verbatim): row `Northem andEastem = 34.438 35,047 37,023 115,352` (SUSPECT: `34.438` amid comma-thousands siblings — same class as reviewer's 34.438 vs 34,438; needs vision check); row `Paraopeba (Mutuca, = 4.997 7,109 10,352 24,637` (SUSPECT: `4.997` amid comma siblings); letter-digit/context garble in same OCR: `Northem`, `andEastem`, `uelelen Gets,`, `Conceigao`, `$11D`, `IRON ORE 78,344 704 1 1,97`, `PRODUCTION! 8,3 86,704 100,988 301,972`.

### S2 — Q3 capesize price chart (decimal/comma mix + word/month garble)

- `doc_id`: `breakwave_insights_insights_2020-06-12_2020_06_12_highest_capesize_index_print_of_the_year_as_brazil_comes_back_to_life`
- `node_id`: `breakwave_insights_insights_2020-06-12_2020_06_12_highest_capesize_index_print_of_the_year_as_brazil_comes_back_to_life__s05_linked_asset_2020_06_12_highest_capesize_index_print_of_the_year_as_brazil_comes_back_to_life_img_q3capesize_8e5616c79c4c_jpg`
- Mirror: `reports/breakwave/2020/assets/2020-06-12_highest-capesize-index-print-of-the-year-as-brazil-comes-back-to-life_img_q3capesize_8e5616c79c4c.jpg` (JPEG 734x394, 24,293 bytes)
- Suspect strings (verbatim): `PIM Mid Price 15366,000 18000`, `7 High on 09/24/19 17483,000`, `Avorage 15200.035`, `1 Low on 05/13/20 9407.00` (SUSPECTS: `15366,000` / `17483,000` comma-3-decimals vs `15200.035` dot-3-decimals vs `9407.00` dot-2-decimals in one legend; `Avorage` for Average); axis/month garble: `159000` amid `16000/17000`, `on wal hey Sp Oct Nov Dec Jen Feb Mor ie Yay Jun`.

### S3 — Dollar-index chart (comma/dot axis-label mix + label garble)

- `doc_id`: `breakwave_insights_insights_2020-11-23_2020_11_23_vaccines_investor_optimism_and_a_weaker_us_dollar`
- `node_id`: `breakwave_insights_insights_2020-11-23_2020_11_23_vaccines_investor_optimism_and_a_weaker_us_dollar__s03_linked_asset_2020_11_23_vaccines_investor_optimism_and_a_weaker_us_dollar_img_ulf80_7abfec11426d_jpg`
- Mirror: `reports/breakwave/2020/assets/2020-11-23_vaccines-investor-optimism-and-a-weaker-us-dollar_img_ulf80_7abfec11426d.jpg` (JPEG 451x283, 16,720 bytes)
- Suspect strings (verbatim): `Dollar ingex`, `102.00 101.00 100,00 38.000 96.000 97,000 96.000 $6,000 94.000 93.000 82.900` (SUSPECTS: `100,00` comma-decimal amid dot-decimals; `38.000/96.000/94.000/93.000/82.900` dot-3-decimals amid `97,000/$6,000` comma-thousands); `Jen Mar May Col Sep Now`, `SOURCE TRAGINGECONCENCS COM`.

## 7. Scope + vision status

- Evidence only: the profile (§1-5) and suspects (§6) do not commit to any reprocessing scope. **P0 reversal / re-OCR scope needs the user's call.**
- Vision status — W1 CORRECTION (reviewer W1, verified in-worktree 2026-09-06): the sandbox process environment carries no vision keys (re-verified: zero matches for `REDUCTO/LLAMA/ANTHROPIC/OPENAI/API_KEY/LLAMACLOUD/REPLICATE/HUGGINGFACE/AZURE_OPENAI`, and no `API/KEY/TOKEN`) — but that is a **sandbox-only** fact, not a project block. The project-level path exists: CI secrets `NIM_API_KEY` + `OLLAMA_BASE_URL`/`OLLAMA_API_KEY`/`OLLAMA_MODEL` are wired into the knowledge pipeline (`.github/workflows/daily_knowledge_update.yml:66-79`, `.github/workflows/process_knowledge.yml:90-99`; `OPENROUTER_API_KEY`/`GROQ_API_KEY` project secrets are wired to the brief workflow, `.github/workflows/daily_brief.yml:64-65`, consumed by `scripts/generate_brief.py:94-108`), and `scripts/process_knowledge.py:28-38` already runs an NIM/Ollama client (`_call_ollama_once`, `:1519-1561`; `_call_nim_once`, `:1607-1614`) whose payloads are text-only (`"messages": [{"role": "user", "content": prompt}]`, `:1524` and `:1612`; zero `image_url`/multipart matches in either client, `scripts/generate_brief.py` included). Unblock = add a multimodal call path (`image_url`/multipart) to the existing client + a CI run against a vision-capable model + written spend approval. **This is explicitly NOT authorization to run a batch — spec/analysis only.** Suspects above are flagged for vision check under such an approved run; no ground-truth correction is asserted here.

## 8. Reviewer follow-ups V1/V2/V4 (2026-09-06, re-measured in-worktree, no commits)

- V1 — summary cap is not data loss. `LINKED_TEXT_CHAR_LIMIT` verified by reading `scripts/process_knowledge.py:73`: `LINKED_TEXT_CHAR_LIMIT = int(os.environ.get("LINKED_TEXT_CHAR_LIMIT", "70000"))` → **70,000**. S1 prefix proof (§5 correction): tree summary 1,126 chars ⊂ chunk concat 1,539 chars, no truncation marker. All §5 wording that implied truncation/data-loss is corrected above; summary = display field, `chunks/` = complete.
- V2 — corruption is retrieval-layer, 228 is a lower bound. Quoted S1 chunk text confirmed verbatim in `knowledge/chunks/breakwave_insights_insights.jsonl` (chunk `..._in_the_sout_005`, section `...__s03_linked_asset_..._img_img_1960_8a20a313afb5_jpg`, len 1,164): contains `Northem andEastem = 34.438 35,047 37,023 115,352`, `Paraopeba (Mutuca, = 4.997 7,109 10,352 24,637`, `IRON ORE` / `78,344 704 1 1,97` / `PRODUCTION! 8,3 86,704 100,988 301,972`. Separator-mix = lower-bound class; row-splitting/scatter = second damage class (§6). Both need vision; no ground-truth claims.
- V4 — repair split, re-measured from `data/derived/asset_dispositions.jsonl` (22,106 records): **failed 91 = 89 `pdf` (86 `PDFSyntaxError` + 3 `unknown_extraction_failure`) + 2 `link`** (1 `unresolvable_relative_ref`, mirror-less `/s/congestions.png`; 1 `unknown_extraction_failure`, 2022-04-10 FFA `.html`); **all 10,894 ingested `img` carry `node_id`** (0 ingested-null; `ingested`-requires-`node_id` gate holds). NOTE: the brief's "8 link" figure is the superseded pre-D2 97-null accounting (89+8); 7 of those 8 were CNBC-linked html text assets re-matched as ingested via the `Linked asset:`-title fallback (per INVENTORY §16 D2), leaving the 2 current failed-`link`. Repair split recorded: **image ingestion is structurally healthy but low-quality** (OCR noise/skips — vision re-OCR repair) **vs linked-PDF ingestion fails outright** (89 failed, mostly `PDFSyntaxError` — extractor/parser repair, no vision needed). Two different repairs; only the first needs vision.

## 9. Reviewer E1/E2 follow-ups — D1–D4 PASS entry (2026-09-06, re-measured in-worktree, no commits)

Evidence and specification only: no scope commitment, no pipeline code changes, no asset recovered or converted.

### E2 — PDFSyntaxError attribution: 79 logged + 7 inferential; errors.jsonl under-logging is pipeline-side

- Measured: 86 disposition `failed`/`PDFSyntaxError` attributions vs 79 `PDFSyntaxError` entries in `knowledge/manifests/errors.jsonl`. Each of the 79 parent docs carries exactly one errors.jsonl entry (verified per-doc); errors.jsonl never logs per-asset file names, so one entry can directly attest only one failed asset per doc.
- The 7 surplus assets are the 2nd/3rd failed PDFs inside 6 multi-asset parent docs (one doc ×3, five ×2). They keep `reason` = `PDFSyntaxError` with weaker evidence (`.pdf` mirror + doc failed tally; all 7 mirrors self-read as `<!DOCTYPE html`, consistent with the logged sibling raising the same way). Rule encoded in `asset_dispositions.jsonl` as `attribution` (`split_skip_causes.py` docstring; replay order, deterministic): first failed sibling per parent doc = `logged` (79), further siblings sharing that single entry = `inferential` (7); failed records with no parent entry at all = `unlogged` (5 = 4 `unknown_extraction_failure` + 1 `unresolvable_relative_ref`); ingested/skipped = null. Gate still green (13,591/8,424/91, zero mismatches); regen byte-identical across reruns.
- The 7 inferential records (doc_id → mirror tail): `...2023-06-09_metals_gain_as_supply_issues_resurface` → `..._commodity-call-fine-china_bd06483485bb.pdf` AND `..._opec-saudi-arabia-makes-additional-p_d7cc7cb6c504.pdf`; `...2023-09-08_strong_chinese_imports_fails_to_excite_markets` → `..._commodity-call-light-at-the-end-of-t_b84b499b58de.pdf`; `...2024-03-22_positive_economic_data_pushes_metals_higher` → `..._commodity-call-iron-ore-s-steely-res_4dd6f116f6c9.pdf`; `...2024-04-03_strong_economic_data_boosts_sentiment` → `..._commodities-in-ten-charts-supply-cur_9a0e28388a76.pdf`; `...2024-06-24_uncertain_macro_backdrop_stymies_commodities_rally` → `..._commodity-call-lost-in-translation_9ea39700c719.pdf`; `...2024-07-15_sentiment_supported_by_strong_commodity_imports_into_china` → `..._commodity-call-lithium-not-yet-disch_d8f1cff74ab5.pdf` (all `breakwave_insights_insights_*`, full ids in dispositions).
- Pipeline-side gap, not ours: `errors.jsonl` holds 83 entries vs 91 failed assets (Δ8). Only 79 entries cover failed docs; 4 cover non-failed material (2× `[Errno 36] File name too long` on unrelated breakwave HTML docs, 2× fatal tracebacks naming `scripts/process_knowledge.py` itself); 5 failed docs have no entry at all. Per-doc logging plus non-failed entries explain the raw-count gap; asset-level coverage is 79 logged + 7 shared-entry + 5 absent.

### E1 — failed-mirror magic-byte survey + single recovery candidate (identified, NOT recovered)

- Method: self-read first 16 bytes of all 90 present failed mirrors in-worktree (91 failed − 1 mirror-less `/s/congestions.png`, the `unresolvable_relative_ref`). Result: **89 HTML** (leading `<!doctype html` / `<html`) **+ 1 PK zip** — confirms the reviewer 89/1 split; the 89 are archived error pages saved under `.pdf` names (hence `PDFSyntaxError: No /Root object!` at extraction).
- Single manual-recovery candidate (only non-HTML failed mirror; scope call pending — NOT recovered/converted): doc_id `hellenic_iron_ore_2022-01-31_2022_01_31_mmi_daily_iron_ore_index_report_january_31_2022`, href `../pdfs/2022-01-31_mmi-daily-iron-ore-index-report-january-31-2022_mmi-daily-iron-ore-report-for-31th-j_44d2e87a7411.pdf`, mirror `reports/hellenic/iron_ore/pdfs/2022-01-31_mmi-daily-iron-ore-index-report-january-31-2022_mmi-daily-iron-ore-report-for-31th-j_44d2e87a7411.pdf`, **1,013,779 bytes**, magic `PK\x03\x04`, verified valid OOXML `.docx` (67 zip entries incl `word/document.xml` + embedded `word/media/*` images). Recovery = rename-to-`.docx` + text/image extraction; everything else in the failed set is HTML error pages with nothing to recover.

### E1 — scraper-defect proposal (SPECIFICATION ONLY, no code changed)

1. Defect: `mirror_asset` (`scripts/breakwave_insights_scraper.py:229-262`, `scripts/hellenic_scraper.py:355-391`) writes any fetched payload under a URL-derived extension (`infer_asset_extension`, `scripts/source_archive_utils_v2.py:169-177`, URL suffix/link-text only, never response content).
2. `.pdf`-inferred links route to `pdfs/` (`breakwave_insights_scraper.py:318-319`, `hellenic_scraper.py:533`), so HTML error pages + the OOXML docx above land as `*.pdf` and fail downstream in `extract_linked_text_asset` (`scripts/process_knowledge.py:2374-2382`).
3. Fix location: inside BOTH `mirror_asset` functions, after download, before `write_bytes` — validate (a) `Content-Type` header contains `pdf` for `.pdf`-inferred assets (in-repo precedent: `breakwave_scraper.py:190-200` refuses html content-types), AND (b) magic bytes are `%PDF` (first 4 bytes; sniff lstrip-lower `<html`/`<!doctype` as HTML, `PK\x03\x04` as zip/Office).
4. On mismatch: do NOT archive as `.pdf` — quarantine (`_quarantine/` + digest stem + sniffed real ext `.html`/`.docx`/`.bin`), log page_url + absolute URL + expected/got content-type + magic, return `None` so the anchor keeps the absolute URL (today's mirror-miss behavior).
5. Regression sketch: mock-response unit tests on `mirror_asset` — (i) `.pdf` URL serving `text/html`+`<html` bytes → None, no `pdfs/` write, quarantined `.html`; (ii) `.pdf` URL serving `PK` bytes → quarantined `.docx`; (iii) real `%PDF` bytes → archived `.pdf` as today; (iv) the 90 failed mirrors as fixtures: 89 must classify HTML, 1 PK.

## V3. Appendix — 20-sample of OCR-skipped-small images (2026-09-06, no commits)

Population correction: the 451 with no legacy `OCR text:` header = **347 truly-skipped-small** (`[OCR skipped for small image (< 90000 pixels).]`, exact node-summary join, 0 missing over 10,894) **+ 104 others, where the 104 = 86 OCR-attempted-but-empty** (`[No OCR text detected in linked image.]`, OCR ran, zero text — many not small, e.g. 2× JPEG 2500x1667, 656x330, 481x289; 74 share one 2500x2186 RGBA duplicate) **+ 6 `[OCR unavailable; install pytesseract]` (2026 docs) + 12 new-format `[structured table]`/`[raw ocr]` nodes (HAVE OCR content — counted as OCR-bearing, not no-OCR; true no-OCR = 347 + 86 + 6 = 439; OCR-bearing = 10,443 + 12 = 10,455)**. The 347-class profile: median 120x100 px (12,000 px; range 12,000–89,856) / 12,716 bytes; 254/347 carry logo/icon/sprite/thumb/ship-like filename signals; only 85 distinct (bytes, dims) — heavy cross-doc icon duplication (262 dup instances); by source hellenic 250 + breakwave_insights 97. (Adjacent: 18 ingested-`link` image nodes + 4 `< 150000 pixels` variant nodes also carry skip markers; 6 marker nodes match no disposition, incl. one `__root` quoting the marker.)

Sample: first 20 of the 347 in disposition-ledger order (`asset_dispositions.jsonl` file order). Dims via PIL, bytes on disk; parent = tree parent-section summary (all 20 parents are data-heavy market pieces — rates/futures/volumes — but none describes the image content itself; #18/#20 parents explicitly cite the file as the article hero image). Grades: **confirm-no-data 4 / cannot-confirm-without-vision 16**.

| # | file (mirror tail) | dims = px · bytes | parent (source date) | filename signal | parent data-bearing? | grade |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | `..._img_vale-squarelogo-1416638537406_5c990e34d64f.png` | 197x200 = 39,400 · 29,086 | breakwave 2020-06-26, Brazil-tsunami-capes | "squarelogo", byte-identical ×3 (#1,6,15) | market piece; image not discussed | confirm-no-data (logo triplicate) |
| 2 | `..._img_2hsettle_d495ef717307.jpg` | 360x242 = 87,120 · 22,890 | breakwave 2020-06-30, skeptical-capesize | "settle" = settlement-price table? | YES — spot/futures rally + settlement talk | cannot-confirm-without-vision |
| 3 | `..._img_boywolf_d3a90130e6e7.jpg` | 236x323 = 76,228 · 17,472 | breakwave 2020-08-19, boy-who-cried-wolf iron ore | story illustration? | parable + $130/t iron ore context | cannot-confirm-without-vision |
| 4 | `..._img_ironroe2_9e95a4cdceb6.jpg` | 300x168 = 50,400 · 9,903 | same doc | thumbnail, byte-identical w/ #20 | same | cannot-confirm-without-vision |
| 5 | `..._img_simandou_92431aef2043.jpg` | 275x183 = 50,325 · 19,785 | same doc | Simandou mine photo? | same | cannot-confirm-without-vision |
| 6 | `..._img_vale_a2ba1d038768.png` | 197x200 = 39,400 · 29,086 | breakwave 2020-09-23, Q4-futures-above-20k | logo, byte-identical | futures>20k market piece | confirm-no-data (logo triplicate) |
| 7 | `..._img_porto-teluk_7197bca2bc4b.jpg` | 340x240 = 81,600 · 20,196 | breakwave 2020-10-20, Vale-where-is-iron-ore | port photo? ("teluk" = bay) | YES — Vale production/sales figures | cannot-confirm-without-vision |
| 8 | `..._img_settlements_7170d416cb42.png` | 303x289 = 87,567 · 5,294 | breakwave 2020-10-28, Q1-rates-surprise | "settlements" table? flat-color bytes | YES — Q1 futures-curve discussion | cannot-confirm-without-vision |
| 9 | `..._img_klav21_c2fd1136c2b5.png` | 624x116 = 72,384 · 14,508 | breakwave 2020-10-30, outlook-iron-ore | "klav" = Klaveness strip; wide-short aspect | YES — iron-ore fundamentals (Klaveness series) | cannot-confirm-without-vision |
| 10 | `..._img_klav22_cbeb861d00f7.png` | 624x124 = 77,376 · 19,003 | same doc | same | same | cannot-confirm-without-vision |
| 11 | `..._img_klav12_4c117816b8b9.png` | 268x250 = 67,000 · 23,075 | breakwave 2020-11-06, outlook-coal | same series | coal fundamentals | cannot-confirm-without-vision |
| 12 | `..._img_klav16_027ab0f6f533.png` | 245x186 = 45,570 · 23,642 | same doc | same series | same | cannot-confirm-without-vision |
| 13 | `..._img_tableseason_64f1661d2f43.png` | 717x121 = 86,757 · 7,593 | breakwave 2020-11-11, turnaround-turkey-day | NAME SAYS TABLE; wide strip | YES — capesize spot/charterer discussion | cannot-confirm-without-vision (most suspicious) |
| 14 | `..._img_klav22_5359dab6db3e.png` | 633x90 = 56,970 · 15,489 | breakwave 2020-11-13, outlook-grains | wide strip | grains outlook | cannot-confirm-without-vision |
| 15 | `..._img_vale_3b19f8b50dd0.png` | 197x200 = 39,400 · 29,086 | breakwave 2020-12-09, brazil-iron-ore | logo, byte-identical | market piece | confirm-no-data (logo triplicate) |
| 16 | `..._img_valeironore_8df1d54c5aac.jpg` | 299x168 = 50,232 · 13,654 | breakwave 2020-12-21, commodities-regaining | thumbnail | commodities piece (Ulf Bergman) | cannot-confirm-without-vision |
| 17 | `..._img_valeironore2_073efe0c6191.jpg` | 299x168 = 50,232 · 10,088 | breakwave 2021-01-12, capesizes-where-next | thumbnail, byte-identical w/ #18 | capesize rally piece | cannot-confirm-without-vision |
| 18 | same file, different doc 2021-01-27, xi-industrial-records | 299x168 = 50,232 · 10,088 | breakwave 2021-01-27 | same thumbnail | YES — cited as hero image; China IP +7.3% data piece | cannot-confirm-without-vision |
| 19 | `..._img_erlingnaess_6e808382cad7.jpg` | 172x293 = 50,396 · 11,973 | breakwave 2021-02-26, macro-opportunity | person's name + portrait aspect | macro op-ed | confirm-no-data (author portrait; residual pixel uncertainty noted) |
| 20 | same file as #4, different doc 2021-03-29, australia-exports | 300x168 = 50,400 · 9,903 | breakwave 2021-03-29 | same thumbnail | YES — cited as hero image; AUD 136bn export-value data piece | cannot-confirm-without-vision |

Grade counts: **confirm-no-data 4** (#1, #6, #15 byte-identical Vale logos; #19 named-person portrait) · **cannot-confirm-without-vision 16** (6 wide-strip/table-named graphics #2, #8–#14 most plausibly data-bearing; 10 thumbnails/photos #3–#5, #7, #16–#18, #20). Dims+names+parent context cannot rule out overlaid numbers/text on any of the 16.

Vision status (re-verified 2026-09-06): process-environment name scan returns **zero matches** for `REDUCTO/LLAMA/ANTHROPIC/OPENAI/API/KEY/TOKEN` (extended: `REPLICATE/HUGGINGFACE/AZURE/NIM/OLLAMA` also absent; 60 env vars total, none matching) — no vision keys exist **in this sandbox** (W1, §7: project-level path exists via CI NIM/Ollama secrets + a multimodal call-path addition; NOT a batch authorization), so pixel-content judgment on all 16 stays out of reach from here. No ground-truth claims in this appendix.

## 10. W2 — vision-candidate prioritization: 86 empty-OCR over 347 small-skip (ANALYSIS ONLY, 2026-09-06, no commits)

- Selection rule (reviewer W2): among true no-OCR images (439 = 347 `[OCR skipped for small image]` + 86 `[No OCR text detected in linked image.]` + 6 `[OCR unavailable; install pytesseract]`, §5), the **86 empty-OCR rank first** — OCR *ran* (image passed the `MIN_IMAGE_OCR_PIXELS` gate, `scripts/process_knowledge.py:2249-2250`) and returned zero text, so these are the likeliest silent content losses. The 347 small-skip rank second (OCR never attempted; §V3 shows most are icons/logos/thumbnails). The 6 `unavailable` are env artifacts, not content signal (W3, §11).
- Counts (re-measured in-worktree 2026-09-06: exact tree-node summary join over all 10,894 ingested-`img` node_ids → `data/derived/asset_dispositions.jsonl` node join → PIL/disk stat): **86/86 empty-OCR nodes join to a disposition with a local mirror on disk (0 missing)**; **79/86 are ≥300,000 px** (i.e. decisively NOT small — OCR genuinely found nothing); only **13 distinct (dims, bytes)** pairs across the 86 (heavy cross-doc duplication, e.g. 74 share the 2500x2186 BRS-monogram bytes per §V3).
- NO queue file created: per the brief, a `data/derived/vision_candidate_queue.jsonl` would imply batch scope, so the queue exists only as this prioritization + rule + exemplars. **No batch authorized.**
- 5 exemplar rows (first-in-class mix: 1 data-suspect filename + 2 logos + 2 large ≥2500px JPEGs; dims via PIL, bytes on disk; all `disposition=ingested`):

| # | doc_id (parent) | mirror rel | dims · bytes | note |
| --- | --- | --- | --- | --- |
| 1 | `breakwave_insights_insights_2020-10-07_2020_10_07_chinas_coal_black_box_for_shipping` | `reports/breakwave/2020/assets/2020-10-07_chinas-coal-black-box-for-shipping_img_arrowcoalprices_3cef6f2a9103.png` | 481x289 · 43,988 | filename says coal-prices arrow graphic; OCR empty |
| 2 | `breakwave_insights_insights_2023-01-24_2023_01_24_brs_dry_bulk_weekly_newsletter` | `reports/breakwave/2023/assets/2023-01-24_brs-dry-bulk-weekly-newsletter_img_logo-brs-withoutgroup_f5408ebf6fb3.jpg` | 656x330 · 24,166 | BRS logo class |
| 3 | `breakwave_insights_insights_2023-07-03_2023_07_03_brs_dry_bulk_weekly_newsletter` | `reports/breakwave/2023/assets/2023-07-03_brs-dry-bulk-weekly-newsletter_img_rgb-color-brs-shipbrokers-monogram-l_ee79606c8e74.png` | 2500x2186 · 196,130 | monogram dup family (74 instances, same bytes) |
| 4 | `breakwave_insights_insights_2023-08-26_2023_08_26_weekly_insights_review` | `reports/breakwave/2023/assets/2023-08-26_weekly-insights-review_img_unnamed284329_727e7c0b0be2.jpg` | 2500x1667 · 179,038 | large JPEG, OCR empty — genuine content question |
| 5 | `breakwave_insights_insights_2024-06-14_2024_06_14_weekly_insights_review` | `reports/breakwave/2024/assets/2024-06-14_weekly-insights-review_img_image-asset28729_d4c3a4a5f3bf.jpeg` | 2500x1667 · 287,506 | large JPEG, OCR empty — genuine content question |

## 11. W3/W4 verification notes (2026-09-06, no commits, no code changed)

- W3 — 6 `[OCR unavailable; install pytesseract]` nodes are **non-CI-env artifacts, not a live defect** (verified, not chased): the marker is emitted by the single `except Exception` handler at `scripts/process_knowledge.py:2282-2283`, which catches missing-`pytesseract`, missing tesseract binary, AND any transient OCR error identically. This sandbox has neither (INVENTORY §8 tooling verdict: tesseract/pytesseract ABSENT; Windows, no apt). CI does: both knowledge workflows apt-install `tesseract-ocr tesseract-ocr-eng poppler-utils` (`.github/workflows/daily_knowledge_update.yml:29-33`, `.github/workflows/process_knowledge.yml:52-56`) and `pip install -r requirements_knowledge.txt` (`daily_knowledge_update.yml:36`, `process_knowledge.yml:59`), which pins `pytesseract>=0.3.10` (`requirements_knowledge.txt:11`) alongside `Pillow>=10.0.0` (`:10`). CI-run OCR therefore does not hit this marker; the 6 nodes are sandbox-run artifacts.
- W4 — `google-generativeai` is a **dead dependency** (verified: `requirements_knowledge.txt:4` pins `google-generativeai>=0.5.0`, but `rg "generativeai|from google|import google" scripts/` returns **zero importers** — no match). Proposed: removal from `requirements_knowledge.txt`, but **requirements are NOT edited here — that is the main owners' call** (no pipeline client code touched per brief).
