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
- Vision remains **BLOCKED**: re-verified 2026-09-06 — zero matches for `REDUCTO/LLAMA/ANTHROPIC/OPENAI/API_KEY/LLAMACLOUD/REPLICATE/HUGGINGFACE/AZURE_OPENAI` (and no `API/KEY/TOKEN`) in process environment. Suspects above are flagged for vision check once keys are provisioned; no ground-truth correction is asserted here.

## 8. Reviewer follow-ups V1/V2/V4 (2026-09-06, re-measured in-worktree, no commits)

- V1 — summary cap is not data loss. `LINKED_TEXT_CHAR_LIMIT` verified by reading `scripts/process_knowledge.py:73`: `LINKED_TEXT_CHAR_LIMIT = int(os.environ.get("LINKED_TEXT_CHAR_LIMIT", "70000"))` → **70,000**. S1 prefix proof (§5 correction): tree summary 1,126 chars ⊂ chunk concat 1,539 chars, no truncation marker. All §5 wording that implied truncation/data-loss is corrected above; summary = display field, `chunks/` = complete.
- V2 — corruption is retrieval-layer, 228 is a lower bound. Quoted S1 chunk text confirmed verbatim in `knowledge/chunks/breakwave_insights_insights.jsonl` (chunk `..._in_the_sout_005`, section `...__s03_linked_asset_..._img_img_1960_8a20a313afb5_jpg`, len 1,164): contains `Northem andEastem = 34.438 35,047 37,023 115,352`, `Paraopeba (Mutuca, = 4.997 7,109 10,352 24,637`, `IRON ORE` / `78,344 704 1 1,97` / `PRODUCTION! 8,3 86,704 100,988 301,972`. Separator-mix = lower-bound class; row-splitting/scatter = second damage class (§6). Both need vision; no ground-truth claims.
- V4 — repair split, re-measured from `data/derived/asset_dispositions.jsonl` (22,106 records): **failed 91 = 89 `pdf` (86 `PDFSyntaxError` + 3 `unknown_extraction_failure`) + 2 `link`** (1 `unresolvable_relative_ref`, mirror-less `/s/congestions.png`; 1 `unknown_extraction_failure`, 2022-04-10 FFA `.html`); **all 10,894 ingested `img` carry `node_id`** (0 ingested-null; `ingested`-requires-`node_id` gate holds). NOTE: the brief's "8 link" figure is the superseded pre-D2 97-null accounting (89+8); 7 of those 8 were CNBC-linked html text assets re-matched as ingested via the `Linked asset:`-title fallback (per INVENTORY §16 D2), leaving the 2 current failed-`link`. Repair split recorded: **image ingestion is structurally healthy but low-quality** (OCR noise/skips — vision re-OCR repair) **vs linked-PDF ingestion fails outright** (89 failed, mostly `PDFSyntaxError` — extractor/parser repair, no vision needed). Two different repairs; only the first needs vision.

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

Vision status (re-verified 2026-09-06): process-environment name scan returns **zero matches** for `REDUCTO/LLAMA/ANTHROPIC/OPENAI/API/KEY/TOKEN` (extended: `REPLICATE/HUGGINGFACE/AZURE/NIM/OLLAMA` also absent; 60 env vars total, none matching) — no vision keys exist, so pixel-content judgment on all 16 stays out of reach. No ground-truth claims in this appendix.
