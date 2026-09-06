# Ingested-image audit — muse-spark

- Author: muse-spark
- Date: 2026-09-06
- Branch: agent/muse-spark (worktree `C:\Users\Dell\Github\shipping-muse-spark`, no commits)
- Inputs: `data/derived/asset_dispositions.jsonl` + disk + `knowledge/manifests/documents.jsonl` + `knowledge/trees/` shards (read-only; throwaway scripts under `C:\Users\Dell\AppData\Local\Temp\opencode`)
- Scope: evidence only. **Reprocessing (P0 reversal / vision re-OCR) scope awaits the user's call — no scope commitment in this document.**

## 1. Which set is profiled

- Ledger (`documents.jsonl`): discovered 22,106 = mirrored 13,716 + skipped 8,424 + failed 91; ingested **13,591** (breakwave_insights 8,859 + hellenic 4,732).
- Dispositions (`asset_dispositions.jsonl`, 22,106 records = ledger discovered): ingested-proxy **13,681** (breakwave_insights 8,925 + hellenic 4,756) + skipped 8,425 (8,341 `unresolvable_external` + 48 `non_content_link` + 35 `duplicate_path` + 1 null failed-branch).
- Reconciliation: mirrored 13,716 = disposition-ingested 13,681 + `duplicate_path` 35 (all mirrors on disk); disposition-ingested 13,681 = ledger-ingested 13,591 + 90 extract-time failures indistinguishable in a read-only replay (script docstring) + 1 failed-branch counted as skipped/null. Profile below covers the **13,681 disposition-ingested mirrors** (the superset containing the 13,591 ledger-ingested set); ledger figures are cited for reference.

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
- `summary` content length (OCR portion after `OCR text:` for images; text after `Source asset:` header otherwise): n=13,584, median **245**, mean 375.1, min 1, max 1,100, p10 100 / p25 163 / p75 485 / p90 952; buckets `1-99` 1,357 · `100-499` 8,927 · `500-1999` 3,300 · `0` 0. Per kind medians: `img` 209 (mean 282) · `link` 350 (mean 335) · `pdf` 1,039 (mean 826; summaries truncated ~1,100 chars).
- Image OCR specifically: 10,442 / 10,894 with non-empty OCR text (95.9%); 451 with `[OCR skipped for small image (< 90000 pixels)]` (logos/icons, no `OCR text:` marker); 1 with marker but only-pipe OCR (`...arrowcoalinventories...png`). The 228 mixed-separator candidates below come from the 10,442 OCR-bearing images.

## 6. Corruption suspects (image OCR, vision check required — no ground truth asserted)

Method: regex scan of the 10,442 image OCR strings for `\d{1,3}\.\d{3}` co-occurring with `\d{1,3},\d{3}` (mixed thousands separators in one chart/table) or with `\d+\.\d{2}` (3-decimal vs 2-decimal decimals); 228 mixed-separator candidates repo-wide. Three are recorded below as **suspects needing vision check** — spellings/numbers are quoted exactly as stored; corrections are not asserted.

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
