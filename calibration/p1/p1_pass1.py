"""P1 PASS 1 - EXTRACTOR (muse-spark). Native libs only. No harness import here.

Committed fixture script (M2): scrubbed to POSIX repo-relative paths (B5).
``REPO_ROOT`` derives from this file's location; outputs land beside it in
``calibration/p1/``. Logic is otherwise identical to the pre-merge temp run
(``p1_pass1_extract.json`` fixture); rerunning reproduces the recorded counts
(A raw 9x10, pass1a 8 rows, pass1b-redo 5 rows; B 13 visual fragments -> 9
parsed rows; C 5 ledger-first linked-image entries).
"""
import json
import re
from pathlib import Path

import pdfplumber
import pymupdf
from PIL import Image

REPO_ROOT = Path(__file__).resolve().parents[2]
OUT_DIR = Path(__file__).resolve().parent
WORK = REPO_ROOT
TMP = OUT_DIR
A = WORK / "reports/hellenic/demolition/pdfs/2021-07-03_best-oasis-weekly-recycling-market-report-02-july-2021_weekly-ship-recycling-report_137b264ac3ac.pdf"
B = WORK / "docs/BDRY-BWET_Form10-Q_March-31-2026.pdf"
out = {}

# ---------- Sample A ----------
doc = pymupdf.open(A)
pg6 = doc[5]
out["A"] = {"n_pages": len(doc), "page_rect": list(pg6.rect),
             "page6_text_len": len(pg6.get_text())}
tf = pg6.find_tables()
tabs = []
for ti, tab in enumerate(tf.tables):
    m = tab.extract()
    tabs.append({"bbox": list(tab.bbox), "nrows_raw": len(m),
                 "ncols": len(m[0]), "matrix": m})
out["A"]["pymupdf_tables_p6"] = [
    {k: v for k, v in t.items() if k != "matrix"} for t in tabs]
raw = tabs[0]["matrix"]
out["A"]["raw_nrows"] = len(raw); out["A"]["raw_ncols"] = len(raw[0])
out["A"]["raw_matrix"] = [[(c or "") for c in r] for r in raw]
# naive first attempt: headers=row0, data=rows1.. (keeps header-fragment rows)
out["A"]["pass1a"] = {"headers": [(c or "") for c in raw[0]],
                      "rows": [[(c or "") for c in r] for r in raw[1:]]}
# redo: repair multi-line header (fold rows 1-3 fragments), drop fragment rows
frags = {}
for r in raw[1:4]:
    for ci, c in enumerate(r):
        c = (c or "").strip()
        if c: frags[ci] = frags.get(ci, "") + (" " if frags.get(ci) else "") + c
headers = []
for ci, h in enumerate(raw[0]):
    h = (h or "").replace("\n", " ").strip()
    if frags.get(ci): h = (h + " " + frags[ci]).strip()
    headers.append(h)
# ruling-split repair: the 'Year of Build' label fragment landed in col 3
# while the year values sit in col 2 -> move label onto the data column
if headers[2] == "" and headers[3] != "":
    headers[2], headers[3] = headers[3], ""
out["A"]["pass1b"] = {"headers": headers,
                      "rows": [[(c or "").replace("\n", " ") for c in r]
                               for r in raw[4:]]}
# manual source-page vessel count (human read of p6): vessel names in data rows
out["A"]["manual_vessel_names"] = [r[0] for r in raw[4:]]
doc.close()

# ---------- Sample B : BDRY futures block, PDF p6 (index 5) ----------
with pdfplumber.open(str(B)) as pdf:
    print("B pages:", len(pdf.pages))
    out["B"] = {"n_pages": len(pdf.pages)}
    pg = pdf.pages[5]
    vt = pg.extract_tables() or []
    out["B"]["pymupdf_style_visual_tables_p6"] = [
        {"nrows": len(t), "ncols": len(t[0])} for t in vt]
    text = pg.extract_text()
    lines = text.split("\n")
out["B"]["p6_n_text_lines"] = len(lines)
# block: lines 16..36 (BDRY futures header + 9 contracts + subtotal at 37)
block = lines[16:37]
joined, buf = [], ""
for ln in lines[19:37]:
    buf = (buf + " " + ln).strip() if buf else ln
    if re.search(r"\d%\s*$", ln):
        joined.append(buf); buf = ""
out["B"]["block_text_lines"] = block
out["B"]["joined_candidate_rows"] = joined
pat = re.compile(r"^(.*\(\d+ contracts\))\s+\$?\s*(\([\d,]+\)|[\d,]+)\s+\$?\s*([\d,]+)\s+(\d+%)$")
rows, fails = [], []
for j in joined:
    m = pat.match(j.strip())
    if m: rows.append([m.group(1).strip(), m.group(2), m.group(3), m.group(4)])
    else: fails.append(j)
out["B"]["pass1"] = {
    "headers": ["Futures contract (Apr-Jun 2026 expiries)",
                "Unrealized appreciation/(depreciation) ($)",
                "Notional value ($)", "% of capital"],
    "rows": rows, "unparsed": fails,
    "subtotal_line": lines[37] if len(lines) > 37 else ""}
out["B"]["statement"] = ("Combined Schedules of Investments / BREAKWAVE DRY BULK "
                         "SHIPPING ETF futures contracts, March 31, 2026 (Unaudited), PDF p6")

# ---------- Sample C : 5 image-type skipped-queue entries ----------
p = WORK / "knowledge/manifests/documents.jsonl"
entries = []
with open(str(p), encoding="utf-8") as f:
    for line in f:
        d = json.loads(line)
        if d.get("linked_assets_skipped", 0) > 0:
            tp = WORK / d["tree_path"]
            if not tp.exists(): continue
            t = json.loads(tp.read_text(encoding="utf-8"))
            for k in t.get("children", []):
                if k.get("section_type") == "linked_image_asset":
                    s = k.get("summary") or ""
                    rel = s.split("Source asset:")[-1].strip().split()[0]
                    entries.append({"parent_doc_id": d["doc_id"],
                                    "node_id": k.get("node_id"),
                                    "rel": rel})
                    if len(entries) >= 5: break
        if len(entries) >= 5: break
for e in entries:
    fp = WORK / e["rel"]
    e["exists"] = fp.exists()
    if fp.exists():
        im = Image.open(fp)
        e.update({"format": im.format, "width": im.width,
                  "height": im.height, "mode": im.mode,
                  "bytes": fp.stat().st_size})
out["C"] = {"entries": entries}

(TMP / "p1_pass1_extract.json").write_text(json.dumps(out, indent=1), encoding="utf-8")
print("A raw:", out["A"]["raw_nrows"], "x", out["A"]["raw_ncols"],
      "| pass1a rows:", len(out["A"]["pass1a"]["rows"]),
      "| pass1b rows:", len(out["A"]["pass1b"]["rows"]))
print("A pass1b headers:", out["A"]["pass1b"]["headers"])
print("B visual tables p6:", out["B"]["pymupdf_style_visual_tables_p6"])
print("B joined:", len(joined), "parsed:", len(rows), "unparsed:", len(fails))
print("B subtotal:", out["B"]["pass1"]["subtotal_line"][:80])
for e in entries:
    print("C:", e.get("format"), e.get("width"), "x", e.get("height"),
          e.get("bytes"), e["rel"][-60:], "|", e["parent_doc_id"][:50])
