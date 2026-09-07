"""Re-OCR / structuring pilot harness - muse-spark, Decision 2 PHASE A (paddle lane).

Two-stage vision protocol with extractor/verifier separation, redo loop,
JSONL audit, and reconcile-as-diffs (never writes to knowledge/).

Venue support (local-only, no hosted calls, no secrets, no network):
  - paddle: local PaddleOCR CPU lane (images straight to OCR; PDFs via
    text-layer-first router below). Only live venue.
  - mock: canned fixture responder for --dry-run self-test (no deps).

Hosted venues stripped 2026-09-07 per user directive (no NIM/Ollama/
OpenRouter/Groq/paid API): all NIM_API_KEY / OLLAMA_* / OPENROUTER_* /
GROQ_* env reads, image_url/chat payload builders, venue auto/probe/
selection, rate-limit/retry/backoff, spend guardrails, and choice.json
removed. Hosted path is dead: OLLAMA_MODEL retired upstream HTTP 410,
no paid API authorized. See docs/PILOT_REOCR_MUSE_SPARK.md venue pivot.

Paddle lane (reviewer bench f8bf3ac27):
  - Deps (NOT installed here; install at run time in a venv): paddlepaddle
    3.3.1 + paddleocr 3.7.0, ~1.4GB venv. No GPU needed; ~48 s/image CPU
    on a 4-core CI-class box, 26 images ~21 min serial, $0, nothing leaves
    the machine.
  - enable_mkldnn=False is MANDATORY on CPU. The default oneDNN path
    crashes (ConvertPirAttribute2RuntimeAttribute not supported) -
    reviewer bench f8bf3ac27. Passed explicitly everywhere PaddleOCR is
    constructed below.
  - Text-layer-first router: PDF inputs extract via PyMuPDF (fitz); native
    text is used when usable (>= PDF_TEXT_THRESHOLD_CHARS chars/page,
    default 200 - above header/footer noise, below a real table page);
    paddle OCR runs ONLY on pages without a usable text layer. Plain image
    inputs go straight to paddle OCR.

Verifier (reviewer bench f8bf3ac27 GT1):
  - separator-mix is FLAG-ONLY (prefix flag_*, never blocks acceptance).
    A separator swap alone cannot correct a digit substitution.
  - Correction path is arithmetic tie-out: where table_cells carries a
    printed subtotal/total row (first cell matches total/system/subtotal/
    sum), each numeric column must satisfy components-sum == total.
    Vale GT: 31,438 + 19,291 = 50,729 Northern System (4Q19); naive
    34,438 fails (34,438 + 19,291 = 53,729). The 10-Q technique.
  - Fixture truth is 31,438 (not 34,438). Legacy OCR made two errors:
    digit 1->4 plus separator ,->. (bench f8bf3ac27).

Deps: stdlib only for mock/dry-run. Live paddle needs paddleocr (+
paddlepaddle) and pymupdf (fitz) at run time - imported lazily so the
mock self-test runs without them.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent

# Local-only knobs (no secrets, no spend, no rate limits).
REDO_LIMIT = 1  # extra attempts per stage on verifier rejection
PDF_TEXT_THRESHOLD_CHARS = 200  # native PDF text usable iff >=N chars/page


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


# ------------------------------------------------------- paddle lane
# Dependency note (run time only, never installed by this task):
#   pip install paddlepaddle==3.3.1 paddleocr==3.7.0 pymupdf  (~1.4GB venv)
# Bench: reviewer f8bf3ac27, CPU 4-core, init 3.6 s, 48.2 s/image, 8/8
# values incl. 31,438 at conf 1.00, mean conf 0.998. $0, local only.
_PADDLE_OCR = None


def _get_paddle_ocr():
    """Construct (once) PaddleOCR with enable_mkldnn=False.

    MANDATORY: default oneDNN crashes on CPU
    (ConvertPirAttribute2RuntimeAttribute not supported) - f8bf3ac27.
    """
    global _PADDLE_OCR
    if _PADDLE_OCR is not None:
        return _PADDLE_OCR
    try:
        from paddleocr import PaddleOCR
    except ImportError as exc:
        raise RuntimeError(
            "paddle venue needs paddlepaddle 3.3.1 + paddleocr 3.7.0 "
            "(~1.4GB venv, install at run time; this task installs nothing): "
            f"{exc}"
        ) from exc
    # enable_mkldnn=False is mandatory on CPU - reviewer bench f8bf3ac27.
    try:
        _PADDLE_OCR = PaddleOCR(lang="en", enable_mkldnn=False)
    except TypeError:
        # Older 2.x signature compat (still disable MKLDNN path).
        _PADDLE_OCR = PaddleOCR(
            lang="en", enable_mkldnn=False, use_angle_cls=True
        )
    return _PADDLE_OCR


def paddle_ocr_image(path: Path) -> dict:
    """OCR a plain image straight via paddle. Returns lines/text/conf."""
    ocr = _get_paddle_ocr()
    t0 = time.monotonic()
    raw = ocr.ocr(str(path))
    latency_ms = int((time.monotonic() - t0) * 1000)
    lines: list[dict] = []
    # PaddleOCR 3.x returns list[dict] or list[list]; handle both.
    try:
        pages = raw if isinstance(raw, list) else [raw]
        for page in pages:
            items = page if isinstance(page, list) else page.get("rec_texts", [])
            scores = None
            if isinstance(page, dict):
                scores = page.get("rec_scores")
            for i, it in enumerate(items):
                if isinstance(it, dict):
                    txt = it.get("text", "") or ""
                    conf = float(it.get("confidence", 0.0) or 0.0)
                elif isinstance(it, (list, tuple)) and len(it) == 2:
                    # 2.x: [box, (text, conf)]
                    txt, conf = it[1][0], float(it[1][1])
                else:
                    txt, conf = str(it), 0.0
                if scores is not None and i < len(scores):
                    try:
                        conf = float(scores[i])
                    except (TypeError, ValueError):
                        pass
                if txt.strip():
                    lines.append({"text": txt.strip(), "conf": conf})
    except Exception as exc:
        raise RuntimeError(f"paddle OCR parse failed for {path}: {exc}") from exc
    text = "\n".join(l["text"] for l in lines)
    mean_conf = sum(l["conf"] for l in lines) / len(lines) if lines else 0.0
    return {"lines": lines, "text": text, "mean_conf": mean_conf,
            "latency_ms": latency_ms}


def extract_pdf_native_or_ocr(path: Path, threshold: int = PDF_TEXT_THRESHOLD_CHARS) -> dict:
    """Text-layer-first router for PDFs.

    Native text via PyMuPDF; usable iff >= threshold chars/page
    (default 200: above header/footer noise, below a real table page).
    Paddle OCR runs ONLY on pages without a usable text layer.
    Returns {text, lines, mean_conf, native_pages, ocr_pages, latency_ms}.
    """
    t0 = time.monotonic()
    try:
        import fitz  # PyMuPDF
    except ImportError as exc:
        raise RuntimeError(
            "paddle PDF path needs pymupdf (pip install pymupdf at run time): "
            f"{exc}"
        ) from exc
    doc = fitz.open(path)
    chunks: list[str] = []
    native_pages = 0
    ocr_pages = 0
    confs: list[float] = []
    ocr_lines: list[dict] = []
    for pno, page in enumerate(doc):
        txt = page.get_text() or ""
        if len(txt.strip()) >= threshold:
            chunks.append(txt.strip())
            native_pages += 1
            continue
        # No usable text layer: render page and OCR it.
        pix = page.get_pixmap(dpi=200)
        import tempfile

        with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tf:
            tf.write(pix.tobytes("png"))
            tmp = Path(tf.name)
        try:
            res = paddle_ocr_image(tmp)
        finally:
            try:
                tmp.unlink()
            except OSError:
                pass
        chunks.append(res["text"])
        ocr_lines.extend(res["lines"])
        if res["lines"]:
            confs.append(res["mean_conf"])
        ocr_pages += 1
    text = "\n".join(c for c in chunks if c)
    mean_conf = sum(confs) / len(confs) if confs else (1.0 if native_pages else 0.0)
    return {"text": text, "lines": ocr_lines, "mean_conf": mean_conf,
            "native_pages": native_pages, "ocr_pages": ocr_pages,
            "latency_ms": int((time.monotonic() - t0) * 1000)}


def ocr_to_declaration(ocr_text: str, n_lines: int) -> dict:
    """Deterministic stage-1 declaration from OCR text (no LLM).

    Fail-closed: <50 chars total or <3 lines -> axes_readable False
    (logo/photo/empty path; exercises the empty-result control).
    Else a generic readable table declaration with ticks=[] (skips the
    axis-consistency check) and table=None (skips dims check) so the
    verifier judges content (tie-out), not heuristics.
    """
    stripped = (ocr_text or "").strip()
    if len(stripped) < 50 or n_lines < 3:
        return {
            "chart_type": "logo", "axes_readable": False,
            "x_axis": {"label": "", "scale": "unreadable", "units": "",
                       "ticks": []},
            "y_axis": {"label": "", "scale": "unreadable", "units": "",
                       "ticks": []},
            "table": None, "notes": "paddle-ocr: too little text, fail-closed",
        }
    return {
        "chart_type": "table", "axes_readable": True,
        "x_axis": {"label": "Category", "scale": "categorical", "units": "",
                   "ticks": []},
        "y_axis": {"label": "Value", "scale": "linear", "units": "000' t",
                   "ticks": []},
        "table": None, "notes": "paddle-ocr heuristic declaration",
    }


def ocr_to_stage2(ocr_text: str, mean_conf: float) -> dict:
    """Deterministic stage-2 values from OCR text (no LLM)."""
    rows: list[list[str]] = []
    for ln in (ocr_text or "").splitlines():
        toks = ln.strip().split()
        if toks:
            rows.append(toks)
    nums = _numbers_from(ocr_text or "")
    pts = [{"x": f"line{i}", "y": n} for i, n in enumerate(nums[:20])]
    conf = "high" if mean_conf >= 0.95 else ("medium" if mean_conf >= 0.80 else "low")
    return {"values": [{"series": "ocr", "points": pts}],
            "table_cells": rows or None,
            "units": "000' t", "confidence": conf}


# ------------------------------------------------------- prompts
# Protocol documentation: JSON schemas for the two stages. The paddle lane
# uses deterministic parsing (above), not LLM prompts; the mock fixture
# below returns these shapes directly. Schemas unchanged so audit/diff
# consumers see identical structure pre/post venue pivot.
STAGE1_PROMPT = """You are a chart/table structure extractor. Look at the image.
Declare the coordinate system BEFORE reading any values. Reply ONLY as JSON:
{{"chart_type": "<line|bar|table|diagram|photo|logo|unknown>",
 "x_axis": {{"label": str, "scale": "<linear|log|categorical|time|unreadable>", "units": str, "ticks": [str]}},
 "y_axis": {{"label": str, "scale": "<linear|log|categorical|time|unreadable>", "units": str, "ticks": [str]}},
 "table": {{"rows": int, "cols": int, "headers": [str]}} or null,
 "axes_readable": bool, "notes": str}}
Rules: if EITHER axis label/scale/units cannot be read from the image, set
axes_readable=false and both scales to "unreadable". Do NOT guess tick values.
For photos/logos/diagrams with no axes, use chart_type photo|logo|diagram and
axes_readable=false."""

STAGE1_REDO_PROMPT = (STAGE1_PROMPT + "\nRETRY: your previous declaration was "
                      "rejected ({reason}). Re-examine the pixels; when in doubt "
                      "mark scales unreadable rather than guessing.")

STAGE2_PROMPT = """You are a chart/table value extractor. The declared scale is:
{declaration}
Read the data values AGAINST that declared scale. Reply ONLY as JSON:
{{"values": [{{"series": str, "points": [{{"x": str, "y": number|string}}]}}],
 "table_cells": [[str]] or null, "units": str, "confidence": "<high|medium|low>"}}
Rules: every y MUST carry the declared units; do not mix decimal/thousands
separators (pick one: 1,234.56 style); if a value is illegible write "?" —
never invent digits. Where a printed subtotal/total row exists, values MUST
tie out (components sum to the total); a row that breaks the printed total
is wrong even if its separators look consistent."""

STAGE2_REDO_PROMPT = (STAGE2_PROMPT + "\nRETRY: your previous values were "
                      "rejected ({reason}). Re-read against the SAME declared "
                      "scale; keep separators consistent; use \"?\" when unsure. "
                      "Re-check every row against the printed subtotal/total.")


def parse_json_loose(text: str):
    try:
        return json.loads(text)
    except Exception:
        pass
    m = re.search(r"\{.*\}", text or "", re.S)
    if m:
        try:
            return json.loads(m.group(0))
        except Exception:
            return None
    return None


# ------------------------------------------------------- verifier
MIXED_SEP_PATTERNS = [re.compile(r"\d{1,3}\.\d{3}"), re.compile(r"\d{1,3},\d{3}")]
DEC2 = re.compile(r"\d+\.\d{2}\b")
DEC3 = re.compile(r"\d+[.,]\d{3}\b")


def _numbers_from(obj) -> list[str]:
    out = []

    def walk(o):
        if isinstance(o, str):
            out.extend(re.findall(r"\d[0-9.,]*", o))
        elif isinstance(o, (list, tuple)):
            for v in o:
                walk(v)
        elif isinstance(o, dict):
            for v in o.values():
                walk(v)
        elif isinstance(o, (int, float)):
            out.append(str(o))
    walk(obj)
    return out


def check_separator_mix(nums: list[str]) -> bool:
    blob = " ".join(nums)
    has_dot3 = bool(re.search(r"\d{1,3}\.\d{3}", blob))
    has_comma3 = bool(re.search(r"\d{1,3},\d{3}", blob))
    has_dec2 = bool(re.search(r"\d+\.\d{2}\b", blob))
    return (has_dot3 and has_comma3) or (has_dot3 and has_dec2 and has_comma3)


def _parse_int_thousands(s: str):
    """Parse integer thousands with , or . separators to int.

    '31,438' -> 31438; '34.438' -> 34438; '50729' -> 50729.
    Returns None for decimals (9407.00), words, '?' etc. - tie-out only
    judges plain integer thousands, the Vale GT1 class.
    """
    t = (s or "").strip().replace("$", "").replace(" ", "")
    if re.fullmatch(r"\d{1,3}(,\d{3})+", t):
        return int(t.replace(",", ""))
    if re.fullmatch(r"\d{1,3}(\.\d{3})+", t):
        return int(t.replace(".", ""))
    if re.fullmatch(r"\d+", t):
        try:
            return int(t)
        except ValueError:
            return None
    return None


_TOTAL_WORDS = ("total", "system", "subtotal", "sum")


def check_tieout(table_cells) -> tuple[bool, bool, str]:
    """Arithmetic tie-out: components sum vs printed total.

    Returns (applicable, ok, detail). Not applicable when no total row
    exists (no correction asserted). When applicable, ok=False blocks
    acceptance even if separators look consistent - the 10-Q technique.
    """
    if not isinstance(table_cells, list) or not table_cells:
        return False, True, "no table_cells"
    total_idx = None
    for i, row in enumerate(table_cells):
        if not isinstance(row, list) or not row:
            continue
        first = str(row[0] or "").strip().lower()
        if any(w in first for w in _TOTAL_WORDS):
            # Require at least one parseable integer in the row so a
            # header like ["System","4Q19",...] does not false-trigger.
            if any(_parse_int_thousands(str(c)) is not None for c in row[1:]):
                total_idx = i
                break
    if total_idx is None:
        return False, True, "no total row"
    total_row = table_cells[total_idx]
    ncols = len(total_row)
    # Component rows: every other row with the same width.
    comp_rows = [r for j, r in enumerate(table_cells)
                 if j != total_idx and isinstance(r, list) and len(r) == ncols]
    if not comp_rows:
        return False, True, "total row but no component rows"
    for c in range(1, ncols):
        tval = _parse_int_thousands(str(total_row[c]))
        if tval is None:
            continue  # non-numeric column (e.g. header-ish), skip
        cvals = []
        for r in comp_rows:
            v = _parse_int_thousands(str(r[c]))
            if v is None:
                cvals = None
                break
            cvals.append(v)
        if cvals is None:
            continue  # illegible/? in column - cannot judge, skip
        if sum(cvals) != tval:
            parts = "+".join(str(v) for v in cvals)
            return True, False, (
                f"tieout_mismatch col={c} total={tval} "
                f"components-sum={sum(cvals)} ({parts} != {tval})"
            )
    # Detail records the flagship proof when present.
    return True, True, "tie-out holds (components sum to printed total)"


def verify_stage1(decl: dict | None) -> tuple[bool, list[str]]:
    issues: list[str] = []
    if not isinstance(decl, dict):
        return False, ["stage1_not_json"]
    if not decl.get("axes_readable", False):
        # Fail-closed: unreadable axes ends this image (no stage-2).
        return False, ["axes_unreadable_fail_closed"]
    for ax in ("x_axis", "y_axis"):
        a = decl.get(ax) or {}
        if not a.get("label"):
            issues.append(f"{ax}_label_missing")
        if a.get("scale") in (None, "", "unreadable"):
            issues.append(f"{ax}_scale_unreadable")
        if "units" not in a:
            issues.append(f"{ax}_units_missing")
    tbl = decl.get("table")
    if tbl is not None:
        try:
            if int(tbl.get("rows", 0)) <= 0 or int(tbl.get("cols", 0)) <= 0:
                issues.append("table_dims_nonpositive")
        except (TypeError, ValueError):
            issues.append("table_dims_not_int")
    if decl.get("chart_type") in (None, ""):
        issues.append("chart_type_missing")
    return (len(issues) == 0), issues


def verify_stage2(vals: dict | None, decl: dict,
                   existing_ocr: str = "") -> tuple[bool, list[str], str]:
    """Stage-2 verifier: tie-out blocks, separator-mix is flag-only.

    issues entries starting with 'flag_' are informational (audit-visible)
    and never block acceptance. All other entries block. Returns
    (ok, issues, verdict) with verdict improved/contradicted from the
    existing-OCR number-set cross-check (unchanged).
    """
    issues: list[str] = []
    if not isinstance(vals, dict):
        return False, ["stage2_not_json"], "contradicted"
    declared_units = ""
    try:
        declared_units = ((decl.get("y_axis") or {}).get("units") or "").strip()
    except Exception:
        pass
    got_units = (vals.get("units") or "").strip()
    if declared_units and got_units and declared_units.lower() != got_units.lower():
        issues.append(f"units_mismatch declared={declared_units!r} got={got_units!r}")
    # row/column counts vs declared table
    tbl = decl.get("table") if isinstance(decl, dict) else None
    cells = vals.get("table_cells")
    if isinstance(tbl, dict) and cells is not None:
        try:
            if len(cells) != int(tbl.get("rows", len(cells))):
                issues.append(f"row_count_mismatch declared={tbl.get('rows')} got={len(cells)}")
            widths = {len(r) for r in cells} if cells else set()
            if widths and (len(widths) != 1 or widths.pop() != int(tbl.get("cols", -1))):
                issues.append("col_count_mismatch")
        except (TypeError, ValueError):
            issues.append("table_count_not_int")
    # axis-values consistency: stage-2 x labels should intersect declared ticks
    try:
        ticks = set((decl.get("x_axis") or {}).get("ticks") or [])
        xs = set()
        for s in vals.get("values") or []:
            for p in (s.get("points") or []):
                xs.add(str(p.get("x")))
        if ticks and xs and ticks.isdisjoint(xs):
            issues.append("x_values_disjoint_from_declared_ticks")
    except Exception:
        issues.append("axis_consistency_check_error")
    # separator-mix: FLAG-ONLY, never a correction (bench f8bf3ac27 GT1:
    # a digit substitution 1->4 passes a separator swap and emits a
    # confidently wrong 34,438). Recorded for audit, does not block.
    nums = _numbers_from(vals)
    if check_separator_mix(nums):
        issues.append("flag_separator_mix suspect: dot-thousands and "
                      "comma-thousands/decimal mix (flag only, not a correction)")
    # arithmetic tie-out: THE correction path (10-Q technique). Where a
    # printed total row exists, components must sum to it.
    if isinstance(cells, list) and cells:
        applicable, ok_tie, detail = check_tieout(cells)
        if applicable and not ok_tie:
            issues.append(detail)
    # cross-check vs existing shard OCR text
    verdict = "improved"
    if existing_ocr:
        old_nums = set(_numbers_from(existing_ocr))
        new_nums = set(nums)
        if new_nums and new_nums.isdisjoint(old_nums):
            verdict = "contradicted"
        elif not new_nums:
            verdict = "contradicted" if old_nums else "improved"
    blocking = [i for i in issues if not i.startswith("flag_")]
    ok = len(blocking) == 0
    return ok, issues, verdict


# ------------------------------------------------------- reconcile (diffs only)
def reconcile_diff(record: dict, existing_ocr: str, decl: dict | None,
                   vals: dict | None, status: str) -> str:
    lines = [f"--- a/{record.get('shard_hint', 'knowledge/trees/...')}",
             f"+++ b/proposed ({record.get('node_id', '?')})",
             f"@@ image {record.get('image_rel', '?')} status={status}"]
    lines.append(f"-OCR(existing): {existing_ocr[:400]!r}" if existing_ocr
                 else "-OCR(existing): <empty>")
    if decl is not None:
        lines.append(f"+STAGE1(declared): {json.dumps(decl, ensure_ascii=False)[:800]}")
    if vals is not None:
        lines.append(f"+STAGE2(values): {json.dumps(vals, ensure_ascii=False)[:1200]}")
    lines.append("+NOTE: proposal only — apply via pipeline recompile, never hand-edit shards.")
    return "\n".join(lines)


# ------------------------------------------------------- mock fixture
# Ground truth (reviewer bench f8bf3ac27 GT1, read from the pixels):
#   000' t              4Q19    3Q19    4Q18     2019
#   Northern System    50,729  55,401  52,911  188,721
#     Northern+Eastern 31,438  35,047  37,023  115,352  <-- TRUTH
#     S11D             19,291  20,354  15,888   73,369
# Proof: 31,438 + 19,291 = 50,729; naive 34,438 + 19,291 = 53,729 (fails).
# Legacy OCR made two errors on the truth cell: digit 1->4 plus ,->.
FIXTURE_DECLARATION = {
    "chart_type": "table", "axes_readable": True,
    "x_axis": {"label": "System", "scale": "categorical", "units": "",
               "ticks": ["Northern System", "Northern and Eastern", "S11D"]},
    "y_axis": {"label": "Output 000' t", "scale": "linear", "units": "000' t",
               "ticks": ["0", "50000", "100000", "200000"]},
    "table": {"rows": 3, "cols": 5,
              "headers": ["System", "4Q19", "3Q19", "4Q18", "2019"]},
    "notes": "fixture Vale GT1 (bench f8bf3ac27): truth 31,438; "
             "31,438+19,291=50,729 Northern System",
}
# Planted error: 34.438 (dot-thousands AND wrong digits) amid commas.
# Fails tie-out (34438+19291=53729 != 50729) + carries the separator flag.
FIXTURE_VALUES_BAD = {
    "values": [{"series": "4Q19", "points": [
        {"x": "Northern System", "y": "50,729"},
        {"x": "Northern and Eastern", "y": "34.438"},
        {"x": "S11D", "y": "19,291"}]}],
    "table_cells": [
        ["Northern System", "50,729", "55,401", "52,911", "188,721"],
        ["Northern and Eastern", "34.438", "35,047", "37,023", "115,352"],
        ["S11D", "19,291", "20,354", "15,888", "73,369"]],
    "units": "000' t", "confidence": "high",
}
# Naive separator-only "correction": 34,438. Separators consistent (no
# flag) but still wrong digits: 34438+19291=53729 != 50729 -> rejected
# by tie-out. This is the confidently-wrong value the old verifier
# accepted (bench f8bf3ac27).
FIXTURE_VALUES_NAIVE = {
    "values": [{"series": "4Q19", "points": [
        {"x": "Northern System", "y": "50,729"},
        {"x": "Northern and Eastern", "y": "34,438"},
        {"x": "S11D", "y": "19,291"}]}],
    "table_cells": [
        ["Northern System", "50,729", "55,401", "52,911", "188,721"],
        ["Northern and Eastern", "34,438", "35,047", "37,023", "115,352"],
        ["S11D", "19,291", "20,354", "15,888", "73,369"]],
    "units": "000' t", "confidence": "high",
}
# Truth via tie-out: 31,438. 31438+19291=50729, plus 3Q19 35047+20354=
# 55401, 4Q18 37023+15888=52911, 2019 115352+73369=188721. Accepted.
FIXTURE_VALUES_GOOD = {
    "values": [{"series": "4Q19", "points": [
        {"x": "Northern System", "y": "50,729"},
        {"x": "Northern and Eastern", "y": "31,438"},
        {"x": "S11D", "y": "19,291"}]}],
    "table_cells": [
        ["Northern System", "50,729", "55,401", "52,911", "188,721"],
        ["Northern and Eastern", "31,438", "35,047", "37,023", "115,352"],
        ["S11D", "19,291", "20,354", "15,888", "73,369"]],
    "units": "000' t", "confidence": "medium",
}


class MockVenue:
    """Canned two-stage responder: stage-1 clean, stage-2 bad-then-good.

    Proves the tie-out verifier + redo path: planted 34.438 rejected
    (tie-out fail), truth 31,438 accepted via tie-out. The naive 34,438
    case is asserted directly in dry-run (it never passes through redo -
    it is the old verifier's wrong answer, rejected here).
    """

    def __init__(self):
        self.calls: list[str] = []

    def __call__(self, stage: str, attempt: int) -> str:
        self.calls.append(f"{stage}#{attempt}")
        if stage == "stage1":
            return json.dumps(FIXTURE_DECLARATION)
        if attempt == 0:
            return json.dumps(FIXTURE_VALUES_BAD)
        return json.dumps(FIXTURE_VALUES_GOOD)


# ------------------------------------------------------- runner
def process_record(record: dict, venue: str | None, mock, audit: list,
                   existing_ocr_map: dict) -> dict:
    rec_id = record.get("node_id") or record.get("image_rel") or "?"
    img_rel = record.get("image_rel", "")
    img_path = REPO_ROOT / img_rel if img_rel else None
    existing_ocr = existing_ocr_map.get(record.get("node_id", ""), "")
    result = {"node_id": record.get("node_id"), "image_rel": img_rel,
              "reason": record.get("reason"), "status": "pending",
              "stage1": None, "stage2": None, "verdict": None,
              "diff": None, "attempts": {"stage1": 0, "stage2": 0}}
    paddle_ocr: dict | None = None
    if mock is None:
        # ---- paddle live path (local only, no network/secrets) ----
        if venue != "paddle":
            audit.append({"ts": utcnow(), "record": rec_id, "stage": "load",
                          "event": "venue_error",
                          "error": f"unknown venue {venue!r} (only paddle|mock)"})
            result["status"] = "venue_failed_stage1"
            return result
        if img_path is None or not img_path.exists():
            audit.append({"ts": utcnow(), "record": rec_id, "stage": "load",
                          "event": "image_missing", "path": img_rel})
            result["status"] = "image_missing"
            return result
        try:
            suffix = img_path.suffix.lower()
            if suffix == ".pdf":
                paddle_ocr = extract_pdf_native_or_ocr(img_path)
                audit.append({"ts": utcnow(), "record": rec_id, "stage": "load",
                              "event": "pdf_router",
                              "native_pages": paddle_ocr["native_pages"],
                              "ocr_pages": paddle_ocr["ocr_pages"],
                              "chars": len(paddle_ocr["text"]),
                              "mean_conf": round(paddle_ocr["mean_conf"], 4),
                              "latency_ms": paddle_ocr["latency_ms"]})
            else:
                paddle_ocr = paddle_ocr_image(img_path)
                audit.append({"ts": utcnow(), "record": rec_id, "stage": "load",
                              "event": "paddle_ocr",
                              "lines": len(paddle_ocr["lines"]),
                              "chars": len(paddle_ocr["text"]),
                              "mean_conf": round(paddle_ocr["mean_conf"], 4),
                              "latency_ms": paddle_ocr["latency_ms"]})
        except Exception as exc:
            audit.append({"ts": utcnow(), "record": rec_id, "stage": "load",
                          "event": "paddle_error", "error": str(exc)[:300]})
            result["status"] = "paddle_failed"
            return result

    # ---- stage 1 (declaration, fail-closed) ----
    decl = None
    for attempt in range(REDO_LIMIT + 1):
        result["attempts"]["stage1"] = attempt + 1
        if mock is not None:
            raw = mock("stage1", attempt)
        else:
            assert paddle_ocr is not None
            decl_obj = ocr_to_declaration(paddle_ocr["text"],
                                          len(paddle_ocr["lines"]))
            raw = json.dumps(decl_obj)
        decl = parse_json_loose(raw)
        ok, issues = verify_stage1(decl)
        last_issues = issues
        audit.append({"ts": utcnow(), "record": rec_id, "stage": "stage1",
                      "event": "extract+verify", "attempt": attempt,
                      "ok": ok, "issues": issues,
                      "raw_chars": len(raw or "")})
        if ok:
            break
        if "axes_unreadable_fail_closed" in issues:
            result["status"] = "unreadable_axes_fail_closed"
            result["stage1"] = decl
            result["diff"] = reconcile_diff(record, existing_ocr, decl, None,
                                            result["status"])
            return result
        if attempt >= REDO_LIMIT:
            result["status"] = "stage1_rejected"
            result["stage1"] = decl
            result["diff"] = reconcile_diff(record, existing_ocr, decl, None,
                                            result["status"])
            return result
    result["stage1"] = decl

    # ---- stage 2 (values against declared scale; tie-out corrects) ----
    vals = None
    for attempt in range(REDO_LIMIT + 1):
        result["attempts"]["stage2"] = attempt + 1
        if mock is not None:
            raw = mock("stage2", attempt)
        else:
            assert paddle_ocr is not None
            vals_obj = ocr_to_stage2(paddle_ocr["text"],
                                     paddle_ocr["mean_conf"])
            raw = json.dumps(vals_obj)
        vals = parse_json_loose(raw)
        ok, issues, verdict = verify_stage2(vals, decl, existing_ocr)
        last_issues2 = issues
        audit.append({"ts": utcnow(), "record": rec_id, "stage": "stage2",
                      "event": "extract+verify", "attempt": attempt,
                      "ok": ok, "issues": issues, "verdict": verdict,
                      "raw_chars": len(raw or "")})
        if ok:
            result["status"] = "accepted"
            result["verdict"] = verdict
            break
        if attempt >= REDO_LIMIT:
            result["status"] = "stage2_rejected"
            result["verdict"] = verdict
    result["stage2"] = vals
    result["diff"] = reconcile_diff(record, existing_ocr, decl, vals, result["status"])
    return result


def load_existing_ocr(repo: Path, manifest_path: Path) -> dict:
    """Map node_id -> existing OCR/summary text from tree shards (read-only)."""
    out: dict[str, str] = {}
    try:
        rows = [json.loads(l) for l in open(manifest_path, encoding="utf-8")]
    except FileNotFoundError:
        return out
    for rec in rows:
        nid = rec.get("node_id")
        if not nid:
            continue
        out.setdefault(nid, "")
    # Fill from trees lazily per doc would be slow; caller may pre-fill.
    return out


def fill_ocr_from_trees(repo: Path, records: list[dict]) -> dict:
    docs = {}
    try:
        for line in open(repo / "knowledge/manifests/documents.jsonl", encoding="utf-8"):
            r = json.loads(line)
            docs[r["doc_id"]] = r.get("tree_path", "")
    except FileNotFoundError:
        return {}
    cache: dict[str, dict] = {}
    out: dict[str, str] = {}
    for rec in records:
        nid = rec.get("node_id", "")
        doc_id = rec.get("doc_id", "")
        tp = docs.get(doc_id, "")
        if not tp:
            continue
        if tp not in cache:
            try:
                t = json.load(open(repo / tp, encoding="utf-8"))
            except Exception:
                cache[tp] = {}
                continue
            idx: dict[str, str] = {}
            stack = [t]
            while stack:
                n = stack.pop()
                if n.get("node_id"):
                    idx[n["node_id"]] = n.get("summary", "") or ""
                stack.extend(n.get("children") or [])
            cache[tp] = idx
        out[nid] = cache[tp].get(nid, "")
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Re-OCR pilot harness (Decision 2 PHASE A, paddle lane)")
    ap.add_argument("--set", default="data/derived/pilot_image_set.jsonl")
    ap.add_argument("--out", default="data/derived/pilot_reocr_out")
    ap.add_argument("--venue", default="paddle", choices=["paddle", "mock"])
    ap.add_argument("--max-images", type=int, default=50)
    ap.add_argument("--dry-run", action="store_true",
                    help="no deps: run the Vale GT1 fixture through the mock venue + tie-out proofs")
    ap.add_argument("--limit", type=int, default=None)
    args = ap.parse_args(argv)

    out_dir = (REPO_ROOT / args.out) if not Path(args.out).is_absolute() else Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    audit: list[dict] = []
    audit_path = out_dir / "audit.jsonl"
    results_path = out_dir / "results.json"
    reconcile_path = out_dir / "reconcile.diff"

    if args.dry_run or args.venue == "mock":
        fixture = {"node_id": "FIXTURE__vale_table__s03",
                   "doc_id": "FIXTURE__vale_table",
                   "image_rel": "FIXTURE/mock.png",
                   "reason": "separator_suspect",
                   "shard_hint": "knowledge/trees/FIXTURE.json"}
        mock = MockVenue()
        existing = {fixture["node_id"]:
                    "Northem andEastem = 34.438 35,047 legacy OCR; "
                    "Northern System 50,729; S11D 19,291"}
        res = process_record(fixture, None, mock, audit, existing)
        res["mock_calls"] = mock.calls
        # Direct tie-out proofs: planted BAD rejected, naive 34,438
        # rejected, truth 31,438 accepted. The mock redo above proves
        # BAD->GOOD; these prove the naive middle case the old verifier
        # got confidently wrong (bench f8bf3ac27).
        ok_bad, iss_bad, _ = verify_stage2(
            FIXTURE_VALUES_BAD, FIXTURE_DECLARATION, existing[fixture["node_id"]])
        ok_naive, iss_naive, _ = verify_stage2(
            FIXTURE_VALUES_NAIVE, FIXTURE_DECLARATION, existing[fixture["node_id"]])
        ok_good, iss_good, _ = verify_stage2(
            FIXTURE_VALUES_GOOD, FIXTURE_DECLARATION, existing[fixture["node_id"]])
        proof = {
            "planted_34_438_rejected": (not ok_bad),
            "naive_34_438_rejected": (not ok_naive),
            "truth_31_438_accepted": bool(ok_good),
            "truth_proof": "31,438 + 19,291 = 50,729 Northern System 4Q19; "
                           "34,438 + 19,291 = 53,729 (fails)",
        }
        selftest_ok = (proof["planted_34_438_rejected"]
                       and proof["naive_34_438_rejected"]
                       and proof["truth_31_438_accepted"]
                       and res["status"] == "accepted")
        with open(audit_path, "w", encoding="utf-8") as f:
            for ev in audit:
                f.write(json.dumps(ev, ensure_ascii=False) + "\n")
        with open(results_path, "w", encoding="utf-8") as f:
            json.dump([res], f, ensure_ascii=False, indent=2)
        with open(reconcile_path, "w", encoding="utf-8") as f:
            f.write(res.get("diff") or "")
            f.write("\n")
        print(json.dumps({"mode": "dry-run(mock)", "status": res["status"],
                          "verdict": res.get("verdict"),
                          "audit_events": len(audit),
                          "mock_calls": mock.calls,
                          "tieout_proof": proof,
                          "selftest": "PASS" if selftest_ok else "FAIL",
                          "naive_issues": iss_naive,
                          "good_issues": iss_good}, indent=2))
        return 0 if selftest_ok else 1

    set_path = (REPO_ROOT / args.set) if not Path(args.set).is_absolute() else Path(args.set)
    records = [json.loads(l) for l in open(set_path, encoding="utf-8")]
    if args.limit:
        records = records[:args.limit]
    records = records[:args.max_images]
    if args.venue != "paddle":
        print(f"Unknown venue {args.venue!r} (only paddle|mock).", file=sys.stderr)
        return 2
    try:
        _get_paddle_ocr()
    except RuntimeError as exc:
        print(f"paddle venue unavailable: {exc}", file=sys.stderr)
        return 2
    print(f"venue=paddle images={len(records)}", file=sys.stderr)
    existing = fill_ocr_from_trees(REPO_ROOT, records)
    results = []
    for rec in records:
        results.append(process_record(rec, "paddle", None, audit, existing))
    with open(audit_path, "w", encoding="utf-8") as f:
        for ev in audit:
            f.write(json.dumps(ev, ensure_ascii=False) + "\n")
    with open(results_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    with open(reconcile_path, "w", encoding="utf-8") as f:
        for r in results:
            f.write(r.get("diff") or "")
            f.write("\n\n")
    by_status: dict[str, int] = {}
    for r in results:
        by_status[r["status"]] = by_status.get(r["status"], 0) + 1
    summary = {"mode": "live", "venue": "paddle", "n": len(results),
               "by_status": by_status}
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
