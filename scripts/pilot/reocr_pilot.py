"""Re-OCR / structuring pilot harness — Decision 2 (Quality Pilot).

Two-stage vision protocol with extractor/verifier separation, redo loop,
JSONL audit, and reconcile-as-diffs (never writes to knowledge/).

Venue support:
  - Ollama chat (multimodal ``images`` payload; env OLLAMA_BASE_URL /
    OLLAMA_API_KEY / OLLAMA_MODEL)
  - OpenAI-compatible image_url (NIM / OpenRouter / Groq; env NIM_API_KEY /
    NIM_MODEL / NIM_BASE_URL (+NVIDIA_API_KEY alias), OPENROUTER_API_KEY /
    OPENROUTER_MODEL / OPENROUTER_BASE_URL, GROQ_API_KEY / GROQ_MODEL /
    GROQ_BASE_URL)
  - Mock venue: deterministic local runner exercising full 35-image cohort,
    PIL image loading and base64 encoding, stage-1 axis fail-closed gating,
    and planted separator error redo verification on S1 Vale iron ore table.

Rate-limit + retry/backoff mirrors process_knowledge.py client behavior.
All file paths normalized to POSIX forward slashes ('/').
"""
from __future__ import annotations

import argparse
import base64
import io
import json
import os
import random
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

try:
    import requests
except ImportError:  # pragma: no cover
    requests = None

try:
    from PIL import Image
except ImportError:  # pragma: no cover
    Image = None

REPO_ROOT = Path(__file__).resolve().parent.parent.parent

# ---------------------------------------------------------------- env config
def _env(name: str, default: str = "") -> str:
    return (os.environ.get(name, default) or "").strip()


OLLAMA_BASE_URL = _env("OLLAMA_BASE_URL").rstrip("/")
if OLLAMA_BASE_URL and not OLLAMA_BASE_URL.endswith("/api"):
    if OLLAMA_BASE_URL.endswith("/v1"):
        OLLAMA_BASE_URL = OLLAMA_BASE_URL[:-3] + "/api"
    else:
        OLLAMA_BASE_URL = OLLAMA_BASE_URL + "/api"
OLLAMA_API_KEY = _env("OLLAMA_API_KEY")
OLLAMA_MODEL = _env("OLLAMA_MODEL")
NIM_API_KEY = _env("NIM_API_KEY") or _env("NVIDIA_API_KEY")
NIM_MODEL = _env("NIM_MODEL")
NIM_BASE_URL = (_env("NIM_BASE_URL") or "https://integrate.api.nvidia.com/v1").rstrip("/")
OPENROUTER_API_KEY = _env("OPENROUTER_API_KEY")
OPENROUTER_MODEL = _env("OPENROUTER_MODEL") or "meta-llama/llama-3.3-70b-instruct"
OPENROUTER_BASE_URL = (_env("OPENROUTER_BASE_URL") or "https://openrouter.ai/api/v1").rstrip("/")
GROQ_API_KEY = _env("GROQ_API_KEY")
GROQ_MODEL = _env("GROQ_MODEL") or "openai/gpt-oss-120b"
GROQ_BASE_URL = (_env("GROQ_BASE_URL") or "https://api.groq.com/openai/v1").rstrip("/")

MIN_INTERVAL_SEC = float(os.environ.get("PILOT_MIN_INTERVAL_SEC", "1.5"))
MAX_RETRIES = int(os.environ.get("PILOT_MAX_RETRIES", "3"))
BACKOFF_BASE_SEC = float(os.environ.get("PILOT_BACKOFF_BASE_SEC", "1.5"))
MAX_BACKOFF_SEC = float(os.environ.get("PILOT_MAX_BACKOFF_SEC", "15.0"))
REQUEST_TIMEOUT_SEC = float(os.environ.get("PILOT_REQUEST_TIMEOUT_SEC", "90"))
MAX_IMAGE_BYTES = int(os.environ.get("PILOT_MAX_IMAGE_BYTES", "8000000"))
REDO_LIMIT = int(os.environ.get("PILOT_REDO_LIMIT", "1"))

_last_call_ts = 0.0


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def to_posix(path_str: str) -> str:
    return str(path_str).replace("\\", "/")


# ------------------------------------------------------- rate-limit + retry
def _is_rate_limit(text: str) -> bool:
    low = (text or "").lower()
    return ("429" in low or "too many requests" in low
            or "quota" in low or "rate limit" in low)


def _parse_retry_after(text: str) -> float | None:
    m = re.search(r"retry(?:\s+after)?\s+(\d+(?:\.\d+)?)", text or "", re.I)
    if not m:
        return None
    try:
        return float(m.group(1))
    except ValueError:
        return None


def _gate():
    global _last_call_ts
    wait = MIN_INTERVAL_SEC - (time.monotonic() - _last_call_ts)
    if wait > 0:
        time.sleep(wait)


def _backoff(attempt: int, exc_text: str):
    ra = _parse_retry_after(exc_text)
    if ra is not None:
        delay = ra
    elif _is_rate_limit(exc_text):
        delay = BACKOFF_BASE_SEC * (2 ** attempt)
    else:
        delay = BACKOFF_BASE_SEC * (attempt + 1)
    delay = min(delay, MAX_BACKOFF_SEC) + random.uniform(0.1, 0.9)
    time.sleep(delay)


# ------------------------------------------------------- payload builders
def build_ollama_chat_payload(model: str, prompt: str, b64: str) -> dict:
    return {
        "model": model,
        "messages": [{"role": "user", "content": prompt, "images": [b64]}],
        "stream": False,
    }


def build_openai_compat_payload(model: str, prompt: str, b64: str,
                                mime: str = "image/png") -> dict:
    return {
        "model": model,
        "messages": [{
            "role": "user",
            "content": [
                {"type": "text", "text": prompt},
                {"type": "image_url",
                 "image_url": {"url": f"data:{mime};base64,{b64}"}},
            ],
        }],
        "temperature": 0.1,
    }


def encode_image(path: Path):
    """Return (b64, mime, width, height, n_bytes); downscale if huge."""
    raw = path.read_bytes()
    n_bytes = len(raw)
    mime = "image/png"
    suffix = path.suffix.lower()
    if suffix in (".jpg", ".jpeg"):
        mime = "image/jpeg"
    elif suffix == ".webp":
        mime = "image/webp"
    w = h = -1
    if Image is not None:
        try:
            im = Image.open(io.BytesIO(raw))
            w, h = im.size
        except Exception:
            pass
    if n_bytes > MAX_IMAGE_BYTES and Image is not None:
        try:
            im = Image.open(io.BytesIO(raw)).convert("RGB")
            scale = (MAX_IMAGE_BYTES / n_bytes) ** 0.5 * 0.95
            im = im.resize((max(1, int(im.width * scale)),
                            max(1, int(im.height * scale))))
            buf = io.BytesIO()
            im.save(buf, format="JPEG", quality=88)
            raw = buf.getvalue()
            mime = "image/jpeg"
            w, h = im.size
        except Exception:
            pass
    return base64.b64encode(raw).decode("ascii"), mime, w, h, n_bytes


# ------------------------------------------------------- venue client
class VenueCallError(RuntimeError):
    pass


def venue_available(name: str) -> bool:
    if name == "ollama":
        return bool(OLLAMA_BASE_URL and OLLAMA_MODEL)
    if name == "nim":
        return bool(NIM_API_KEY and NIM_MODEL and NIM_BASE_URL)
    if name == "openrouter":
        return bool(OPENROUTER_API_KEY and OPENROUTER_MODEL and OPENROUTER_BASE_URL)
    if name == "groq":
        return bool(GROQ_API_KEY and GROQ_MODEL and GROQ_BASE_URL)
    return False


def detect_venue(prefer: str = "auto") -> str | None:
    if prefer != "auto":
        return prefer if venue_available(prefer) else None
    for cand in ("ollama", "nim", "openrouter", "groq"):
        if venue_available(cand):
            return cand
    return None


def _post_json(url: str, payload: dict, headers: dict) -> dict:
    if requests is None:
        raise VenueCallError("requests not installed")
    try:
        resp = requests.post(url, json=payload, headers=headers,
                             timeout=REQUEST_TIMEOUT_SEC)
    except Exception as exc:
        raise VenueCallError(f"connection error: {exc}") from exc
    if resp.status_code == 429 or resp.status_code >= 400:
        ra = resp.headers.get("Retry-After")
        detail = (resp.text or "")[:500]
        if ra:
            detail = f"{detail} retry after {ra}"
        raise VenueCallError(f"HTTP {resp.status_code}: {detail}")
    try:
        return resp.json()
    except Exception as exc:
        raise VenueCallError(f"non-JSON payload: {resp.text[:200]}") from exc


def call_venue(venue: str, prompt: str, b64: str, mime: str) -> str:
    global _last_call_ts
    _gate()
    try:
        if venue == "ollama":
            url = f"{OLLAMA_BASE_URL}/chat"
            headers = {"Content-Type": "application/json", "Accept": "application/json"}
            if OLLAMA_API_KEY:
                headers["Authorization"] = f"Bearer {OLLAMA_API_KEY}"
            data = _post_json(url, build_ollama_chat_payload(OLLAMA_MODEL, prompt, b64), headers)
            text = ((data.get("message") or {}).get("content") or "").strip()
        else:
            conf = {"nim": (NIM_BASE_URL, NIM_MODEL, NIM_API_KEY),
                    "openrouter": (OPENROUTER_BASE_URL, OPENROUTER_MODEL, OPENROUTER_API_KEY),
                    "groq": (GROQ_BASE_URL, GROQ_MODEL, GROQ_API_KEY)}[venue]
            base, model, key = conf
            url = f"{base}/chat/completions"
            headers = {"Content-Type": "application/json", "Accept": "application/json",
                       "Authorization": f"Bearer {key}"}
            data = _post_json(url, build_openai_compat_payload(model, prompt, b64, mime), headers)
            choices = data.get("choices") or []
            text = (((choices[0].get("message") if choices else {}) or {}).get("content") or "").strip()
        _last_call_ts = time.monotonic()
        if not text:
            raise VenueCallError("empty completion")
        return text
    except VenueCallError:
        _last_call_ts = time.monotonic()
        raise


def call_with_retry(venue: str, prompt: str, b64: str, mime: str,
                    audit: list, rec_id: str, stage: str) -> str | None:
    for attempt in range(MAX_RETRIES):
        try:
            return call_venue(venue, prompt, b64, mime)
        except VenueCallError as exc:
            audit.append({"ts": utcnow(), "record": rec_id, "stage": stage,
                          "event": "venue_error", "attempt": attempt,
                          "error": str(exc)[:300]})
            if attempt < MAX_RETRIES - 1:
                _backoff(attempt, str(exc))
            else:
                return None
    return None


# ------------------------------------------------------- prompts
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
never invent digits."""

STAGE2_REDO_PROMPT = (STAGE2_PROMPT + "\nRETRY: your previous values were "
                      "rejected ({reason}). Re-read against the SAME declared "
                      "scale; keep separators consistent; use \"?\" when unsure.")


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


def verify_stage1(decl: dict | None) -> tuple[bool, list[str]]:
    issues: list[str] = []
    if not isinstance(decl, dict):
        return False, ["stage1_not_json"]
    if not decl.get("axes_readable", False):
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
    
    nums = _numbers_from(vals)
    if check_separator_mix(nums):
        issues.append("separator_mix suspect: dot-thousands and comma-thousands/decimal mix")
    
    verdict = "improved"
    if existing_ocr:
        old_nums = set(_numbers_from(existing_ocr))
        new_nums = set(nums)
        if new_nums and new_nums.isdisjoint(old_nums):
            verdict = "contradicted"
        elif not new_nums:
            verdict = "contradicted" if old_nums else "improved"
    ok = len(issues) == 0
    return ok, issues, verdict


# ------------------------------------------------------- reconcile (diffs only)
def reconcile_diff(record: dict, existing_ocr: str, decl: dict | None,
                   vals: dict | None, status: str) -> str:
    shard = to_posix(record.get("shard_hint", "knowledge/trees/..."))
    img_rel = to_posix(record.get("image_rel", "?"))
    node_id = record.get("node_id", "?")
    lines = [f"--- a/{shard}",
             f"+++ b/proposed ({node_id})",
             f"@@ image {img_rel} status={status}"]
    lines.append(f"-OCR(existing): {existing_ocr[:400]!r}" if existing_ocr
                 else "-OCR(existing): <empty>")
    if decl is not None:
        lines.append(f"+STAGE1(declared): {json.dumps(decl, ensure_ascii=False)[:800]}")
    if vals is not None:
        lines.append(f"+STAGE2(values): {json.dumps(vals, ensure_ascii=False)[:1200]}")
    lines.append("+NOTE: proposal only — apply via pipeline recompile, never hand-edit shards.")
    return "\n".join(lines)


# ------------------------------------------------------- mock venue generator
class SmartMockVenue:
    """Mock venue handling full 35-image cohort realistically.
    Exercises:
      1. Fail-closed stage 1 on logos, monograms, photos, decorative graphics.
      2. Valid stage 1 declarations on charts, plots, and tables.
      3. Planted separator error on S1 Vale iron ore table on attempt 0 ('34.438' vs '35,047'),
         followed by clean corrected value '34,438' on attempt 1 (proving verifier redo).
      4. Valid stage 2 extraction on other charts and tables.
    """

    def __init__(self):
        self.calls: list[str] = []

    def __call__(self, record: dict, stage: str, attempt: int) -> str:
        img_rel = record.get("image_rel", "").lower()
        reason = record.get("reason", "")
        self.calls.append(f"{record.get('node_id', '?')}#{stage}#{attempt}")

        # Non-data assets fail closed at stage 1
        is_non_data = any(x in img_rel for x in [
            "logo", "monogram", "porto-teluk", "boywolf", "simandou", "erlingnaess", "thumb"
        ])

        if stage == "stage1":
            if is_non_data:
                chart_type = "logo" if "logo" in img_rel or "monogram" in img_rel else "photo"
                return json.dumps({
                    "chart_type": chart_type,
                    "axes_readable": False,
                    "x_axis": {"label": "", "scale": "unreadable", "units": "", "ticks": []},
                    "y_axis": {"label": "", "scale": "unreadable", "units": "", "ticks": []},
                    "table": None,
                    "notes": f"Non-data image ({chart_type}); axes unreadable",
                })
            elif "img-1960" in img_rel or "vale" in img_rel:
                # S1 Vale iron ore table
                return json.dumps({
                    "chart_type": "table",
                    "axes_readable": True,
                    "x_axis": {"label": "Mine/System", "scale": "categorical", "units": "",
                               "ticks": ["Northern", "Paraopeba", "Total"]},
                    "y_axis": {"label": "Production", "scale": "linear", "units": "kt",
                               "ticks": ["0", "50000", "100000"]},
                    "table": {"rows": 3, "cols": 4, "headers": ["System", "Q1 20", "Q4 19", "Q1 19"]},
                    "notes": "Vale production and sales table",
                })
            elif "q3capesize" in img_rel or "settle" in img_rel or "arrowcoal" in img_rel:
                return json.dumps({
                    "chart_type": "line",
                    "axes_readable": True,
                    "x_axis": {"label": "Date", "scale": "time", "units": "",
                               "ticks": ["Jan", "Feb", "Mar", "Apr", "May", "Jun"]},
                    "y_axis": {"label": "Freight Rate", "scale": "linear", "units": "$/day",
                               "ticks": ["5000", "10000", "15000", "20000"]},
                    "table": None,
                    "notes": "Freight market time series chart",
                })
            else:
                # General chart / table
                return json.dumps({
                    "chart_type": "table" if "table" in img_rel else "bar",
                    "axes_readable": True,
                    "x_axis": {"label": "Category", "scale": "categorical", "units": "",
                               "ticks": ["2018", "2019", "2020"]},
                    "y_axis": {"label": "Volume", "scale": "linear", "units": "Mt",
                               "ticks": ["0", "50", "100"]},
                    "table": {"rows": 2, "cols": 3, "headers": ["Category", "2019", "2020"]} if "table" in img_rel else None,
                    "notes": "Market fundamental series",
                })

        # Stage 2
        if "img-1960" in img_rel or "vale" in img_rel:
            if attempt == 0:
                # Planted separator error: '34.438' dot-thousands amidst comma-thousands
                return json.dumps({
                    "values": [{"series": "Production", "points": [
                        {"x": "Northern", "y": "34.438"},
                        {"x": "Paraopeba", "y": "35,047"},
                        {"x": "Total", "y": "69,485"},
                    ]}],
                    "table_cells": [
                        ["Northern", "34.438", "35,047", "37,023"],
                        ["Paraopeba", "4.997", "7,109", "10,352"],
                        ["Total", "59,605", "78,344", "67,375"],
                    ],
                    "units": "kt",
                    "confidence": "high",
                })
            else:
                # Cleaned correction: '34,438' comma-thousands consistent
                return json.dumps({
                    "values": [{"series": "Production", "points": [
                        {"x": "Northern", "y": "34,438"},
                        {"x": "Paraopeba", "y": "35,047"},
                        {"x": "Total", "y": "69,485"},
                    ]}],
                    "table_cells": [
                        ["Northern", "34,438", "35,047", "37,023"],
                        ["Paraopeba", "4,997", "7,109", "10,352"],
                        ["Total", "59,605", "78,344", "67,375"],
                    ],
                    "units": "kt",
                    "confidence": "high",
                })
        elif "q3capesize" in img_rel or "settle" in img_rel or "arrowcoal" in img_rel:
            return json.dumps({
                "values": [{"series": "Capesize Spot", "points": [
                    {"x": "Jan", "y": 8500},
                    {"x": "Mar", "y": 12300},
                    {"x": "Jun", "y": 19450},
                ]}],
                "table_cells": None,
                "units": "$/day",
                "confidence": "high",
            })
        else:
            return json.dumps({
                "values": [{"series": "Volume", "points": [
                    {"x": "2018", "y": 45.2},
                    {"x": "2019", "y": 52.8},
                    {"x": "2020", "y": 48.1},
                ]}],
                "table_cells": [["Row1", "45.2", "52.8"], ["Row2", "12.1", "14.3"]] if "table" in img_rel else None,
                "units": "Mt",
                "confidence": "medium",
            })


# ------------------------------------------------------- runner
def process_record(record: dict, venue: str | None, mock: SmartMockVenue | None,
                   audit: list, existing_ocr_map: dict) -> dict:
    rec_id = record.get("node_id") or record.get("image_rel") or "?"
    img_rel = to_posix(record.get("image_rel", ""))
    img_path = REPO_ROOT / img_rel if img_rel else None
    existing_ocr = existing_ocr_map.get(record.get("node_id", ""), "")
    result = {
        "node_id": record.get("node_id"),
        "image_rel": img_rel,
        "reason": record.get("reason"),
        "status": "pending",
        "stage1": None,
        "stage2": None,
        "verdict": None,
        "diff": None,
        "attempts": {"stage1": 0, "stage2": 0},
    }

    # Verify physical file existence and encode bytes
    if img_path is None or not img_path.exists():
        audit.append({"ts": utcnow(), "record": rec_id, "stage": "load",
                      "event": "image_missing", "path": img_rel})
        result["status"] = "image_missing"
        return result

    try:
        b64, mime, w, h, n = encode_image(img_path)
        audit.append({"ts": utcnow(), "record": rec_id, "stage": "load",
                      "event": "image_loaded", "bytes": n, "dims": [w, h],
                      "path": img_rel})
    except Exception as exc:
        audit.append({"ts": utcnow(), "record": rec_id, "stage": "load",
                      "event": "image_error", "error": str(exc)[:200],
                      "path": img_rel})
        result["status"] = "image_error"
        return result

    # ---- Stage 1: Declaration & Fail-Closed Axis Gating
    decl = None
    last_issues = []
    for attempt in range(REDO_LIMIT + 1):
        result["attempts"]["stage1"] = attempt + 1
        if mock is not None:
            raw = mock(record, "stage1", attempt)
        else:
            prompt = STAGE1_PROMPT if attempt == 0 else STAGE1_REDO_PROMPT.format(
                reason="; ".join(last_issues) if last_issues else "parse/verify")
            raw = call_with_retry(venue, prompt, b64, mime, audit, rec_id, "stage1")
            if raw is None:
                audit.append({"ts": utcnow(), "record": rec_id, "stage": "stage1",
                              "event": "venue_failed"})
                result["status"] = "venue_failed_stage1"
                return result

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
            result["diff"] = reconcile_diff(record, existing_ocr, decl, None, result["status"])
            return result
        if attempt >= REDO_LIMIT:
            result["status"] = "stage1_rejected"
            result["stage1"] = decl
            result["diff"] = reconcile_diff(record, existing_ocr, decl, None, result["status"])
            return result

    result["stage1"] = decl

    # ---- Stage 2: Value Extraction Against Declared Scale
    vals = None
    last_issues2 = []
    for attempt in range(REDO_LIMIT + 1):
        result["attempts"]["stage2"] = attempt + 1
        if mock is not None:
            raw = mock(record, "stage2", attempt)
        else:
            base = STAGE2_PROMPT.format(declaration=json.dumps(decl, ensure_ascii=False)[:1500])
            prompt = base if attempt == 0 else STAGE2_REDO_PROMPT.format(
                declaration=json.dumps(decl, ensure_ascii=False)[:1500],
                reason="; ".join(last_issues2) if last_issues2 else "verify")
            raw = call_with_retry(venue, prompt, b64, mime, audit, rec_id, "stage2")
            if raw is None:
                result["status"] = "venue_failed_stage2"
                return result

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


def fill_ocr_from_trees(repo: Path, records: list[dict]) -> dict:
    cache: dict[str, dict] = {}
    out: dict[str, str] = {}
    for rec in records:
        nid = rec.get("node_id", "")
        shard_hint = rec.get("shard_hint", "")
        if not shard_hint:
            continue
        p = repo / shard_hint
        if shard_hint not in cache:
            if not p.exists():
                cache[shard_hint] = {}
                continue
            try:
                with open(p, "r", encoding="utf-8") as f:
                    t = json.load(f)
            except Exception:
                cache[shard_hint] = {}
                continue
            idx: dict[str, str] = {}
            stack = [t]
            while stack:
                n = stack.pop()
                if n.get("node_id"):
                    idx[n["node_id"]] = n.get("summary", "") or ""
                stack.extend(n.get("children") or [])
            cache[shard_hint] = idx
        out[nid] = cache[shard_hint].get(nid, "")
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Re-OCR pilot harness (Decision 2)")
    ap.add_argument("--set", default="data/derived/pilot_image_set.jsonl")
    ap.add_argument("--out", default="data/derived/pilot_reocr_out")
    ap.add_argument("--venue", default="auto",
                    choices=["auto", "ollama", "nim", "openrouter", "groq", "mock"])
    ap.add_argument("--max-images", type=int, default=50)
    ap.add_argument("--dry-run", action="store_true",
                    help="smoke test on single fixture record")
    ap.add_argument("--limit", type=int, default=None)
    args = ap.parse_args(argv)

    out_dir = (REPO_ROOT / args.out) if not Path(args.out).is_absolute() else Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    audit: list[dict] = []
    audit_path = out_dir / "audit.jsonl"
    results_path = out_dir / "results.json"
    reconcile_path = out_dir / "reconcile.diff"

    set_path = (REPO_ROOT / args.set) if not Path(args.set).is_absolute() else Path(args.set)
    if not set_path.exists():
        print(f"Pilot set file not found: {set_path}", file=sys.stderr)
        return 1

    records = [json.loads(l) for l in open(set_path, encoding="utf-8") if l.strip()]
    if args.dry_run:
        records = records[:1]
    elif args.limit:
        records = records[:args.limit]
    records = records[:args.max_images]

    venue = detect_venue(args.venue)
    mock = None
    if venue is None or args.venue == "mock":
        venue_mode = "mock"
        mock = SmartMockVenue()
        print(f"[PILOT] Running in offline mock mode across {len(records)} images", file=sys.stderr)
    else:
        venue_mode = f"live({venue})"
        print(f"[PILOT] Running in live venue={venue} across {len(records)} images", file=sys.stderr)

    existing = fill_ocr_from_trees(REPO_ROOT, records)
    results = []
    for rec in records:
        res = process_record(rec, venue, mock, audit, existing)
        results.append(res)

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
    by_reason: dict[str, dict] = {}
    redo_count = 0
    for r in results:
        st = r["status"]
        by_status[st] = by_status.get(st, 0) + 1
        rs = r.get("reason", "unknown")
        if rs not in by_reason:
            by_reason[rs] = {}
        by_reason[rs][st] = by_reason[rs].get(st, 0) + 1
        if r.get("attempts", {}).get("stage2", 0) > 1 or r.get("attempts", {}).get("stage1", 0) > 1:
            redo_count += 1

    summary = {
        "mode": venue_mode,
        "n_images": len(results),
        "by_status": by_status,
        "by_cohort": by_reason,
        "redo_triggered": redo_count,
        "audit_events": len(audit),
        "audit_path": to_posix(str(audit_path.relative_to(REPO_ROOT))),
        "results_path": to_posix(str(results_path.relative_to(REPO_ROOT))),
        "reconcile_path": to_posix(str(reconcile_path.relative_to(REPO_ROOT))),
    }
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
