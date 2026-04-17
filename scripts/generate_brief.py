"""
Daily shipping market brief generator.

Reads quantitative CSV data + recent Breakwave signals + wiki context and writes:
  knowledge/briefs/latest.json
  knowledge/briefs/YYYY-MM-DD.json

LLM provider order defaults to: ollama -> gemini -> nim
If all providers fail, a deterministic template brief is generated.
"""
from __future__ import annotations

import csv
import json
import math
import os
import random
import re
import sys
import time
from collections import Counter
from datetime import date, datetime, timezone
from pathlib import Path
from urllib import error as urllib_error
from urllib import request as urllib_request

ROOT = Path(__file__).resolve().parent.parent
KNOWLEDGE = ROOT / "knowledge"
DERIVED = KNOWLEDGE / "derived"
WIKI = KNOWLEDGE / "wiki"
BRIEFS = KNOWLEDGE / "briefs"

SIGNALS_FILE = DERIVED / "signals.jsonl"

# CSV files: key -> path  (DD-MM-YYYY, Index, %Change)
CSV_FILES = {
    "bdi": ROOT / "bdiy_historical.csv",
    "capesize": ROOT / "cape_historical.csv",
    "panamax": ROOT / "panama_historical.csv",
    "supramax": ROOT / "suprama_historical.csv",
    "handysize": ROOT / "handysize_historical.csv",
    "clean_tanker": ROOT / "cleantanker_historical.csv",
    "dirty_tanker": ROOT / "dirtytanker_historical.csv",
}

WIKI_EXCERPTS = {
    "dry_bulk": WIKI / "dry_bulk_market.md",
    "capesize": WIKI / "capesize.md",
    "tanker": WIKI / "tanker_market.md",
}

CONFLUENCE_TYPES = {"BULL_CONFLUENCE", "BEAR_CONFLUENCE", "DIVERGENCE", "NEUTRAL"}
RECENT_REPORTS = 4

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "").strip()
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-1.5-flash").strip()
GEMINI_MIN_INTERVAL_SEC = float(os.environ.get("GEMINI_MIN_INTERVAL_SEC", "1.5"))
GEMINI_MAX_RETRIES = int(os.environ.get("GEMINI_MAX_RETRIES", "3"))
GEMINI_BACKOFF_BASE_SEC = float(os.environ.get("GEMINI_BACKOFF_BASE_SEC", "2.0"))
GEMINI_MAX_BACKOFF_SEC = float(os.environ.get("GEMINI_MAX_BACKOFF_SEC", "20.0"))

OLLAMA_API_KEY = os.environ.get("OLLAMA_API_KEY", "").strip()
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "").strip()
OLLAMA_BASE_URL = (os.environ.get("OLLAMA_BASE_URL") or "").strip().rstrip("/")
if OLLAMA_BASE_URL and not OLLAMA_BASE_URL.endswith("/api"):
    if OLLAMA_BASE_URL.endswith("/v1"):
        OLLAMA_BASE_URL = OLLAMA_BASE_URL[:-3] + "/api"
    else:
        OLLAMA_BASE_URL = OLLAMA_BASE_URL + "/api"
OLLAMA_MIN_INTERVAL_SEC = float(os.environ.get("OLLAMA_MIN_INTERVAL_SEC", "1.5"))
OLLAMA_MAX_RETRIES = int(os.environ.get("OLLAMA_MAX_RETRIES", "3"))
OLLAMA_BACKOFF_BASE_SEC = float(os.environ.get("OLLAMA_BACKOFF_BASE_SEC", "1.5"))
OLLAMA_MAX_BACKOFF_SEC = float(os.environ.get("OLLAMA_MAX_BACKOFF_SEC", "15.0"))

NIM_API_KEY = os.environ.get("NIM_API_KEY", "").strip()
NIM_MODEL = os.environ.get("NIM_MODEL", "").strip()
NIM_BASE_URL = (os.environ.get("NIM_BASE_URL") or "https://integrate.api.nvidia.com/v1").strip().rstrip("/")
NIM_MIN_INTERVAL_SEC = float(os.environ.get("NIM_MIN_INTERVAL_SEC", "1.5"))
NIM_MAX_RETRIES = int(os.environ.get("NIM_MAX_RETRIES", "3"))
NIM_BACKOFF_BASE_SEC = float(os.environ.get("NIM_BACKOFF_BASE_SEC", "1.5"))
NIM_MAX_BACKOFF_SEC = float(os.environ.get("NIM_MAX_BACKOFF_SEC", "15.0"))

ALLOWED_PROVIDERS = {"ollama", "gemini", "nim"}
LLM_PROVIDER_ORDER = [
    part.strip().lower()
    for part in os.environ.get("LLM_PROVIDER_ORDER", "ollama,gemini,nim").split(",")
    if part.strip().lower() in ALLOWED_PROVIDERS
]
if not LLM_PROVIDER_ORDER:
    LLM_PROVIDER_ORDER = ["ollama", "gemini", "nim"]

_last_gemini_call_ts = 0.0
_last_ollama_call_ts = 0.0
_last_nim_call_ts = 0.0
_gemini_model_client = None

_QUAL_SCORES = {
    "positive": 1.0,
    "constructive": 0.75,
    "cautiously_bullish": 0.5,
    "neutral": 0.0,
    "mixed": 0.0,
    "cautiously_bearish": -0.5,
    "negative": -1.0,
}


for stream_name in ("stdout", "stderr"):
    stream = getattr(sys, stream_name, None)
    if hasattr(stream, "reconfigure"):
        stream.reconfigure(encoding="utf-8", errors="replace")


# ------------------------ Quantitative helpers ------------------------

def parse_csv_series(path: Path) -> list[float | None]:
    """Parse DD-MM-YYYY,Index,Change CSV -> list of values in chronological order."""
    values: list[float | None] = []
    try:
        with open(path, newline="", encoding="utf-8") as f:
            for row in csv.reader(f):
                if len(row) < 2:
                    continue
                raw = row[1].strip()
                try:
                    values.append(float(raw) if raw not in ("", "-", "N/A") else None)
                except ValueError:
                    continue
    except FileNotFoundError:
        pass
    return values


def rolling_mean_std(values: list[float | None], window: int) -> tuple[float | None, float | None]:
    """Mean and population std of the last `window` non-null values."""
    window_vals = [v for v in values[-window:] if v is not None]
    if len(window_vals) < 20:
        return None, None
    mean_value = sum(window_vals) / len(window_vals)
    variance = sum((v - mean_value) ** 2 for v in window_vals) / len(window_vals)
    return mean_value, math.sqrt(variance) if variance > 0 else 0.0


def compute_zscore_252d(values: list[float | None]) -> float | None:
    """Rolling 252-day Z-score of the last value."""
    non_null = [v for v in values if v is not None]
    if not non_null:
        return None
    current = non_null[-1]
    mean_value, std_dev = rolling_mean_std(values, 252)
    if mean_value is None or std_dev is None:
        return None
    return round((current - mean_value) / std_dev, 3) if std_dev > 0 else 0.0


def compute_regime(values: list[float | None]) -> tuple[str, str, float | None, float | None]:
    """
    Matches Momentum Regime logic in index.html:
      MA(200) anchor + ROC(60) velocity.
    Returns (regime, regime_emoji, ma200, roc60_pct).
    """
    non_null = [v for v in values if v is not None]
    if len(non_null) < 201:
        return "INSUFFICIENT_DATA", "N/A", None, None

    current = non_null[-1]
    ma200 = sum(non_null[-200:]) / 200

    if len(non_null) >= 62:
        base = non_null[-61]
        roc60 = ((current - base) / base * 100) if base else 0.0
    else:
        roc60 = 0.0

    if current > ma200 and roc60 > 0:
        regime, regime_emoji = "EXPANSION", "UP"
    elif current > ma200:
        regime, regime_emoji = "DISTRIBUTION", "FLAT"
    elif roc60 > 0:
        regime, regime_emoji = "ACCUMULATION", "RECOVERY"
    else:
        regime, regime_emoji = "CONTRACTION", "DOWN"

    return regime, regime_emoji, round(ma200, 1), round(roc60, 2)


def percentile_5y(values: list[float | None]) -> float | None:
    """5-year (252 * 5 trading days) percentile rank of the last value."""
    non_null = [v for v in values if v is not None]
    if not non_null:
        return None
    current = non_null[-1]
    window = non_null[-(252 * 5) :]
    if not window:
        return None
    return round(sum(1 for v in window if v <= current) / len(window), 3)


def build_market_snapshot() -> dict:
    snapshot: dict[str, dict] = {}
    for name, path in CSV_FILES.items():
        values = parse_csv_series(path)
        non_null = [v for v in values if v is not None]
        if not non_null:
            continue
        current = non_null[-1]
        regime, regime_emoji, ma200, roc60 = compute_regime(values)
        z_score = compute_zscore_252d(values)
        pctl = percentile_5y(values)
        snapshot[name] = {
            "value": round(current, 1),
            "z_score_252d": z_score,
            "pctl_5y": pctl,
            "regime": regime,
            "regime_emoji": regime_emoji,
            "ma200": ma200,
            "roc60": roc60,
        }
    return snapshot


def compute_tanker_z(snapshot: dict) -> float | None:
    clean = snapshot.get("clean_tanker", {}).get("z_score_252d")
    dirty = snapshot.get("dirty_tanker", {}).get("z_score_252d")
    if clean is not None and dirty is not None:
        return round((clean + dirty) / 2, 3)
    return clean if clean is not None else dirty


# ------------------------ Qualitative helpers ------------------------

def load_signals() -> list[dict]:
    signals: list[dict] = []
    try:
        with open(SIGNALS_FILE, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    signals.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    except FileNotFoundError:
        pass
    return signals


def recent_breakwave(signals: list[dict], category: str, n: int = RECENT_REPORTS) -> list[dict]:
    filtered = [
        signal
        for signal in signals
        if signal.get("source") == "breakwave"
        and signal.get("category") == category
        and signal.get("date", "0000") not in ("0000-00-00", "", None)
        and signal.get("sentiment") is not None
    ]
    filtered.sort(key=lambda x: x.get("date", ""), reverse=True)
    return filtered[:n]


def compute_confluence(z_score: float | None, sentiments: list[str]) -> str:
    """Classify confluence between quantitative Z-score and qualitative sentiments."""
    if not sentiments or z_score is None:
        return "NEUTRAL"
    qual_score = sum(_QUAL_SCORES.get(s, 0.0) for s in sentiments) / len(sentiments)
    if z_score > 0.5 and qual_score > 0.25:
        return "BULL_CONFLUENCE"
    if z_score < -0.5 and qual_score < -0.25:
        return "BEAR_CONFLUENCE"
    if (z_score > 0.5 and qual_score < -0.25) or (z_score < -0.5 and qual_score > 0.25):
        return "DIVERGENCE"
    return "NEUTRAL"


def wiki_excerpt(path: Path, max_chars: int = 700) -> str:
    try:
        text = path.read_text(encoding="utf-8")
        if text.startswith("---"):
            end = text.find("---", 3)
            if end > 0:
                text = text[end + 3 :].strip()
        return text[:max_chars]
    except FileNotFoundError:
        return ""


# ------------------------ Prompt + JSON helpers ------------------------

def _fmt_signed(value: float | None, digits: int = 2, suffix: str = "") -> str:
    if value is None:
        return "N/A"
    sign = "+" if value >= 0 else ""
    return f"{sign}{value:.{digits}f}{suffix}"


def _fmt_percentile(value: float | None) -> str:
    if value is None:
        return "N/A"
    return f"{value * 100:.1f}%"


def _fmt_snapshot_line(name: str, snap: dict) -> str:
    value = snap.get("value")
    value_txt = "N/A" if value is None else f"{value:.1f}"
    return (
        f"{name.upper():15s} "
        f"value={value_txt} "
        f"z={_fmt_signed(snap.get('z_score_252d'), 2, 'sigma')} "
        f"regime={snap.get('regime', 'N/A')} "
        f"roc60={_fmt_signed(snap.get('roc60'), 1, '%')} "
        f"pctl_5y={_fmt_percentile(snap.get('pctl_5y'))}"
    )


def _fmt_signal(signal: dict) -> str:
    return (
        f"{signal.get('date')}: "
        f"sentiment={signal.get('sentiment')} "
        f"momentum={signal.get('momentum')} "
        f"fundamentals={signal.get('fundamentals')}"
    )


def build_prompt(
    snapshot: dict,
    dry_signals: list[dict],
    tanker_signals: list[dict],
    wiki_dry: str,
    wiki_tanker: str,
    wiki_cape: str,
) -> str:
    today = date.today().isoformat()
    snapshot_lines = "\n".join(_fmt_snapshot_line(key, value) for key, value in snapshot.items())
    dry_block = "\n".join(_fmt_signal(s) for s in dry_signals) or "No recent reports."
    tanker_block = "\n".join(_fmt_signal(s) for s in tanker_signals) or "No recent reports."
    return f"""You are a senior shipping freight market analyst generating a daily brief for {today}.

Quantitative Market Snapshot:
{snapshot_lines}

Recent Breakwave Dry Bulk Reports:
{dry_block}

Recent Breakwave Tanker Reports:
{tanker_block}

Knowledge Base Excerpts:
Dry bulk:
{wiki_dry}

Capesize:
{wiki_cape}

Tanker:
{wiki_tanker}

Task:
Analyze confluence or divergence between quantitative (Z-score, regime, ROC) and
qualitative (sentiment, momentum, fundamentals) signals.
Return only valid JSON matching this schema:
{{
  "vessel_classes": {{
    "dry_bulk": {{
      "confluence_type": "<BULL_CONFLUENCE|BEAR_CONFLUENCE|DIVERGENCE|NEUTRAL>",
      "confluence_note": "<1-2 sentences>",
      "summary": "<2-3 sentences>",
      "key_signals": ["<signal 1>", "<signal 2>", "<signal 3>"],
      "outlook": "<1 sentence>",
      "watch": "<1 sentence>"
    }},
    "tanker": {{
      "confluence_type": "<BULL_CONFLUENCE|BEAR_CONFLUENCE|DIVERGENCE|NEUTRAL>",
      "confluence_note": "<1-2 sentences>",
      "summary": "<2-3 sentences>",
      "key_signals": ["<signal 1>", "<signal 2>", "<signal 3>"],
      "outlook": "<1 sentence>",
      "watch": "<1 sentence>"
    }}
  }},
  "macro_note": "<1-2 sentences>"
}}"""


def _extract_json_payload(text: str | None) -> dict | None:
    if not text:
        return None
    raw = text.strip()
    if raw.startswith("```"):
        raw = re.sub(r"^```(?:json)?", "", raw).strip()
        raw = re.sub(r"```$", "", raw).strip()
    match = re.search(r"\{.*\}", raw, re.S)
    if match:
        raw = match.group(0)
    try:
        payload = json.loads(raw)
        if isinstance(payload, dict):
            return payload
        return None
    except json.JSONDecodeError:
        return None


def _clean_text(value) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return text


def _clean_signals(values) -> list[str]:
    if not isinstance(values, list):
        return []
    cleaned = []
    for value in values:
        text = _clean_text(value)
        if text:
            cleaned.append(text)
    return cleaned[:5]


# ------------------------ Provider utilities ------------------------

def _is_rate_limit_error(exc_text: str) -> bool:
    lower = (exc_text or "").lower()
    return "429" in lower or "too many requests" in lower or "quota" in lower or "rate limit" in lower


def _parse_retry_after(exc_text: str) -> float | None:
    match = re.search(r"retry_after\s+([0-9]+(?:\.[0-9]+)?)", exc_text or "", re.I)
    if not match:
        return None
    try:
        return float(match.group(1))
    except ValueError:
        return None


def _apply_interval(last_ts: float, min_interval: float) -> float:
    now = time.monotonic()
    elapsed = now - last_ts
    wait_for = min_interval - elapsed
    if wait_for > 0:
        time.sleep(wait_for)
    return time.monotonic()


def _backoff_sleep(
    attempt: int,
    exc_text: str,
    base_delay: float,
    max_delay: float,
) -> None:
    retry_after = _parse_retry_after(exc_text)
    if retry_after is not None:
        delay = retry_after
    elif _is_rate_limit_error(exc_text):
        delay = base_delay * (2 ** attempt)
    else:
        delay = base_delay * (attempt + 1)
    delay = min(delay, max_delay)
    delay += random.uniform(0.1, 0.9)
    time.sleep(delay)


# ------------------------ Provider calls ------------------------

def gemini_available() -> bool:
    return bool(GEMINI_API_KEY)


def _gemini_model():
    global _gemini_model_client
    if _gemini_model_client is not None:
        return _gemini_model_client
    if not gemini_available():
        return None
    try:
        import google.generativeai as genai
    except Exception as exc:
        print(f"[brief] Gemini import failed: {exc}", file=sys.stderr)
        return None
    genai.configure(api_key=GEMINI_API_KEY)
    _gemini_model_client = genai.GenerativeModel(
        GEMINI_MODEL,
        generation_config={"temperature": 0.25, "max_output_tokens": 1200},
    )
    return _gemini_model_client


def call_gemini_text(prompt: str, retries: int | None = None) -> str | None:
    if not gemini_available():
        return None
    model = _gemini_model()
    if model is None:
        return None
    retries = retries or GEMINI_MAX_RETRIES
    global _last_gemini_call_ts
    for attempt in range(retries):
        try:
            _last_gemini_call_ts = _apply_interval(_last_gemini_call_ts, GEMINI_MIN_INTERVAL_SEC)
            response = model.generate_content(prompt)
            text = getattr(response, "text", None)
            if text:
                return str(text).strip()
            return None
        except Exception as exc:
            exc_text = str(exc)
            if attempt < retries - 1:
                _backoff_sleep(attempt, exc_text, GEMINI_BACKOFF_BASE_SEC, GEMINI_MAX_BACKOFF_SEC)
            else:
                print(f"[brief] Gemini failed: {exc_text}", file=sys.stderr)
                return None
    return None


def ollama_available() -> bool:
    return bool(OLLAMA_BASE_URL and OLLAMA_MODEL)


def _call_ollama_once(prompt: str) -> str | None:
    payload = {
        "model": OLLAMA_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "stream": False,
    }
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    if OLLAMA_API_KEY:
        headers["Authorization"] = f"Bearer {OLLAMA_API_KEY}"
    req = urllib_request.Request(
        f"{OLLAMA_BASE_URL}/chat",
        data=body,
        headers=headers,
        method="POST",
    )
    try:
        with urllib_request.urlopen(req, timeout=90) as response:
            raw = response.read().decode("utf-8", errors="replace")
    except urllib_error.HTTPError as exc:
        retry_after = exc.headers.get("Retry-After") if exc.headers else None
        err_body = exc.read().decode("utf-8", errors="replace")
        details = err_body or str(exc)
        if retry_after:
            details = f"{details} retry_after {retry_after}"
        raise RuntimeError(f"Ollama HTTP {exc.code}: {details}") from exc
    except urllib_error.URLError as exc:
        raise RuntimeError(f"Ollama connection error: {exc}") from exc
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Ollama returned non-JSON payload: {raw[:200]}") from exc
    message = data.get("message") or {}
    text = _clean_text(message.get("content"))
    return text or None


def call_ollama_text(prompt: str, retries: int | None = None) -> str | None:
    if not ollama_available():
        return None
    retries = retries or OLLAMA_MAX_RETRIES
    global _last_ollama_call_ts
    for attempt in range(retries):
        try:
            _last_ollama_call_ts = _apply_interval(_last_ollama_call_ts, OLLAMA_MIN_INTERVAL_SEC)
            return _call_ollama_once(prompt)
        except Exception as exc:
            exc_text = str(exc)
            if attempt < retries - 1:
                _backoff_sleep(attempt, exc_text, OLLAMA_BACKOFF_BASE_SEC, OLLAMA_MAX_BACKOFF_SEC)
            else:
                print(f"[brief] Ollama failed: {exc_text}", file=sys.stderr)
                return None
    return None


def nim_available() -> bool:
    return bool(NIM_API_KEY and NIM_MODEL and NIM_BASE_URL)


def _call_nim_once(prompt: str) -> str | None:
    payload = {
        "model": NIM_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.25,
        "max_tokens": 1200,
    }
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {NIM_API_KEY}",
    }
    req = urllib_request.Request(
        f"{NIM_BASE_URL}/chat/completions",
        data=body,
        headers=headers,
        method="POST",
    )
    try:
        with urllib_request.urlopen(req, timeout=90) as response:
            raw = response.read().decode("utf-8", errors="replace")
    except urllib_error.HTTPError as exc:
        retry_after = exc.headers.get("Retry-After") if exc.headers else None
        err_body = exc.read().decode("utf-8", errors="replace")
        details = err_body or str(exc)
        if retry_after:
            details = f"{details} retry_after {retry_after}"
        raise RuntimeError(f"NIM HTTP {exc.code}: {details}") from exc
    except urllib_error.URLError as exc:
        raise RuntimeError(f"NIM connection error: {exc}") from exc
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"NIM returned non-JSON payload: {raw[:200]}") from exc
    choices = data.get("choices") or []
    if not choices:
        return None
    message = choices[0].get("message") or {}
    text = _clean_text(message.get("content"))
    return text or None


def call_nim_text(prompt: str, retries: int | None = None) -> str | None:
    if not nim_available():
        return None
    retries = retries or NIM_MAX_RETRIES
    global _last_nim_call_ts
    for attempt in range(retries):
        try:
            _last_nim_call_ts = _apply_interval(_last_nim_call_ts, NIM_MIN_INTERVAL_SEC)
            return _call_nim_once(prompt)
        except Exception as exc:
            exc_text = str(exc)
            if attempt < retries - 1:
                _backoff_sleep(attempt, exc_text, NIM_BACKOFF_BASE_SEC, NIM_MAX_BACKOFF_SEC)
            else:
                print(f"[brief] NIM failed: {exc_text}", file=sys.stderr)
                return None
    return None


def call_llm_payload(prompt: str) -> tuple[dict | None, str | None, list[str]]:
    attempted: list[str] = []
    for provider in LLM_PROVIDER_ORDER:
        attempted.append(provider)
        if provider == "ollama":
            text = call_ollama_text(prompt)
        elif provider == "gemini":
            text = call_gemini_text(prompt)
        elif provider == "nim":
            text = call_nim_text(prompt)
        else:
            continue
        if not text:
            continue
        payload = _extract_json_payload(text)
        if payload:
            return payload, provider, attempted
        print(f"[brief] {provider} returned non-JSON output; trying next provider.", file=sys.stderr)
    return None, None, attempted


# ------------------------ Deterministic templates ------------------------

def _sentiment_mix(signals: list[dict]) -> tuple[str, float, str]:
    if not signals:
        return "neutral", 0.0, "no recent analyst sentiment records"
    sentiments = [_clean_text(s.get("sentiment")) or "neutral" for s in signals]
    counts = Counter(sentiments)
    dominant = counts.most_common(1)[0][0]
    score = sum(_QUAL_SCORES.get(s, 0.0) for s in sentiments) / len(sentiments)
    parts = [f"{name}:{count}" for name, count in counts.items()]
    return dominant, score, ", ".join(parts)


def _template_confluence_note(confluence: str, label: str, z_score: float | None, qual_score: float) -> str:
    z_txt = _fmt_signed(z_score, 2, "sigma")
    q_txt = _fmt_signed(qual_score, 2)
    if confluence == "BULL_CONFLUENCE":
        return f"Quant momentum and analyst tone align bullishly for {label} (quant {z_txt}, qual {q_txt})."
    if confluence == "BEAR_CONFLUENCE":
        return f"Quant momentum and analyst tone align bearishly for {label} (quant {z_txt}, qual {q_txt})."
    if confluence == "DIVERGENCE":
        return f"Quant and analyst signals disagree for {label} (quant {z_txt}, qual {q_txt}), creating a two-way setup."
    return f"Signal alignment is mixed for {label} (quant {z_txt}, qual {q_txt}); conviction remains limited."


def _template_outlook(confluence: str, label: str) -> str:
    if confluence == "BULL_CONFLUENCE":
        return f"Bias stays constructive for {label} while momentum and sentiment remain aligned."
    if confluence == "BEAR_CONFLUENCE":
        return f"Bias stays defensive for {label} unless sentiment and momentum materially improve."
    if confluence == "DIVERGENCE":
        return f"{label} remains tactical; resolution should come from either analyst upgrades or price mean reversion."
    return f"{label} outlook is range-bound until either quant momentum or analyst tone breaks decisively."


def _template_watch(confluence: str, latest_signal: dict | None) -> str:
    if confluence == "DIVERGENCE":
        return "Watch whether the next analyst print confirms momentum or rejects it."
    if latest_signal and latest_signal.get("fundamentals"):
        return f"Watch fundamentals trend in the next report ({latest_signal.get('fundamentals')})."
    if confluence == "BULL_CONFLUENCE":
        return "Watch for momentum rollover in spot rates or a downshift in report sentiment."
    if confluence == "BEAR_CONFLUENCE":
        return "Watch for sentiment stabilization that could trigger a countertrend rebound."
    return "Watch for a clear break in both momentum and analyst tone."


def _template_macro_note(dry_conf: str, tanker_conf: str) -> str:
    if dry_conf == tanker_conf and dry_conf in {"BULL_CONFLUENCE", "BEAR_CONFLUENCE"}:
        direction = "risk-on" if dry_conf == "BULL_CONFLUENCE" else "risk-off"
        return f"Cross-sector signal alignment is {direction}: dry bulk and tanker narratives point in the same direction."
    if "DIVERGENCE" in {dry_conf, tanker_conf}:
        return "Cross-sector setup is mixed: at least one vessel class is in divergence, so relative-value positioning may outperform outright beta."
    return "Cross-sector signals are mixed with no broad confluence across dry bulk and tanker segments."


def _template_vessel_entry(
    vessel_key: str,
    pre_conf: str,
    qual_signals: list[dict],
    snapshot: dict,
    tanker_z: float | None,
) -> dict:
    is_dry = vessel_key == "dry_bulk"
    label = "dry bulk" if is_dry else "tanker"
    primary_key = "bdi" if is_dry else "clean_tanker"
    secondary_key = "capesize" if is_dry else "dirty_tanker"
    primary = snapshot.get(primary_key, {})
    secondary = snapshot.get(secondary_key, {})
    primary_value = primary.get("value")
    primary_regime = primary.get("regime", "N/A")
    primary_z = primary.get("z_score_252d")
    primary_roc = primary.get("roc60")
    primary_pctl = primary.get("pctl_5y")
    z_for_logic = primary_z if is_dry else tanker_z
    latest_signal = qual_signals[0] if qual_signals else None
    dominant_sentiment, qual_score, sentiment_mix = _sentiment_mix(qual_signals)

    summary_parts = [
        f"{label.title()} is in {primary_regime.lower()} regime at {primary_value if primary_value is not None else 'N/A'}, "
        f"with z-score {_fmt_signed(z_for_logic, 2, 'sigma')} and ROC60 {_fmt_signed(primary_roc, 1, '%')}.",
        f"Recent analyst sentiment skews {dominant_sentiment} ({sentiment_mix}).",
        _template_confluence_note(pre_conf, label, z_for_logic, qual_score),
    ]
    summary = " ".join(part.strip() for part in summary_parts if part.strip())

    key_signals = [
        f"Quant: {primary_key.upper()} value={primary_value if primary_value is not None else 'N/A'}, "
        f"z={_fmt_signed(z_for_logic, 2, 'sigma')}, 5Y percentile={_fmt_percentile(primary_pctl)}.",
        f"Qual: last {len(qual_signals)} reports sentiment mix -> {sentiment_mix}.",
    ]
    if secondary:
        key_signals.append(
            f"Cross-check: {secondary_key.upper()} value={secondary.get('value', 'N/A')}, "
            f"z={_fmt_signed(secondary.get('z_score_252d'), 2, 'sigma')}."
        )
    if latest_signal:
        key_signals.append(
            f"Latest report {latest_signal.get('date')}: momentum={latest_signal.get('momentum') or 'N/A'}, "
            f"fundamentals={latest_signal.get('fundamentals') or 'N/A'}."
        )

    return {
        "confluence_type": pre_conf if pre_conf in CONFLUENCE_TYPES else "NEUTRAL",
        "confluence_note": _template_confluence_note(pre_conf, label, z_for_logic, qual_score),
        "summary": summary,
        "key_signals": key_signals[:4],
        "outlook": _template_outlook(pre_conf, label),
        "watch": _template_watch(pre_conf, latest_signal),
        "report_dates": [s.get("date") for s in qual_signals if s.get("date")],
    }


def _overlay_vessel(template_entry: dict, llm_entry: dict | None) -> dict:
    result = dict(template_entry)
    if not isinstance(llm_entry, dict):
        return result

    confluence = _clean_text(llm_entry.get("confluence_type")).upper()
    if confluence in CONFLUENCE_TYPES:
        result["confluence_type"] = confluence

    for key in ("confluence_note", "summary", "outlook", "watch"):
        text = _clean_text(llm_entry.get(key))
        if text:
            result[key] = text

    key_signals = _clean_signals(llm_entry.get("key_signals"))
    if key_signals:
        result["key_signals"] = key_signals

    return result


# ------------------------ Main ------------------------

def main() -> None:
    BRIEFS.mkdir(parents=True, exist_ok=True)

    print("[brief] Building market snapshot from CSVs...")
    snapshot = build_market_snapshot()
    if not snapshot:
        print("[brief] ERROR: no CSV data found; aborting.", file=sys.stderr)
        sys.exit(1)

    print("[brief] Loading qualitative signals...")
    signals = load_signals()
    dry_signals = recent_breakwave(signals, "drybulk")
    tanker_signals = recent_breakwave(signals, "tankers")

    dry_z = snapshot.get("bdi", {}).get("z_score_252d")
    tanker_z = compute_tanker_z(snapshot)
    pre_dry_conf = compute_confluence(dry_z, [s.get("sentiment", "neutral") for s in dry_signals])
    pre_tanker_conf = compute_confluence(tanker_z, [s.get("sentiment", "neutral") for s in tanker_signals])

    print("[brief] Loading wiki excerpts...")
    wiki_dry = wiki_excerpt(WIKI_EXCERPTS["dry_bulk"])
    wiki_tanker = wiki_excerpt(WIKI_EXCERPTS["tanker"])
    wiki_cape = wiki_excerpt(WIKI_EXCERPTS["capesize"])

    print(f"[brief] Provider order: {','.join(LLM_PROVIDER_ORDER)}")
    prompt = build_prompt(snapshot, dry_signals, tanker_signals, wiki_dry, wiki_tanker, wiki_cape)
    llm_payload, provider_used, attempted = call_llm_payload(prompt)
    if provider_used:
        print(f"[brief] LLM response accepted from: {provider_used}")
    else:
        print("[brief] All providers unavailable or invalid; using deterministic template.")

    template_dry = _template_vessel_entry("dry_bulk", pre_dry_conf, dry_signals, snapshot, tanker_z)
    template_tanker = _template_vessel_entry("tanker", pre_tanker_conf, tanker_signals, snapshot, tanker_z)

    llm_vessel = (llm_payload or {}).get("vessel_classes", {})
    dry_entry = _overlay_vessel(template_dry, llm_vessel.get("dry_bulk"))
    tanker_entry = _overlay_vessel(template_tanker, llm_vessel.get("tanker"))

    macro_note = _clean_text((llm_payload or {}).get("macro_note"))
    if not macro_note:
        macro_note = _template_macro_note(dry_entry["confluence_type"], tanker_entry["confluence_type"])

    today = date.today().isoformat()
    generated_at = datetime.now(timezone.utc).isoformat()
    generation_mode = "llm" if provider_used else "template"
    generation_provider = provider_used or "template"

    output = {
        "generated_at": generated_at,
        "brief_date": today,
        "generation": {
            "mode": generation_mode,
            "provider_used": generation_provider,
            "provider_order": LLM_PROVIDER_ORDER,
            "attempted_providers": attempted,
        },
        "market_snapshot": snapshot,
        "vessel_classes": {
            "dry_bulk": dry_entry,
            "tanker": tanker_entry,
        },
        "macro_note": macro_note,
        "sources": [s["doc_id"] for s in dry_signals + tanker_signals if s.get("doc_id")],
    }

    latest_path = BRIEFS / "latest.json"
    dated_path = BRIEFS / f"{today}.json"
    for out_path in (latest_path, dated_path):
        out_path.write_text(json.dumps(output, indent=2, ensure_ascii=False), encoding="utf-8")
        try:
            display_path = out_path.relative_to(ROOT)
        except ValueError:
            display_path = out_path
        print(f"[brief] Wrote {display_path}")

    print(
        "[brief] Done "
        f"dry={output['vessel_classes']['dry_bulk']['confluence_type']} "
        f"tanker={output['vessel_classes']['tanker']['confluence_type']} "
        f"mode={generation_mode} provider={generation_provider}"
    )


if __name__ == "__main__":
    main()
