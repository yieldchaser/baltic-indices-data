"""
Daily shipping market brief generator.

Reads quantitative CSV data + recent Breakwave signals + wiki context,
calls Gemini to produce a trader-focused JSON brief, and writes:
  knowledge/briefs/latest.json
  knowledge/briefs/YYYY-MM-DD.json

Designed to run from the repo root as:
  python scripts/generate_brief.py
"""
from __future__ import annotations

import csv
import json
import math
import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
KNOWLEDGE = ROOT / "knowledge"
DERIVED   = KNOWLEDGE / "derived"
WIKI      = KNOWLEDGE / "wiki"
BRIEFS    = KNOWLEDGE / "briefs"

SIGNALS_FILE = DERIVED / "signals.jsonl"

# CSV files: key -> path  (DD-MM-YYYY, Index, %Change)
CSV_FILES = {
    "bdi":          ROOT / "bdiy_historical.csv",
    "capesize":     ROOT / "cape_historical.csv",
    "panamax":      ROOT / "panama_historical.csv",
    "supramax":     ROOT / "suprama_historical.csv",
    "clean_tanker": ROOT / "cleantanker_historical.csv",
    "dirty_tanker": ROOT / "dirtytanker_historical.csv",
}

WIKI_EXCERPTS = {
    "dry_bulk":  WIKI / "dry_bulk_market.md",
    "capesize":  WIKI / "capesize.md",
    "tanker":    WIKI / "tanker_market.md",
}

GEMINI_MODEL   = "gemini-1.5-flash"
RECENT_REPORTS = 4   # most recent breakwave reports per category


# ── Quantitative helpers ──────────────────────────────────────────────────────

def parse_csv_series(path: Path) -> list[float | None]:
    """Parse DD-MM-YYYY,Index,Change CSV → list of values in chronological order."""
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
    m = sum(window_vals) / len(window_vals)
    variance = sum((v - m) ** 2 for v in window_vals) / len(window_vals)
    return m, math.sqrt(variance) if variance > 0 else 0.0


def compute_zscore_252d(values: list[float | None]) -> float | None:
    """Rolling 252-day Z-score of the last value. Matches index.html logic."""
    non_null = [v for v in values if v is not None]
    if not non_null:
        return None
    current = non_null[-1]
    m, sd = rolling_mean_std(values, 252)
    if m is None or sd is None:
        return None
    return round((current - m) / sd, 3) if sd > 0 else 0.0


def compute_regime(values: list[float | None]) -> tuple[str, str, float | None, float | None]:
    """
    Matches the Momentum Regime logic in index.html:
      MA(200) anchor + ROC(60) velocity.
    Returns (regime, emoji, ma200, roc60_pct).
    """
    non_null = [v for v in values if v is not None]
    if len(non_null) < 201:
        return "INSUFFICIENT_DATA", "⚪", None, None

    current = non_null[-1]
    ma200   = sum(non_null[-200:]) / 200

    if len(non_null) >= 62:
        base = non_null[-61]
        roc60 = ((current - base) / base * 100) if base else 0.0
    else:
        roc60 = 0.0

    if current > ma200 and roc60 > 0:
        regime, emoji = "EXPANSION", "🟢"
    elif current > ma200:
        regime, emoji = "DISTRIBUTION", "🟡"
    elif roc60 > 0:
        regime, emoji = "ACCUMULATION", "🔵"
    else:
        regime, emoji = "CONTRACTION", "🔴"

    return regime, emoji, round(ma200, 1), round(roc60, 2)


def percentile_5y(values: list[float | None]) -> float | None:
    """5-year (252 × 5 trading days) percentile rank of the last value."""
    non_null = [v for v in values if v is not None]
    if not non_null:
        return None
    current = non_null[-1]
    window  = non_null[-(252 * 5):]
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
        current                = non_null[-1]
        regime, emoji, ma200, roc60 = compute_regime(values)
        z                      = compute_zscore_252d(values)
        pctl                   = percentile_5y(values)
        snapshot[name] = {
            "value":        round(current, 1),
            "z_score_252d": z,
            "pctl_5y":      pctl,
            "regime":       regime,
            "regime_emoji": emoji,
            "ma200":        ma200,
            "roc60":        roc60,
        }
    return snapshot


# ── Qualitative helpers ───────────────────────────────────────────────────────

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
        s for s in signals
        if s.get("source") == "breakwave"
        and s.get("category") == category
        and s.get("date", "0000") not in ("0000-00-00", "", None)
        and s.get("sentiment") is not None
    ]
    filtered.sort(key=lambda x: x.get("date", ""), reverse=True)
    return filtered[:n]


# ── Confluence engine (pre-LLM, also used as fallback) ───────────────────────

_QUAL_SCORES = {
    "positive": 1.0, "constructive": 0.75, "cautiously_bullish": 0.5,
    "neutral": 0.0,  "mixed": 0.0,
    "cautiously_bearish": -0.5, "negative": -1.0,
}

def compute_confluence(z_score: float | None, sentiments: list[str]) -> str:
    """
    Classify confluence between quantitative Z-score and qualitative sentiments.
    DIVERGENCE fires when quant and qual point in opposite directions — this is
    the high-value trader signal the system prioritises.
    """
    if not sentiments or z_score is None:
        return "NEUTRAL"
    qual = sum(_QUAL_SCORES.get(s, 0.0) for s in sentiments) / len(sentiments)
    if z_score > 0.5  and qual > 0.25:  return "BULL_CONFLUENCE"
    if z_score < -0.5 and qual < -0.25: return "BEAR_CONFLUENCE"
    if (z_score > 0.5 and qual < -0.25) or (z_score < -0.5 and qual > 0.25):
        return "DIVERGENCE"
    return "NEUTRAL"


# ── Wiki context ──────────────────────────────────────────────────────────────

def wiki_excerpt(path: Path, max_chars: int = 700) -> str:
    try:
        text = path.read_text(encoding="utf-8")
        if text.startswith("---"):
            end = text.find("---", 3)
            if end > 0:
                text = text[end + 3:].strip()
        return text[:max_chars]
    except FileNotFoundError:
        return ""


# ── Gemini prompt ─────────────────────────────────────────────────────────────

def _fmt_snapshot_line(name: str, snap: dict) -> str:
    z  = snap.get("z_score_252d")
    r  = snap.get("roc60")
    p  = snap.get("pctl_5y")
    return (
        f"  {name.upper():15s} val={snap.get('value', 'N/A'):.0f}  "
        f"Z={z:+.2f}σ  "
        f"regime={snap.get('regime')} {snap.get('regime_emoji','')}  "
        f"ROC60={r:+.1f}%  "
        f"5Y_pctl={p:.0%}"
    ).replace("=None", "=N/A")


def _fmt_signal(s: dict) -> str:
    parts = [f"  {s.get('date')}: sentiment={s.get('sentiment')}  "
             f"momentum={s.get('momentum')}  fundamentals={s.get('fundamentals')}"]
    if s.get("bdryff_30d_pct") is not None:
        parts.append(f"    BDRYFF 30D={s['bdryff_30d_pct']:+.1f}%  YTD={s.get('bdryff_ytd_pct',0):+.1f}%  YoY={s.get('bdryff_yoy_pct',0):+.1f}%")
    if s.get("bdi_30d_pct") is not None:
        parts.append(f"    BDI  30D={s['bdi_30d_pct']:+.1f}%  YTD={s.get('bdi_ytd_pct',0):+.1f}%  YoY={s.get('bdi_yoy_pct',0):+.1f}%")
    if s.get("china_iron_ore_imports_yoy") is not None:
        parts.append(f"    China iron ore YoY={s['china_iron_ore_imports_yoy']:+.1f}%  fleet YoY={s.get('dry_bulk_fleet_yoy',0):+.1f}%")
    return "\n".join(parts)


def build_prompt(snapshot: dict, dry_sigs: list[dict], tanker_sigs: list[dict],
                 wiki_dry: str, wiki_tanker: str, wiki_cape: str) -> str:
    today = date.today().isoformat()

    snap_lines = "\n".join(
        _fmt_snapshot_line(k, v) for k, v in snapshot.items()
    )
    dry_block    = "\n\n".join(_fmt_signal(s) for s in dry_sigs)   or "  No recent reports."
    tanker_block = "\n\n".join(_fmt_signal(s) for s in tanker_sigs) or "  No recent reports."

    return f"""You are a senior shipping freight market analyst generating a daily brief for {today}.

## Quantitative Market Snapshot (from live CSV data)
Momentum Regime = MA(200) anchor + ROC(60) velocity — mirrors the Dashboard tab logic.
{snap_lines}

## Recent Breakwave Dry Bulk Reports (last {len(dry_sigs)})
{dry_block}

## Recent Breakwave Tanker Reports (last {len(tanker_sigs)})
{tanker_block}

## Knowledge Base — Dry Bulk Market
{wiki_dry}

## Knowledge Base — Capesize Dynamics
{wiki_cape}

## Knowledge Base — Tanker Market
{wiki_tanker}

## Task
Analyse the CONFLUENCE or DIVERGENCE between quantitative (Z-score, regime) and qualitative
(Breakwave sentiment/momentum/fundamentals) signals. When they disagree, that divergence IS the
signal — explain it clearly so a trader can act on it.

Return ONLY valid JSON (no markdown fences) matching exactly this schema:
{{
  "vessel_classes": {{
    "dry_bulk": {{
      "confluence_type": "<BULL_CONFLUENCE|BEAR_CONFLUENCE|DIVERGENCE|NEUTRAL>",
      "confluence_note": "<1-2 sentences: does quant agree with qual? what does the gap mean?>",
      "summary": "<2-3 sentences, trader-focused, reference specific index levels or regime>",
      "key_signals": ["<signal 1>", "<signal 2>", "<signal 3>"],
      "outlook": "<1 sentence directional view + key catalyst>",
      "watch": "<1 risk or catalyst to monitor>"
    }},
    "tanker": {{
      "confluence_type": "<BULL_CONFLUENCE|BEAR_CONFLUENCE|DIVERGENCE|NEUTRAL>",
      "confluence_note": "<1-2 sentences>",
      "summary": "<2-3 sentences>",
      "key_signals": ["<signal 1>", "<signal 2>", "<signal 3>"],
      "outlook": "<1 sentence>",
      "watch": "<1 risk or catalyst>"
    }}
  }},
  "macro_note": "<1-2 sentences cross-sector observation, or empty string>"
}}"""


# ── Gemini call ───────────────────────────────────────────────────────────────

def call_gemini(prompt: str) -> dict | None:
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        print("[brief] GEMINI_API_KEY not set — skipping LLM step", file=sys.stderr)
        return None
    try:
        import google.generativeai as genai
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel(
            GEMINI_MODEL,
            generation_config={"temperature": 0.25, "max_output_tokens": 1200},
        )
        response = model.generate_content(prompt)
        raw = response.text.strip()
        # Strip markdown code fences if present
        if raw.startswith("```"):
            raw = raw.split("```", 2)[1]
            if raw.startswith("json"):
                raw = raw[4:]
            raw = raw.rsplit("```", 1)[0].strip()
        return json.loads(raw)
    except Exception as exc:
        print(f"[brief] Gemini error: {exc}", file=sys.stderr)
        return None


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    BRIEFS.mkdir(parents=True, exist_ok=True)

    print("[brief] Building market snapshot from CSVs...")
    snapshot = build_market_snapshot()
    if not snapshot:
        print("[brief] ERROR: no CSV data found — aborting.", file=sys.stderr)
        sys.exit(1)

    print("[brief] Loading qualitative signals...")
    signals     = load_signals()
    dry_sigs    = recent_breakwave(signals, "drybulk")
    tanker_sigs = recent_breakwave(signals, "tankers")

    # Pre-compute confluence (LLM may override, but this is the hard fallback)
    dry_z    = snapshot.get("bdi", {}).get("z_score_252d")
    tanker_z = None
    if "clean_tanker" in snapshot and "dirty_tanker" in snapshot:
        cz = snapshot["clean_tanker"].get("z_score_252d")
        dz = snapshot["dirty_tanker"].get("z_score_252d")
        if cz is not None and dz is not None:
            tanker_z = (cz + dz) / 2
        else:
            tanker_z = cz or dz
    elif "clean_tanker" in snapshot:
        tanker_z = snapshot["clean_tanker"].get("z_score_252d")

    pre_dry_conf    = compute_confluence(dry_z,    [s.get("sentiment","neutral") for s in dry_sigs])
    pre_tanker_conf = compute_confluence(tanker_z, [s.get("sentiment","neutral") for s in tanker_sigs])

    print("[brief] Loading wiki excerpts...")
    w_dry    = wiki_excerpt(WIKI_EXCERPTS["dry_bulk"])
    w_tanker = wiki_excerpt(WIKI_EXCERPTS["tanker"])
    w_cape   = wiki_excerpt(WIKI_EXCERPTS["capesize"])

    # ── LLM narrative ───────────────────────────────────────────────────────
    llm: dict | None = None
    if os.environ.get("GEMINI_API_KEY"):
        print("[brief] Calling Gemini for narrative generation...")
        prompt = build_prompt(snapshot, dry_sigs, tanker_sigs, w_dry, w_tanker, w_cape)
        llm    = call_gemini(prompt)
        if llm:
            print("[brief] Gemini response OK.")
        else:
            print("[brief] Gemini unavailable — using rule-based confluence only.")
    else:
        print("[brief] No GEMINI_API_KEY — using rule-based confluence only.")

    # ── Assemble output ──────────────────────────────────────────────────────
    def vessel_entry(pre_conf: str, llm_vc: dict | None, qual_sigs: list[dict]) -> dict:
        base: dict = {
            "confluence_type": pre_conf,
            "confluence_note": "",
            "summary":         "",
            "key_signals":     [],
            "outlook":         "",
            "watch":           "",
            "report_dates":    [s.get("date") for s in qual_sigs if s.get("date")],
        }
        if llm_vc:
            for k in ("confluence_type", "confluence_note", "summary", "key_signals", "outlook", "watch"):
                if k in llm_vc:
                    base[k] = llm_vc[k]
        return base

    llm_vc       = (llm or {}).get("vessel_classes", {})
    today        = date.today().isoformat()
    generated_at = datetime.now(timezone.utc).isoformat()

    output = {
        "generated_at":  generated_at,
        "brief_date":    today,
        "market_snapshot": snapshot,
        "vessel_classes": {
            "dry_bulk": vessel_entry(pre_dry_conf,    llm_vc.get("dry_bulk"), dry_sigs),
            "tanker":   vessel_entry(pre_tanker_conf, llm_vc.get("tanker"),   tanker_sigs),
        },
        "macro_note": (llm or {}).get("macro_note", ""),
        "sources": [s["doc_id"] for s in dry_sigs + tanker_sigs if s.get("doc_id")],
    }

    latest_path = BRIEFS / "latest.json"
    dated_path  = BRIEFS / f"{today}.json"
    for p in (latest_path, dated_path):
        p.write_text(json.dumps(output, indent=2, ensure_ascii=False))
        print(f"[brief] Wrote {p.relative_to(ROOT)}")

    dry_conf    = output["vessel_classes"]["dry_bulk"]["confluence_type"]
    tanker_conf = output["vessel_classes"]["tanker"]["confluence_type"]
    print(f"[brief] Done — dry={dry_conf}  tanker={tanker_conf}")


if __name__ == "__main__":
    main()
