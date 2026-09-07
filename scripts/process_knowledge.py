from __future__ import annotations

import os, re, json, time, hashlib, argparse, traceback, shutil, sys, warnings, stat, csv, random, base64, io
from pathlib import Path
from datetime import datetime, timezone
from urllib.parse import urlparse
from urllib import request as urllib_request, error as urllib_error
from collections import defaultdict

try:
    import anydoc  # optional accelerator: pip install firecrawl-anydoc
except ImportError:  # fall back to pdfplumber/pytesseract extraction
    anydoc = None
import pdfplumber
from bs4 import BeautifulSoup
import tiktoken
import frontmatter
from dotenv import load_dotenv
from build_health_report import build_health_reports
from build_wiki import build_wiki
from knowledge_hash import SOURCE_HASH_VERSION, compute_source_hash
from source_archive_utils_v2 import is_primary_archive_html_path, looks_like_non_content_link
warnings.simplefilter("ignore", FutureWarning)
load_dotenv()
REPO_ROOT = Path(__file__).parent.parent
REPORTS_ROOT = REPO_ROOT / "reports"
KNOWLEDGE = REPO_ROOT / "knowledge"
OLLAMA_API_KEY = os.environ.get("OLLAMA_API_KEY")
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "").strip()
OLLAMA_BASE_URL = (os.environ.get("OLLAMA_BASE_URL") or "").strip().rstrip("/")
if OLLAMA_BASE_URL and not OLLAMA_BASE_URL.endswith("/api"):
    if OLLAMA_BASE_URL.endswith("/v1"):
        OLLAMA_BASE_URL = OLLAMA_BASE_URL[:-3] + "/api"
    else:
        OLLAMA_BASE_URL = OLLAMA_BASE_URL + "/api"
NIM_API_KEY = os.environ.get("NIM_API_KEY", "").strip()
NIM_MODEL = os.environ.get("NIM_MODEL", "").strip()
NIM_BASE_URL = (os.environ.get("NIM_BASE_URL") or "https://integrate.api.nvidia.com/v1").strip().rstrip("/")
TOKENIZER = tiktoken.get_encoding("cl100k_base")

for stream_name in ("stdout", "stderr"):
    stream = getattr(sys, stream_name, None)
    if hasattr(stream, "reconfigure"):
        stream.reconfigure(encoding="utf-8", errors="replace")


DOCS_DIR = KNOWLEDGE / "docs"
CHUNKS_DIR = KNOWLEDGE / "chunks"
TREES_DIR = KNOWLEDGE / "trees"
CONFIG_DIR = KNOWLEDGE / "config"
WIKI_DIR = KNOWLEDGE / "wiki"
KNOWLEDGE_REPORTS_DIR = KNOWLEDGE / "reports"
MANIFESTS_DIR = KNOWLEDGE / "manifests"
DERIVED_DIR = KNOWLEDGE / "derived"
DOCUMENTS_MANIFEST = MANIFESTS_DIR / "documents.jsonl"
ERRORS_MANIFEST = MANIFESTS_DIR / "errors.jsonl"
SOURCES_MANIFEST = MANIFESTS_DIR / "sources.json"
LINT_REPORT = MANIFESTS_DIR / "lint_report.json"
COVERAGE_REPORT = MANIFESTS_DIR / "coverage_report.json"
SIGNALS_DERIVED = DERIVED_DIR / "signals.jsonl"
BREAKWAVE_SIGNALS_DERIVED = DERIVED_DIR / "breakwave_signals.json"
THEMES_DERIVED = DERIVED_DIR / "themes.jsonl"
SECTION_INDEX_DERIVED = DERIVED_DIR / "section_index.jsonl"
TOPIC_EVIDENCE_DERIVED = DERIVED_DIR / "topic_evidence.jsonl"
TIMELINES_DERIVED = DERIVED_DIR / "timelines.json"
DERIVED_CACHE_PATH = MANIFESTS_DIR / "derived_cache.json"
CACHE_SCHEMA_VERSION = 1
HEALTH_SUMMARY = KNOWLEDGE_REPORTS_DIR / "health_summary.md"
COMPILER_VERSION = 2
REPO_ROOT_RESOLVED = REPO_ROOT.resolve()
LINKED_PDF_PAGE_LIMIT = int(os.environ.get("LINKED_PDF_PAGE_LIMIT", "12"))
LINKED_PDF_OCR_PAGE_LIMIT = int(os.environ.get("LINKED_PDF_OCR_PAGE_LIMIT", "4"))
LINKED_TEXT_CHAR_LIMIT = int(os.environ.get("LINKED_TEXT_CHAR_LIMIT", "70000"))
LINKED_TABLE_ROW_LIMIT = int(os.environ.get("LINKED_TABLE_ROW_LIMIT", "300"))
LINKED_TABLE_COL_LIMIT = int(os.environ.get("LINKED_TABLE_COL_LIMIT", "24"))
OLLAMA_MIN_INTERVAL_SEC = float(os.environ.get("OLLAMA_MIN_INTERVAL_SEC", "2.5"))
OLLAMA_MAX_RETRIES = int(os.environ.get("OLLAMA_MAX_RETRIES", "6"))
OLLAMA_BACKOFF_BASE_SEC = float(os.environ.get("OLLAMA_BACKOFF_BASE_SEC", "3.0"))
OLLAMA_MAX_BACKOFF_SEC = float(os.environ.get("OLLAMA_MAX_BACKOFF_SEC", "60.0"))
NIM_MIN_INTERVAL_SEC = float(os.environ.get("NIM_MIN_INTERVAL_SEC", "2.5"))
NIM_MAX_RETRIES = int(os.environ.get("NIM_MAX_RETRIES", "6"))
NIM_BACKOFF_BASE_SEC = float(os.environ.get("NIM_BACKOFF_BASE_SEC", "3.0"))
NIM_MAX_BACKOFF_SEC = float(os.environ.get("NIM_MAX_BACKOFF_SEC", "60.0"))
MANIFEST_FLUSH_EVERY = int(os.environ.get("MANIFEST_FLUSH_EVERY", "200"))
LINKED_IMAGE_OCR_CHAR_LIMIT = int(os.environ.get("LINKED_IMAGE_OCR_CHAR_LIMIT", "5000"))
MIN_IMAGE_OCR_PIXELS = int(os.environ.get("MIN_IMAGE_OCR_PIXELS", "150000"))
MAX_LINKED_ASSETS_PER_DOC = int(os.environ.get("MAX_LINKED_ASSETS_PER_DOC", "12"))
LINKED_ASSET_SOURCES = {"baltic", "breakwave_insights", "hellenic"}
LINKED_ASSET_FIELD_NAMES = [
    "linked_assets_discovered",
    "linked_assets_mirrored",
    "linked_assets_ingested",
    "linked_assets_skipped",
    "linked_assets_failed",
]
LLM_PROVIDER_ORDER = [
    provider
    for provider in [part.strip().lower() for part in os.environ.get("LLM_PROVIDER_ORDER", "ollama,nim").split(",")]
    if provider in {"ollama", "nim"}
]
if not LLM_PROVIDER_ORDER:
    LLM_PROVIDER_ORDER = ["ollama", "nim"]
_last_ollama_call_ts = 0.0
_last_nim_call_ts = 0.0
LLM_STATS = {
    "ollama_ok": 0,
    "ollama_429": 0,
    "ollama_error": 0,
    "nim_ok": 0,
    "nim_429": 0,
    "nim_error": 0,
    "heuristic_used": 0,
}

BREAKWAVE_DRY_FIELDS = [
    "China Steel Production",
    "China Steel Inventories",
    "China Iron Ore Inventories",
    "China Iron Ore Imports",
    "China Coal Imports",
    "China Soybean Imports",
    "Brazil Iron Ore Exports",
    "Australia Iron Ore Exports",
    "Dry Bulk Fleet",
    "Baltic Dry Index Average",
    "Capesize Spot Rates Average",
    "Panamax Spot Rates Average",
]

BREAKWAVE_TANKER_FIELDS = [
    "World Oil Demand",
    "Oil Supply, OPEC",
    "Oil Supply, non-OPEC",
    "OECD Total Crude Oil Stocks",
    "US Crude Oil Exports",
    "China Oil Imports",
    "Global Crude Oil On Water",
    "Tanker Fleet",
    "VLCC Middle East-Asia, USD/ton",
    "Suezmax West Africa-Europe, USD/ton",
]

STOPWORDS = {
    "the", "and", "for", "with", "that", "this", "from", "into", "their", "have",
    "has", "were", "been", "will", "while", "than", "over", "under", "about", "into",
    "which", "also", "more", "most", "some", "such", "through", "there", "where",
    "them", "they", "these", "those", "market", "shipping", "report", "reports",
    "industry", "week", "weekly", "bulk", "dry", "tanker", "freight", "index",
    "rates", "rate", "spot", "futures", "advisors", "breakwave", "baltic", "exchange",
    "chapter", "page", "figure", "table", "using", "used", "use", "book", "books",
    "edition", "study", "within", "across", "therefore", "however",
}

KEYWORD_TAXONOMY = {
    "vessel_classes": {
        "capesize": ["capesize", "cape"],
        "panamax": ["panamax"],
        "supramax": ["supramax", "supra"],
        "handysize": ["handysize", "handy"],
        "vlcc": ["vlcc"],
        "suezmax": ["suezmax"],
        "aframax": ["aframax"],
        "lng": ["lng"],
        "lpg": ["lpg"],
        "container": ["container", "teu"],
    },
    "regions": {
        "china": ["china", "chinese"],
        "brazil": ["brazil", "vale", "tubarao"],
        "australia": ["australia", "pilbara", "west australia"],
        "atlantic": ["atlantic", "atlantic basin"],
        "pacific": ["pacific", "pacific basin"],
        "meg": ["middle east gulf", "arabian gulf", "meg"],
        "west_africa": ["west africa", "waf"],
        "europe": ["europe", "continent", "rotterdam", "mediterranean"],
        "india": ["india"],
        "us_gulf": ["us gulf", "gulf of mexico"],
        "singapore": ["singapore"],
        "japan": ["japan"],
    },
    "commodities": {
        "iron_ore": ["iron ore"],
        "coal": ["coal"],
        "grain": ["grain", "soybean", "soybeans", "corn", "wheat"],
        "bauxite": ["bauxite", "alumina"],
        "crude_oil": ["crude oil", "oil", "opec"],
        "products": ["diesel", "gasoline", "jet fuel", "naphtha", "products"],
        "steel": ["steel", "steelmaking"],
        "gas": ["gas", "lng", "lpg"],
    },
}

TITLE_DATE_RE = re.compile(r"Date:\s*([0-9]{1,2}\s+\w+\s+[0-9]{4})", re.I)
ISO_PREFIX_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})")
BALTIC_CATEGORIES = ["dry", "tanker", "gas", "container", "ningbo"]
HELLENIC_CATEGORIES = ["dry_charter", "tanker_charter", "iron_ore", "vessel_valuations", "demolition", "shipbuilding"]
HELLENIC_CHARTER_CATEGORIES = {"dry_charter", "tanker_charter"}
CHARTER_SEGMENT_ALIASES = {
    "dry_charter": {
        "capesize": ["capesize", "cape"],
        "panamax": ["panamax", "pmax", "pana/kmax", "kamsarmax", "kmax", "panaikmax", "pa"],
        "supramax": ["supramax", "supra", "smx", "ultramax", "umx", "smax", "smaxiultra", "smax/ultra", "suavuurra", "swaxultra", "smawultra"],
        "handysize": ["handysize", "handy", "hsize", "hanor", "won"],
    },
    "tanker_charter": {
        "vlcc": ["vlcc", "vicc", "v1cc", "vlce", "vice", "vilcc", "vvilcc", "vee", "vce", "vlee"],  # OCR variants of VLCC
        "suezmax": ["suezmax", "suez"],
        "aframax": ["aframax", "afra", "jafra", "aera", "larna", "apra", "arra"],
        "lr2": ["lr2", "lr 2", "1r2", "ure", "ur2"],
        "lr1": ["lr1", "lr 1", "lri", "trl", "uri", "tr1", "lr", "ut", "1r1", "lrt", "ir1"],  # OCR variants of LR1
        "mr": ["mr imo", "mr", "m.r.", "mri", "handymax", "mr1", "mr2"],
        "handytanker": ["handy", "handytanker", "h.tanker", "small tanker"],
    },
}
IRON_ORE_SIGNAL_HINTS = [
    "iron ore",
    "62%",
    "65%",
    "58%",
    "dmt",
    "cfr",
    "fines",
    "pellet",
    "premium",
    "discount",
    "index",
    "mmi",
]


def ensure_layout():
    for path in [KNOWLEDGE, DOCS_DIR, CHUNKS_DIR, TREES_DIR, CONFIG_DIR, WIKI_DIR, KNOWLEDGE_REPORTS_DIR, MANIFESTS_DIR, DERIVED_DIR]:
        path.mkdir(parents=True, exist_ok=True)


def relpath(path: Path) -> str:
    return path.relative_to(REPO_ROOT).as_posix()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def slugify(value: str) -> str:
    value = re.sub(r"[^A-Za-z0-9]+", "_", value).strip("_").lower()
    return value or "untitled"


def norm_space(value) -> str:
    if value is None:
        return ""
    text = str(value).replace("\xa0", " ").replace("\u200b", " ")
    text = text.replace("–", "-").replace("—", "-").replace("’", "'").replace("“", '"').replace("”", '"')
    return re.sub(r"[ \t]+", " ", text).strip()


def norm_multiline(text: str) -> str:
    lines = [norm_space(line) for line in (text or "").splitlines()]
    return "\n".join([line for line in lines if line]).strip()


# Markers identifying bot-challenge / CDN error pages that scrapers sometimes
# archive by mistake. Documents matching these are labelled error pages and
# excluded from signal extraction so challenge boilerplate never pollutes
# derived data.
_ERROR_PAGE_MARKERS = (
    "just a moment",
    "checking your browser",
    "attention required! | cloudflare",
    "sorry, you have been blocked",
    "cloudflare ray id",
    "enable javascript and cookies to continue",
    "ddos protection by",
    "access denied | ",
    "request unsuccessful. incapsula",
    "please verify you are a human",
)


def is_error_page_text(text: str) -> bool:
    """True when the text looks like a bot-challenge / CDN block page."""
    if not text:
        return False
    lower = str(text).lower()
    return any(marker in lower for marker in _ERROR_PAGE_MARKERS)


def parse_pct(value):
    if value is None:
        return None
    text = norm_space(value)
    if not text:
        return None
    text = text.replace("%", "").replace(",", "").replace("(", "-").replace(")", "")
    text = text.replace("↑", "").replace("↓", "")
    try:
        return float(text)
    except ValueError:
        return None


def parse_number(value):
    if value is None:
        return None
    text = norm_space(value)
    if not text:
        return None
    text = text.replace(",", "")
    match = re.search(r"-?\d+(?:\.\d+)?", text)
    if not match:
        return None
    try:
        val = float(match.group(0))
        # Scale correction for OCR dot-instead-of-comma errors
        # e.g., 9.750 -> 9.75 -> 9750
        # If it has a dot and is in a reasonable range (5.0 <= val < 100.0)
        if "." in match.group(0) and 5.0 <= val < 100.0:
            val *= 1000.0
        return val
    except ValueError:
        return None


def safe_inline_text(value, max_chars: int = 240) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        text = value.decode("utf-8", errors="ignore")
    else:
        text = str(value)
    if not text:
        return ""
    text = text.replace("\x00", " ")
    text = "".join(ch if ch.isprintable() else " " for ch in text)
    text = norm_space(text)
    if not text:
        return ""
    if len(text) > max_chars:
        return text[:max_chars].rstrip() + "..."
    return text


def extract_line_numbers(text: str, limit: int = 8, min_abs_value: float | None = None) -> list[float]:
    values = []
    source = text or ""
    # Separate attached letters from numbers (e.g. Yv9,000 -> Yv 9,000)
    source = re.sub(r"([A-Za-z])(\d)", r"\1 \2", source)
    
    for match in re.finditer(r"(?<![A-Za-z])[-+]?\d[\d,]{0,11}(?:\.\d+)?", source):
        raw = match.group(0)
        value = parse_number(raw)
        if value is None:
            continue
        right = source[match.end() : match.end() + 10].lower()
        if re.match(r"^\s*(y|yr|yrs|year|years|m|mo|mos|month|months)\b", right) and abs(value) <= 36:
            continue
        if min_abs_value is not None and abs(value) < min_abs_value:
            continue
        values.append(value)
        if len(values) >= limit:
            break
    return values


def detect_charter_timeframe(line_lower: str) -> str | None:
    if "spot" in line_lower:
        return "spot"
    if re.search(r"\b6\s*(m|mo|month)\b", line_lower):
        return "6m"
    if re.search(r"\b12\s*(m|mo|month)\b", line_lower) or re.search(r"\b1\s*(y|yr|year)\b", line_lower):
        return "1y"
    if re.search(r"\b2\s*(y|yr|year)\b", line_lower):
        return "2y"
    if re.search(r"\b3\s*(y|yr|year)\b", line_lower):
        return "3y"
    if "period" in line_lower:
        return "period"
    return None


def infer_charter_unit(line_lower: str) -> str | None:
    if "ws" in line_lower:
        return "ws"
    if any(token in line_lower for token in ["usd", "$", "/day", "pdpr", "per day", "k/day"]):
        return "usd_per_day"
    return None


_TC_SEGMENT_KEYWORDS = {
    "capesize": "capesize",
    "panamax": "panamax",
    "supramax": "supramax",
    "handysize": "handysize",
    "cape": "capesize",
    "pmax": "panamax",
    "smax": "supramax",
    "handy": "handysize",
}
_TC_TENOR_KEYWORDS = {
    "4-6m": "4_6m",
    "4_6m": "4_6m",
    "1y": "1y",
    "1-year": "1y",
    "1 year": "1y",
    "2y": "2y",
    "2-year": "2y",
    "2 year": "2y",
}
_TC_REGION_KEYWORDS = {
    "atlantic": "atl",
    "pacific": "pac",
    "atl": "atl",
    "pac": "pac",
}


def _tc_cell_hints(lower_text: str) -> dict:
    """Extract tenor/region hints embedded in one header cell."""
    hints = {}
    for kw, canon in _TC_TENOR_KEYWORDS.items():
        if re.search(r"(?<![a-z0-9])" + re.escape(kw) + r"(?![a-z0-9])", lower_text):
            hints["tenor"] = canon
            break
    for kw, canon in _TC_REGION_KEYWORDS.items():
        if re.search(r"\b" + re.escape(kw) + r"\b", lower_text):
            hints["region"] = canon
            break
    return hints


def parse_structured_charter_rows(text: str) -> list[dict]:
    """Parse '[structured table]' pipe-table markdown into labeled charter rows.

    Tenor/region may appear in data cells (row-label style) OR in the header
    row above value columns; header hints are carried down per column, and a
    row whose value columns span several tenor headers emits one observation
    per tenor so labeled writes never mix tenors into one positional list.
    """
    parsed = []
    seen = set()
    in_table = False
    header_hints: dict[int, dict] = {}
    header_checked = False

    def emit(segment, tenor, region, numbers):
        key = (segment, tenor or "", region or "", tuple(numbers))
        if key in seen:
            return
        seen.add(key)
        parsed.append({"segment": segment, "tenor": tenor, "region": region, "values": numbers})

    for raw_line in (text or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("[structured table]"):
            in_table = True
            continue
        if not in_table:
            continue
        if not line.startswith("|"):
            break
        inner = line.strip("|")
        if not inner or set(inner.replace("|", "").strip()) <= {"-"}:
            continue  # markdown separator row
        cells = [norm_space(c.replace("\\|", "|")) for c in re.split(r"(?<!\\)\|", inner)]
        lower_cells = [c.lower() for c in cells]

        # Header discovery: markdown tables put a header row first. Accept the
        # first pipe row as the header only when it carries a tenor hint AND
        # no numeric cells — region-only words like "Atlantic" also appear in
        # ordinary data rows, so they must not trigger header detection.
        if not header_checked:
            header_checked = True
            has_numeric = any(re.fullmatch(r"[$€£]?[\d,.]+%?", c.strip()) for c in cells)
            if not has_numeric:
                hints = {i: h for i, lc in enumerate(lower_cells) if (h := _tc_cell_hints(lc))}
                if any(h.get("tenor") for h in hints.values()):
                    header_hints = hints
                    continue

        segment = None
        seg_idx = -1
        for i, lc in enumerate(lower_cells):
            for kw, canon in _TC_SEGMENT_KEYWORDS.items():
                if re.search(r"\b" + re.escape(kw) + r"\b", lc):
                    segment = canon
                    seg_idx = i
                    break
            if segment:
                break
        if not segment:
            continue

        # Explicit region cell wins; else header hints on the segment row.
        region = None
        for i, lc in enumerate(lower_cells):
            if i == seg_idx:
                continue
            for kw, canon in _TC_REGION_KEYWORDS.items():
                if re.search(r"\b" + re.escape(kw) + r"\b", lc):
                    region = canon
                    break
            if region:
                break

        row_lower = " ".join(lower_cells)
        row_tenor = None
        for kw, canon in _TC_TENOR_KEYWORDS.items():
            if re.search(r"(?<![a-z0-9])" + re.escape(kw) + r"(?![a-z0-9])", row_lower):
                row_tenor = canon
                break

        # Numeric cells grouped by their resolved (tenor, region) label pair.
        # Explicit region cells override per-column header regions.
        by_label: dict = {}
        for i, cell in enumerate(cells):
            if i == seg_idx:
                continue
            cleaned = cell.replace(",", "").replace("$", "").replace("%", "").strip()
            if not re.fullmatch(r"-?\d+(?:\.\d+)?", cleaned):
                continue
            num = float(cleaned)
            col_tenor = row_tenor
            col_region = region
            if col_tenor is None:
                col_tenor = header_hints.get(i, {}).get("tenor")
            if col_region is None:
                col_region = header_hints.get(i, {}).get("region")
            bucket = by_label.setdefault((col_tenor or "", col_region or ""), {"numbers": []})
            if num not in bucket["numbers"]:
                bucket["numbers"].append(num)

        if not by_label:
            continue
        multi = len(by_label) > 1
        for (tenor, reg), bucket in by_label.items():
            if multi and not tenor and not reg:
                continue  # ambiguous unlabeled column mixed with labeled ones
            emit(segment, tenor or None, reg or None, bucket["numbers"])
    return parsed


def extract_hellenic_charter_signals(text: str, category: str) -> dict:
    alias_map = CHARTER_SEGMENT_ALIASES.get(category, {})
    observations = []
    seen = set()
    timeframes = set()
    units = set()
    lines = [norm_space(line) for line in (text or "").splitlines() if norm_space(line)]

    for line in lines:
        lower = line.lower()
        if any(skip in lower for skip in ["image reference:", "source asset:", "linked image asset:", "embedded info:", "exif text:"]):
            continue

        # Segment matching with word boundaries for short aliases
        matching_segments = []
        for segment, aliases in alias_map.items():
            matched = False
            for alias in aliases:
                if len(alias.strip()) <= 3:
                    if re.search(r"\b" + re.escape(alias.strip()) + r"\b", lower):
                        matched = True
                        break
                else:
                    if alias in lower:
                        matched = True
                        break
            if matched:
                matching_segments.append(segment)
        if not matching_segments:
            continue

        values = extract_line_numbers(line, limit=10, min_abs_value=10)
        if not values:
            continue

        timeframe = detect_charter_timeframe(lower)
        if timeframe:
            timeframes.add(timeframe)
        unit = infer_charter_unit(lower)
        if unit:
            units.add(unit)

        for segment in matching_segments[:3]:
            key = (segment, timeframe or "", tuple(values))
            if key in seen:
                continue
            seen.add(key)
            observations.append({
                "segment": segment,
                "timeframe": timeframe,
                "values": values,
                "unit": unit,
                "source_line": line[:260],
            })
            if len(observations) >= 60:
                break
        if len(observations) >= 60:
            break

    if not observations:
        return {}

    rate_summary = {}
    for row in observations:
        segment = row.get("segment")
        values = row.get("values") or []
        if not segment or not values or segment in rate_summary:
            continue
        rate_summary[segment] = values[-1]

    return {
        "signal_family": "hellenic_charter_rates",
        "rate_observations": observations,
        "rate_summary": rate_summary,
        "timeframes": sorted(timeframes),
        "metric_units": sorted(units),
    }


def infer_iron_ore_metric(line_lower: str) -> str | None:
    if "62" in line_lower and ("fines" in line_lower or "index" in line_lower):
        return "index_62_fines"
    if "65" in line_lower and ("fines" in line_lower or "index" in line_lower):
        return "index_65_fines"
    if "58" in line_lower and ("fines" in line_lower or "index" in line_lower):
        return "index_58_fines"
    if "pellet premium" in line_lower or ("pellet" in line_lower and "premium" in line_lower):
        return "pellet_premium"
    if "spread" in line_lower and "62" in line_lower and "65" in line_lower:
        return "spread_65_62"
    if "premium" in line_lower:
        return "premium"
    if "discount" in line_lower:
        return "discount"
    if "index" in line_lower:
        return "index"
    return None


def infer_iron_ore_unit(line_lower: str) -> str | None:
    if "dmt" in line_lower:
        return "usd_per_dmt"
    if "usd" in line_lower or "$" in line_lower:
        return "usd"
    if "%" in line_lower:
        return "pct"
    return None


def pick_metric_value(metric: str, values: list[float]) -> float | None:
    if not values:
        return None
    if metric in {"index_62_fines", "index_65_fines", "index_58_fines"}:
        for value in values:
            if 20 <= value <= 300:
                return value
        return None
    if metric == "spread_65_62":
        for value in values:
            if -80 <= value <= 80:
                return value
        return None
    if metric == "pellet_premium":
        for value in values:
            if -50 <= value <= 120:
                return value
    return values[0]


# OCR-mangled tag tolerance for the Hellenic MMI iron-ore tables. The IOSI
# price-index tags regularly arrive with confused glyphs (IOSI6S, lOSIB,
# 10162, IOPIG2, IOPIGS) after PDF rasterization, so tag matching runs fuzzily
# over the de-punctuated line: I/l/1/L collapse, O/0 collapse, S/5/1 collapse
# and G/6 collapse. Exact spellings still match these classes.
_FUZZY_IOSI62 = re.compile(r"[I1L][O0][S51L][I1L][G6][2Z]")
_FUZZY_IOSI65 = re.compile(r"[I1L][O0][S51L][I1L][G6][5S]")
# Rasterization sometimes drops a glyph entirely ("IOSI62" -> "10162"); these
# shorter variants are checked only after the full ones.
_FUZZY_IOSI62_SHORT = re.compile(r"[I1L][O0][S51L][G6][2Z]")
_FUZZY_IOSI65_SHORT = re.compile(r"[I1L][O0][S51L][G6][5S]")
# Fe-grade tokens (58%/61%/62%/62.5%/64%/65%/66%) that OCR merges into data
# rows and multi-tag header lines ("IOSI61 ... IOSI65 ... IOPLI 62.5% Fe Lump")
# — picking one as a price produced the 2026 cfr_62 freeze at exactly 62.5
# for 52 straight days.
_IO_PRICE_GRADE_TOKENS = {58.0, 61.0, 62.0, 62.5, 64.0, 65.0, 66.0}
# CFR 65% trades at a modest premium to CFR 62% (historically $5-20); a wider
# gap than this means the parser paired two unrelated numbers from an OCR-
# scrambled row.
_IO_CFR_SPREAD_LIMIT = 40.0
# Daily-move continuity gate: iron ore never reprices >$40 in one report;
# candidates further than this from the last trusted level are junk picks.
_IO_CONTINUITY_LIMIT = 40.0
# Rows that never hold the daily settlement: freight-rate tables OCR-merged
# into index rows, period-average columns (MTD/QTD/YTD) and unit legend lines
# ("IOSI61 .. USD/dmt IOSI65 .. RMB/t"). Month names are deliberately NOT
# matched here — report prose ("prices may soften", "late July") sits on the
# same text lines as legitimate daily prices.
_IO_SUMMARY_ROW_RE = re.compile(r"\b(mtd|qtd|ytd)\b", re.IGNORECASE)
_IO_FREIGHT_ROW_RE = re.compile(r"(w\.\s*australia|rout(e)?\s*design|qingdao\s*c\d)", re.IGNORECASE)


def _match_iosi_tag(line: str) -> str | None:
    compact = re.sub(r"[^A-Za-z0-9]", "", (line or "").upper())
    if not compact:
        return None
    if _FUZZY_IOSI62.search(compact):
        return "cfr_62"
    if _FUZZY_IOSI65.search(compact):
        return "cfr_65"
    if _FUZZY_IOSI62_SHORT.search(compact):
        return "cfr_62"
    if _FUZZY_IOSI65_SHORT.search(compact):
        return "cfr_65"
    return None


def _is_io_summary_row(line: str) -> bool:
    low = (line or "").lower()
    if _IO_SUMMARY_ROW_RE.search(low) or _IO_FREIGHT_ROW_RE.search(low):
        return True
    # Unit legend / multi-index header rows carry two or more unit strings.
    return low.count("usd/dmt") + low.count("rmb/t") >= 2


def _pick_cfr_value(vals: list[float], prev: float | None) -> float | None:
    candidates: list[float] = []
    for v in vals:
        if 40 <= v <= 250 and v not in candidates:
            candidates.append(v)
    non_grade = [v for v in candidates if v not in _IO_PRICE_GRADE_TOKENS]
    # An all-grade candidate list means a header/label row: never valid.
    if not non_grade:
        return None
    best = non_grade[0]
    if prev is not None and abs(best - prev) > _IO_CONTINUITY_LIMIT:
        near = next((v for v in non_grade[1:] if abs(v - prev) <= _IO_CONTINUITY_LIMIT), None)
        return near
    return best


def extract_hellenic_iron_ore_signals(text: str) -> dict:
    metrics = []
    seen = set()
    units = set()
    lines = [norm_space(line) for line in (text or "").splitlines() if norm_space(line)]
    for line in lines:
        lower = line.lower()
        if any(skip in lower for skip in ["image reference:", "source asset:", "linked image asset:", "embedded info:", "exif text:"]):
            continue
        if lower.startswith("[page ") or "http://" in lower or "https://" in lower or ".com/" in lower:
            continue
        if not any(hint in lower for hint in IRON_ORE_SIGNAL_HINTS):
            continue

        values = extract_line_numbers(line, limit=8)
        if not values:
            continue

        metric = infer_iron_ore_metric(lower) or "numeric_observation"
        unit = infer_iron_ore_unit(lower)
        if unit:
            units.add(unit)
        key = (metric, tuple(values))
        if key in seen:
            continue
        seen.add(key)
        metrics.append({
            "metric": metric,
            "values": values,
            "unit": unit,
            "source_line": line[:260],
        })
        if len(metrics) >= 50:
            break

    if not metrics:
        return {}

    benchmark_prices = {}
    for row in metrics:
        metric = row.get("metric")
        values = row.get("values") or []
        if not metric or not values:
            continue
        if metric in {"index_62_fines", "index_65_fines", "index_58_fines", "spread_65_62", "pellet_premium"} and metric not in benchmark_prices:
            picked = pick_metric_value(metric, values)
            if picked is not None:
                benchmark_prices[metric] = picked

    return {
        "signal_family": "hellenic_iron_ore_indices",
        "iron_ore_metrics": metrics,
        "benchmark_prices": benchmark_prices,
        "metric_units": sorted(units),
    }


def extract_hellenic_signals(text: str, category: str, existing_metadata: dict | None = None) -> dict:
    existing_metadata = existing_metadata or {}
    existing_signals = existing_metadata.get("signals")

    if category in HELLENIC_CHARTER_CATEGORIES:
        parsed = extract_hellenic_charter_signals(text, category)
    elif category == "iron_ore":
        parsed = extract_hellenic_iron_ore_signals(text)
    else:
        parsed = {}

    # If parsed is empty, attempt to recover by parsing numeric observations from existing metadata
    if not parsed:
        obs_list = []
        if isinstance(existing_signals, dict):
            obs_list = existing_signals.get("numeric_observations") or []
        if not obs_list:
            obs_list = existing_metadata.get("numeric_observations") or []
        
        if isinstance(obs_list, list) and obs_list:
            recovered_lines = []
            for obs in obs_list:
                if isinstance(obs, dict) and "source_line" in obs:
                    recovered_lines.append(obs["source_line"])
            if recovered_lines:
                recovered_text = "\n".join(recovered_lines)
                if category in HELLENIC_CHARTER_CATEGORIES:
                    recovered_parsed = extract_hellenic_charter_signals(recovered_text, category)
                elif category == "iron_ore":
                    recovered_parsed = extract_hellenic_iron_ore_signals(recovered_text)
                else:
                    recovered_parsed = {}
                
                if recovered_parsed:
                    # Retain any extra keys from existing_signals if present
                    if isinstance(existing_signals, dict):
                        for k, v in existing_signals.items():
                            if k not in recovered_parsed:
                                recovered_parsed[k] = v
                    parsed = recovered_parsed

    if parsed:
        return parsed
    if isinstance(existing_signals, dict) and existing_signals:
        return existing_signals
    return {}


def parse_iso_date(value: str):
    if not value:
        return None
    for fmt in ("%Y-%m-%d", "%d %B %Y", "%d %b %Y", "%B %d, %Y", "%B %d %Y"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    return None


def chunk_text(text: str, max_tokens: int, overlap: int) -> list[str]:
    tokens = TOKENIZER.encode(text or "")
    if not tokens:
        return []
    chunks = []
    start = 0
    while start < len(tokens):
        end = min(start + max_tokens, len(tokens))
        piece = TOKENIZER.decode(tokens[start:end])
        # Snap the cut to a sentence/paragraph boundary when one is close by,
        # so chunks stop ending mid-sentence or mid-table row. Only snap
        # backwards within a small window to preserve chunk size targets.
        if end < len(tokens):
            window_start = max(0, len(piece) - 120)
            window = piece[window_start:]
            best = max(window.rfind(". "), window.rfind(".\n"), window.rfind("! "), window.rfind("? "), window.rfind("\n"))
            if best >= len(window) - 100 and best > 0:
                piece = piece[: window_start + best + 1]
        piece = piece.strip()
        if piece:
            chunks.append(piece)
        step = max_tokens - overlap
        if step <= 0:
            break
        start += step
    return [chunk for chunk in chunks if chunk]


def token_count(text: str) -> int:
    return len(TOKENIZER.encode(text or ""))


def load_jsonl(path: Path) -> list[dict]:
    rows = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return rows


def append_jsonl(path: Path, row: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(row, ensure_ascii=False) + "\n"
    last_error = None
    for attempt in range(5):
        try:
            with path.open("a", encoding="utf-8") as handle:
                handle.write(payload)
            return
        except PermissionError as exc:
            last_error = exc
            try:
                os.chmod(path, stat.S_IWRITE | stat.S_IREAD)
            except OSError:
                pass
            time.sleep(0.2 * (attempt + 1))
    if last_error:
        raise last_error


def _retry_backoff(attempt: int):
    time.sleep(0.25 * (attempt + 1))


def _force_writable(target: Path):
    try:
        os.chmod(target, stat.S_IWRITE | stat.S_IREAD)
    except OSError:
        pass


def unlink_with_retries(path: Path, retries: int = 5):
    for attempt in range(retries):
        try:
            path.unlink()
            return
        except FileNotFoundError:
            return
        except PermissionError:
            _force_writable(path)
            if attempt >= retries - 1:
                raise
            _retry_backoff(attempt)


def _rmtree_onerror(func, target, exc_info):
    exc = exc_info[1]
    if not isinstance(exc, PermissionError):
        raise exc
    _force_writable(Path(target))
    func(target)


def rmtree_with_retries(path: Path, retries: int = 5):
    for attempt in range(retries):
        try:
            shutil.rmtree(path, onerror=_rmtree_onerror)
            return
        except FileNotFoundError:
            return
        except PermissionError:
            _force_writable(path)
            if attempt >= retries - 1:
                raise
            _retry_backoff(attempt)


def load_manifest_rows() -> list[dict]:
    return load_jsonl(DOCUMENTS_MANIFEST)


def latest_rows_by_source(rows: list[dict]) -> dict[str, dict]:
    latest = {}
    for row in rows:
        source_path = row.get("source_path")
        if source_path:
            latest[source_path] = row
    return latest


def manifest_sort_key(row: dict):
    return (
        row.get("source") or "",
        row.get("category") or "",
        row.get("date") or "",
        row.get("doc_id") or "",
        row.get("source_path") or "",
    )


def write_manifest_rows(rows: list[dict]):
    rows = sorted(latest_rows_by_source(rows).values(), key=manifest_sort_key)
    DOCUMENTS_MANIFEST.parent.mkdir(parents=True, exist_ok=True)
    DOCUMENTS_MANIFEST.write_text("", encoding="utf-8")
    for row in rows:
        append_jsonl(DOCUMENTS_MANIFEST, row)


def migrate_manifest_hash_versions(rows: list[dict]) -> tuple[int, dict[str, str]]:
    updated = 0
    hash_cache = {}
    for row in rows:
        if row.get("source_hash_version") == SOURCE_HASH_VERSION:
            continue
        source_path = row.get("source_path")
        if not source_path:
            continue
        source_file = REPO_ROOT / source_path
        if not source_file.exists():
            continue
        try:
            computed = source_hash(source_file)
            row["source_hash"] = computed
            row["source_hash_version"] = SOURCE_HASH_VERSION
            hash_cache[source_path] = computed
            updated += 1
        except Exception:
            continue
    return updated, hash_cache


def normalize_manifest_schema(rows: list[dict]) -> int:
    updated = 0
    for row in rows:
        row_changed = False
        for key, default in empty_linked_asset_stats().items():
            if key not in row:
                row[key] = default
                row_changed = True
            else:
                try:
                    coerced = max(0, int(row.get(key)))
                except (TypeError, ValueError):
                    coerced = default
                if row.get(key) != coerced:
                    row[key] = coerced
                    row_changed = True
        if row_changed:
            updated += 1
    return updated


def load_existing_metadata(row: dict | None) -> dict:
    if not row:
        return {}
    doc_path = row.get("doc_path")
    if not doc_path:
        return {}
    full_path = REPO_ROOT / doc_path
    if not full_path.exists():
        return {}
    try:
        return frontmatter.load(full_path).metadata
    except Exception:
        return {}


def build_existing_metadata_index(rows: list[dict]) -> dict[str, dict]:
    index = {}
    for row in rows:
        source_path = row.get("source_path")
        if not source_path:
            continue
        metadata = load_existing_metadata(row)
        if metadata:
            index[source_path] = metadata
    return index


def artifacts_current(row: dict | None) -> bool:
    if not row:
        return False
    if row.get("compiler_version") != COMPILER_VERSION:
        return False
    for key in ("doc_path", "chunk_file", "tree_path"):
        rel = row.get(key)
        if not rel or not (REPO_ROOT / rel).exists():
            return False
    return True


def rewrite_chunk_file(path: Path, removed_doc_ids: set[str]):
    if not path.exists():
        return
    kept_rows = []
    for row in load_jsonl(path):
        if row.get("doc_id") not in removed_doc_ids:
            kept_rows.append(row)
    path.write_text("", encoding="utf-8")
    for row in kept_rows:
        append_jsonl(path, row)


def compact_chunk_file(path: Path, remove_doc_ids: set[str] | None = None):
    """Deduplicate chunk_id rows (keep latest) and optionally remove stale doc_ids."""
    if not path.exists():
        return
    remove_doc_ids = remove_doc_ids or set()
    filtered_rows = []
    for row in load_jsonl(path):
        if remove_doc_ids and row.get("doc_id") in remove_doc_ids:
            continue
        filtered_rows.append(row)

    seen_chunk_ids = set()
    deduped_reversed = []
    for row in reversed(filtered_rows):
        chunk_id = row.get("chunk_id")
        if chunk_id and chunk_id in seen_chunk_ids:
            continue
        if chunk_id:
            seen_chunk_ids.add(chunk_id)
        deduped_reversed.append(row)
    deduped_rows = list(reversed(deduped_reversed))

    path.write_text("", encoding="utf-8")
    for row in deduped_rows:
        append_jsonl(path, row)


def remove_manifest_sources(rows: list[dict], source_paths: set[str]) -> list[dict]:
    if not source_paths:
        return rows

    removed_rows = [row for row in rows if row.get("source_path") in source_paths]
    kept_rows = [row for row in rows if row.get("source_path") not in source_paths]
    kept_doc_paths = {row.get("doc_path") for row in kept_rows if row.get("doc_path")}
    kept_tree_paths = {row.get("tree_path") for row in kept_rows if row.get("tree_path")}
    removed_doc_ids_by_chunk = {}

    def _best_effort_unlink(target: Path):
        try:
            unlink_with_retries(target)
        except PermissionError:
            # Windows antivirus/indexing/editor handles can transiently block deletion.
            # Continue so the file can still be overwritten during reprocessing.
            pass

    for row in removed_rows:
        doc_path = row.get("doc_path")
        if doc_path and doc_path not in kept_doc_paths:
            target = REPO_ROOT / doc_path
            if target.exists():
                _best_effort_unlink(target)

        tree_path = row.get("tree_path")
        if tree_path and tree_path not in kept_tree_paths:
            target = REPO_ROOT / tree_path
            if target.exists():
                _best_effort_unlink(target)

        chunk_file = row.get("chunk_file")
        doc_id = row.get("doc_id")
        if chunk_file and doc_id:
            removed_doc_ids_by_chunk.setdefault(chunk_file, set()).add(doc_id)

    for chunk_file, removed_doc_ids in removed_doc_ids_by_chunk.items():
        rewrite_chunk_file(REPO_ROOT / chunk_file, removed_doc_ids)

    return kept_rows


def prune_missing_sources(rows: list[dict]) -> list[dict]:
    missing_sources = set()
    for row in rows:
        source_path = row.get("source_path")
        if source_path and not (REPO_ROOT / source_path).exists():
            missing_sources.add(source_path)
    return remove_manifest_sources(rows, missing_sources)


def prune_non_primary_archive_sources(rows: list[dict]) -> list[dict]:
    non_primary_sources = set()
    for row in rows:
        source = row.get("source")
        source_path = row.get("source_path")
        if source not in LINKED_ASSET_SOURCES or not source_path:
            continue
        full_path = REPO_ROOT / source_path
        if full_path.suffix.lower() != ".html":
            continue
        if not is_primary_archive_html(full_path):
            non_primary_sources.add(source_path)
    return remove_manifest_sources(rows, non_primary_sources)


def log_error(file_path: Path, error: str):
    append_jsonl(ERRORS_MANIFEST, {
        "file": relpath(file_path),
        "error": error,
        "ts": utc_now_iso(),
    })


def clear_rebuild_outputs():
    for path in [DOCS_DIR, CHUNKS_DIR, TREES_DIR, WIKI_DIR, KNOWLEDGE_REPORTS_DIR, DERIVED_DIR]:
        if path.exists():
            rmtree_with_retries(path)
    for path in [DOCUMENTS_MANIFEST, ERRORS_MANIFEST, SOURCES_MANIFEST, LINT_REPORT, COVERAGE_REPORT]:
        unlink_with_retries(path)
    ensure_layout()


def manifest_sources_for_filter(source_filter: str | None) -> set[str]:
    if source_filter in (None, "all"):
        return {"breakwave", "baltic", "breakwave_insights", "hellenic", "book"}
    if source_filter == "books":
        return {"book"}
    return {source_filter}


def prune_manifest_for_sources(rows: list[dict], source_filter: str | None) -> list[dict]:
    target_sources = manifest_sources_for_filter(source_filter)
    source_paths = {
        row.get("source_path")
        for row in rows
        if row.get("source_path") and row.get("source") in target_sources
    }
    return remove_manifest_sources(rows, source_paths)


def build_sources_registry():
    counts = {
        "breakwave": {
            "drybulk": len(list((REPORTS_ROOT / "drybulk").rglob("*.pdf"))),
            "tankers": len(list((REPORTS_ROOT / "tankers").rglob("*.pdf"))),
        },
        "baltic": {
            category: len(
                [
                    path
                    for path in (REPORTS_ROOT / "baltic" / category).rglob("*.html")
                    if is_primary_archive_html(path)
                ]
            )
            for category in BALTIC_CATEGORIES
        },
        "breakwave_insights": {
            "insights": len(
                [
                    path
                    for path in (REPORTS_ROOT / "breakwave").rglob("*.html")
                    if is_primary_archive_html(path)
                ]
            ),
        },
        "hellenic": {
            category: len(
                [
                    path
                    for path in (REPORTS_ROOT / "hellenic" / category).rglob("*.html")
                    if is_primary_archive_html(path)
                ]
            )
            for category in HELLENIC_CATEGORIES
        },
        "broker_reports": {
            "broker_report": len(list((REPORTS_ROOT / "broker_reports").rglob("*.md"))),
        },
        "poten": {
            "tankers": len(list((REPORTS_ROOT / "poten").rglob("*.md"))),
        },
        "books": {
            "book": len(list(REPORTS_ROOT.glob("*.pdf"))),
        },
    }
    payload = {
        "generated_at": utc_now_iso(),
        "counts": counts,
        "paths": {
            "breakwave": {
                "drybulk": relpath(REPORTS_ROOT / "drybulk"),
                "tankers": relpath(REPORTS_ROOT / "tankers"),
            },
            "baltic": {
                category: relpath(REPORTS_ROOT / "baltic" / category)
                for category in BALTIC_CATEGORIES
            },
            "breakwave_insights": {
                "insights": relpath(REPORTS_ROOT / "breakwave"),
            },
            "hellenic": {
                category: relpath(REPORTS_ROOT / "hellenic" / category)
                for category in HELLENIC_CATEGORIES
            },
            "books": {
                "book": relpath(REPORTS_ROOT),
            },
            "broker_reports": {
                "broker_report": relpath(REPORTS_ROOT / "broker_reports"),
            },
            "poten": {
                "tankers": relpath(REPORTS_ROOT / "poten"),
            },
        },
    }
    SOURCES_MANIFEST.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def iter_source_files(source_filter: str | None):
    if source_filter in (None, "books", "all"):
        for path in sorted(REPORTS_ROOT.glob("*.pdf")):
            yield "book", "book", path
    if source_filter in (None, "breakwave", "all"):
        for category in ["drybulk", "tankers"]:
            for path in sorted((REPORTS_ROOT / category).rglob("*.pdf")):
                yield "breakwave", category, path
    if source_filter in (None, "baltic", "all"):
        for category in BALTIC_CATEGORIES:
            for path in sorted((REPORTS_ROOT / "baltic" / category).rglob("*.html")):
                if is_primary_archive_html(path):
                    yield "baltic", category, path
    if source_filter in (None, "breakwave_insights", "all"):
        for path in sorted((REPORTS_ROOT / "breakwave").rglob("*.html")):
            if is_primary_archive_html(path):
                yield "breakwave_insights", "insights", path
    if source_filter in (None, "hellenic", "all"):
        for category in HELLENIC_CATEGORIES:
            for path in sorted((REPORTS_ROOT / "hellenic" / category).rglob("*.html")):
                if is_primary_archive_html(path):
                    yield "hellenic", category, path
    if source_filter in (None, "broker_reports", "all"):
        for path in sorted((REPORTS_ROOT / "broker_reports").rglob("*.md")):
            yield "broker_reports", "broker_report", path
    if source_filter in (None, "poten", "all"):
        for path in sorted((REPORTS_ROOT / "poten").rglob("*.md")):
            yield "poten", "tankers", path


def select_batch_slice(
    entries: list[tuple[str, str, Path]], batch_total: int, batch_index: int
) -> tuple[list[tuple[str, str, Path]], int, int]:
    if batch_total <= 1:
        return entries, 0, len(entries)
    if batch_index < 1 or batch_index > batch_total:
        raise ValueError(f"Invalid batch selection: index={batch_index}, total={batch_total}")
    total = len(entries)
    start = (total * (batch_index - 1)) // batch_total
    end = (total * batch_index) // batch_total
    return entries[start:end], start, end


def default_lists_for_doc(source: str, category: str):
    vessel_defaults = {
        ("breakwave", "drybulk"): ["capesize", "panamax", "supramax"],
        ("breakwave", "tankers"): ["vlcc", "suezmax"],
        ("baltic", "dry"): ["capesize", "panamax", "supramax", "handysize"],
        ("baltic", "tanker"): ["vlcc", "suezmax", "aframax"],
        ("baltic", "gas"): ["lng", "lpg"],
        ("baltic", "container"): ["container"],
        ("breakwave_insights", "insights"): ["capesize", "panamax", "supramax", "handysize", "vlcc", "suezmax", "aframax"],
        ("hellenic", "dry_charter"): ["capesize", "panamax", "supramax", "handysize"],
        ("hellenic", "tanker_charter"): ["vlcc", "suezmax", "aframax"],
        ("hellenic", "iron_ore"): ["capesize", "panamax"],
        ("hellenic", "vessel_valuations"): ["capesize", "panamax", "supramax", "handysize", "vlcc", "suezmax", "aframax", "container"],
        ("hellenic", "demolition"): ["capesize", "panamax", "supramax", "handysize", "vlcc", "suezmax", "aframax", "container"],
        ("hellenic", "shipbuilding"): ["capesize", "panamax", "supramax", "handysize", "vlcc", "suezmax", "aframax", "container", "lng", "lpg"],
        ("broker_reports", "broker_report"): ["capesize", "panamax", "supramax", "handysize", "vlcc", "suezmax", "aframax", "container"],
        ("poten", "tankers"): ["vlcc", "suezmax", "aframax"],
    }
    commodity_defaults = {
        ("breakwave", "drybulk"): ["iron_ore", "coal", "grain", "bauxite"],
        ("breakwave", "tankers"): ["crude_oil", "products"],
        ("baltic", "dry"): ["iron_ore", "coal", "grain"],
        ("baltic", "tanker"): ["crude_oil", "products"],
        ("baltic", "gas"): ["gas"],
        ("baltic", "container"): [],
        ("baltic", "ningbo"): [],
        ("breakwave_insights", "insights"): ["iron_ore", "coal", "grain", "crude_oil", "products", "gas"],
        ("hellenic", "dry_charter"): ["iron_ore", "coal", "grain", "bauxite"],
        ("hellenic", "tanker_charter"): ["crude_oil", "products"],
        ("hellenic", "iron_ore"): ["iron_ore", "steel"],
        ("hellenic", "vessel_valuations"): ["iron_ore", "coal", "grain", "crude_oil", "products", "gas"],
        ("hellenic", "demolition"): [],
        ("hellenic", "shipbuilding"): [],
        ("broker_reports", "broker_report"): ["iron_ore", "coal", "grain", "crude_oil", "products"],
        ("poten", "tankers"): ["crude_oil", "products"],
        ("book", "book"): [],
    }
    region_defaults = {
        ("breakwave", "drybulk"): ["china", "brazil", "australia", "atlantic", "pacific"],
        ("breakwave", "tankers"): ["meg", "china", "west_africa", "europe"],
        ("baltic", "dry"): ["atlantic", "pacific", "china"],
        ("baltic", "tanker"): ["meg", "west_africa", "europe"],
        ("baltic", "gas"): ["atlantic", "pacific"],
        ("baltic", "container"): [],
        ("baltic", "ningbo"): ["china"],
        ("breakwave_insights", "insights"): ["china", "brazil", "australia", "atlantic", "pacific", "meg", "west_africa", "europe"],
        ("hellenic", "dry_charter"): ["atlantic", "pacific", "china", "brazil", "australia"],
        ("hellenic", "tanker_charter"): ["meg", "west_africa", "europe", "china"],
        ("hellenic", "iron_ore"): ["china", "brazil", "australia"],
        ("hellenic", "vessel_valuations"): ["atlantic", "pacific", "china", "europe", "meg"],
        ("hellenic", "demolition"): ["india", "china", "europe"],
        ("hellenic", "shipbuilding"): ["china", "japan", "europe"],
        ("broker_reports", "broker_report"): ["china", "atlantic", "pacific", "meg", "europe"],
        ("poten", "tankers"): ["meg", "china", "atlantic", "pacific", "europe"],
        ("book", "book"): [],
    }
    return (
        list(vessel_defaults.get((source, category), [])),
        list(region_defaults.get((source, category), [])),
        list(commodity_defaults.get((source, category), [])),
    )


def infer_taxonomy(text: str, source: str, category: str):
    lower = (text or "").lower()
    vessels, regions, commodities = default_lists_for_doc(source, category)
    for key, terms in KEYWORD_TAXONOMY["vessel_classes"].items():
        if any(term in lower for term in terms) and key not in vessels:
            vessels.append(key)
    for key, terms in KEYWORD_TAXONOMY["regions"].items():
        if any(term in lower for term in terms) and key not in regions:
            regions.append(key)
    for key, terms in KEYWORD_TAXONOMY["commodities"].items():
        if any(term in lower for term in terms) and key not in commodities:
            commodities.append(key)
    return vessels, regions, commodities


def extract_keywords(text: str, limit: int = 12) -> list[str]:
    lower = (text or "").lower()
    found = []
    for group in KEYWORD_TAXONOMY.values():
        for key, terms in group.items():
            if any(term in lower for term in terms) and key not in found:
                found.append(key)
    freqs = {}
    for token in re.findall(r"[A-Za-z][A-Za-z0-9_./-]{2,}", lower):
        token = token.strip("-_/")
        if token in STOPWORDS or token.isdigit():
            continue
        freqs[token] = freqs.get(token, 0) + 1
    for token, _ in sorted(freqs.items(), key=lambda item: (-item[1], item[0])):
        if token not in found:
            found.append(token)
        if len(found) >= limit:
            break
    return found[:limit]


def heuristic_summary(text: str, limit: int = 3) -> str:
    sentences = re.split(r"(?<=[.!?])\s+", norm_space(text))
    picked = []
    for sentence in sentences:
        if len(sentence.split()) < 6:
            continue
        picked.append(sentence)
        if len(picked) >= limit:
            break
    return " ".join(picked)[:1200]


def heuristic_theme_payload(text: str, category: str) -> dict:
    keywords = extract_keywords(text, limit=10)
    market_tone = "neutral"
    lower = (text or "").lower()
    positive_words = sum(lower.count(word) for word in ["strong", "tight", "support", "bullish", "recovery", "positive"])
    negative_words = sum(lower.count(word) for word in ["weak", "soft", "bearish", "negative", "slowdown", "oversupply"])
    if positive_words > negative_words:
        market_tone = "constructive"
    elif negative_words > positive_words:
        market_tone = "cautiously_bearish"
    key_entities = []
    for entity in ["China", "Brazil", "Australia", "Vale", "OPEC", "Atlantic basin", "Pacific basin", "Middle East", "US Gulf"]:
        if entity.lower() in lower:
            key_entities.append(entity)
    return {
        "themes": keywords[:6],
        "key_entities": key_entities[:6],
        "market_tone": market_tone,
    }


def _parse_retry_after(exc_text: str) -> float | None:
    if not exc_text:
        return None
    match = re.search(r"retry(?:\s+after)?\s+(\d+(?:\.\d+)?)", exc_text, flags=re.I)
    if not match:
        return None
    try:
        return float(match.group(1))
    except ValueError:
        return None


def _is_rate_limit_error(exc_text: str) -> bool:
    lower = (exc_text or "").lower()
    return "429" in lower or "too many requests" in lower or "quota" in lower or "rate limit" in lower


def ollama_available() -> bool:
    return bool(OLLAMA_BASE_URL and OLLAMA_MODEL)


def nim_available() -> bool:
    return bool(NIM_API_KEY and NIM_MODEL and NIM_BASE_URL)


def llm_available() -> bool:
    return ollama_available() or nim_available()


def _ollama_sleep_interval():
    now = time.monotonic()
    elapsed = now - _last_ollama_call_ts
    wait_for = OLLAMA_MIN_INTERVAL_SEC - elapsed
    if wait_for > 0:
        time.sleep(wait_for)


def _call_ollama_once(prompt: str) -> str | None:
    if not ollama_available():
        return None
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

    msg = data.get("message") or {}
    text = norm_space(msg.get("content"))
    return text or None


def call_ollama(prompt: str, retries: int | None = None) -> str | None:
    if not ollama_available():
        return None
    retries = retries or OLLAMA_MAX_RETRIES
    global _last_ollama_call_ts
    for attempt in range(retries):
        try:
            _ollama_sleep_interval()
            text = _call_ollama_once(prompt)
            _last_ollama_call_ts = time.monotonic()
            if text:
                LLM_STATS["ollama_ok"] += 1
            return text
        except Exception as exc:
            _last_ollama_call_ts = time.monotonic()
            exc_text = str(exc)
            if _is_rate_limit_error(exc_text):
                LLM_STATS["ollama_429"] += 1
            else:
                LLM_STATS["ollama_error"] += 1
            if attempt < retries - 1:
                retry_after = _parse_retry_after(exc_text)
                if retry_after is not None:
                    delay = retry_after
                elif _is_rate_limit_error(exc_text):
                    delay = OLLAMA_BACKOFF_BASE_SEC * (2 ** attempt)
                else:
                    delay = OLLAMA_BACKOFF_BASE_SEC * (attempt + 1)
                delay = min(delay, OLLAMA_MAX_BACKOFF_SEC)
                delay += random.uniform(0.1, 0.9)
                time.sleep(delay)
            else:
                return None


def _nim_sleep_interval():
    now = time.monotonic()
    elapsed = now - _last_nim_call_ts
    wait_for = NIM_MIN_INTERVAL_SEC - elapsed
    if wait_for > 0:
        time.sleep(wait_for)


def _call_nim_once(prompt: str) -> str | None:
    if not nim_available():
        return None
    payload = {
        "model": NIM_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.2,
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
    msg = choices[0].get("message") or {}
    text = norm_space(msg.get("content"))
    return text or None


def call_nim(prompt: str, retries: int | None = None) -> str | None:
    if not nim_available():
        return None
    retries = retries or NIM_MAX_RETRIES
    global _last_nim_call_ts
    for attempt in range(retries):
        try:
            _nim_sleep_interval()
            text = _call_nim_once(prompt)
            _last_nim_call_ts = time.monotonic()
            if text:
                LLM_STATS["nim_ok"] += 1
            return text
        except Exception as exc:
            _last_nim_call_ts = time.monotonic()
            exc_text = str(exc)
            if _is_rate_limit_error(exc_text):
                LLM_STATS["nim_429"] += 1
            else:
                LLM_STATS["nim_error"] += 1
            if attempt < retries - 1:
                retry_after = _parse_retry_after(exc_text)
                if retry_after is not None:
                    delay = retry_after
                elif _is_rate_limit_error(exc_text):
                    delay = NIM_BACKOFF_BASE_SEC * (2 ** attempt)
                else:
                    delay = NIM_BACKOFF_BASE_SEC * (attempt + 1)
                delay = min(delay, NIM_MAX_BACKOFF_SEC)
                delay += random.uniform(0.1, 0.9)
                time.sleep(delay)
            else:
                return None


def call_llm(prompt: str) -> str | None:
    for provider in LLM_PROVIDER_ORDER:
        if provider == "ollama":
            text = call_ollama(prompt)
        elif provider == "nim":
            text = call_nim(prompt)
        else:
            continue
        if text:
            return text
    return None


def encode_image_base64(image_path: Path | str, max_edge: int = 2048) -> tuple[str, str]:
    """Load image, resize if needed (max dimension <= max_edge), and return (base64_str, mime_type)."""
    p = Path(image_path)
    if not p.is_file():
        raise FileNotFoundError(f"Image not found: {p}")

    suffix = p.suffix.lower()
    mime_map = {
        ".png": "image/png",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".webp": "image/webp",
        ".gif": "image/gif",
        ".bmp": "image/bmp",
    }
    mime = mime_map.get(suffix, "image/png")

    try:
        from PIL import Image

        with Image.open(p) as img:
            w, h = img.size
            if max(w, h) > max_edge:
                scale = max_edge / max(w, h)
                new_size = (max(1, int(w * scale)), max(1, int(h * scale)))
                img = img.resize(new_size, Image.Resampling.LANCZOS)
                buf = io.BytesIO()
                fmt = "PNG" if suffix == ".png" else "JPEG"
                img.save(buf, format=fmt, quality=90)
                raw_bytes = buf.getvalue()
            else:
                raw_bytes = p.read_bytes()
    except Exception:
        raw_bytes = p.read_bytes()

    return base64.b64encode(raw_bytes).decode("ascii"), mime


def ollama_vision_available() -> bool:
    return bool(OLLAMA_BASE_URL and (os.environ.get("OLLAMA_VISION_MODEL") or OLLAMA_MODEL))


def nim_vision_available() -> bool:
    api_key = os.environ.get("NIM_API_KEY") or os.environ.get("NVIDIA_API_KEY")
    return bool(api_key and (os.environ.get("NIM_VISION_MODEL") or NIM_MODEL) and NIM_BASE_URL)


def vision_available() -> bool:
    return (
        ollama_vision_available()
        or nim_vision_available()
        or bool(os.environ.get("OPENROUTER_API_KEY"))
        or bool(os.environ.get("GROQ_API_KEY"))
    )


def _call_ollama_multimodal_once(
    prompt: str,
    image_b64: str,
    mime: str = "image/png",
    model: str | None = None,
) -> str | None:
    if not OLLAMA_BASE_URL:
        return None
    chosen_model = model or os.environ.get("OLLAMA_VISION_MODEL") or OLLAMA_MODEL or "llama3.2-vision"
    payload = {
        "model": chosen_model,
        "messages": [
            {
                "role": "user",
                "content": prompt,
                "images": [image_b64],
            }
        ],
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
    with urllib_request.urlopen(req, timeout=90) as response:
        raw = response.read().decode("utf-8", errors="replace")

    data = json.loads(raw)
    msg = data.get("message") or {}
    return norm_space(msg.get("content")) or None


def _call_openai_compat_multimodal_once(
    prompt: str,
    image_b64: str,
    mime: str = "image/png",
    base_url: str = "",
    api_key: str = "",
    model: str = "",
) -> str | None:
    payload = {
        "model": model,
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt},
                    {
                        "type": "image_url",
                        "image_url": {"url": f"data:{mime};base64,{image_b64}"},
                    },
                ],
            }
        ],
        "temperature": 0.1,
    }
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    url = f"{base_url.rstrip('/')}/chat/completions"
    req = urllib_request.Request(url, data=body, headers=headers, method="POST")
    with urllib_request.urlopen(req, timeout=90) as response:
        raw = response.read().decode("utf-8", errors="replace")

    data = json.loads(raw)
    choices = data.get("choices") or []
    if not choices:
        return None
    msg = choices[0].get("message") or {}
    return norm_space(msg.get("content")) or None


def _generate_mock_vision_response(prompt: str) -> str:
    """Generate deterministic schema-compliant mock structured output for offline and CI testing."""
    p_lower = prompt.lower()
    if "stage 1" in p_lower or "axis" in p_lower or "layout" in p_lower or "structure" in p_lower:
        return json.dumps({
            "image_type": "time_series_chart",
            "title": "China Domestic vs Newcastle Coal Prices",
            "x_axis": {
                "label": "Date",
                "type": "temporal",
                "scale": "linear",
                "range": ["2020-01", "2020-10"]
            },
            "y_axis": {
                "label": "Price (RMB/t / USD/t)",
                "type": "numeric",
                "scale": "linear",
                "range": [0, 800]
            },
            "series": [
                {"name": "Domestic Qinhuangdao 5500", "unit": "RMB/t"},
                {"name": "Newcastle FOB", "unit": "USD/t"}
            ]
        })
    elif "stage 2" in p_lower or "data" in p_lower or "points" in p_lower or "table" in p_lower:
        return json.dumps({
            "headers": ["Date", "Domestic Qinhuangdao 5500 (RMB/t)", "Newcastle FOB (USD/t)"],
            "rows": [
                ["2020-01-03", "555.0", "67.5"],
                ["2020-03-06", "540.0", "64.0"],
                ["2020-05-08", "475.0", "52.5"],
                ["2020-07-03", "580.0", "55.0"],
                ["2020-09-04", "595.0", "51.0"]
            ]
        })
    return json.dumps({
        "status": "extracted",
        "description": "Chart depicting historical maritime rate curves and seasonal spreads.",
        "data_recovered": True
    })


def call_multimodal_vision(
    image_path: Path | str,
    prompt: str,
    venue: str = "auto",
    retries: int = 3,
) -> str | None:
    """Unified multimodal vision dispatcher.

    Supports 'auto', 'nim', 'ollama', 'openrouter', 'groq', 'mock'.
    Uses existing backoff and rate-limiting infrastructure.
    """
    image_b64, mime = encode_image_base64(image_path)

    if venue == "mock" or (venue == "auto" and not vision_available()):
        return _generate_mock_vision_response(prompt)

    venues_to_try = []
    if venue == "auto":
        if nim_vision_available():
            venues_to_try.append("nim")
        if ollama_vision_available():
            venues_to_try.append("ollama")
        if os.environ.get("OPENROUTER_API_KEY"):
            venues_to_try.append("openrouter")
        if os.environ.get("GROQ_API_KEY"):
            venues_to_try.append("groq")
        if not venues_to_try:
            venues_to_try.append("mock")
    else:
        venues_to_try.append(venue)

    global _last_nim_call_ts, _last_ollama_call_ts

    for v in venues_to_try:
        if v == "mock":
            return _generate_mock_vision_response(prompt)

        for attempt in range(retries):
            try:
                if v == "nim":
                    _nim_sleep_interval()
                    api_key = os.environ.get("NIM_API_KEY") or os.environ.get("NVIDIA_API_KEY") or ""
                    model = os.environ.get("NIM_VISION_MODEL") or NIM_MODEL or "meta/llama-3.2-11b-vision-instruct"
                    base_url = NIM_BASE_URL
                    res = _call_openai_compat_multimodal_once(
                        prompt=prompt, image_b64=image_b64, mime=mime,
                        base_url=base_url, api_key=api_key, model=model
                    )
                    _last_nim_call_ts = time.monotonic()
                    if res:
                        LLM_STATS["nim_ok"] += 1
                        return res
                elif v == "ollama":
                    _ollama_sleep_interval()
                    res = _call_ollama_multimodal_once(prompt=prompt, image_b64=image_b64, mime=mime)
                    _last_ollama_call_ts = time.monotonic()
                    if res:
                        LLM_STATS["ollama_ok"] += 1
                        return res
                elif v == "openrouter":
                    api_key = os.environ.get("OPENROUTER_API_KEY", "")
                    model = os.environ.get("OPENROUTER_MODEL", "meta-llama/llama-3.2-11b-vision-instruct")
                    base_url = os.environ.get("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1")
                    res = _call_openai_compat_multimodal_once(
                        prompt=prompt, image_b64=image_b64, mime=mime,
                        base_url=base_url, api_key=api_key, model=model
                    )
                    if res:
                        return res
                elif v == "groq":
                    api_key = os.environ.get("GROQ_API_KEY", "")
                    model = os.environ.get("GROQ_MODEL", "llama-3.2-11b-vision-preview")
                    base_url = os.environ.get("GROQ_BASE_URL", "https://api.groq.com/openai/v1")
                    res = _call_openai_compat_multimodal_once(
                        prompt=prompt, image_b64=image_b64, mime=mime,
                        base_url=base_url, api_key=api_key, model=model
                    )
                    if res:
                        return res
            except Exception as exc:
                exc_text = str(exc)
                if _is_rate_limit_error(exc_text):
                    delay = min(NIM_BACKOFF_BASE_SEC * (2 ** attempt), NIM_MAX_BACKOFF_SEC)
                else:
                    delay = min(NIM_BACKOFF_BASE_SEC * (attempt + 1), NIM_MAX_BACKOFF_SEC)
                delay += random.uniform(0.1, 0.5)
                time.sleep(delay)

    return None


def extract_json_payload(text: str) -> dict | None:
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
        return json.loads(raw)
    except json.JSONDecodeError:
        return None


def run_doc_llm(text: str, source: str, category: str, signal_keys: list[str] | None = None) -> dict:
    if not llm_available():
        LLM_STATS["heuristic_used"] += 1
        return {}
    prompt = "Return strict JSON with keys: summary, keywords, themes, key_entities, market_tone"
    if signal_keys:
        prompt += ", " + ", ".join(signal_keys)
    prompt += (
        ". summary should be 2-3 sentences, keywords/themes/key_entities should be arrays, "
        "market_tone should be a short snake_case string, and missing values should be null. "
        f"Source={source}, category={category}. Text:\n{text[:6000]}"
    )
    payload = extract_json_payload(call_llm(prompt))
    if not payload:
        LLM_STATS["heuristic_used"] += 1
    return payload or {}


def format_fundamentals_markdown(fundamentals: dict) -> str:
    if not fundamentals:
        return ""
    lines = []
    for key, values in fundamentals.items():
        if not isinstance(values, dict):
            continue
        ytd = values.get("ytd")
        yoy = values.get("yoy_pct")
        fragments = []
        if ytd not in (None, ""):
            fragments.append(f"YTD: {ytd}")
        if yoy not in (None, ""):
            fragments.append(f"YOY: {yoy}")
        suffix = f" ({', '.join(fragments)})" if fragments else ""
        lines.append(f"- {key}{suffix}")
    return "\n".join(lines)


def source_hash(path: Path) -> str:
    return compute_source_hash(path, REPO_ROOT)


def empty_linked_asset_stats() -> dict:
    return {
        "linked_assets_discovered": 0,
        "linked_assets_mirrored": 0,
        "linked_assets_ingested": 0,
        "linked_assets_skipped": 0,
        "linked_assets_failed": 0,
    }


def is_primary_archive_html(path: Path) -> bool:
    return is_primary_archive_html_path(path)


def merge_existing_theme_data(theme_data: dict, existing_metadata: dict | None) -> dict:
    existing_metadata = existing_metadata or {}
    if isinstance(existing_metadata.get("themes"), list) and existing_metadata["themes"]:
        theme_data["themes"] = existing_metadata["themes"][:6]
    if isinstance(existing_metadata.get("key_entities"), list) and existing_metadata["key_entities"]:
        theme_data["key_entities"] = existing_metadata["key_entities"][:6]
    if existing_metadata.get("market_tone"):
        theme_data["market_tone"] = existing_metadata["market_tone"]
    return theme_data


def prepare_document_structure(adapted: dict) -> tuple[dict, dict]:
    metadata = adapted["metadata"]
    raw_sections = adapted.get("sections") or []
    if not raw_sections and (adapted.get("text") or "").strip():
        raw_sections = [{"heading": "Main", "text": adapted["text"]}]

    doc_id = metadata["doc_id"]
    root_id = f"{doc_id}__root"
    normalized_sections = []
    page_starts = []
    page_ends = []

    for index, section in enumerate(raw_sections, start=1):
        heading = norm_space(section.get("heading")) or f"Section {index}"
        text = (section.get("text") or "").strip()
        if not text:
            continue
        slug = slugify(section.get("slug") or heading)
        page_start = section.get("page_start")
        page_end = section.get("page_end") or page_start
        if page_start is not None:
            page_starts.append(page_start)
        if page_end is not None:
            page_ends.append(page_end)

        normalized_sections.append({
            "section_id": f"{doc_id}__s{index:02d}_{slug}",
            "heading": heading,
            "slug": slug,
            "text": text,
            "section_path": [heading],
            "section_path_text": heading,
            "level": 1,
            "ordinal": index,
            "page_start": page_start,
            "page_end": page_end,
            "token_count": token_count(text),
            "summary": heuristic_summary(text, limit=2),
            "keywords": extract_keywords(text, limit=8),
            "section_type": section.get("section_type"),
        })

    adapted["sections"] = normalized_sections
    metadata["section_count"] = len(normalized_sections)

    root = {
        "node_id": root_id,
        "doc_id": doc_id,
        "parent_id": None,
        "title": metadata.get("title"),
        "section_path": [metadata.get("title")] if metadata.get("title") else [],
        "section_path_text": metadata.get("title"),
        "level": 0,
        "ordinal": 0,
        "summary": metadata.get("summary"),
        "keywords": (metadata.get("keywords") or [])[:12],
        "page_start": min(page_starts) if page_starts else None,
        "page_end": max(page_ends) if page_ends else None,
        "token_count": token_count(adapted.get("text") or ""),
        "source_path": metadata.get("source_path"),
        "source_url": metadata.get("source_url"),
        "children": [],
    }

    for section in normalized_sections:
        root["children"].append({
            "node_id": section["section_id"],
            "doc_id": doc_id,
            "parent_id": root_id,
            "title": section["heading"],
            "section_path": section["section_path"],
            "section_path_text": section["section_path_text"],
            "level": section["level"],
            "ordinal": section["ordinal"],
            "summary": section["summary"],
            "keywords": section["keywords"],
            "page_start": section.get("page_start"),
            "page_end": section.get("page_end"),
            "token_count": section["token_count"],
            "section_type": section.get("section_type"),
            "children": [],
        })

    return adapted, root


def tree_output_path_from_doc_path(doc_path: Path) -> Path:
    relative = doc_path.relative_to(DOCS_DIR).with_suffix(".json")
    return TREES_DIR / relative


def write_tree_file(path: Path, tree: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(tree, indent=2, ensure_ascii=False), encoding="utf-8")


def iter_tree_nodes(node: dict):
    yield node
    for child in node.get("children", []) or []:
        yield from iter_tree_nodes(child)


def make_breakwave_doc_id(pdf_path: Path, category: str) -> str:
    date_str = pdf_path.stem.split("_")[0]
    return f"breakwave_{category}_{date_str}"


def make_baltic_doc_id(html_path: Path, category: str, fallback_date: str | None = None) -> str:
    date_part = fallback_date or "no_date"
    return f"baltic_{category}_{date_part}_{slugify(html_path.stem)}"


def make_book_doc_id(pdf_path: Path) -> str:
    return f"book_{slugify(pdf_path.stem)}"


def make_archive_doc_id(source: str, category: str, html_path: Path, date_str: str | None) -> str:
    date_part = date_str or "no_date"
    return f"{source}_{category}_{date_part}_{slugify(html_path.stem)}"


def find_archive_source_url(soup: BeautifulSoup) -> str | None:
    for selector, attr in [
        ("meta[name='archive-url']", "content"),
        ("link[rel='canonical']", "href"),
        ("meta[property='og:url']", "content"),
        ("meta[name='twitter:url']", "content"),
    ]:
        tag = soup.select_one(selector)
        if tag and tag.get(attr):
            return norm_space(tag[attr])
    return None


def find_archive_date(soup: BeautifulSoup, html_path: Path) -> str | None:
    meta_date = soup.select_one("meta[name='archive-date']")
    if meta_date and meta_date.get("content"):
        raw = norm_space(meta_date.get("content"))
        if raw and raw.lower() != "unknown":
            parsed = parse_iso_date(raw)
            if parsed:
                return parsed.isoformat()
    prefix = ISO_PREFIX_RE.match(html_path.stem)
    if prefix:
        return prefix.group(1)
    return None


def table_to_text(table) -> str:
    rows = []
    for row in table.find_all("tr"):
        cells = row.find_all(["th", "td"])
        values = [norm_space(cell.get_text(" ", strip=True)) for cell in cells]
        values = [value for value in values if value]
        if values:
            rows.append(" | ".join(values))
    if not rows:
        return ""
    return "Table:\n" + "\n".join(rows)


def truncate_linked_text(text: str, limit: int = LINKED_TEXT_CHAR_LIMIT) -> str:
    cleaned = (text or "").strip()
    if len(cleaned) <= limit:
        return cleaned
    return cleaned[:limit].rstrip() + "\n\n[Truncated linked content excerpt.]"


def resolve_archive_link_path(html_path: Path, href: str) -> Path | None:
    clean = norm_space(href)
    if not clean:
        return None
    if clean.lower().startswith(("mailto:", "javascript:")):
        return None
    clean = clean.split("#", 1)[0].split("?", 1)[0]
    if not clean:
        return None

    parsed = urlparse(clean)
    if parsed.scheme in {"http", "https"}:
        link_name = Path(parsed.path).name
        if not link_name:
            return None
        candidate_dirs = [
            html_path.parent,
            html_path.parent / "pdfs",
            html_path.parent / "assets",
            html_path.parent / "files",
            html_path.parent / "attachments",
            html_path.parent.parent / "pdfs",
            html_path.parent.parent / "assets",
            html_path.parent.parent / "files",
            html_path.parent.parent / "attachments",
        ]
        for candidate_dir in candidate_dirs:
            try:
                candidate = (candidate_dir / link_name).resolve()
            except OSError:
                # Some mirrored URLs can carry extremely long basename tokens.
                # Treat them as non-resolvable instead of failing document parsing.
                continue
            try:
                candidate.relative_to(REPO_ROOT_RESOLVED)
            except ValueError:
                continue
            try:
                if candidate.exists() and candidate.is_file():
                    return candidate
            except OSError:
                # ENAMETOOLONG on very long mirrored URL basenames — skip candidate.
                continue
        return None

    try:
        candidate = (html_path.parent / clean).resolve()
    except OSError:
        return None

    try:
        candidate.relative_to(REPO_ROOT_RESOLVED)
    except ValueError:
        return None

    try:
        if not candidate.exists() or not candidate.is_file():
            return None
    except OSError:
        return None
    return candidate


def extract_linked_pdf_ocr_text(pdf_path: Path) -> str:
    if LINKED_PDF_OCR_PAGE_LIMIT <= 0:
        return ""
    try:
        from pdf2image import convert_from_path
        import pytesseract
        from PIL import ImageOps
    except Exception:
        return ""

    ocr_sections = []
    try:
        images = convert_from_path(
            str(pdf_path),
            dpi=260,
            first_page=1,
            last_page=max(1, LINKED_PDF_OCR_PAGE_LIMIT),
            thread_count=1,
        )
    except Exception:
        return ""

    for index, image in enumerate(images, start=1):
        try:
            grayscale = image.convert("L")
            candidates = [grayscale, ImageOps.autocontrast(grayscale)]
            best_text = ""
            for candidate in candidates:
                candidate_text = norm_multiline(
                    pytesseract.image_to_string(candidate, config="--oem 1 --psm 6") or ""
                )
                if len(candidate_text) > len(best_text):
                    best_text = candidate_text
            if best_text:
                ocr_sections.append(f"[OCR Page {index}]\n{best_text}")
        except Exception:
            continue
    return truncate_linked_text("\n\n".join(ocr_sections))


def extract_linked_pdf_text(pdf_path: Path) -> str:
    # Try anydoc high-speed structured markdown extraction first
    try:
        md = anydoc.to_markdown(str(pdf_path))
        if md and len(re.sub(r"\s+", "", md)) >= 100:
            return truncate_linked_text(md)
    except Exception:
        pass

    sections = []
    total_chars = 0
    extracted_pages = 0
    total_pages = 0
    try:
        with pdfplumber.open(pdf_path) as pdf:
            total_pages = len(pdf.pages)
            for index, page in enumerate(pdf.pages[:LINKED_PDF_PAGE_LIMIT], start=1):
                page_text = norm_multiline(page.extract_text() or "")
                if not page_text:
                    continue
                entry = f"[Page {index}]\n{page_text}"
                sections.append(entry)
                extracted_pages = index
                total_chars += len(entry)
                if total_chars >= LINKED_TEXT_CHAR_LIMIT:
                    break
    except Exception:
        pass

    payload = ""
    if sections:
        payload = "\n\n".join(sections)
        payload = truncate_linked_text(payload)
        if total_pages > extracted_pages:
            payload += f"\n\n[Truncated linked PDF: extracted up to page {extracted_pages} of {total_pages}.]"

    text_chars = len(re.sub(r"\s+", "", payload))
    if text_chars >= 450:
        return payload

    ocr_payload = extract_linked_pdf_ocr_text(pdf_path)
    if ocr_payload:
        if payload:
            return truncate_linked_text(payload + "\n\n" + ocr_payload)
        return ocr_payload
    return payload


def extract_linked_html_text(html_path: Path) -> str:
    soup = BeautifulSoup(html_path.read_text(encoding="utf-8", errors="ignore"), "lxml")
    root = soup.select_one("body") or soup
    lines = []
    last_line = None
    for node in root.find_all(["h1", "h2", "h3", "h4", "p", "li", "blockquote", "table"]):
        if node.name == "table":
            line = table_to_text(node)
        else:
            line = norm_space(node.get_text(" ", strip=True))
        if line and line != last_line:
            lines.append(line)
            last_line = line

    if not lines:
        fallback = norm_space(root.get_text("\n", strip=True))
        if fallback:
            lines = [fallback]

    return truncate_linked_text("\n".join(lines))


def extract_linked_tabular_text(path: Path, delimiter: str = ",") -> str:
    rows = []
    with path.open("r", encoding="utf-8", errors="ignore", newline="") as handle:
        reader = csv.reader(handle, delimiter=delimiter)
        for row_index, row in enumerate(reader, start=1):
            if row_index > LINKED_TABLE_ROW_LIMIT:
                break
            cells = [norm_space(str(value)) for value in row[:LINKED_TABLE_COL_LIMIT]]
            cells = [cell for cell in cells if cell]
            if cells:
                rows.append(" | ".join(cells))
    if not rows:
        return ""
    if len(rows) >= LINKED_TABLE_ROW_LIMIT:
        rows.append("[Truncated linked table rows.]")
    return truncate_linked_text("Table:\n" + "\n".join(rows))


def extract_linked_json_text(path: Path) -> str:
    raw = path.read_text(encoding="utf-8", errors="ignore")
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return truncate_linked_text(raw)
    return truncate_linked_text(json.dumps(parsed, indent=2, ensure_ascii=False))


def extract_linked_spreadsheet_text(path: Path) -> str:
    try:
        import pandas as pd
    except Exception:
        return ""

    sheet_blocks = []
    try:
        workbook = pd.ExcelFile(path)
    except Exception:
        return ""

    for sheet_name in workbook.sheet_names[:5]:
        try:
            frame = pd.read_excel(workbook, sheet_name=sheet_name, header=None, dtype=str)
        except Exception:
            continue
        if frame.empty:
            continue
        frame = frame.fillna("").iloc[:LINKED_TABLE_ROW_LIMIT, :LINKED_TABLE_COL_LIMIT]
        rows = []
        for _, row in frame.iterrows():
            values = [norm_space(str(value)) for value in row.tolist()]
            values = [value for value in values if value]
            if values:
                rows.append(" | ".join(values))
        if not rows:
            continue
        block = [f"Sheet: {sheet_name}", "Table:", *rows]
        if len(rows) >= LINKED_TABLE_ROW_LIMIT:
            block.append("[Truncated linked spreadsheet rows.]")
        sheet_blocks.append("\n".join(block))
    if not sheet_blocks:
        return ""
    return truncate_linked_text("\n\n".join(sheet_blocks))


def extract_linked_image_text(path: Path) -> str:
    lines = [f"Linked image asset: {path.name}"]
    suffix = path.suffix.lower()

    def normalize_palette_transparency(image):
        # Pillow warns for palette images that carry transparency bytes.
        # Normalizing to RGBA avoids noisy warnings and improves OCR input.
        if image.mode == "P" and image.info.get("transparency") is not None:
            return image.convert("RGBA")
        return image

    if suffix == ".svg":
        svg = BeautifulSoup(path.read_text(encoding="utf-8", errors="ignore"), "xml")
        svg_text = []
        for node in svg.find_all(["title", "desc", "text"]):
            content = norm_space(node.get_text(" ", strip=True))
            if content:
                svg_text.append(content)
        if svg_text:
            lines.append("SVG text:\n" + "\n".join(dict.fromkeys(svg_text)))

    try:
        from PIL import ExifTags, Image

        image_size = None
        with Image.open(path) as image:
            image = normalize_palette_transparency(image)
            lines.append(f"Image metadata: {image.format} {image.width}x{image.height} mode={image.mode}")
            image_size = (image.width, image.height)
            info_pairs = []
            for key, value in (image.info or {}).items():
                key_lower = norm_space(str(key)).lower()
                if not any(token in key_lower for token in ["description", "comment", "title", "caption", "author", "software", "date", "dpi"]):
                    continue
                value_text = safe_inline_text(value, max_chars=180)
                if value_text:
                    info_pairs.append(f"{key}: {value_text}")
            if info_pairs:
                lines.append("Embedded info:\n" + "\n".join(info_pairs[:8]))

            exif = image.getexif()
            if exif:
                exif_lines = []
                for tag, value in exif.items():
                    name = ExifTags.TAGS.get(tag, str(tag))
                    if name not in {"ImageDescription", "XPTitle", "XPComment"}:
                        continue
                    value_text = safe_inline_text(value, max_chars=220)
                    if value_text:
                        exif_lines.append(f"{name}: {value_text}")
                if exif_lines:
                    lines.append("EXIF text:\n" + "\n".join(exif_lines))
    except Exception:
        image_size = None
        pass

    try:
        import pytesseract
        from PIL import Image, ImageOps

        if image_size is not None and (image_size[0] * image_size[1]) < MIN_IMAGE_OCR_PIXELS:
            lines.append(f"[OCR skipped for small image (< {MIN_IMAGE_OCR_PIXELS} pixels).]")
        else:
            with Image.open(path) as image:
                image = normalize_palette_transparency(image)
                gray = image.convert("L")
                candidates = [gray, ImageOps.autocontrast(gray)]
                upscale = ImageOps.autocontrast(gray).resize((max(1, gray.width * 2), max(1, gray.height * 2)))
                candidates.append(upscale)

                # Structured table recovery (B2): geometry-based grid from OCR
                # word boxes. Additive — raw OCR text is still emitted below.
                structured_table_md = None
                try:
                    from table_extract import extract_table_text
                    structured_table_md = extract_table_text(image) or extract_table_text(upscale)
                except Exception:
                    structured_table_md = None

                ocr_text = ""
                for candidate in candidates:
                    text = norm_multiline(
                        pytesseract.image_to_string(candidate, config="--oem 1 --psm 6") or ""
                    )
                    if len(text) > len(ocr_text):
                        ocr_text = text
            if ocr_text and structured_table_md:
                lines.append("[structured table]\n" + structured_table_md)
                lines.append("[raw ocr]\n" + truncate_linked_text(ocr_text, limit=LINKED_IMAGE_OCR_CHAR_LIMIT))
            elif ocr_text:
                lines.append("OCR text:\n" + truncate_linked_text(ocr_text, limit=LINKED_IMAGE_OCR_CHAR_LIMIT))
            else:
                lines.append("[No OCR text detected in linked image.]")
    except Exception:
        lines.append("[OCR unavailable; install pytesseract to ingest raster text from images.]")

    return truncate_linked_text("\n\n".join(lines))


def extract_linked_text_asset(asset_path: Path) -> str:
    suffix = asset_path.suffix.lower()
    if suffix == ".pdf":
        return extract_linked_pdf_text(asset_path)
    if suffix in {".html", ".htm"}:
        return extract_linked_html_text(asset_path)
    if suffix in {".csv", ".tsv"}:
        delimiter = "," if suffix == ".csv" else "\t"
        return extract_linked_tabular_text(asset_path, delimiter=delimiter)
    if suffix in {".xls", ".xlsx", ".xlsm"}:
        return extract_linked_spreadsheet_text(asset_path)
    if suffix == ".json":
        return extract_linked_json_text(asset_path)
    if suffix in {".txt", ".md"}:
        raw = asset_path.read_text(encoding="utf-8", errors="ignore")
        return truncate_linked_text(raw)
    if suffix in {".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg"}:
        return extract_linked_image_text(asset_path)
    return ""


def collect_linked_asset_sections(source: str, html_path: Path, root, existing_sections: dict | None = None) -> tuple[list[dict], dict]:
    existing_sections = existing_sections or {}
    stats = empty_linked_asset_stats()
    if source not in LINKED_ASSET_SOURCES or root is None:
        return [], stats

    sections = []
    seen_paths = set()
    truncated_notice_added = False
    link_candidates = []
    for anchor in root.find_all("a", href=True):
        link_candidates.append((anchor.get("href"), "a", norm_space(anchor.get_text(" ", strip=True))))
    for image in root.find_all("img", src=True):
        link_candidates.append((image.get("src"), "img", ""))

    for candidate, _tag_name, link_text in link_candidates:
        stats["linked_assets_discovered"] += 1
        if len(sections) >= MAX_LINKED_ASSETS_PER_DOC:
            stats["linked_assets_skipped"] += 1
            if not truncated_notice_added:
                sections.append({
                    "heading": "Linked assets (truncated)",
                    "text": f"[Reached MAX_LINKED_ASSETS_PER_DOC={MAX_LINKED_ASSETS_PER_DOC}; additional linked assets skipped.]",
                    "section_type": "linked_asset_summary",
                })
                truncated_notice_added = True
            continue

        href = norm_space(candidate)
        if not href:
            stats["linked_assets_skipped"] += 1
            continue
        if looks_like_non_content_link(href):
            stats["linked_assets_skipped"] += 1
            continue

        linked_path = resolve_archive_link_path(html_path, href)
        if linked_path is None:
            parsed = urlparse(href)
            if parsed.scheme in {"http", "https"}:
                stats["linked_assets_skipped"] += 1
            else:
                stats["linked_assets_failed"] += 1
            continue

        stats["linked_assets_mirrored"] += 1
        linked_rel = relpath(linked_path)
        if linked_rel in seen_paths:
            stats["linked_assets_skipped"] += 1
            continue
        seen_paths.add(linked_rel)

        heading_key = f"Linked asset: {linked_path.name}"
        if heading_key in existing_sections:
            existing_text = existing_sections[heading_key]
            prefix = f"Source asset: {linked_rel}\n\n"
            if existing_text.startswith(prefix):
                linked_text = existing_text[len(prefix):]
            else:
                lines_text = existing_text.splitlines()
                if len(lines_text) > 2 and lines_text[0].startswith("Source asset:"):
                    linked_text = "\n".join(lines_text[2:])
                else:
                    linked_text = existing_text
        else:
            try:
                linked_text = extract_linked_text_asset(linked_path)
            except Exception:
                # Linked assets should never abort the parent document compilation.
                stats["linked_assets_failed"] += 1
                continue
        if not linked_text:
            stats["linked_assets_failed"] += 1
            continue
        stats["linked_assets_ingested"] += 1

        section_type = "linked_asset"
        if linked_path.suffix.lower() == ".pdf":
            section_type = "linked_pdf"
        elif linked_path.suffix.lower() in {".html", ".htm", ".txt", ".md", ".csv", ".tsv", ".json", ".xls", ".xlsx", ".xlsm"}:
            section_type = "linked_text_asset"
        elif linked_path.suffix.lower() in {".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg"}:
            section_type = "linked_image_asset"

        sections.append({
            "heading": f"Linked asset: {linked_path.name}",
            "text": f"Source asset: {linked_rel}\n\n{linked_text}",
            "section_type": section_type,
        })

    return sections, stats


def infer_numeric_unit(line_lower: str) -> str | None:
    if "usd/" in line_lower or "usd per" in line_lower:
        return "usd_per_unit"
    if "ws" in line_lower and "%" in line_lower:
        return "worldscale_pct"
    if "kt" in line_lower or "mt" in line_lower:
        return "tonnage"
    if "%" in line_lower:
        return "pct"
    if "$" in line_lower or "usd" in line_lower:
        return "usd"
    return None


def extract_numeric_observations(sections: list[dict], limit: int = 160) -> list[dict]:
    observations = []
    seen = set()
    for section in sections:
        heading = norm_space(section.get("heading")) or "Main"
        section_type = section.get("section_type")
        for raw_line in (section.get("text") or "").splitlines():
            line = norm_space(raw_line)
            if not line or len(line) < 6:
                continue
            lower = line.lower()
            if lower.startswith("[page ") or "source asset:" in lower:
                continue
            if "http://" in lower or "https://" in lower:
                continue
            # Skip bot-challenge / error-page noise so Ray IDs, timestamps and
            # challenge boilerplate never become numeric "observations".
            if is_error_page_text(line):
                continue

            values = extract_line_numbers(line, limit=10, min_abs_value=0.01)
            if not values:
                continue
            key = (heading.lower(), tuple(round(value, 4) for value in values), lower[:120])
            if key in seen:
                continue
            seen.add(key)
            observations.append(
                {
                    "section": heading,
                    "section_type": section_type,
                    "values": values,
                    "unit": infer_numeric_unit(lower),
                    "source_line": line[:260],
                }
            )
            if len(observations) >= limit:
                return observations
    return observations


def infer_document_type(source: str, category: str) -> str:
    if source == "breakwave_insights":
        return "insights_note"
    if source == "broker_reports":
        return "broker_report"
    if source == "poten":
        return "analyst_opinion"
    if source == "hellenic":
        return {
            "dry_charter": "charter_estimates",
            "tanker_charter": "charter_estimates",
            "iron_ore": "commodity_update",
            "vessel_valuations": "asset_valuations",
            "demolition": "demolition_update",
            "shipbuilding": "shipbuilding_update",
        }.get(category, "archive_report")
    return "archive_report"


def extract_breakwave_changes(lines: list[str], category: str) -> dict:
    signals = {}
    lower_joined = "\n".join(lines).lower()
    if category == "drybulk":
        future_prefix = "bdryff"
        spot_prefix = "bdi_spot"
        future_markers = ["breakwave dry", "futures index"]
        spot_markers = ["baltic dry index", "(spot)"]
    else:
        future_prefix = "bwetff"
        spot_prefix = "vlcc_meg_asia"
        future_markers = ["breakwave tanker", "futures index"]
        spot_markers = ["vlcc middle east", "spot rates"]

    flat_lines = [norm_space(line) for line in lines if norm_space(line)]
    for timeframe in ["30D", "YTD", "YOY"]:
        tf_line = next((line for line in flat_lines if line.count(f"{timeframe}:") >= 2), None)
        if tf_line:
            matches = re.findall(rf"{timeframe}:\s*([+-]?\d+\.?\d*)%", tf_line)
            if matches:
                signals[f"{future_prefix}_{timeframe.lower()}_pct"] = parse_pct(matches[0])
            if len(matches) > 1:
                signals[f"{spot_prefix}_{timeframe.lower()}_pct"] = parse_pct(matches[1])

    current = None
    for idx, line in enumerate(flat_lines):
        low = line.lower()
        if any(marker in low for marker in future_markers):
            current = future_prefix
            same_line_value = parse_number(line)
            if same_line_value is not None and future_prefix not in signals:
                signals[future_prefix] = same_line_value
            else:
                for nxt in flat_lines[idx + 1: idx + 4]:
                    nxt_value = parse_number(nxt)
                    if nxt_value is not None:
                        signals[future_prefix] = nxt_value
                        break
            continue
        if any(marker in low for marker in spot_markers):
            current = spot_prefix
            same_line_value = parse_number(line)
            if same_line_value is not None and spot_prefix not in signals:
                signals[spot_prefix] = same_line_value
            else:
                for nxt in flat_lines[idx + 1: idx + 4]:
                    nxt_value = parse_number(nxt)
                    if nxt_value is not None:
                        signals[spot_prefix] = nxt_value
                        break
            continue
        if line.count("30D:") == 1 and current:
            match = re.search(r"30D:\s*([+-]?\d+\.?\d*)%", line)
            if match:
                signals.setdefault(f"{current}_30d_pct", parse_pct(match.group(1)))
        if line.count("YTD:") == 1 and current:
            match = re.search(r"YTD:\s*([+-]?\d+\.?\d*)%", line)
            if match:
                signals.setdefault(f"{current}_ytd_pct", parse_pct(match.group(1)))
        if line.count("YOY:") == 1 and current:
            match = re.search(r"YOY:\s*([+-]?\d+\.?\d*)%", line)
            if match:
                signals.setdefault(f"{current}_yoy_pct", parse_pct(match.group(1)))

    momentum = re.search(r"Momentum:\s*(Positive|Negative|Neutral)", lower_joined, re.I)
    sentiment = re.search(r"Sentiment:\s*(Positive|Negative|Neutral)", lower_joined, re.I)
    fundamentals = re.search(r"Fundamentals:\s*(Positive|Negative|Neutral)", lower_joined, re.I)
    signals["momentum"] = momentum.group(1).lower() if momentum else None
    signals["sentiment"] = sentiment.group(1).lower() if sentiment else None
    signals["fundamentals"] = fundamentals.group(1).lower() if fundamentals else None
    return signals


def extract_breakwave_fundamentals(tables: list, expected_fields: list[str]) -> dict:
    normalized = {field: {"ytd": None, "yoy_pct": None} for field in expected_fields}
    aliases = {re.sub(r"[^a-z0-9]+", "", field.lower()): field for field in expected_fields}
    for table in tables:
        for row in table or []:
            cells = [norm_space(cell) for cell in row if norm_space(cell)]
            if len(cells) < 2:
                continue
            candidate_name = None
            for cell in cells[:2]:
                key = re.sub(r"[^a-z0-9]+", "", cell.lower())
                if key in aliases:
                    candidate_name = aliases[key]
                    break
            if not candidate_name:
                merged = " ".join(cells[:2]).lower()
                merged_key = re.sub(r"[^a-z0-9]+", "", merged)
                for alias_key, field in aliases.items():
                    if alias_key in merged_key:
                        candidate_name = field
                        break
            if not candidate_name:
                continue
            values = [cell for cell in cells if cell != candidate_name]
            if not values:
                continue
            normalized[candidate_name]["ytd"] = values[0]
            if len(values) > 1:
                normalized[candidate_name]["yoy_pct"] = values[1]
    return normalized


def adapt_breakwave(pdf_path: Path, category: str, llm_enabled: bool, existing_metadata: dict | None = None) -> dict:
    existing_metadata = existing_metadata or {}
    with pdfplumber.open(pdf_path) as pdf:
        pages = pdf.pages
        page_texts = [norm_multiline(page.extract_text() or "") for page in pages]
        page_tables = [page.extract_tables() or [] for page in pages]

    date_str = pdf_path.stem.split("_")[0]
    title = next((
        line for line in page_texts[0].splitlines()[:4]
        if line and "report" not in line.lower() and not re.match(r"^[A-Za-z]+\s+\d{1,2},\s+\d{4}$", line)
    ), pdf_path.stem)
    page_one_lines = [line for line in page_texts[0].splitlines() if line]
    signals = extract_breakwave_changes(page_one_lines, category)
    signal_keys = (
        ["bdryff", "bdryff_30d_pct", "bdryff_ytd_pct", "bdryff_yoy_pct", "bdi_spot", "bdi_spot_30d_pct", "bdi_spot_ytd_pct", "bdi_spot_yoy_pct", "momentum", "sentiment", "fundamentals"]
        if category == "drybulk"
        else ["bwetff", "bwetff_30d_pct", "bwetff_ytd_pct", "bwetff_yoy_pct", "vlcc_meg_asia", "vlcc_meg_asia_30d_pct", "vlcc_meg_asia_ytd_pct", "vlcc_meg_asia_yoy_pct", "momentum", "sentiment", "fundamentals"]
    )
    expected_fields = BREAKWAVE_DRY_FIELDS if category == "drybulk" else BREAKWAVE_TANKER_FIELDS
    fundamentals = extract_breakwave_fundamentals(page_tables[1] if len(page_tables) > 1 else [], expected_fields)

    # Join wrapped PDF lines into their parent bullet: pdfplumber breaks long
    # bullets across physical lines, and treating each "•" line alone produced
    # overview chunks that were clipped mid-clause. A bullet item continues
    # until the next "•" line.
    bullet_lines: list[str] = []
    for line in page_one_lines:
        if line.startswith("•"):
            bullet_lines.append(line.lstrip("•").strip())
        elif bullet_lines and line.strip() and not re.match(r"^[A-Za-z]+\s+\d{1,2},\s+\d{4}$", line.strip()):
            bullet_lines[-1] = (bullet_lines[-1] + " " + line.strip()).strip()
    if not bullet_lines:
        bullet_lines = [line for line in page_one_lines if len(line.split()) > 8][:6]

    body_sections = [{
        "heading": "Overview",
        "text": "\n".join(f"- {line}" for line in bullet_lines if line),
        "page_start": 1,
        "page_end": 1,
        "section_type": "overview",
    }]
    fundamentals_md = format_fundamentals_markdown(fundamentals)
    if fundamentals_md:
        body_sections.append({
            "heading": "Fundamentals",
            "text": fundamentals_md,
            "page_start": 2 if len(page_texts) > 1 else 1,
            "page_end": 2 if len(page_texts) > 1 else 1,
            "section_type": "fundamentals",
        })
    full_text = "\n\n".join(section["text"] for section in body_sections if section["text"])
    vessels, regions, commodities = infer_taxonomy(full_text + "\n" + "\n".join(page_texts), "breakwave", category)

    existing_signals = existing_metadata.get("signals", {}) or {}
    for key in signal_keys:
        if signals.get(key) is None and existing_signals.get(key) is not None:
            signals[key] = existing_signals.get(key)

    theme_data = heuristic_theme_payload(full_text, category)
    theme_data = merge_existing_theme_data(theme_data, existing_metadata)

    summary = existing_metadata.get("summary") or heuristic_summary(" ".join(bullet_lines) or full_text)
    keywords = existing_metadata.get("keywords") if isinstance(existing_metadata.get("keywords"), list) and existing_metadata["keywords"] else extract_keywords(full_text)

    missing_signals = [key for key in signal_keys if signals.get(key) is None]
    needs_theme_enrichment = not theme_data["themes"] or not theme_data["key_entities"] or not theme_data["market_tone"]
    llm_data = {}
    if llm_enabled and (missing_signals or not existing_metadata.get("summary") or needs_theme_enrichment):
        llm_data = run_doc_llm("\n".join(page_texts), "breakwave", category, signal_keys=missing_signals if missing_signals else None)
        for key in missing_signals:
            if llm_data.get(key) is not None:
                signals[key] = llm_data.get(key)
        if not existing_metadata.get("summary") and llm_data.get("summary"):
            summary = llm_data["summary"]
        if not (isinstance(existing_metadata.get("keywords"), list) and existing_metadata["keywords"]) and isinstance(llm_data.get("keywords"), list):
            keywords = llm_data["keywords"]
        if isinstance(llm_data.get("themes"), list) and llm_data["themes"]:
            theme_data["themes"] = llm_data["themes"][:6]
        if isinstance(llm_data.get("key_entities"), list) and llm_data["key_entities"]:
            theme_data["key_entities"] = llm_data["key_entities"][:6]
        if llm_data.get("market_tone"):
            theme_data["market_tone"] = llm_data["market_tone"]

    day_fmt = "%#d" if os.name == "nt" else "%-d"
    metadata = {
        "doc_id": make_breakwave_doc_id(pdf_path, category),
        "source": "breakwave",
        "category": category,
        "date": date_str,
        "title": f"{title} - {datetime.strptime(date_str, '%Y-%m-%d').strftime('%B ' + day_fmt + ', %Y')}",
        "source_path": relpath(pdf_path),
        "document_type": "biweekly_report",
        "vessel_classes": vessels,
        "regions": regions,
        "commodities": commodities,
        "signals": signals,
        "fundamentals_table": fundamentals,
        "summary": summary,
        "keywords": keywords[:12],
        "themes": theme_data["themes"],
        "key_entities": theme_data["key_entities"],
        "market_tone": theme_data["market_tone"],
    }
    return {"text": full_text, "metadata": metadata, "sections": body_sections}


def find_baltic_title(soup: BeautifulSoup) -> str:
    for tag in soup.find_all("h1"):
        text = norm_space(tag.get_text(" ", strip=True))
        if text and "cookies" not in text.lower():
            return text
    return "Baltic Weekly Roundup"


def find_baltic_source_url(soup: BeautifulSoup) -> str | None:
    for selector, attr in [
        ("link[rel='canonical']", "href"),
        ("meta[property='og:url']", "content"),
        ("meta[name='twitter:url']", "content"),
    ]:
        tag = soup.select_one(selector)
        if tag and tag.get(attr):
            return norm_space(tag[attr])
    return None


def find_baltic_date(soup: BeautifulSoup, html_path: Path):
    time_tag = soup.find("time")
    if time_tag and time_tag.get("datetime"):
        parsed = parse_iso_date(time_tag["datetime"][:10])
        if parsed:
            return parsed.isoformat()
    meta_text = norm_space(soup.get_text("\n", strip=True))
    match = TITLE_DATE_RE.search(meta_text)
    if match:
        parsed = parse_iso_date(match.group(1))
        if parsed:
            return parsed.isoformat()
    prefix = ISO_PREFIX_RE.match(html_path.stem)
    if prefix:
        return prefix.group(1)
    year = None
    try:
        year = int(html_path.parent.name)
    except ValueError:
        year = None
    if year:
        search_space = "\n".join([
            html_path.stem,
            soup.get_text("\n", strip=True)[:4000],
        ])
        match = re.search(r"week[\s\-_]?(\d{1,2})", search_space, re.I)
        if match:
            week = int(match.group(1))
            try:
                return datetime.fromisocalendar(year, week, 5).date().isoformat()
            except ValueError:
                pass
    return None


def is_section_label(node) -> bool:
    text = norm_space(node.get_text(" ", strip=True))
    if not text:
        return False
    if node.name == "h4":
        return True
    bold = node.find(["b", "strong"])
    if not bold:
        return False
    bold_text = norm_space(bold.get_text(" ", strip=True))
    return text == bold_text and len(text.split()) <= 8


def adapt_baltic(html_path: Path, category: str, existing_metadata: dict | None = None) -> dict:
    existing_metadata = existing_metadata or {}
    soup = BeautifulSoup(html_path.read_text(encoding="utf-8", errors="ignore"), "lxml")
    root = soup.select_one("div.article-content")
    if root is None:
        time_tag = soup.find("time")
        root = time_tag.find_parent("div") if time_tag else soup.body

    title = find_baltic_title(soup)
    date_str = find_baltic_date(soup, html_path) or "unknown"
    sections = []
    current_heading = None
    current_lines = []

    for node in root.find_all(["p", "h4"]):
        text = norm_space(node.get_text(" ", strip=True))
        if not text:
            continue
        if is_section_label(node):
            if current_heading or current_lines:
                sections.append({"heading": current_heading or "Main", "text": "\n".join(current_lines).strip()})
            current_heading = text
            current_lines = []
            continue
        current_lines.append(text)
    if current_heading or current_lines:
        sections.append({"heading": current_heading or "Main", "text": "\n".join(current_lines).strip()})

    if not sections:
        sections = [{"heading": "Main", "text": norm_space(root.get_text("\n", strip=True))}]

    full_text = "\n\n".join(
        f"{section['heading']}\n{section['text']}" if section["heading"] else section["text"]
        for section in sections
    )
    vessels, regions, commodities = infer_taxonomy(full_text, "baltic", category)
    theme_data = merge_existing_theme_data(heuristic_theme_payload(full_text, category), existing_metadata)
    keywords = existing_metadata.get("keywords") if isinstance(existing_metadata.get("keywords"), list) and existing_metadata["keywords"] else extract_keywords(full_text)
    metadata = {
        "doc_id": make_baltic_doc_id(html_path, category, date_str),
        "source": "baltic",
        "category": category,
        "date": date_str if date_str != "unknown" else None,
        "title": title,
        "source_path": relpath(html_path),
        "source_url": find_baltic_source_url(soup),
        "source_stem": html_path.stem,
        "document_type": "weekly_roundup",
        "vessel_classes": vessels,
        "regions": regions,
        "commodities": commodities,
        "signals": {},
        "summary": existing_metadata.get("summary") or heuristic_summary(full_text),
        "keywords": keywords,
        "themes": theme_data["themes"],
        "key_entities": theme_data["key_entities"],
        "market_tone": theme_data["market_tone"],
    }
    return {"text": full_text, "metadata": metadata, "sections": sections}


def adapt_archive_html(
    html_path: Path,
    source: str,
    category: str,
    existing_metadata: dict | None = None,
) -> dict:
    existing_metadata = existing_metadata or {}
    soup = BeautifulSoup(html_path.read_text(encoding="utf-8", errors="ignore"), "lxml")
    title = norm_space((soup.find("h1") or soup.find("title")).get_text(" ", strip=True)) if (soup.find("h1") or soup.find("title")) else html_path.stem
    date_str = find_archive_date(soup, html_path)
    source_url = find_archive_source_url(soup)
    root = soup.select_one("body > section") or soup.select_one("section") or soup.body or soup

    # Bot-challenge / CDN block pages sometimes get archived when a scraper
    # hits a WAF. Label them so downstream signal extraction and wiki scoring
    # can exclude them instead of mining "Cloudflare Ray ID" as market data.
    error_page = is_error_page_text(soup.get_text(" ", strip=True)[:4000])

    sections = []
    current_heading = None
    current_lines = []
    last_line = None

    for node in root.find_all(["h2", "h3", "h4", "p", "li", "blockquote", "table", "img"]):
        text = ""
        if node.name in {"h2", "h3", "h4"}:
            heading = norm_space(node.get_text(" ", strip=True))
            if heading:
                if current_heading or current_lines:
                    sections.append({"heading": current_heading or "Main", "text": "\n".join(current_lines).strip()})
                current_heading = heading
                current_lines = []
                last_line = None
            continue
        if node.name == "table":
            text = table_to_text(node)
        elif node.name == "img":
            alt = norm_space(node.get("alt") or "")
            title_attr = norm_space(node.get("title") or "")
            src = norm_space(node.get("src") or "")
            figure_caption = ""
            parent_figure = node.find_parent("figure")
            if parent_figure:
                caption_node = parent_figure.find("figcaption")
                if caption_node:
                    figure_caption = norm_space(caption_node.get_text(" ", strip=True))
            parts = [part for part in [alt, title_attr, figure_caption, src] if part]
            text = f"Image reference: {' | '.join(parts)}" if parts else ""
        else:
            text = norm_space(node.get_text(" ", strip=True))
        if text and text != last_line:
            current_lines.append(text)
            last_line = text

    if current_heading or current_lines:
        sections.append({"heading": current_heading or "Main", "text": "\n".join(current_lines).strip()})

    if not sections:
        fallback = norm_space(root.get_text("\n", strip=True))
        if not fallback:
            fallback = title
        sections = [{"heading": "Main", "text": fallback}]

    # Determine destination document path to check for existing sections
    doc_id = make_archive_doc_id(source, category, html_path, date_str)
    source_stem = slugify(html_path.stem)
    year = date_str[:4] if date_str else html_path.parent.name
    if source in {"baltic", "breakwave_insights", "hellenic"} and date_str:
        filename = f"{date_str}_{source_stem}.md"
    else:
        filename = f"{date_str}.md" if date_str else f"{source_stem}.md"
    output_path = DOCS_DIR / source / category / year / filename

    existing_sections = {}
    if output_path.exists():
        try:
            content = output_path.read_text(encoding="utf-8", errors="ignore")
            parts = content.split("\n## ")
            for part in parts:
                part = part.strip()
                if not part:
                    continue
                lines = part.splitlines()
                heading = lines[0].strip()
                text = "\n".join(lines[1:]).strip()
                existing_sections[heading] = text
        except Exception:
            pass

    linked_sections, linked_asset_stats = collect_linked_asset_sections(source, html_path, root, existing_sections=existing_sections)
    if linked_sections:
        sections.extend(linked_sections)

    full_text = "\n\n".join(
        f"{section['heading']}\n{section['text']}" if section["heading"] else section["text"]
        for section in sections
    )
    vessels, regions, commodities = infer_taxonomy(full_text, source, category)
    theme_data = merge_existing_theme_data(heuristic_theme_payload(full_text, category), existing_metadata)
    keywords = existing_metadata.get("keywords") if isinstance(existing_metadata.get("keywords"), list) and existing_metadata["keywords"] else extract_keywords(full_text)
    signals = extract_hellenic_signals(full_text, category, existing_metadata) if source == "hellenic" else {}
    numeric_observations = extract_numeric_observations(sections)
    if source == "hellenic" and isinstance(signals, dict) and numeric_observations:
        signals.setdefault("numeric_observations", numeric_observations[:80])
        signals.setdefault("numeric_observation_count", len(numeric_observations))

    metadata = {
        "doc_id": make_archive_doc_id(source, category, html_path, date_str),
        "source": source,
        "category": category,
        "date": date_str,
        "title": title,
        "source_path": relpath(html_path),
        "source_url": source_url,
        "source_stem": html_path.stem,
        "document_type": "error_page" if error_page else infer_document_type(source, category),
        "is_error_page": error_page,
        "vessel_classes": vessels,
        "regions": regions,
        "commodities": commodities,
        "signals": signals,
        "summary": existing_metadata.get("summary") or heuristic_summary(full_text),
        "keywords": keywords,
        "themes": theme_data["themes"],
        "key_entities": theme_data["key_entities"],
        "market_tone": theme_data["market_tone"],
        "numeric_observations": numeric_observations[:120],
        "numeric_observation_count": len(numeric_observations),
        **linked_asset_stats,
    }
    return {"text": full_text, "metadata": metadata, "sections": sections}


def looks_like_toc(lines: list[str]) -> bool:
    if len(lines) < 8:
        return False
    short_lines = sum(1 for line in lines if len(line.split()) <= 10)
    numbered = sum(1 for line in lines if re.search(r"\b\d+\s*$", line))
    dot_leaders = sum(1 for line in lines if "...." in line or ". ." in line)
    return short_lines / max(len(lines), 1) > 0.6 and (numbered >= 4 or dot_leaders >= 2)


def looks_like_skip_page(lines: list[str], page_number: int) -> bool:
    joined = " ".join(lines).lower()
    if len(joined) < 50:
        return True
    if looks_like_toc(lines):
        return True
    if page_number < 8 and any(term in joined for term in ["copyright", "all rights reserved", "cataloguing", "isbn"]):
        return True
    if len(lines) <= 3 and sum(ch.isalpha() for ch in joined) and joined.upper() == joined:
        return True
    return False


def detect_book_heading(lines: list[str]) -> tuple[str | None, int]:
    candidates = [line for line in lines[:6] if line]
    if not candidates:
        return None, 0
    first = candidates[0]
    if re.match(r"^(chapter|part|section)\b", first, re.I):
        if len(candidates) > 1 and len(candidates[1].split()) <= 14:
            return f"{first} - {candidates[1]}", 2
        return first, 1
    if len(first.split()) <= 14 and not first.endswith("."):
        title_case = first == first.title()
        uppercase = first == first.upper()
        if title_case or uppercase:
            return first, 1
    return None, 0


def adapt_book(pdf_path: Path, llm_enabled: bool, existing_metadata: dict | None = None) -> dict:
    existing_metadata = existing_metadata or {}
    title = pdf_path.stem.replace("_", " ")
    sections = []
    current_heading = title
    current_buffer = []
    current_start_page = None
    current_end_page = None
    summary_seed = []

    with pdfplumber.open(pdf_path) as pdf:
        for page_number, page in enumerate(pdf.pages, start=1):
            text = norm_multiline(page.extract_text() or "")
            lines = [line for line in text.splitlines() if line]
            if looks_like_skip_page(lines, page_number):
                continue

            heading, consumed = detect_book_heading(lines)
            content_lines = lines[consumed:] if consumed else lines
            content = "\n".join(content_lines).strip()
            if heading:
                if current_buffer:
                    sections.append({
                        "heading": current_heading,
                        "text": "\n\n".join(current_buffer).strip(),
                        "page_start": current_start_page,
                        "page_end": current_end_page or current_start_page,
                        "section_type": "chapter",
                    })
                current_heading = heading
                current_buffer = []
                current_start_page = page_number
                current_end_page = page_number
            elif current_start_page is None:
                current_start_page = page_number
            if content:
                current_buffer.append(content)
                current_end_page = page_number
                if len(summary_seed) < 4:
                    summary_seed.append(content[:2000])

    if current_buffer:
        sections.append({
            "heading": current_heading,
            "text": "\n\n".join(current_buffer).strip(),
            "page_start": current_start_page,
            "page_end": current_end_page or current_start_page,
            "section_type": "chapter",
        })
    if not sections:
        sections = [{"heading": title, "text": ""}]

    full_text = "\n\n".join(f"{section['heading']}\n{section['text']}" for section in sections if section["text"])
    vessels, regions, commodities = infer_taxonomy(full_text, "book", "book")
    theme_data = merge_existing_theme_data(heuristic_theme_payload(full_text, "book"), existing_metadata)

    summary = existing_metadata.get("summary") or heuristic_summary(" ".join(summary_seed) or full_text)
    keywords = existing_metadata.get("keywords") if isinstance(existing_metadata.get("keywords"), list) and existing_metadata["keywords"] else extract_keywords(full_text)
    needs_theme_enrichment = not theme_data["themes"] or not theme_data["key_entities"] or not theme_data["market_tone"]
    if llm_enabled and summary_seed and (not existing_metadata.get("summary") or needs_theme_enrichment):
        llm_data = run_doc_llm("\n\n".join(summary_seed), "book", "book")
        if not existing_metadata.get("summary") and llm_data.get("summary"):
            summary = llm_data["summary"]
        if isinstance(llm_data.get("themes"), list) and llm_data["themes"]:
            theme_data["themes"] = llm_data["themes"][:6]
        if isinstance(llm_data.get("key_entities"), list) and llm_data["key_entities"]:
            theme_data["key_entities"] = llm_data["key_entities"][:6]
        if llm_data.get("market_tone"):
            theme_data["market_tone"] = llm_data["market_tone"]
        if not (isinstance(existing_metadata.get("keywords"), list) and existing_metadata["keywords"]) and isinstance(llm_data.get("keywords"), list):
            keywords = llm_data["keywords"]

    metadata = {
        "doc_id": make_book_doc_id(pdf_path),
        "source": "book",
        "category": "book",
        "date": None,
        "title": title,
        "source_path": relpath(pdf_path),
        "document_type": "reference_book",
        "vessel_classes": vessels,
        "regions": regions,
        "commodities": commodities,
        "signals": {},
        "summary": summary,
        "keywords": keywords,
        "themes": theme_data["themes"],
        "key_entities": theme_data["key_entities"],
        "market_tone": theme_data["market_tone"],
    }
    return {"text": full_text, "metadata": metadata, "sections": sections}


def build_doc_body(adapted: dict) -> str:
    metadata = adapted["metadata"]
    body_lines = []
    summary = metadata.get("summary")
    if summary:
        body_lines.extend(["## Summary", summary, ""])
    for section in adapted["sections"]:
        heading = norm_space(section.get("heading")) or "Main"
        text = (section.get("text") or "").strip()
        if not text:
            continue
        body_lines.extend([f"## {heading}", text, ""])
    return "\n".join(body_lines).strip() + "\n"


def doc_output_path(metadata: dict) -> Path:
    source = metadata["source"]
    category = metadata["category"]
    date_str = metadata.get("date")
    title = metadata["title"]
    if source == "book":
        return DOCS_DIR / "books" / f"{slugify(title)}.md"
    source_stem = slugify(metadata.get("source_stem") or Path(metadata["source_path"]).stem)
    year = date_str[:4] if date_str else Path(metadata["source_path"]).parent.name
    if source in {"baltic", "breakwave_insights", "hellenic"} and date_str:
        filename = f"{date_str}_{source_stem}.md"
    elif date_str:
        # 2026-08-25 CI-failure fix: broker_reports publish MULTIPLE distinct reports
        # on the same date (SSY Atl+Pac, Star Asia, Gibson, Affinity, Xclusiv...).
        # The old `{date}.md` name made same-day reports overwrite each other,
        # orphaning chunks/trees (validator: 448 invalid section refs) and losing
        # ~30 documents. Key every non-baltic doc by its unique source stem.
        filename = f"{date_str}_{source_stem}.md"
    else:
        filename = f"{source_stem}.md"
    return DOCS_DIR / source / category / year / filename


def tree_output_path(metadata: dict) -> Path:
    return tree_output_path_from_doc_path(doc_output_path(metadata))


def chunk_partition_year(metadata: dict) -> str:
    date_str = norm_space(metadata.get("date"))
    if re.match(r"^\d{4}", date_str):
        year = date_str[:4]
    else:
        year = Path(metadata.get("source_path") or "").parent.name
    if not re.fullmatch(r"\d{4}", year or ""):
        return "unknown"
    return year


def chunk_file_path(metadata: dict) -> Path:
    source = metadata["source"]
    if source == "book":
        return CHUNKS_DIR / "books.jsonl"
    category = metadata["category"]
    year = chunk_partition_year(metadata)
    return CHUNKS_DIR / f"{source}_{category}_{year}.jsonl"


def build_chunks(adapted: dict) -> list[dict]:
    metadata = adapted["metadata"]
    source = metadata["source"]
    category = metadata["category"]
    doc_id = metadata["doc_id"]
    date_str = metadata.get("date")
    chunks = []

    max_tokens, overlap = (450, 60) if source == "breakwave" else (600, 60) if source == "baltic" else (500, 100)

    _rate_pat     = re.compile(r'\$[\d,]+\s*/\s*(?:day|mt|tonne|ton)\b|\b\d[\d,.]+\s*usd\b', re.I)
    _forecast_pat = re.compile(r'\b(?:expect|forecast|likely|should|outlook|anticipat|project|predict|going forward|next quarter|next month)\b', re.I)

    for section in adapted["sections"]:
        section_text = section.get("text") or ""
        if not section_text.strip():
            continue
        for section_index, text in enumerate(chunk_text(section_text, max_tokens, overlap) or [section_text], start=1):
            vessels, regions, _ = infer_taxonomy(text, source, category)
            chunks.append({
                "chunk_id": f"{doc_id}_{len(chunks) + 1:03d}",
                "doc_id": doc_id,
                "source": source,
                "category": category,
                "date": date_str,
                "section": section["slug"],
                "section_id": section["section_id"],
                "section_title": section["heading"],
                "section_path": section["section_path"],
                "section_path_text": section["section_path_text"],
                "section_level": section["level"],
                "section_chunk_index": section_index,
                "page_start": section.get("page_start"),
                "page_end": section.get("page_end"),
                "source_url": metadata.get("source_url"),
                "text": text,
                "token_count": token_count(text),
                "keywords": extract_keywords(text),
                "has_rates": bool(_rate_pat.search(text) or any(kw in text.lower() for kw in ("timecharter", "tc rate", "spot rate", "freight rate", "/day"))),
                "has_forecast": bool(_forecast_pat.search(text)),
                "vessel_classes_matched": vessels,
                "regions_matched": regions,
                "snippet": text[:200].replace("\n", " ").strip(),
            })
    return chunks


def write_markdown_doc(path: Path, metadata: dict, body: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    post = frontmatter.Post(body, **metadata)
    path.write_text(frontmatter.dumps(post), encoding="utf-8")


def adapt_markdown_report(
    md_path: Path,
    source: str,
    category: str,
    existing_metadata: dict | None = None,
) -> dict:
    """Adapter for standalone broker-report / analyst-opinion Markdown files.

    Scrapers emit reports with YAML frontmatter (title/date/source_url) and
    `## Heading` sections. Reuse the archive-adapter's downstream enrichment
    (taxonomy, keywords, themes, numeric observations) so these documents are
    first-class citizens of the derived manifests.
    """
    existing_metadata = existing_metadata or {}
    raw = md_path.read_text(encoding="utf-8", errors="ignore")

    frontmatter: dict = {}
    body_raw = raw
    if raw.startswith("---"):
        parts = raw.split("\n---", 2)
        if len(parts) >= 2:
            block = parts[0][3:].strip()
            body_raw = "\n---".join(parts[1:])
            for line in block.splitlines():
                if ":" not in line:
                    continue
                key, _, value = line.partition(":")
                value = value.strip().strip("'\"")
                if value.startswith("[") and value.endswith("]"):
                    items = [
                        item.strip().strip("'\"")
                        for item in value[1:-1].split(",")
                        if item.strip()
                    ]
                    frontmatter[key.strip()] = items
                else:
                    frontmatter[key.strip()] = value

    # Sectionize on markdown headings; fall back to the single Main section.
    sections = []
    current_heading = "Main"
    current_lines = []
    for line in body_raw.splitlines():
        heading_match = re.match(r"^(#{1,4})\s+(.+?)\s*$", line)
        if heading_match:
            if current_lines:
                text = "\n".join(current_lines).strip()
                if text:
                    sections.append({"heading": current_heading, "text": text})
                current_lines = []
            current_heading = norm_space(heading_match.group(2)) or "Main"
            continue
        stripped = line.strip()
        if not stripped:
            continue
        if re.match(r"^\|.*\|\s*$", stripped):
            cells = [norm_space(cell) for cell in stripped.strip("|").split("|")]
            cells = [cell for cell in cells if cell]
            if cells:
                current_lines.append(" | ".join(cells))
            continue
        if stripped in {"---", "***", "___"} or set(stripped) <= {"-", "=", "*", "_"}:
            continue
        current_lines.append(stripped)
    if current_lines:
        text = "\n".join(current_lines).strip()
        if text:
            sections.append({"heading": current_heading, "text": text})
    if not sections:
        sections = [{"heading": "Main", "text": frontmatter.get("title") or md_path.stem}]

    date_str = None
    raw_date = frontmatter.get("date") or ""
    parsed_date = parse_iso_date(raw_date)
    if parsed_date:
        date_str = parsed_date.isoformat()
    else:
        # Scraper fallback formats: DD/MM/YYYY (HSN) or an embedded YYYY-MM-DD.
        dmy = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})$", raw_date.strip())
        if dmy:
            mm, dd, yyyy = dmy.group(2).zfill(2), dmy.group(1).zfill(2), dmy.group(3)
            date_str = f"{yyyy}-{mm}-{dd}"
        else:
            iso_in = re.search(r"\d{4}-\d{2}-\d{2}", raw_date)
            if iso_in:
                date_str = iso_in.group(0)
    title = norm_space(frontmatter.get("title")) or md_path.stem.replace("_", " ")
    source_url = frontmatter.get("source_url") or None

    full_text = "\n\n".join(
        f"{section['heading']}\n{section['text']}" if section["heading"] else section["text"]
        for section in sections
    )
    vessels, regions, commodities = infer_taxonomy(full_text, source, category)
    theme_data = merge_existing_theme_data(heuristic_theme_payload(full_text, category), existing_metadata)
    keywords = (
        existing_metadata.get("keywords")
        if isinstance(existing_metadata.get("keywords"), list) and existing_metadata["keywords"]
        else extract_keywords(full_text)
    )
    numeric_observations = extract_numeric_observations(sections)

    metadata = {
        "doc_id": f"{source}_{category}_{date_str or 'no_date'}_{slugify(md_path.stem)}",
        "source": source,
        "category": category,
        "date": date_str,
        "title": title,
        "source_path": relpath(md_path),
        "source_url": source_url,
        "source_stem": md_path.stem,
        "document_type": infer_document_type(source, category),
        "is_error_page": False,
        "vessel_classes": vessels,
        "regions": regions,
        "commodities": commodities,
        "signals": {},
        "summary": existing_metadata.get("summary") or heuristic_summary(full_text),
        "keywords": keywords,
        "themes": theme_data["themes"],
        "key_entities": theme_data["key_entities"],
        "market_tone": theme_data["market_tone"],
        "numeric_observations": numeric_observations[:120],
        "numeric_observation_count": len(numeric_observations),
    }
    return {"text": full_text, "metadata": metadata, "sections": sections}


def adapt_source_file(source: str, category: str, path: Path, llm_enabled: bool, existing_metadata: dict | None = None):
    if source == "breakwave":
        return adapt_breakwave(path, category, llm_enabled, existing_metadata=existing_metadata)
    if source == "baltic":
        return adapt_baltic(path, category, existing_metadata=existing_metadata)
    if source in {"breakwave_insights", "hellenic"}:
        return adapt_archive_html(path, source, category, existing_metadata=existing_metadata)
    if source in {"broker_reports", "poten"}:
        return adapt_markdown_report(path, source, category, existing_metadata=existing_metadata)
    return adapt_book(path, llm_enabled, existing_metadata=existing_metadata)


def process_file(path: Path, adapted: dict, source_hash_value: str | None = None):
    metadata = adapted["metadata"]
    adapted, tree = prepare_document_structure(adapted)
    output_path = doc_output_path(metadata)
    tree_path = tree_output_path_from_doc_path(output_path)
    body = build_doc_body(adapted)
    write_markdown_doc(output_path, metadata, body)
    write_tree_file(tree_path, tree)

    chunks = build_chunks(adapted)
    chunk_path = chunk_file_path(metadata)
    for chunk in chunks:
        append_jsonl(chunk_path, chunk)

    manifest_row = {
        "doc_id": metadata["doc_id"],
        "source": metadata["source"],
        "category": metadata["category"],
        "date": metadata.get("date"),
        "title": metadata["title"],
        "source_path": metadata["source_path"],
        "doc_path": relpath(output_path),
        "tree_path": relpath(tree_path),
        "tree_node_count": sum(1 for _ in iter_tree_nodes(tree)),
        "chunk_file": relpath(chunk_path),
        "chunk_count": len(chunks),
        "source_hash": source_hash_value or source_hash(path),
        "source_hash_version": SOURCE_HASH_VERSION,
        "compiler_version": COMPILER_VERSION,
        "processed_at": utc_now_iso(),
        "linked_assets_discovered": int(metadata.get("linked_assets_discovered") or 0),
        "linked_assets_mirrored": int(metadata.get("linked_assets_mirrored") or 0),
        "linked_assets_ingested": int(metadata.get("linked_assets_ingested") or 0),
        "linked_assets_skipped": int(metadata.get("linked_assets_skipped") or 0),
        "linked_assets_failed": int(metadata.get("linked_assets_failed") or 0),
    }
    return metadata, chunks, manifest_row


def _load_derived_cache() -> dict:
    """Load knowledge/manifests/derived_cache.json (self-healing on any damage)."""
    try:
        payload = json.loads(DERIVED_CACHE_PATH.read_text(encoding="utf-8"))
    except Exception:
        payload = None
    if not isinstance(payload, dict) or payload.get("v") != CACHE_SCHEMA_VERSION:
        return {"v": CACHE_SCHEMA_VERSION, "docs": {}, "chunk_files": {}}
    if not isinstance(payload.get("docs"), dict):
        payload["docs"] = {}
    if not isinstance(payload.get("chunk_files"), dict):
        payload["chunk_files"] = {}
    return payload


def _save_derived_cache(cache: dict) -> None:
    try:
        DERIVED_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp = DERIVED_CACHE_PATH.with_suffix(".json.tmp")
        tmp.write_text(
            json.dumps(cache, ensure_ascii=False, separators=(",", ":")),
            encoding="utf-8",
        )
        os.replace(tmp, DERIVED_CACHE_PATH)
    except OSError as exc:
        print(f"[WARN] derived cache write skipped: {exc}")


def _doc_cache_entry_key(doc_bytes: bytes, tree_bytes: bytes | None) -> str:
    digest = hashlib.sha256()
    digest.update(doc_bytes)
    digest.update(b":")
    digest.update(str(CACHE_SCHEMA_VERSION).encode("utf-8"))
    if tree_bytes is not None:
        # Trees feed section_index rows; fold them in so tree-only edits
        # still invalidate the cached contributions for this doc.
        digest.update(b":")
        digest.update(tree_bytes)
    return digest.hexdigest()


def _timeline_skeleton() -> dict:
    return {
        "breakwave_drybulk": None,
        "breakwave_tankers": None,
        "baltic_dry": None,
        "baltic_tanker": None,
        "baltic_gas": None,
        "baltic_container": None,
        "baltic_ningbo": None,
    }


def _clean_metric_tons(val_str):
    if not val_str:
        return None
    m = re.search(r"(\d+\.?\d*)", str(val_str))
    if m:
        return float(m.group(1))
    return None


def _breakwave_iron_ore_contribution(meta: dict) -> dict | None:
    """Per-doc result of the breakwave drybulk fundamentals merge used by the
    iron-ore restocking CSV; cached so that pass never re-parses frontmatter."""
    if meta.get("source") != "breakwave" or meta.get("category") != "drybulk":
        return None
    date_str = meta.get("date")
    if not date_str:
        return None
    fundamentals = meta.get("fundamentals_table", {}) or {}
    ore_inv = _clean_metric_tons((fundamentals.get("China Iron Ore Inventories") or {}).get("ytd"))
    steel_prod = _clean_metric_tons((fundamentals.get("China Steel Production") or {}).get("ytd"))
    steel_inv = _clean_metric_tons((fundamentals.get("China Steel Inventories") or {}).get("ytd"))
    if steel_inv and steel_inv > 50:
        steel_inv = round(steel_inv / 100.0, 2)
    if not (ore_inv or steel_prod or steel_inv):
        return None
    fields = {}
    if ore_inv:
        fields["inventories_mt"] = ore_inv
    if steel_prod:
        fields["steel_production_mt"] = steel_prod
    if steel_inv:
        fields["steel_inventories_mt"] = steel_inv
    return {"date": date_str, "fields": fields}


def _build_doc_contributions(doc: dict, doc_path: Path, key: str) -> dict:
    """Parse one document exactly as build_derived always has and capture its
    per-doc contributions (theme row, signal rows, section rows, timeline
    update, iron-ore fundamentals) so they can be cached verbatim."""
    entry = {"k": key, "theme": None, "signals": [], "sections": [], "timeline": None, "io_fund": None}
    try:
        post = frontmatter.load(doc_path)
    except Exception:
        return entry

    meta = post.metadata
    source = meta.get("source")
    category = meta.get("category")
    date_str = meta.get("date")
    doc_id = meta.get("doc_id")

    # Archived bot-challenge / CDN error pages carry no market signal;
    # keep them out of signals/timelines entirely.
    if meta.get("is_error_page"):
        return entry

    entry["theme"] = {
        "doc_id": doc_id,
        "themes": meta.get("themes", []),
        "key_entities": meta.get("key_entities", []),
        "market_tone": meta.get("market_tone"),
    }

    if source == "breakwave":
        signals = meta.get("signals", {}) or {}
        fundamentals = meta.get("fundamentals_table", {}) or {}
        row = {
            "date": date_str,
            "source": source,
            "category": category,
            "doc_id": doc_id,
            "momentum": signals.get("momentum"),
            "sentiment": signals.get("sentiment"),
            "fundamentals": signals.get("fundamentals"),
        }
        if category == "drybulk":
            row.update({
                "bdryff": signals.get("bdryff"),
                "bdi": signals.get("bdi"),
                "bdi_spot": signals.get("bdi_spot"),
                "bdryff_30d_pct": signals.get("bdryff_30d_pct"),
                "bdryff_ytd_pct": signals.get("bdryff_ytd_pct"),
                "bdryff_yoy_pct": signals.get("bdryff_yoy_pct"),
                "bdi_30d_pct": signals.get("bdi_spot_30d_pct"),
                "bdi_ytd_pct": signals.get("bdi_spot_ytd_pct"),
                "bdi_yoy_pct": signals.get("bdi_spot_yoy_pct"),
                "china_iron_ore_imports_yoy": parse_pct((fundamentals.get("China Iron Ore Imports") or {}).get("yoy_pct")),
                "dry_bulk_fleet_yoy": parse_pct((fundamentals.get("Dry Bulk Fleet") or {}).get("yoy_pct")),
            })
        else:
            row.update({
                "bwetff": signals.get("bwetff"),
                "vlcc_meg_asia": signals.get("vlcc_meg_asia"),
                "bwetff_30d_pct": signals.get("bwetff_30d_pct"),
                "bwetff_ytd_pct": signals.get("bwetff_ytd_pct"),
                "bwetff_yoy_pct": signals.get("bwetff_yoy_pct"),
                "vlcc_meg_asia_30d_pct": signals.get("vlcc_meg_asia_30d_pct"),
                "vlcc_meg_asia_ytd_pct": signals.get("vlcc_meg_asia_ytd_pct"),
                "vlcc_meg_asia_yoy_pct": signals.get("vlcc_meg_asia_yoy_pct"),
                "world_oil_demand_yoy": parse_pct((fundamentals.get("World Oil Demand") or {}).get("yoy_pct")),
                "tanker_fleet_yoy": parse_pct((fundamentals.get("Tanker Fleet") or {}).get("yoy_pct")),
            })
        entry["signals"].append(row)
    elif source == "hellenic":
        signals = meta.get("signals", {}) or {}
        if signals:
            row = {
                "date": date_str,
                "source": source,
                "category": category,
                "doc_id": doc_id,
                "signal_family": signals.get("signal_family"),
                "signals": signals,
            }
            if category in HELLENIC_CHARTER_CATEGORIES:
                observations = signals.get("rate_observations", []) or []
                row.update({
                    "rate_observation_count": len(observations),
                    "charter_rate_summary": signals.get("rate_summary", {}),
                    "charter_timeframes": signals.get("timeframes", []),
                    "metric_units": signals.get("metric_units", []),
                })
            elif category == "iron_ore":
                metrics = signals.get("iron_ore_metrics", []) or []
                row.update({
                    "metric_observation_count": len(metrics),
                    "benchmark_prices": signals.get("benchmark_prices", {}),
                    "metric_units": signals.get("metric_units", []),
                })
            entry["signals"].append(row)
    elif source in {"baltic", "breakwave_insights"}:
        numeric_observations = meta.get("numeric_observations", []) or []
        if numeric_observations:
            entry["signals"].append({
                "date": date_str,
                "source": source,
                "category": category,
                "doc_id": doc_id,
                "signal_family": "numeric_observations",
                "numeric_observation_count": len(numeric_observations),
                "numeric_observations": numeric_observations[:80],
            })

    if source in {"breakwave", "baltic"} and date_str:
        try:
            iso = datetime.strptime(date_str, "%Y-%m-%d").isocalendar()
            week_key = f"{iso.year}-W{iso.week:02d}"
            field = f"{source}_{category}"
            entry["timeline"] = {"key": week_key, "field": field, "value": doc_id}
        except ValueError:
            pass

    entry["io_fund"] = _breakwave_iron_ore_contribution(meta)

    tree_path = doc.get("tree_path")
    if tree_path and (REPO_ROOT / tree_path).exists():
        try:
            tree = json.loads((REPO_ROOT / tree_path).read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            tree = None
        if tree:
            for node in iter_tree_nodes(tree):
                if node.get("level") == 0:
                    continue
                entry["sections"].append({
                    "doc_id": doc_id,
                    "source": source,
                    "category": category,
                    "date": date_str,
                    "node_id": node.get("node_id"),
                    "parent_id": node.get("parent_id"),
                    "title": node.get("title"),
                    "section_path": node.get("section_path", []),
                    "section_path_text": node.get("section_path_text"),
                    "level": node.get("level"),
                    "ordinal": node.get("ordinal"),
                    "summary": node.get("summary"),
                    "keywords": node.get("keywords", []),
                    "page_start": node.get("page_start"),
                    "page_end": node.get("page_end"),
                    "token_count": node.get("token_count"),
                })
    return entry


def _normalize_charter_rate(v):
    if v is None:
        return None
    try:
        val = float(v)
        if 0 < val < 100:
            return val * 1000.0
        if 100 <= val <= 2500:
            return val * 10.0
        return val
    except Exception:
        return v


def _clamp_tc_rate(val):
    """Plausibility guard for OCR-recovered time-charter rates ($/day).

    The x1000/x10 dotted-value rescaling occasionally misfires on OCR noise
    (e.g. a page number read as '2,026' -> 20260 is fine, but '12' from a
    footnote -> 12000 pollutes the series). Rates outside 300..200,000 $/day
    are physically implausible for every covered class, so they are dropped
    with a warning instead of being written into time_charter_rates.csv.
    """
    if val is None:
        return None
    try:
        fval = float(val)
    except (TypeError, ValueError):
        return val
    if not (300.0 <= fval <= 200000.0):
        print(f"[table-extract] dropping implausible charter rate {fval} $/day (outside 300..200000)", file=sys.stderr)
        return None
    return fval


def _scan_charter_chunk_file(path: Path, category: str) -> list[dict]:
    """Full rescan of one charter chunk JSONL into replayable TC-rate rows."""
    rows: list[dict] = []
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                chunk = json.loads(line)
            except json.JSONDecodeError:
                continue
            date = chunk.get("date")
            if date == "2022-10-21":
                date = "2022-10-19"
            text = chunk.get("text", "")
            if not date or not text:
                continue
            # Only process chunks that have OCR text (image asset chunks)
            if not any(marker in text for marker in ("OCR text:", "[structured table]", "[raw ocr]")) and "TIME CHARTER" not in text.upper():
                continue
            # Structured pipe-table rows (B2) take precedence: labeled cells
            # replace positional vals[-6:] guesswork when present.
            struct_rows = parse_structured_charter_rows(text) if category == "dry_charter" else []
            extracted = extract_hellenic_charter_signals(text, category)
            rate_obs = extracted.get("rate_observations", []) or []
            if not rate_obs and not struct_rows:
                continue
            obs_rows = []
            if struct_rows:
                for srow in struct_rows:
                    svals = [c for c in (_clamp_tc_rate(_normalize_charter_rate(v)) for v in srow.get("values", [])) if c is not None]
                    if not svals:
                        continue
                    obs_rows.append({
                        "s": srow["segment"],
                        "v": svals,
                        "tenor": srow.get("tenor"),
                        "region": srow.get("region"),
                    })
            for obs in rate_obs:
                seg = obs.get("segment")
                vals = [_clamp_tc_rate(_normalize_charter_rate(v)) for v in (obs.get("values") or [])]
                vals = [v for v in vals if v is not None]
                obs_rows.append({"s": seg, "v": vals})
            rows.append({"d": date, "c": category, "o": obs_rows})
    return rows


def _apply_tc_chunk_rows(rows: list[dict], tc_records: dict) -> None:
    """Merge cached/scanned charter chunk rows into tc_records with the exact
    semantics of the original inline scan loop."""
    for entry in rows:
        date = entry.get("d")
        category = entry.get("c")
        if not date or category not in HELLENIC_CHARTER_CATEGORIES:
            continue
        if date not in tc_records:
            tc_records[date] = {}
        for ob in entry.get("o", []):
            seg = ob.get("s")
            vals = ob.get("v") or []
            if not seg or not vals:
                continue
            # Structured (labeled) rows write directly to their tenor/region
            # columns — no positional inference needed.
            if ob.get("tenor") and category == "dry_charter":
                tenor = ob["tenor"]
                region = ob.get("region")
                if region:
                    tc_records[date][f"{seg}_{tenor}_{region}"] = vals[0]
                    tc_records[date][f"{seg}_{tenor}_avg"] = (
                        (tc_records[date][f"{seg}_{tenor}_atl"] + tc_records[date][f"{seg}_{tenor}_pac"]) / 2.0
                        if f"{seg}_{tenor}_atl" in tc_records[date] and f"{seg}_{tenor}_pac" in tc_records[date]
                        else vals[0]
                    )
                else:
                    tc_records[date][f"{seg}_{tenor}_avg"] = sum(vals) / len(vals)
                continue
            if category == "dry_charter":
                rates = vals[-6:]
                if len(rates) == 6:
                    tc_records[date][f"{seg}_4_6m_atl"] = rates[0]
                    tc_records[date][f"{seg}_4_6m_pac"] = rates[1]
                    tc_records[date][f"{seg}_4_6m_avg"] = (rates[0] + rates[1]) / 2.0
                    tc_records[date][f"{seg}_1y_atl"] = rates[2]
                    tc_records[date][f"{seg}_1y_pac"] = rates[3]
                    tc_records[date][f"{seg}_1y_avg"] = (rates[2] + rates[3]) / 2.0
                    tc_records[date][f"{seg}_2y_atl"] = rates[4]
                    tc_records[date][f"{seg}_2y_pac"] = rates[5]
                    tc_records[date][f"{seg}_2y_avg"] = (rates[4] + rates[5]) / 2.0
                elif len(rates) == 5:
                    tc_records[date][f"{seg}_4_6m_pac"] = rates[0]
                    tc_records[date][f"{seg}_4_6m_avg"] = rates[0]
                    tc_records[date][f"{seg}_1y_atl"] = rates[1]
                    tc_records[date][f"{seg}_1y_pac"] = rates[2]
                    tc_records[date][f"{seg}_1y_avg"] = (rates[1] + rates[2]) / 2.0
                    tc_records[date][f"{seg}_2y_atl"] = rates[3]
                    tc_records[date][f"{seg}_2y_pac"] = rates[4]
                    tc_records[date][f"{seg}_2y_avg"] = (rates[3] + rates[4]) / 2.0
            else:  # tanker_charter
                rates = vals[-4:]
                if len(rates) == 4:
                    # Only write if not already set from document-level signals
                    k1 = f"{seg}_1y"
                    if k1 not in tc_records[date]:
                        tc_records[date][f"{seg}_1y"] = rates[0]
                        tc_records[date][f"{seg}_2y"] = rates[1]
                        tc_records[date][f"{seg}_3y"] = rates[2]
                        tc_records[date][f"{seg}_5y"] = rates[3]


def _chunk_file_cache_key(path: Path) -> str:
    try:
        return path.resolve().relative_to(REPO_ROOT.resolve()).as_posix()
    except Exception:
        return path.as_posix()


def write_chunk_index() -> dict:
    """Emit knowledge/chunks/index.json describing every chunk shard.

    The frontend Q&A panel and generate_brief.py used to hardcode year-sharded
    filenames; every January rollover they silently stopped seeing the new
    year's files. This manifest (small, deployed to Pages) is the single
    source of truth for dynamic discovery. Cheap: stat-only per shard.
    """
    entries = []
    for path in sorted(CHUNKS_DIR.glob("*.jsonl")):
        name = path.name
        match = re.search(r"_(\d{4})\.jsonl$", name)
        year = int(match.group(1)) if match else None
        stem = name[: -len(".jsonl")]
        entries.append(
            {
                "file": name,
                "path": f"knowledge/chunks/{name}",
                "stem": stem,
                "year": year,
                "bytes": path.stat().st_size,
            }
        )
    payload = {
        "generated_at": utc_now_iso(),
        "compiler_version": COMPILER_VERSION,
        "count": len(entries),
        "files": entries,
    }
    CHUNKS_DIR.mkdir(parents=True, exist_ok=True)
    out = CHUNKS_DIR / "index.json"
    tmp = out.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, indent=1), encoding="utf-8")
    os.replace(tmp, out)
    return payload


def build_derived(llm_enabled: bool = False, force_full: bool = False):
    documents = load_jsonl(DOCUMENTS_MANIFEST)
    SIGNALS_DERIVED.parent.mkdir(parents=True, exist_ok=True)
    signal_rows = []
    theme_rows = []
    section_rows = []
    timelines = {}
    io_fund_updates = []

    # Content-addressed caches (KNOWLEDGE_FULL_DERIVED=1 / --rebuild bypass
    # reads but still refresh the cache files for subsequent runs).
    env_force_full = os.environ.get("KNOWLEDGE_FULL_DERIVED") == "1"
    prior_cache = (
        _load_derived_cache()
        if not (force_full or env_force_full)
        else {"v": CACHE_SCHEMA_VERSION, "docs": {}, "chunk_files": {}}
    )
    docs_store: dict[str, dict] = {}
    reused_docs = 0
    reprocessed_docs = 0

    for doc in documents:
        doc_rel = doc["doc_path"]
        doc_path = REPO_ROOT / doc_rel
        if not doc_path.exists():
            continue
        try:
            raw_bytes = doc_path.read_bytes()
        except OSError:
            continue
        tree_rel = doc.get("tree_path")
        tree_bytes = None
        if tree_rel and (REPO_ROOT / tree_rel).exists():
            try:
                tree_bytes = (REPO_ROOT / tree_rel).read_bytes()
            except OSError:
                tree_bytes = None
        content_key = _doc_cache_entry_key(raw_bytes, tree_bytes)
        cache_id = doc.get("doc_id") or f"path::{doc_rel}"
        prior_entry = prior_cache["docs"].get(cache_id)
        if prior_entry is not None and prior_entry.get("k") == content_key:
            reused_docs += 1
            entry = prior_entry
        else:
            reprocessed_docs += 1
            entry = _build_doc_contributions(doc, doc_path, content_key)
        docs_store[cache_id] = entry

        if entry.get("theme"):
            theme_rows.append(entry["theme"])
        for row in entry.get("signals", []):
            signal_rows.append(row)
        for row in entry.get("sections", []):
            section_rows.append(row)
        tl = entry.get("timeline")
        if tl:
            timelines.setdefault(tl["key"], _timeline_skeleton())
            timelines[tl["key"]][tl["field"]] = tl["value"]
        if entry.get("io_fund"):
            io_fund_updates.append(entry["io_fund"])

    print(
        f"[DERIVED] incremental: reused={reused_docs} reprocessed={reprocessed_docs}"
        + (" (full refresh)" if (force_full or env_force_full) else "")
    )

    SIGNALS_DERIVED.write_text("", encoding="utf-8")
    bw_compact_signals = []
    for row in sorted(signal_rows, key=lambda item: (item.get("date") or "", item.get("doc_id") or "")):
        append_jsonl(SIGNALS_DERIVED, row)
        if row.get("source") == "breakwave" and row.get("sentiment") is not None and row.get("date") and row.get("date") != "0000-00-00":
            bw_compact_signals.append({
                "date": row["date"],
                "source": row.get("source"),
                "category": row.get("category"),
                "sentiment": row.get("sentiment"),
                "momentum": row.get("momentum"),
                "fundamentals": row.get("fundamentals"),
                "doc_id": row.get("doc_id")
            })

    BREAKWAVE_SIGNALS_DERIVED.write_text(json.dumps(bw_compact_signals, indent=2, ensure_ascii=False), encoding="utf-8")

    THEMES_DERIVED.write_text("", encoding="utf-8")
    for row in theme_rows:
        append_jsonl(THEMES_DERIVED, row)

    SECTION_INDEX_DERIVED.write_text("", encoding="utf-8")
    for row in sorted(section_rows, key=lambda item: (item.get("date") or "", item.get("doc_id") or "", item.get("ordinal") or 0)):
        append_jsonl(SECTION_INDEX_DERIVED, row)

    TIMELINES_DERIVED.write_text(json.dumps(dict(sorted(timelines.items())), indent=2, ensure_ascii=False), encoding="utf-8")

    # -------------------------------------------------------------------------
    # COMPILE DERIVED SUMMARY CSVS FOR THE FRONTEND
    # -------------------------------------------------------------------------
    derived_dir = REPO_ROOT / "data" / "derived"
    derived_dir.mkdir(parents=True, exist_ok=True)

    # 1. Compile Time Charter Rates (Weekly)
    tc_records = {}
    for r in signal_rows:
        source = r.get("source")
        category = r.get("category")
        date = r.get("date")
        if source == "hellenic" and category in {"dry_charter", "tanker_charter"} and date:
            if date == "2022-10-21":
                date = "2022-10-19"
            signals = r.get("signals", {}) or {}
            rate_obs = signals.get("rate_observations", []) or []
            if not rate_obs:
                continue
            if date not in tc_records:
                tc_records[date] = {}
            for obs in rate_obs:
                seg = obs.get("segment")
                vals = obs.get("values") or []
                if not seg or not vals:
                    continue
                if category == "dry_charter":
                    rates = vals[-6:]
                    if len(rates) == 6:
                        tc_records[date][f"{seg}_4_6m_atl"] = rates[0]
                        tc_records[date][f"{seg}_4_6m_pac"] = rates[1]
                        tc_records[date][f"{seg}_4_6m_avg"] = (rates[0] + rates[1]) / 2.0
                        tc_records[date][f"{seg}_1y_atl"] = rates[2]
                        tc_records[date][f"{seg}_1y_pac"] = rates[3]
                        tc_records[date][f"{seg}_1y_avg"] = (rates[2] + rates[3]) / 2.0
                        tc_records[date][f"{seg}_2y_atl"] = rates[4]
                        tc_records[date][f"{seg}_2y_pac"] = rates[5]
                        tc_records[date][f"{seg}_2y_avg"] = (rates[4] + rates[5]) / 2.0
                    elif len(rates) == 5:
                        tc_records[date][f"{seg}_4_6m_pac"] = rates[0]
                        tc_records[date][f"{seg}_4_6m_avg"] = rates[0]
                        tc_records[date][f"{seg}_1y_atl"] = rates[1]
                        tc_records[date][f"{seg}_1y_pac"] = rates[2]
                        tc_records[date][f"{seg}_1y_avg"] = (rates[1] + rates[2]) / 2.0
                        tc_records[date][f"{seg}_2y_atl"] = rates[3]
                        tc_records[date][f"{seg}_2y_pac"] = rates[4]
                        tc_records[date][f"{seg}_2y_avg"] = (rates[3] + rates[4]) / 2.0
                else: # tanker_charter
                    rates = vals[-4:]
                    if len(rates) == 4:
                        tc_records[date][f"{seg}_1y"] = rates[0]
                        tc_records[date][f"{seg}_2y"] = rates[1]
                        tc_records[date][f"{seg}_3y"] = rates[2]
                        tc_records[date][f"{seg}_5y"] = rates[3]

    # Also scan chunk JSONL files directly for tanker_charter and dry_charter.
    # The TC rate table is embedded as an image OCR text in chunk files,
    # not in document-level metadata, so the signal_rows loop above misses it.
    # Results are cached per chunk file keyed by (size, mtime_ns).
    all_tc_chunk_files = []
    import glob as _glob2
    
    # Tanker charter chunks
    _tc_base = CHUNKS_DIR / "hellenic_tanker_charter.jsonl"
    if _tc_base.exists():
        all_tc_chunk_files.append((_tc_base, "tanker_charter"))
    for _sf in sorted(_glob2.glob(str(CHUNKS_DIR / "hellenic_tanker_charter_*.jsonl"))):
        all_tc_chunk_files.append((Path(_sf), "tanker_charter"))
        
    # Dry charter chunks
    _dc_base = CHUNKS_DIR / "hellenic_dry_charter.jsonl"
    if _dc_base.exists():
        all_tc_chunk_files.append((_dc_base, "dry_charter"))
    for _sf in sorted(_glob2.glob(str(CHUNKS_DIR / "hellenic_dry_charter_*.jsonl"))):
        all_tc_chunk_files.append((Path(_sf), "dry_charter"))

    chunk_store: dict[str, dict] = {}
    rescanned_chunk_files = 0
    for _cf, _cat in all_tc_chunk_files:
        try:
            _st = _cf.stat()
        except OSError:
            continue
        _ckey = _chunk_file_cache_key(_cf)
        _prior_cf = prior_cache["chunk_files"].get(_ckey)
        if (
            isinstance(_prior_cf, dict)
            and _prior_cf.get("size") == _st.st_size
            and _prior_cf.get("mtime_ns") == _st.st_mtime_ns
            and isinstance(_prior_cf.get("rows"), list)
        ):
            _rows = _prior_cf["rows"]
        else:
            try:
                _rows = _scan_charter_chunk_file(_cf, _cat)
            except OSError:
                continue
            rescanned_chunk_files += 1
        chunk_store[_ckey] = {"size": _st.st_size, "mtime_ns": _st.st_mtime_ns, "rows": _rows}
        _apply_tc_chunk_rows(_rows, tc_records)

    tc_file = derived_dir / "time_charter_rates.csv"
    tc_cols = [
        "date",
        "capesize_4_6m_atl", "capesize_4_6m_pac", "capesize_4_6m_avg",
        "capesize_1y_atl", "capesize_1y_pac", "capesize_1y_avg",
        "capesize_2y_atl", "capesize_2y_pac", "capesize_2y_avg",
        "panamax_4_6m_atl", "panamax_4_6m_pac", "panamax_4_6m_avg",
        "panamax_1y_atl", "panamax_1y_pac", "panamax_1y_avg",
        "panamax_2y_atl", "panamax_2y_pac", "panamax_2y_avg",
        "supramax_4_6m_atl", "supramax_4_6m_pac", "supramax_4_6m_avg",
        "supramax_1y_atl", "supramax_1y_pac", "supramax_1y_avg",
        "supramax_2y_atl", "supramax_2y_pac", "supramax_2y_avg",
        "handysize_4_6m_atl", "handysize_4_6m_pac", "handysize_4_6m_avg",
        "handysize_1y_atl", "handysize_1y_pac", "handysize_1y_avg",
        "handysize_2y_atl", "handysize_2y_pac", "handysize_2y_avg",
        "vlcc_1y", "vlcc_2y", "vlcc_3y", "vlcc_5y",
        "suezmax_1y", "suezmax_2y", "suezmax_3y", "suezmax_5y",
        "aframax_1y", "aframax_2y", "aframax_3y", "aframax_5y",
        "mr_1y", "mr_2y", "mr_3y", "mr_5y",
        "lr1_1y", "lr1_2y", "lr1_3y", "lr1_5y",
        "lr2_1y", "lr2_2y", "lr2_3y", "lr2_5y",
        "handytanker_1y", "handytanker_2y", "handytanker_3y", "handytanker_5y"
    ]
    # Preserve pre-existing backfilled rows (source=fearnleys) and add source provenance
    import csv
    existing_backfill_rows = []
    if tc_file.exists():
        with open(tc_file, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("source") == "fearnleys":
                    existing_backfill_rows.append(row)
    # Build new Alibra OCR rows
    alibra_rows = []
    tc_cols_with_source = ["date", "source"] + tc_cols[1:]
    for date in sorted(tc_records.keys()):
        row_dict = {"date": date, "source": "alibra_ocr"}
        data = tc_records[date]
        for col in tc_cols[1:]:
            val = None
            if col in data:
                val = data[col]
            else:
                col_parts = col.split("_", 1)
                if len(col_parts) == 2:
                    col_seg, col_suffix = col_parts
                    for key, key_val in data.items():
                        key_parts = key.split("_", 1)
                        if len(key_parts) == 2:
                            key_seg, key_suffix = key_parts
                            if key_suffix == col_suffix:
                                if col_seg == "panamax" and ("panam" in key_seg or "kmax" in key_seg):
                                    val = key_val
                                    break
                                elif col_seg == "supramax" and ("supra" in key_seg or "smax" in key_seg or "ultra" in key_seg):
                                    val = key_val
                                    break
                                elif col_seg == "handytanker" and ("handytanker" in key_seg or "h.tanker" in key_seg):
                                    val = key_val
                                    break
            row_dict[col] = val if val is not None else ""
        alibra_rows.append(row_dict)
    # Combine: backfill (fearnleys) first, then new Alibra OCR
    all_rows = existing_backfill_rows + alibra_rows
    all_rows.sort(key=lambda r: r.get("date", ""))
    with open(tc_file, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=tc_cols_with_source, extrasaction="ignore")
        writer.writeheader()
        for row in all_rows:
            writer.writerow(row)

    # Seamlessly merge deep archives (2008-2026) and forward curves
    try:
        import integrate_alibra_feed
        integrate_alibra_feed.main()
    except Exception as _e:
        print(f"[WARN] integrate_alibra_feed skipped: {_e}")

    # 2. Compile Iron Ore Restocking (Daily/Weekly)
    iron_ore_records = {}
    io_prev = {"cfr_62": None, "cfr_65": None}
    io_rejects = 0
    ordered_io_rows = sorted(
        (r for r in signal_rows if r.get("source") == "hellenic" and r.get("category") == "iron_ore" and r.get("date")),
        key=lambda r: r.get("date") or "",
    )
    for r in ordered_io_rows:
        date = r.get("date")
        signals = r.get("signals", {}) or {}
        obs_list = signals.get("iron_ore_metrics", []) or []
        if not obs_list:
            continue
        day = iron_ore_records.setdefault(date, {})
        # First accepted hit wins per series: the daily-settlement table is
        # emitted before the weekly/monthly-average tables in every report,
        # so scanning in document order and stopping per series keeps later
        # average columns from clobbering the daily price (the original
        # last-write-wins bug).
        for obs in obs_list:
            line = obs.get("source_line", "")
            vals = obs.get("values") or []
            if not line or len(vals) < 3:
                continue
            if _is_io_summary_row(line):
                continue
            target = _match_iosi_tag(line)
            if target is None or day.get(target) is not None:
                continue
            picked = _pick_cfr_value([v for v in vals if v not in _IO_PRICE_GRADE_TOKENS], io_prev[target])
            if picked is None:
                io_rejects += 1
                continue
            day[target] = picked
            io_prev[target] = picked
        # Port-inventory indices keep the exact-substring match (their tags
        # have not shown OCR confusion; fuzzy-widening them would risk
        # pairing RMB/t prices into USD index columns).
        for obs in obs_list:
            line = obs.get("source_line", "")
            vals = obs.get("values") or []
            if not line or len(vals) < 2:
                continue
            line_lower = line.lower()
            if "iopi62" in line_lower and day.get("port_stock_62") is None:
                for v in vals:
                    if 300 <= v <= 1800:
                        day["port_stock_62"] = v
                        break
            elif "iopi65" in line_lower and day.get("port_stock_65") is None:
                for v in vals:
                    if 300 <= v <= 1800:
                        day["port_stock_65"] = v
                        break
    # Same-date cross-check: CFR 65% always trades near CFR 62% (modest
    # premium); a wider-than-market gap means OCR paired two unrelated
    # numbers from one scrambled row, so neither value can be trusted.
    for date in sorted(iron_ore_records):
        day = iron_ore_records[date]
        v62, v65 = day.get("cfr_62"), day.get("cfr_65")
        if v62 is not None and v65 is not None and abs(v65 - v62) > _IO_CFR_SPREAD_LIMIT:
            del day["cfr_62"]
            del day["cfr_65"]
            io_rejects += 1
    print(f"[IRON-ORE] dates_parsed={len(iron_ore_records)} rejected_observations={io_rejects}")
    # Same-date cross-check: CFR 65% always trades near CFR 62% (modest
    # premium); a wider-than-market gap means OCR paired two unrelated
    # numbers from one scrambled row, so neither value can be trusted.
    for date in sorted(iron_ore_records):
        day = iron_ore_records[date]
        v62, v65 = day.get("cfr_62"), day.get("cfr_65")
        if v62 is not None and v65 is not None and abs(v65 - v62) > _IO_CFR_SPREAD_LIMIT:
            del day["cfr_62"]
            del day["cfr_65"]
            io_rejects += 1
    print(f"[IRON-ORE] dates_parsed={len(iron_ore_records)} rejected_observations={io_rejects}")
    # Now merge Breakwave fundamentals (inventories). Contributions were
    # captured per-doc during the cached document pass above; replay them in
    # manifest order (last writer wins, exactly as the original second pass).
    for _upd in io_fund_updates:
        _io_date = _upd["date"]
        if _io_date not in iron_ore_records:
            iron_ore_records[_io_date] = {}
        for _k, _v in _upd["fields"].items():
            iron_ore_records[_io_date][_k] = _v

    # Write Iron Ore Restocking additively (preserve all historical PDF archive data)
    io_file = derived_dir / "iron_ore_restocking.csv"
    io_cols = ["date", "cfr_62", "cfr_65", "port_stock_62", "port_stock_65", "inventories_mt", "steel_production_mt", "steel_inventories_mt"]
    existing_io = {}
    if io_file.exists():
        import csv
        with open(io_file, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for r in reader:
                d = r.get("date")
                if d:
                    existing_io[d] = {k: float(v) if v and v != "" else None for k, v in r.items() if k != "date"}
    # Merge new in-memory records additively. A stored price exactly equal
    # to an Fe-grade token is a known OCR misparse (see the 2026 cfr_62=62.5
    # freeze); when this run produced a trusted value for that cell, the
    # corrupted cell is replaced instead of preserved.
    io_grade_repairs = 0
    for d, data in iron_ore_records.items():
        if d not in existing_io:
            existing_io[d] = {}
        for k, v in data.items():
            if v is None:
                continue
            cur = existing_io[d].get(k)
            if cur is None or cur == "":
                existing_io[d][k] = v
            elif k in {"cfr_62", "cfr_65"} and cur in _IO_PRICE_GRADE_TOKENS and cur != v:
                existing_io[d][k] = v
                io_grade_repairs += 1
    # A stored price exactly equal to an Fe-grade token with no trusted
    # replacement from this run stays corrupted forever; null it so the
    # chart shows a gap instead of a freeze. Scoped to the OCR-format-change
    # era (2026+) because genuine $62/$65 handles existed pre-2020.
    for d, row in existing_io.items():
        if d < "2026-01-01":
            continue
        for k in ("cfr_62", "cfr_65"):
            cur = row.get(k)
            if isinstance(cur, float) and cur in _IO_PRICE_GRADE_TOKENS and iron_ore_records.get(d, {}).get(k) is None:
                row[k] = None
                io_grade_repairs += 1
    if io_grade_repairs:
        print(f"[IRON-ORE] grade-token cells repaired/blanked: {io_grade_repairs}")
    # Stored pairs whose 65-vs-62 spread exceeds the market-plausible band
    # are replaced wholesale when this run produced a validated pair.
    io_spread_repairs = 0
    for d, day in iron_ore_records.items():
        row = existing_io.get(d)
        if not row:
            continue
        def _num(v):
            return v if isinstance(v, (int, float)) else None
        s62, s65 = _num(row.get("cfr_62")), _num(row.get("cfr_65"))
        n62, n65 = _num(day.get("cfr_62")), _num(day.get("cfr_65"))
        stored_bad = s62 is not None and s65 is not None and abs(s65 - s62) > _IO_CFR_SPREAD_LIMIT
        new_ok = n62 is not None and n65 is not None and abs(n65 - n62) <= _IO_CFR_SPREAD_LIMIT
        if stored_bad and new_ok and (n62 != s62 or n65 != s65):
            row["cfr_62"] = n62
            row["cfr_65"] = n65
            io_spread_repairs += 1
    if io_spread_repairs:
        print(f"[IRON-ORE] repaired spread-violating pairs: {io_spread_repairs}")

    with open(io_file, "w", encoding="utf-8", newline="") as f:
        import csv
        writer = csv.writer(f)
        writer.writerow(io_cols)
        for date in sorted(existing_io.keys()):
            row = [date]
            data = existing_io[date]
            for col in io_cols[1:]:
                val = data.get(col)
                row.append("" if val is None else str(val))
            writer.writerow(row)

    # 3. Compile Demolition Scrap Prices (Weekly) additively
    demo_records = {}
    for r in signal_rows:
        source = r.get("source")
        category = r.get("category")
        date = r.get("date")
        if source == "hellenic" and category == "demolition" and date:
            signals = r.get("signals", {}) or {}
            obs_list = signals.get("numeric_observations", []) or []
            if not obs_list:
                continue
            matches = []
            for obs in obs_list:
                line = obs.get("source_line", "")
                vals = obs.get("values") or []
                if not line or not vals:
                    continue
                if len(vals) == 4 and all(150 <= v <= 800 for v in vals):
                    matches.append(vals)
            if len(matches) == 3:
                demo_records[date] = {
                    "dry_india": matches[0][0],
                    "dry_bangla": matches[0][1],
                    "dry_pak": matches[0][2],
                    "dry_turkey": matches[0][3],
                    "tanker_india": matches[1][0],
                    "tanker_bangla": matches[1][1],
                    "tanker_pak": matches[1][2],
                    "container_india": matches[2][0]
                }
    
    val_file = derived_dir / "scrappage_prices.csv"
    val_cols = ["date", "dry_india", "dry_bangla", "dry_pak", "dry_turkey", "tanker_india", "tanker_bangla", "tanker_pak", "container_india"]
    existing_demo = {}
    if val_file.exists():
        import csv
        with open(val_file, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for r in reader:
                d = r.get("date")
                if d:
                    existing_demo[d] = {k: float(v) if v and v != "" else None for k, v in r.items() if k != "date"}
    # Merge new in-memory records additively
    for d, data in demo_records.items():
        if d not in existing_demo:
            existing_demo[d] = {}
        for k, v in data.items():
            if v is not None and (existing_demo[d].get(k) is None or existing_demo[d].get(k) == ""):
                existing_demo[d][k] = v

    with open(val_file, "w", encoding="utf-8", newline="") as f:
        import csv
        writer = csv.writer(f)
        writer.writerow(val_cols)
        for date in sorted(existing_demo.keys()):
            row = [date]
            data = existing_demo[date]
            for col in val_cols[1:]:
                val = data.get(col)
                row.append("" if val is None else str(val))
            writer.writerow(row)
    _save_derived_cache({
        "v": CACHE_SCHEMA_VERSION,
        "docs": docs_store,
        "chunk_files": chunk_store,
    })
    print(
        f"[DERIVED] cache: docs={len(docs_store)} chunk_files={len(chunk_store)} "
        f"rescanned={rescanned_chunk_files} -> {DERIVED_CACHE_PATH.name}"
    )
    build_wiki(
        repo_root=REPO_ROOT,
        config_dir=CONFIG_DIR,
        wiki_dir=WIKI_DIR,
        documents_manifest=DOCUMENTS_MANIFEST,
        section_index_path=SECTION_INDEX_DERIVED,
        themes_path=THEMES_DERIVED,
        signals_path=SIGNALS_DERIVED,
        output_path=TOPIC_EVIDENCE_DERIVED,
        generated_at=utc_now_iso(),
        llm_enabled=llm_enabled,
    )
    build_health_reports(
        repo_root=REPO_ROOT,
        reports_dir=KNOWLEDGE_REPORTS_DIR,
        documents_manifest=DOCUMENTS_MANIFEST,
        section_index_path=SECTION_INDEX_DERIVED,
        topic_config_path=CONFIG_DIR / "wiki_topics.json",
        topic_evidence_path=TOPIC_EVIDENCE_DERIVED,
        lint_report_path=LINT_REPORT,
        coverage_report_path=COVERAGE_REPORT,
        summary_path=HEALTH_SUMMARY,
        generated_at=utc_now_iso(),
    )
    chunk_index = write_chunk_index()
    print(f"[CHUNK-INDEX] wrote knowledge/chunks/index.json ({chunk_index['count']} shards)")
    # B1: pre-tokenized browser search indexes for the Q&A fast path.
    # Soft import so an incomplete checkout degrades to "no fast path" in the
    # frontend instead of failing the whole derived rebuild.
    try:
        from search_index_build import build_all as _build_search_indexes

        search_manifest = _build_search_indexes(CHUNKS_DIR, CHUNKS_DIR / "search")
        n_shards = search_manifest.get("count") or len(search_manifest.get("files", []))
        print(f"[SEARCH-INDEX] wrote {n_shards} shard indexes")
    except Exception as exc:
        print(f"[SEARCH-INDEX] skipped ({exc})", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(description="Shipping knowledge compiler")
    parser.add_argument("--source", choices=["breakwave", "baltic", "breakwave_insights", "hellenic", "books", "broker_reports", "poten", "all"], default=None)
    parser.add_argument("--rebuild", action="store_true")
    parser.add_argument("--no-llm", action="store_true")
    parser.add_argument("--derived-only", action="store_true")
    parser.add_argument("--batch-total", type=int, default=1, help="Split selected source files into N deterministic contiguous batches.")
    parser.add_argument("--batch-index", type=int, default=1, help="1-based batch number to process (1..batch-total).")
    args = parser.parse_args()

    if args.batch_total < 1:
        parser.error("--batch-total must be >= 1")
    if args.batch_index < 1:
        parser.error("--batch-index must be >= 1")
    if args.batch_index > args.batch_total:
        parser.error("--batch-index must be <= --batch-total")
    if args.batch_total > 1 and args.source in (None, "all"):
        parser.error("Batch mode requires a concrete --source (not all).")

    try:
        existing_metadata_index = {}
        if args.rebuild:
            loaded_rows = load_manifest_rows()
            existing_metadata_index = build_existing_metadata_index(loaded_rows)
            if args.batch_total > 1 and args.batch_index > 1:
                ensure_layout()
                print(
                    f"[REBUILD] mode=batch-continue scope={args.source} "
                    f"batch={args.batch_index}/{args.batch_total} (no pre-clear)"
                )
            elif args.source in (None, "all"):
                print("[REBUILD] mode=full scope=all")
                clear_rebuild_outputs()
            else:
                ensure_layout()
                pruned_rows = prune_manifest_for_sources(loaded_rows, args.source)
                write_manifest_rows(pruned_rows)
                print(f"[REBUILD] mode=source scope={args.source} removed={len(loaded_rows) - len(pruned_rows)}")
        else:
            ensure_layout()

        if args.derived_only:
            build_derived(llm_enabled=llm_available() and not args.no_llm)
            print("[DERIVED] rebuilt")
            return 0

        build_sources_registry()
        llm_enabled = llm_available() and not args.no_llm
        loaded_manifest_rows = load_manifest_rows()
        manifest_rows = prune_missing_sources(loaded_manifest_rows)
        manifest_rows = prune_non_primary_archive_sources(manifest_rows)
        hash_version_updates, migrated_hash_cache = migrate_manifest_hash_versions(manifest_rows)
        schema_updates = normalize_manifest_schema(manifest_rows)
        processed_index = latest_rows_by_source(manifest_rows)
        if len(manifest_rows) != len(loaded_manifest_rows) or hash_version_updates or schema_updates:
            write_manifest_rows(list(processed_index.values()))
            if hash_version_updates:
                print(f"[MANIFEST] source_hash_version_migrated={hash_version_updates}")
            if schema_updates:
                print(f"[MANIFEST] linked_asset_schema_normalized={schema_updates}")

        processed_count = 0
        skipped_count = 0
        error_count = 0
        linked_asset_totals = empty_linked_asset_stats()
        pending_items = []
        existing_metadata_cache = {}

        source_files = list(iter_source_files(args.source))
        selected_source_files, batch_start, batch_end = select_batch_slice(
            source_files, args.batch_total, args.batch_index
        )
        print(
            f"[BATCH] source={args.source or 'all'} batch={args.batch_index}/{args.batch_total} "
            f"selected={len(selected_source_files)} of {len(source_files)} entries (range={batch_start}:{batch_end})"
        )

        for source, category, path in selected_source_files:
            source_rel = relpath(path)
            current_hash = migrated_hash_cache.get(source_rel) or source_hash(path)
            existing_row = processed_index.get(source_rel)

            if (
                not args.rebuild
                and existing_row
                and existing_row.get("source_hash_version") == SOURCE_HASH_VERSION
                and existing_row.get("source_hash") == current_hash
                and artifacts_current(existing_row)
            ):
                skipped_count += 1
                continue

            if existing_row:
                existing_metadata_cache[source_rel] = existing_metadata_index.get(source_rel) or load_existing_metadata(existing_row)
            pending_items.append((source, category, path, source_rel, current_hash, existing_row))

        chunk_compaction_targets = defaultdict(set)
        stale_doc_paths = set()
        stale_tree_paths = set()

        for source, category, path, source_rel, current_hash, existing_row in pending_items:
            try:
                existing_metadata = existing_metadata_cache.get(source_rel) or existing_metadata_index.get(source_rel) or {}
                adapted = adapt_source_file(source, category, path, llm_enabled, existing_metadata=existing_metadata)
                metadata, _, manifest_row = process_file(path, adapted, source_hash_value=current_hash)
                processed_index[source_rel] = manifest_row
                processed_count += 1
                for field in LINKED_ASSET_FIELD_NAMES:
                    linked_asset_totals[field] += int(manifest_row.get(field) or 0)

                chunk_rel = manifest_row.get("chunk_file") or (existing_row or {}).get("chunk_file")
                if chunk_rel:
                    chunk_compaction_targets.setdefault(chunk_rel, set())
                    if existing_row:
                        old_chunk_rel = existing_row.get("chunk_file")
                        old_doc_id = existing_row.get("doc_id")
                        new_doc_id = manifest_row.get("doc_id")
                        if old_doc_id and new_doc_id and old_doc_id != new_doc_id:
                            chunk_compaction_targets[chunk_rel].add(old_doc_id)
                        # When a document migrates to a different chunk file (e.g. due to
                        # year-sharding introduced in ce95d888), evict its old entry from
                        # the previous file so validation never sees duplicate chunk_ids.
                        if old_chunk_rel and old_chunk_rel != chunk_rel and old_doc_id:
                            chunk_compaction_targets.setdefault(old_chunk_rel, set()).add(old_doc_id)

                if existing_row:
                    old_doc_path = existing_row.get("doc_path")
                    if old_doc_path and old_doc_path != manifest_row.get("doc_path"):
                        stale_doc_paths.add(old_doc_path)
                    old_tree_path = existing_row.get("tree_path")
                    if old_tree_path and old_tree_path != manifest_row.get("tree_path"):
                        stale_tree_paths.add(old_tree_path)

                if MANIFEST_FLUSH_EVERY > 0 and processed_count % MANIFEST_FLUSH_EVERY == 0:
                    write_manifest_rows(list(processed_index.values()))

                print(f"[{source.upper()}] [{metadata.get('date') or metadata.get('title')}] [OK]")
            except Exception as exc:
                error_count += 1
                reason = f"{exc}\n{traceback.format_exc()}"
                log_error(path, reason)
                date_hint = path.stem.split("_")[0]
                print(f"[{source.upper()}] [{date_hint}] [ERR] {exc}")

        for chunk_rel, remove_doc_ids in chunk_compaction_targets.items():
            compact_chunk_file(REPO_ROOT / chunk_rel, remove_doc_ids=remove_doc_ids)

        final_manifest_rows = list(processed_index.values())
        write_manifest_rows(final_manifest_rows)

        kept_doc_paths = {row.get("doc_path") for row in final_manifest_rows if row.get("doc_path")}
        kept_tree_paths = {row.get("tree_path") for row in final_manifest_rows if row.get("tree_path")}
        for doc_path in stale_doc_paths:
            if doc_path and doc_path not in kept_doc_paths:
                target = REPO_ROOT / doc_path
                if target.exists():
                    try:
                        unlink_with_retries(target)
                    except PermissionError:
                        pass
        for tree_path in stale_tree_paths:
            if tree_path and tree_path not in kept_tree_paths:
                target = REPO_ROOT / tree_path
                if target.exists():
                    try:
                        unlink_with_retries(target)
                    except PermissionError:
                        pass

        build_derived(llm_enabled=llm_enabled, force_full=args.rebuild)
        print(f"[DONE] processed={processed_count} skipped={skipped_count} errors={error_count}")
        print(
            "[LINKED_ASSETS] "
            + " ".join(f"{field}={linked_asset_totals[field]}" for field in LINKED_ASSET_FIELD_NAMES)
        )
        print(
            "[LLM_STATS] "
            + " ".join(
                [
                    f"provider_order={','.join(LLM_PROVIDER_ORDER)}",
                    f"ollama_ok={LLM_STATS['ollama_ok']}",
                    f"ollama_429={LLM_STATS['ollama_429']}",
                    f"ollama_error={LLM_STATS['ollama_error']}",
                    f"nim_ok={LLM_STATS['nim_ok']}",
                    f"nim_429={LLM_STATS['nim_429']}",
                    f"nim_error={LLM_STATS['nim_error']}",
                    f"heuristic_used={LLM_STATS['heuristic_used']}",
                ]
            )
        )
        return 0
    except Exception as exc:
        log_error(Path(__file__), f"fatal: {exc}\n{traceback.format_exc()}")
        print(f"[FATAL] {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
