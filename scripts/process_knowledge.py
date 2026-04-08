import os, re, json, time, hashlib, argparse, traceback, shutil, sys, warnings
from pathlib import Path
from datetime import datetime

import pdfplumber
from bs4 import BeautifulSoup
import tiktoken
import frontmatter
from dotenv import load_dotenv
warnings.simplefilter("ignore", FutureWarning)
import google.generativeai as genai

load_dotenv()
REPO_ROOT = Path(__file__).parent.parent
REPORTS_ROOT = REPO_ROOT / "reports"
KNOWLEDGE = REPO_ROOT / "knowledge"
GEMINI_KEY = os.environ.get("GEMINI_API_KEY")
TOKENIZER = tiktoken.get_encoding("cl100k_base")

if GEMINI_KEY:
    genai.configure(api_key=GEMINI_KEY)
    GEMINI = genai.GenerativeModel("gemini-2.0-flash")
else:
    GEMINI = None

for stream_name in ("stdout", "stderr"):
    stream = getattr(sys, stream_name, None)
    if hasattr(stream, "reconfigure"):
        stream.reconfigure(encoding="utf-8", errors="replace")


DOCS_DIR = KNOWLEDGE / "docs"
CHUNKS_DIR = KNOWLEDGE / "chunks"
MANIFESTS_DIR = KNOWLEDGE / "manifests"
DERIVED_DIR = KNOWLEDGE / "derived"
DOCUMENTS_MANIFEST = MANIFESTS_DIR / "documents.jsonl"
ERRORS_MANIFEST = MANIFESTS_DIR / "errors.jsonl"
SOURCES_MANIFEST = MANIFESTS_DIR / "sources.json"
SIGNALS_DERIVED = DERIVED_DIR / "signals.jsonl"
THEMES_DERIVED = DERIVED_DIR / "themes.jsonl"
TIMELINES_DERIVED = DERIVED_DIR / "timelines.json"

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


def ensure_layout():
    for path in [KNOWLEDGE, DOCS_DIR, CHUNKS_DIR, MANIFESTS_DIR, DERIVED_DIR]:
        path.mkdir(parents=True, exist_ok=True)


def relpath(path: Path) -> str:
    return path.relative_to(REPO_ROOT).as_posix()


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
        return float(match.group(0))
    except ValueError:
        return None


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
        chunks.append(TOKENIZER.decode(tokens[start:end]).strip())
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
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def load_processed_index():
    processed_doc_ids = set()
    processed_sources = set()
    for row in load_jsonl(DOCUMENTS_MANIFEST):
        doc_id = row.get("doc_id")
        source_path = row.get("source_path")
        if doc_id:
            processed_doc_ids.add(doc_id)
        if source_path:
            processed_sources.add(source_path)
    return processed_doc_ids, processed_sources


def log_error(file_path: Path, error: str):
    append_jsonl(ERRORS_MANIFEST, {
        "file": relpath(file_path),
        "error": error,
        "ts": datetime.utcnow().isoformat(timespec="seconds") + "Z",
    })


def clear_rebuild_outputs():
    for path in [DOCS_DIR, CHUNKS_DIR, DERIVED_DIR]:
        if path.exists():
            shutil.rmtree(path)
    for path in [DOCUMENTS_MANIFEST, ERRORS_MANIFEST, SOURCES_MANIFEST]:
        if path.exists():
            path.unlink()
    ensure_layout()


def build_sources_registry():
    counts = {
        "breakwave": {
            "drybulk": len(list((REPORTS_ROOT / "drybulk").rglob("*.pdf"))),
            "tankers": len(list((REPORTS_ROOT / "tankers").rglob("*.pdf"))),
        },
        "baltic": {
            category: len(list((REPORTS_ROOT / "baltic" / category).rglob("*.html")))
            for category in ["dry", "tanker", "gas", "container", "ningbo"]
        },
        "books": {
            "book": len(list(REPORTS_ROOT.glob("*.pdf"))),
        },
    }
    payload = {
        "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "counts": counts,
        "paths": {
            "breakwave": {
                "drybulk": relpath(REPORTS_ROOT / "drybulk"),
                "tankers": relpath(REPORTS_ROOT / "tankers"),
            },
            "baltic": {
                category: relpath(REPORTS_ROOT / "baltic" / category)
                for category in ["dry", "tanker", "gas", "container", "ningbo"]
            },
            "books": {
                "book": relpath(REPORTS_ROOT),
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
        for category in ["dry", "tanker", "gas", "container", "ningbo"]:
            for path in sorted((REPORTS_ROOT / "baltic" / category).rglob("*.html")):
                yield "baltic", category, path


def default_lists_for_doc(source: str, category: str):
    vessel_defaults = {
        ("breakwave", "drybulk"): ["capesize", "panamax", "supramax"],
        ("breakwave", "tankers"): ["vlcc", "suezmax"],
        ("baltic", "dry"): ["capesize", "panamax", "supramax", "handysize"],
        ("baltic", "tanker"): ["vlcc", "suezmax", "aframax"],
        ("baltic", "gas"): ["lng", "lpg"],
        ("baltic", "container"): ["container"],
    }
    commodity_defaults = {
        ("breakwave", "drybulk"): ["iron_ore", "coal", "grain", "bauxite"],
        ("breakwave", "tankers"): ["crude_oil", "products"],
        ("baltic", "dry"): ["iron_ore", "coal", "grain"],
        ("baltic", "tanker"): ["crude_oil", "products"],
        ("baltic", "gas"): ["gas"],
        ("baltic", "container"): [],
        ("baltic", "ningbo"): [],
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


def call_gemini(prompt: str, retries: int = 3) -> str | None:
    if GEMINI is None:
        return None
    for attempt in range(retries):
        try:
            time.sleep(1.5)
            response = GEMINI.generate_content(prompt)
            text = getattr(response, "text", None)
            if text:
                return text.strip()
            return None
        except Exception:
            if attempt < retries - 1:
                time.sleep(4 * (attempt + 1))
            else:
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
    if GEMINI is None:
        return {}
    prompt = "Return strict JSON with keys: summary, keywords, themes, key_entities, market_tone"
    if signal_keys:
        prompt += ", " + ", ".join(signal_keys)
    prompt += (
        ". summary should be 2-3 sentences, keywords/themes/key_entities should be arrays, "
        "market_tone should be a short snake_case string, and missing values should be null. "
        f"Source={source}, category={category}. Text:\n{text[:6000]}"
    )
    payload = extract_json_payload(call_gemini(prompt))
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
    stat = path.stat()
    payload = f"{relpath(path)}:{stat.st_size}:{int(stat.st_mtime)}"
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def make_breakwave_doc_id(pdf_path: Path, category: str) -> str:
    date_str = pdf_path.stem.split("_")[0]
    return f"breakwave_{category}_{date_str}"


def make_baltic_doc_id(html_path: Path, category: str, fallback_date: str | None = None) -> str:
    date_part = fallback_date or "no_date"
    return f"baltic_{category}_{date_part}_{slugify(html_path.stem)}"


def make_book_doc_id(pdf_path: Path) -> str:
    return f"book_{slugify(pdf_path.stem)}"


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


def adapt_breakwave(pdf_path: Path, category: str, llm_enabled: bool) -> dict:
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

    bullet_lines = [line.lstrip("•").strip() for line in page_one_lines if line.startswith("•")]
    if not bullet_lines:
        bullet_lines = [line for line in page_one_lines if len(line.split()) > 8][:6]

    body_sections = [{"heading": "Overview", "text": "\n".join(f"- {line}" for line in bullet_lines if line)}]
    fundamentals_md = format_fundamentals_markdown(fundamentals)
    if fundamentals_md:
        body_sections.append({"heading": "Fundamentals", "text": fundamentals_md})
    full_text = "\n\n".join(section["text"] for section in body_sections if section["text"])
    vessels, regions, commodities = infer_taxonomy(full_text + "\n" + "\n".join(page_texts), "breakwave", category)

    llm_data = {}
    if llm_enabled:
        missing_signals = [key for key in signal_keys if signals.get(key) is None]
        llm_data = run_doc_llm("\n".join(page_texts), "breakwave", category, signal_keys=missing_signals if missing_signals else None)
        for key in missing_signals:
            if llm_data.get(key) is not None:
                signals[key] = llm_data.get(key)

    theme_data = heuristic_theme_payload(full_text, category)
    if llm_data:
        if isinstance(llm_data.get("themes"), list) and llm_data["themes"]:
            theme_data["themes"] = llm_data["themes"][:6]
        if isinstance(llm_data.get("key_entities"), list) and llm_data["key_entities"]:
            theme_data["key_entities"] = llm_data["key_entities"][:6]
        if llm_data.get("market_tone"):
            theme_data["market_tone"] = llm_data["market_tone"]

    summary = llm_data.get("summary") if llm_data else None
    if not summary:
        summary = heuristic_summary(" ".join(bullet_lines) or full_text)
    keywords = llm_data.get("keywords") if llm_data and isinstance(llm_data.get("keywords"), list) else extract_keywords(full_text)

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


def adapt_baltic(html_path: Path, category: str) -> dict:
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
    theme_data = heuristic_theme_payload(full_text, category)
    metadata = {
        "doc_id": make_baltic_doc_id(html_path, category, date_str),
        "source": "baltic",
        "category": category,
        "date": date_str if date_str != "unknown" else None,
        "title": title,
        "source_path": relpath(html_path),
        "source_stem": html_path.stem,
        "document_type": "weekly_roundup",
        "vessel_classes": vessels,
        "regions": regions,
        "commodities": commodities,
        "signals": {},
        "summary": heuristic_summary(full_text),
        "keywords": extract_keywords(full_text),
        "themes": theme_data["themes"],
        "key_entities": theme_data["key_entities"],
        "market_tone": theme_data["market_tone"],
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


def adapt_book(pdf_path: Path, llm_enabled: bool) -> dict:
    title = pdf_path.stem.replace("_", " ")
    sections = []
    current_heading = title
    current_buffer = []
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
                    sections.append({"heading": current_heading, "text": "\n\n".join(current_buffer).strip()})
                current_heading = heading
                current_buffer = []
            if content:
                current_buffer.append(content)
                if len(summary_seed) < 4:
                    summary_seed.append(content[:2000])

    if current_buffer:
        sections.append({"heading": current_heading, "text": "\n\n".join(current_buffer).strip()})
    if not sections:
        sections = [{"heading": title, "text": ""}]

    full_text = "\n\n".join(f"{section['heading']}\n{section['text']}" for section in sections if section["text"])
    vessels, regions, commodities = infer_taxonomy(full_text, "book", "book")
    theme_data = heuristic_theme_payload(full_text, "book")

    summary = heuristic_summary(" ".join(summary_seed) or full_text)
    if llm_enabled and summary_seed:
        llm_data = run_doc_llm("\n\n".join(summary_seed), "book", "book")
        if llm_data.get("summary"):
            summary = llm_data["summary"]
        if isinstance(llm_data.get("themes"), list) and llm_data["themes"]:
            theme_data["themes"] = llm_data["themes"][:6]
        if isinstance(llm_data.get("key_entities"), list) and llm_data["key_entities"]:
            theme_data["key_entities"] = llm_data["key_entities"][:6]
        if llm_data.get("market_tone"):
            theme_data["market_tone"] = llm_data["market_tone"]

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
        "keywords": extract_keywords(full_text),
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
    if source == "baltic" and date_str:
        filename = f"{date_str}_{source_stem}.md"
    else:
        filename = f"{date_str}.md" if date_str else f"{source_stem}.md"
    return DOCS_DIR / source / category / year / filename


def chunk_file_path(source: str, category: str) -> Path:
    if source == "book":
        return CHUNKS_DIR / "books.jsonl"
    return CHUNKS_DIR / f"{source}_{category}.jsonl"


def build_chunks(adapted: dict) -> list[dict]:
    metadata = adapted["metadata"]
    source = metadata["source"]
    category = metadata["category"]
    doc_id = metadata["doc_id"]
    date_str = metadata.get("date")
    chunks = []

    if source == "breakwave":
        texts = chunk_text(adapted["text"], 400, 50)[:3]
        sections = ["main"] * len(texts)
    elif source == "baltic":
        texts = []
        sections = []
        for section in adapted["sections"]:
            section_text = section.get("text") or ""
            if not section_text.strip():
                continue
            section_chunks = chunk_text(section_text, 600, 60) or [section_text]
            for chunk in section_chunks:
                texts.append(chunk)
                sections.append(slugify(section.get("heading") or "main"))
    else:
        texts = []
        sections = []
        for section in adapted["sections"]:
            section_text = section.get("text") or ""
            if not section_text.strip():
                continue
            for chunk in chunk_text(section_text, 500, 100) or [section_text]:
                texts.append(chunk)
                sections.append(slugify(section.get("heading") or "chapter"))

    for index, text in enumerate(texts, start=1):
        chunks.append({
            "chunk_id": f"{doc_id}_{index:03d}",
            "doc_id": doc_id,
            "source": source,
            "category": category,
            "date": date_str,
            "section": sections[index - 1],
            "text": text,
            "token_count": token_count(text),
            "keywords": extract_keywords(text),
        })
    return chunks


def write_markdown_doc(path: Path, metadata: dict, body: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    post = frontmatter.Post(body, **metadata)
    path.write_text(frontmatter.dumps(post), encoding="utf-8")


def process_file(source: str, category: str, path: Path, llm_enabled: bool):
    if source == "breakwave":
        adapted = adapt_breakwave(path, category, llm_enabled)
    elif source == "baltic":
        adapted = adapt_baltic(path, category)
    else:
        adapted = adapt_book(path, llm_enabled)

    metadata = adapted["metadata"]
    output_path = doc_output_path(metadata)
    body = build_doc_body(adapted)
    write_markdown_doc(output_path, metadata, body)

    chunks = build_chunks(adapted)
    chunk_path = chunk_file_path(metadata["source"], metadata["category"])
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
        "chunk_file": relpath(chunk_path),
        "chunk_count": len(chunks),
        "source_hash": source_hash(path),
        "processed_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }
    append_jsonl(DOCUMENTS_MANIFEST, manifest_row)
    return metadata, chunks


def build_derived():
    documents = load_jsonl(DOCUMENTS_MANIFEST)
    SIGNALS_DERIVED.parent.mkdir(parents=True, exist_ok=True)
    signal_rows = []
    theme_rows = []
    timelines = {}

    for doc in documents:
        doc_path = REPO_ROOT / doc["doc_path"]
        if not doc_path.exists():
            continue
        try:
            post = frontmatter.load(doc_path)
        except Exception:
            continue

        meta = post.metadata
        source = meta.get("source")
        category = meta.get("category")
        date_str = meta.get("date")
        doc_id = meta.get("doc_id")

        theme_rows.append({
            "doc_id": doc_id,
            "themes": meta.get("themes", []),
            "key_entities": meta.get("key_entities", []),
            "market_tone": meta.get("market_tone"),
        })

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
            signal_rows.append(row)

        if source in {"breakwave", "baltic"} and date_str:
            try:
                iso = datetime.strptime(date_str, "%Y-%m-%d").isocalendar()
                key = f"{iso.year}-W{iso.week:02d}"
                timelines.setdefault(key, {
                    "breakwave_drybulk": None,
                    "breakwave_tankers": None,
                    "baltic_dry": None,
                    "baltic_tanker": None,
                    "baltic_gas": None,
                    "baltic_container": None,
                    "baltic_ningbo": None,
                })
                if source == "breakwave":
                    timelines[key][f"breakwave_{category}"] = doc_id
                else:
                    timelines[key][f"baltic_{category}"] = doc_id
            except ValueError:
                pass

    SIGNALS_DERIVED.write_text("", encoding="utf-8")
    for row in sorted(signal_rows, key=lambda item: (item.get("date") or "", item.get("doc_id") or "")):
        append_jsonl(SIGNALS_DERIVED, row)

    THEMES_DERIVED.write_text("", encoding="utf-8")
    for row in theme_rows:
        append_jsonl(THEMES_DERIVED, row)

    TIMELINES_DERIVED.write_text(json.dumps(dict(sorted(timelines.items())), indent=2, ensure_ascii=False), encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description="Shipping knowledge compiler")
    parser.add_argument("--source", choices=["breakwave", "baltic", "books", "all"], default=None)
    parser.add_argument("--rebuild", action="store_true")
    parser.add_argument("--no-llm", action="store_true")
    parser.add_argument("--derived-only", action="store_true")
    args = parser.parse_args()

    try:
        if args.rebuild:
            clear_rebuild_outputs()
        ensure_layout()
        build_sources_registry()

        if args.derived_only:
            build_derived()
            print("[DERIVED] rebuilt")
            return 0

        llm_enabled = GEMINI is not None and not args.no_llm
        _, processed_sources = load_processed_index()
        processed_count = 0
        skipped_count = 0
        error_count = 0

        for source, category, path in iter_source_files(args.source):
            source_rel = relpath(path)
            if not args.rebuild and source_rel in processed_sources:
                skipped_count += 1
                continue

            try:
                metadata, _ = process_file(source, category, path, llm_enabled)
                processed_sources.add(source_rel)
                processed_count += 1
                print(f"[{source.upper()}] [{metadata.get('date') or metadata.get('title')}] ✓")
            except Exception as exc:
                error_count += 1
                reason = f"{exc}\n{traceback.format_exc()}"
                log_error(path, reason)
                date_hint = path.stem.split("_")[0]
                print(f"[{source.upper()}] [{date_hint}] ✗ {exc}")

        build_derived()
        print(f"[DONE] processed={processed_count} skipped={skipped_count} errors={error_count}")
        return 0
    except Exception as exc:
        log_error(Path(__file__), f"fatal: {exc}\n{traceback.format_exc()}")
        print(f"[FATAL] {exc}")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
