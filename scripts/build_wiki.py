import json
import re
from collections import Counter, defaultdict
from pathlib import Path

import frontmatter


def load_jsonl(path: Path) -> tuple[list[dict], int]:
    rows = []
    malformed = 0
    if not path.exists():
        return rows, malformed
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                malformed += 1
    return rows, malformed


def norm_space(value) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value).replace("\xa0", " ")).strip()


def slugify(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_") or "topic"


def format_date(value: str | None) -> str:
    return value or "undated"


def evidence_sort_key(row: dict):
    return (
        row.get("date") or "",
        row.get("score") or 0,
        row.get("doc_id") or "",
        row.get("node_id") or "",
    )


def load_topics(config_dir: Path) -> list[dict]:
    config_path = config_dir / "wiki_topics.json"
    if not config_path.exists():
        raise FileNotFoundError(f"Missing wiki topic config: {config_path}")
    payload = json.loads(config_path.read_text(encoding="utf-8"))
    if not isinstance(payload, list) or not payload:
        raise ValueError("wiki_topics.json must contain a non-empty JSON array")
    required = {"topic_id", "title", "description"}
    normalized = []
    topic_ids = set()
    for row in payload:
        if not isinstance(row, dict):
            raise ValueError("Each wiki topic must be a JSON object")
        missing = sorted(required - set(row))
        if missing:
            raise ValueError(f"Wiki topic is missing required keys: {', '.join(missing)}")
        topic_id = row["topic_id"]
        if topic_id in topic_ids:
            raise ValueError(f"Duplicate wiki topic id: {topic_id}")
        topic_ids.add(topic_id)
        normalized.append({
            "topic_id": topic_id,
            "title": row["title"],
            "description": row["description"],
            "keywords": row.get("keywords", []),
            "categories": row.get("categories", []),
            "sources": row.get("sources", []),
            "vessel_classes": row.get("vessel_classes", []),
            "commodities": row.get("commodities", []),
            "regions": row.get("regions", []),
            "signal_keys": row.get("signal_keys", []),
            "related_topics": row.get("related_topics", []),
            "min_score": row.get("min_score", 5),
            "require_section_anchor": bool(row.get("require_section_anchor", False)),
        })
    for row in normalized:
        unknown_related = sorted(topic_id for topic_id in row["related_topics"] if topic_id not in topic_ids)
        if unknown_related:
            raise ValueError(
                f"Wiki topic {row['topic_id']} references unknown related topics: {', '.join(unknown_related)}"
            )
    return normalized


def load_document_metadata(repo_root: Path, documents_manifest: Path) -> dict[str, dict]:
    documents, malformed = load_jsonl(documents_manifest)
    if malformed:
        raise ValueError(f"Malformed document manifest lines: {malformed}")

    metadata_by_doc = {}
    for row in documents:
        doc_id = row.get("doc_id")
        doc_path = row.get("doc_path")
        if not doc_id or not doc_path:
            continue
        full_path = repo_root / doc_path
        if not full_path.exists():
            continue
        post = frontmatter.load(full_path)
        metadata = dict(post.metadata)
        metadata_by_doc[doc_id] = {
            "doc_id": doc_id,
            "title": metadata.get("title"),
            "summary": metadata.get("summary"),
            "keywords": metadata.get("keywords", []),
            "themes": metadata.get("themes", []),
            "key_entities": metadata.get("key_entities", []),
            "market_tone": metadata.get("market_tone"),
            "source": metadata.get("source"),
            "category": metadata.get("category"),
            "date": metadata.get("date"),
            "vessel_classes": metadata.get("vessel_classes", []),
            "commodities": metadata.get("commodities", []),
            "regions": metadata.get("regions", []),
            "doc_path": doc_path,
            "source_path": row.get("source_path"),
            "source_url": metadata.get("source_url"),
        }
    return metadata_by_doc


def build_signal_map(signals_path: Path) -> dict[str, dict]:
    rows, _ = load_jsonl(signals_path)
    return {row.get("doc_id"): row for row in rows if row.get("doc_id")}


def build_search_text(section: dict, doc_meta: dict) -> str:
    parts = [
        doc_meta.get("title", ""),
        doc_meta.get("summary", ""),
        section.get("title", ""),
        section.get("summary", ""),
        " ".join(section.get("keywords", []) or []),
        " ".join(section.get("section_path", []) or []),
        " ".join(doc_meta.get("keywords", []) or []),
        " ".join(doc_meta.get("themes", []) or []),
        " ".join(doc_meta.get("key_entities", []) or []),
        " ".join(doc_meta.get("vessel_classes", []) or []),
        " ".join(doc_meta.get("commodities", []) or []),
        " ".join(doc_meta.get("regions", []) or []),
        doc_meta.get("source", ""),
        doc_meta.get("category", ""),
    ]
    return norm_space(" ".join(parts)).lower()


def score_evidence(topic: dict, section: dict, doc_meta: dict, signal_row: dict | None) -> tuple[int, list[str]]:
    score = 0
    matched_terms = []
    search_text = build_search_text(section, doc_meta)
    section_title = norm_space(section.get("title")).lower()
    section_path_text = norm_space(section.get("section_path_text"))
    section_search_text = norm_space(
        " ".join([
            section.get("title", ""),
            section.get("section_path_text", ""),
            section.get("summary", ""),
            " ".join(section.get("keywords", []) or []),
        ])
    ).lower()
    section_keyword_set = {term.lower() for term in section.get("keywords", []) or []}
    topic_keywords = [term.lower() for term in topic.get("keywords", [])]
    topic_sources = set(topic.get("sources", []))
    topic_categories = set(topic.get("categories", []))
    section_anchor_hits = 0

    if topic_sources and doc_meta.get("source") not in topic_sources:
        return 0, []
    if topic_categories and doc_meta.get("category") not in topic_categories:
        return 0, []

    if doc_meta.get("source") in topic_sources:
        score += 2
    if doc_meta.get("category") in topic_categories:
        score += 4

    for vessel in topic.get("vessel_classes", []):
        vessel_lower = vessel.lower()
        if vessel_lower in section_keyword_set or vessel_lower in section_title or vessel_lower in section_path_text.lower():
            score += 4
            matched_terms.append(vessel)
            section_anchor_hits += 1
        elif vessel in set(doc_meta.get("vessel_classes", [])):
            score += 1

    for commodity in topic.get("commodities", []):
        commodity_lower = commodity.lower()
        commodity_phrase = commodity_lower.replace("_", " ")
        if (
            commodity_lower in section_keyword_set
            or commodity_phrase in section_search_text
            or commodity_lower in section_title
        ):
            score += 3
            matched_terms.append(commodity)
            section_anchor_hits += 1
        elif commodity in set(doc_meta.get("commodities", [])):
            score += 1

    for region in topic.get("regions", []):
        region_lower = region.lower()
        region_phrase = region_lower.replace("_", " ")
        if (
            region_lower in section_keyword_set
            or region_phrase in section_search_text
            or region_lower in section_title
        ):
            score += 2
            matched_terms.append(region)
        elif region in set(doc_meta.get("regions", [])):
            score += 1

    for keyword in topic_keywords:
        if not keyword:
            continue
        if keyword in section_search_text:
            score += 3
            matched_terms.append(keyword)
            section_anchor_hits += 1
            if keyword == section_title:
                score += 2
        elif keyword in search_text:
            score += 2
            matched_terms.append(keyword)

    if signal_row:
        for signal_key in topic.get("signal_keys", []):
            if signal_row.get(signal_key) not in (None, "", []):
                score += 1
                matched_terms.append(signal_key)

    if topic.get("require_section_anchor") and section_anchor_hits == 0:
        return 0, []

    return score, sorted(set(matched_terms))


def select_topic_evidence(topics: list[dict], section_rows: list[dict], docs_by_id: dict[str, dict], signals_by_doc: dict[str, dict], max_rows: int = 250) -> list[dict]:
    rows_by_topic: dict[str, list[dict]] = defaultdict(list)

    for section in section_rows:
        doc_id = section.get("doc_id")
        doc_meta = docs_by_id.get(doc_id)
        if not doc_meta:
            continue
        signal_row = signals_by_doc.get(doc_id)

        for topic in topics:
            score, matched_terms = score_evidence(topic, section, doc_meta, signal_row)
            if score < topic.get("min_score", 5):
                continue

            rows_by_topic[topic["topic_id"]].append({
                "topic_id": topic["topic_id"],
                "topic_title": topic["title"],
                "doc_id": doc_id,
                "doc_title": doc_meta.get("title"),
                "source": doc_meta.get("source"),
                "category": doc_meta.get("category"),
                "date": doc_meta.get("date"),
                "doc_path": doc_meta.get("doc_path"),
                "source_path": doc_meta.get("source_path"),
                "source_url": doc_meta.get("source_url"),
                "node_id": section.get("node_id"),
                "section_title": section.get("title"),
                "section_summary": section.get("summary"),
                "section_keywords": section.get("keywords", []),
                "section_path": section.get("section_path", []),
                "section_path_text": section.get("section_path_text"),
                "page_start": section.get("page_start"),
                "page_end": section.get("page_end"),
                "market_tone": doc_meta.get("market_tone"),
                "score": score,
                "matched_terms": matched_terms,
            })

    selected_rows = []
    for topic in topics:
        topic_rows = sorted(rows_by_topic.get(topic["topic_id"], []), key=evidence_sort_key, reverse=True)
        seen_nodes = set()
        kept = []
        for row in topic_rows:
            node_id = row.get("node_id")
            if node_id in seen_nodes:
                continue
            seen_nodes.add(node_id)
            kept.append(row)
            if len(kept) >= max_rows:
                break
        selected_rows.extend(kept)

    return sorted(selected_rows, key=lambda row: (row.get("topic_id") or "", row.get("date") or "", row.get("score") or 0, row.get("doc_id") or ""), reverse=False)


def citation_label(row: dict) -> str:
    page_start = row.get("page_start")
    page_end = row.get("page_end")
    if page_start and page_end:
        pages = f"pages {page_start}-{page_end}"
    elif page_start:
        pages = f"page {page_start}"
    else:
        pages = "pages n/a"
    return f"doc_id: {row.get('doc_id')} | section_id: {row.get('node_id')} | {pages}"


def coverage_lines(topic: dict, topic_rows: list[dict]) -> list[str]:
    source_counts = Counter(row.get("source") for row in topic_rows if row.get("source"))
    category_counts = Counter(row.get("category") for row in topic_rows if row.get("category"))
    tone_counts = Counter(row.get("market_tone") for row in topic_rows if row.get("market_tone"))

    lines = []
    if source_counts:
        source_text = ", ".join(f"{source}: {count}" for source, count in source_counts.most_common())
        lines.append(f"- Source coverage: {source_text}")
    if category_counts:
        category_text = ", ".join(f"{category}: {count}" for category, count in category_counts.most_common())
        lines.append(f"- Category coverage: {category_text}")
    if tone_counts:
        tone_text = ", ".join(f"{tone}: {count}" for tone, count in tone_counts.most_common())
        lines.append(f"- Tone distribution: {tone_text}")
    if topic.get("related_topics"):
        related = ", ".join(topic["related_topics"])
        lines.append(f"- Related topics: {related}")
    return lines


def historical_pattern_lines(topic_rows: list[dict]) -> list[str]:
    dated_rows = [row for row in topic_rows if row.get("date")]
    if not dated_rows:
        return ["- Historical coverage is currently undated; the topic is still being anchored by source sections and citations."]

    years = Counter(row["date"][:4] for row in dated_rows)
    matched_terms = Counter(term for row in topic_rows for term in row.get("matched_terms", []))
    earliest = min(dated_rows, key=lambda row: row["date"])
    latest = max(dated_rows, key=lambda row: row["date"])

    lines = [
        f"- Coverage span: {earliest['date']} to {latest['date']} across {len(years)} calendar years.",
    ]
    if years:
        lines.append(f"- Most-covered years: {', '.join(f'{year}: {count}' for year, count in years.most_common(3))}")
    if matched_terms:
        lines.append(f"- Recurring evidence markers: {', '.join(term for term, _ in matched_terms.most_common(6))}")
    return lines


def summarize_topic(topic: dict, topic_rows: list[dict]) -> str:
    if not topic_rows:
        return f"{topic['title']} is configured as a knowledge topic, but no supporting evidence has been compiled yet."

    sources = sorted({row.get('source') for row in topic_rows if row.get('source')})
    categories = sorted({row.get('category') for row in topic_rows if row.get('category')})
    latest_dates = [row.get("date") for row in topic_rows if row.get("date")]
    latest_date = max(latest_dates) if latest_dates else "undated"
    term_counts = Counter(term for row in topic_rows for term in row.get("matched_terms", []))
    dominant_terms = ", ".join(term for term, _ in term_counts.most_common(5)) or "source sections"
    return (
        f"{topic['description']} The current wiki page is grounded in {len(topic_rows)} cited sections "
        f"from {len({row.get('doc_id') for row in topic_rows})} documents across {', '.join(sources) or 'the corpus'}. "
        f"Recent evidence runs through {latest_date}, with the strongest recurring markers being {dominant_terms}. "
        f"Primary coverage comes from categories such as {', '.join(categories) or 'mixed market commentary'}."
    )


def write_topic_page(topic: dict, topic_rows: list[dict], wiki_dir: Path, generated_at: str):
    wiki_dir.mkdir(parents=True, exist_ok=True)
    path = wiki_dir / f"{topic['topic_id']}.md"

    unique_docs = []
    seen_docs = set()
    for row in sorted(topic_rows, key=evidence_sort_key, reverse=True):
        doc_id = row.get("doc_id")
        if doc_id in seen_docs:
            continue
        seen_docs.add(doc_id)
        unique_docs.append(row)

    recent_rows = sorted(topic_rows, key=evidence_sort_key, reverse=True)[:8]
    top_docs = unique_docs[:8]
    metadata = {
        "topic_id": topic["topic_id"],
        "title": topic["title"],
        "page_type": "topic_wiki",
        "generated_at": generated_at,
        "evidence_count": len(topic_rows),
        "document_count": len(seen_docs),
        "sources": sorted({row.get("source") for row in topic_rows if row.get("source")}),
        "categories": sorted({row.get("category") for row in topic_rows if row.get("category")}),
        "latest_evidence_date": max((row.get("date") for row in topic_rows if row.get("date")), default=None),
        "related_topics": topic.get("related_topics", []),
    }

    lines = [
        "## Summary",
        summarize_topic(topic, topic_rows),
        "",
        "## Why It Matters",
        topic["description"],
        "",
        "## Recent Evidence",
    ]
    for row in recent_rows:
        lines.append(
            f"- {format_date(row.get('date'))} | {row.get('source')} {row.get('category')} | {row.get('section_title')}: "
            f"{norm_space(row.get('section_summary'))} [{citation_label(row)}]"
        )
    if not recent_rows:
        lines.append("- No recent evidence was found for this topic yet.")

    lines.extend(["", "## Historical Patterns"])
    lines.extend(historical_pattern_lines(topic_rows))

    lines.extend(["", "## Cross-Source View"])
    lines.extend(coverage_lines(topic, topic_rows) or ["- Cross-source coverage has not been populated yet."])

    lines.extend(["", "## Key Documents"])
    for row in top_docs:
        lines.append(
            f"- {format_date(row.get('date'))} | {row.get('doc_title')} [{citation_label(row)}]"
        )
    if not top_docs:
        lines.append("- No key documents have been selected yet.")

    lines.extend(["", "## Related Topics"])
    if topic.get("related_topics"):
        for related in topic["related_topics"]:
            lines.append(f"- {related}")
    else:
        lines.append("- None configured.")

    post = frontmatter.Post("\n".join(lines).strip() + "\n", **metadata)
    path.write_text(frontmatter.dumps(post), encoding="utf-8")


def write_index(topics: list[dict], evidence_rows: list[dict], wiki_dir: Path, generated_at: str):
    counts = Counter(row.get("topic_id") for row in evidence_rows if row.get("topic_id"))
    lines = [
        "# Shipping Topic Wiki",
        "",
        f"Generated at {generated_at}.",
        "",
        "| Topic | Evidence Rows |",
        "|---|---:|",
    ]
    for topic in topics:
        topic_id = topic["topic_id"]
        lines.append(f"| {topic['title']} | {counts.get(topic_id, 0)} |")
    (wiki_dir / "index.md").write_text("\n".join(lines).strip() + "\n", encoding="utf-8")


def build_wiki(
    repo_root: Path,
    config_dir: Path,
    wiki_dir: Path,
    documents_manifest: Path,
    section_index_path: Path,
    themes_path: Path,
    signals_path: Path,
    output_path: Path,
    generated_at: str,
    llm_enabled: bool = False,
):
    del themes_path, llm_enabled
    topics = load_topics(config_dir)
    docs_by_id = load_document_metadata(repo_root, documents_manifest)
    section_rows, malformed_sections = load_jsonl(section_index_path)
    if malformed_sections:
        raise ValueError(f"Malformed section index lines: {malformed_sections}")

    signals_by_doc = build_signal_map(signals_path)
    evidence_rows = select_topic_evidence(topics, section_rows, docs_by_id, signals_by_doc)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("", encoding="utf-8")
    for row in evidence_rows:
        with output_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")

    if wiki_dir.exists():
        for path in wiki_dir.glob("*.md"):
            path.unlink()
    wiki_dir.mkdir(parents=True, exist_ok=True)

    rows_by_topic: dict[str, list[dict]] = defaultdict(list)
    for row in evidence_rows:
        rows_by_topic[row["topic_id"]].append(row)

    for topic in topics:
        write_topic_page(topic, rows_by_topic.get(topic["topic_id"], []), wiki_dir, generated_at)

    write_index(topics, evidence_rows, wiki_dir, generated_at)
