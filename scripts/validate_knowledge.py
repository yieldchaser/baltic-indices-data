from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from urllib.parse import urlparse

import frontmatter
from bs4 import BeautifulSoup
from knowledge_hash import SOURCE_HASH_VERSION, compute_source_hash
from source_archive_utils_v2 import infer_asset_extension, is_primary_archive_html_path, looks_like_non_content_link


REPO_ROOT = Path(__file__).parent.parent
REPORTS_ROOT = REPO_ROOT / "reports"
KNOWLEDGE_ROOT = REPO_ROOT / "knowledge"
DOCS_MANIFEST = KNOWLEDGE_ROOT / "manifests" / "documents.jsonl"
SIGNALS_PATH = KNOWLEDGE_ROOT / "derived" / "signals.jsonl"
SECTION_INDEX_PATH = KNOWLEDGE_ROOT / "derived" / "section_index.jsonl"
TOPIC_EVIDENCE_PATH = KNOWLEDGE_ROOT / "derived" / "topic_evidence.jsonl"
TOPIC_CONFIG_PATH = KNOWLEDGE_ROOT / "config" / "wiki_topics.json"
WIKI_DIR = KNOWLEDGE_ROOT / "wiki"
LINT_REPORT_PATH = KNOWLEDGE_ROOT / "manifests" / "lint_report.json"
COVERAGE_REPORT_PATH = KNOWLEDGE_ROOT / "manifests" / "coverage_report.json"
HEALTH_SUMMARY_PATH = KNOWLEDGE_ROOT / "reports" / "health_summary.md"
COMPILER_VERSION = 2
# Content gate (STATUS BOARD Decision 1 — validator content-length gate).
#
# Why here: build_health_report.py grades cadence/recency only (status_from_age
# over latest dates + publishing gaps) and this validator previously checked
# emptiness for section_index/topic_config/topic_evidence/wiki/health payloads
# but NEVER chunk text — so stub captures passed green. This gate is the
# enforcement point: a firing gate counts toward `failures` (non-zero exit),
# which is what fails the health report. It does not warn.
#
# Calibration (measured 2026-09-06 in this worktree over knowledge/chunks/*.jsonl;
# length = len(chunk["text"] or ""); trailing-50 per (source, category) ordered
# by (date, chunk_id)):
#   baltic/container tail med 38, stub(<120) 50/50 (100%)
#   baltic/dry       tail med 33, stub 50/50 (100%)
#   baltic/gas       tail med 32, stub 50/50 (100%)
#   baltic/tanker    tail med 35, stub 50/50 (100%)
#   baltic/ningbo    tail med 46, stub 44/50 (88%)
#   (258/258 Baltic 2026 chunks are <120-char stubs; pre-2026 Baltic medians:
#   dry 995, gas 1033, container 983, tanker 634.)
#   Nearest non-target tails: hellenic/dry_charter med 346 stub 25/50 (50%),
#   hellenic/tanker_charter med 606 stub 24/50 (48%); all other tails med >= 511
#   except tiny ancient groups skipped by MIN_SAMPLES. Floor 120 sits ~3x above
#   the worst target median (46) and ~3x below the weakest healthy median (346);
#   stub-rate threshold 0.80 sits 30pp below the weakest target (88%) and 30pp
#   above the stubbiest healthy tail (50%).
# Boilerplate rule (Poten): 29/68 poten/tankers chunks carry the
# "Metadata only - body is JS-rendered ... not retrievable via static fetch"
# signature (trailing-50: 20/50 = 40%); zero hits anywhere else in the corpus.
# Threshold 0.30 sits 10pp below Poten and 30pp above the rest of the corpus.
# CG1 per-source median override — STATUS BOARD CG1 (reviewer prefers option 1).
#   Ningbo calibration: historical ningbo median 74 (n=961), p25 74, p75/max 358;
#   recovered 2026 tail median 216. Override 40 clears historical p25 with margin
#   while staying far below healthy output (216).
#   Backstop note (explicit): median-override alone would NOT have caught the 2026
#   stubs (stub tails med 33-46; note 46 > 40), so the stub-rate rule (88% observed
#   for ningbo, threshold 0.80) is the backstop — stub-rate and boilerplate rules
#   are UNCHANGED for ningbo; the override applies ONLY to the median rule.
#   Standing rule: if ningbo fires, fix = per-source override or capture check,
#   NEVER change the global floor (CONTENT_GATE_MEDIAN_FLOOR stays 120).
CONTENT_GATE_WINDOW = 50
CONTENT_GATE_MIN_SAMPLES = 10
CONTENT_GATE_STUB_CHARS = 120
CONTENT_GATE_MEDIAN_FLOOR = 120
CONTENT_GATE_STUB_RATE_THRESHOLD = 0.80
CONTENT_GATE_BOILERPLATE_MARKERS = ("Metadata only", "JS-rendered", "not retrievable via static fetch")
CONTENT_GATE_BOILERPLATE_RATE_THRESHOLD = 0.30
# Per-source median-floor overrides: keyed by (source, category), applied ONLY to
# the median rule. See CG1 note above for calibration / backstop / standing rule.
CONTENT_GATE_MEDIAN_FLOOR_OVERRIDES = {
    ("baltic", "ningbo"): 40,
}


def content_gate_median_floor_for(source: str, category: str) -> int:
    """Return the median floor for (source, category).

    Defaults to CONTENT_GATE_MEDIAN_FLOOR; per-source entries in
    CONTENT_GATE_MEDIAN_FLOOR_OVERRIDES win. Median rule only — callers for
    stub-rate / boilerplate must NOT use this.
    """
    return CONTENT_GATE_MEDIAN_FLOOR_OVERRIDES.get((source, category), CONTENT_GATE_MEDIAN_FLOOR)
LINKED_ASSET_SOURCES = {"baltic", "breakwave_insights", "hellenic"}
LINKED_ASSET_FIELDS = [
    "linked_assets_discovered",
    "linked_assets_mirrored",
    "linked_assets_ingested",
    "linked_assets_skipped",
    "linked_assets_failed",
]


ROW_ORDER = [
    ("breakwave", "drybulk", "breakwave/drybulk"),
    ("breakwave", "tankers", "breakwave/tankers"),
    ("baltic", "dry", "baltic/dry"),
    ("baltic", "tanker", "baltic/tanker"),
    ("baltic", "gas", "baltic/gas"),
    ("baltic", "container", "baltic/container"),
    ("baltic", "ningbo", "baltic/ningbo"),
    ("breakwave_insights", "insights", "breakwave_insights/insights"),
    ("hellenic", "dry_charter", "hellenic/dry_charter"),
    ("hellenic", "tanker_charter", "hellenic/tanker_charter"),
    ("hellenic", "iron_ore", "hellenic/iron_ore"),
    ("hellenic", "vessel_valuations", "hellenic/vessel_valuations"),
    ("hellenic", "demolition", "hellenic/demolition"),
    ("hellenic", "shipbuilding", "hellenic/shipbuilding"),
    ("book", "book", "books"),
]


def normalize_source_filter(raw_source: str | None) -> set[str] | None:
    if raw_source in (None, "", "all"):
        return None
    if raw_source == "books":
        return {"book"}
    return {raw_source}


def filter_documents_by_source(documents: list[dict], selected_sources: set[str] | None) -> list[dict]:
    if not selected_sources:
        return documents
    return [row for row in documents if row.get("source") in selected_sources]


def empty_section_index_issues():
    return {
        "row_count": 0,
        "malformed_lines": 0,
        "duplicate_node_ids": [],
        "unknown_node_ids": [],
        "missing_node_ids": [],
    }


def empty_topic_config_issues():
    return {
        "missing_config": False,
        "malformed_config": False,
        "invalid_topics": [],
        "duplicate_topic_ids": [],
        "unknown_related_topics": [],
        "topic_ids": set(),
    }


def empty_topic_evidence_issues():
    return {
        "row_count": 0,
        "malformed_lines": 0,
        "duplicate_refs": [],
        "unknown_topic_ids": [],
        "missing_doc_ids": [],
        "invalid_section_refs": [],
        "missing_topic_ids": [],
    }


def empty_wiki_page_issues():
    return {
        "missing_pages": [],
        "bad_frontmatter": [],
        "zero_evidence_pages": [],
        "missing_citation_pages": [],
        "unknown_pages": [],
        "missing_index": False,
    }


def empty_health_report_issues():
    return {
        "missing_files": [],
        "malformed_files": [],
        "invalid_payloads": [],
        "warning_count": 0,
        "high_severity_count": 0,
        "divergence_count": 0,
    }


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


def source_hash(path: Path) -> str:
    return compute_source_hash(path, REPO_ROOT)


def iter_tree_nodes(node: dict):
    yield node
    for child in node.get("children", []) or []:
        yield from iter_tree_nodes(child)


def count_source_files():
    return {
        ("breakwave", "drybulk"): len(list((REPORTS_ROOT / "drybulk").rglob("*.pdf"))),
        ("breakwave", "tankers"): len(list((REPORTS_ROOT / "tankers").rglob("*.pdf"))),
        ("baltic", "dry"): len([path for path in (REPORTS_ROOT / "baltic" / "dry").rglob("*.html") if is_primary_archive_html_path(path)]),
        ("baltic", "tanker"): len([path for path in (REPORTS_ROOT / "baltic" / "tanker").rglob("*.html") if is_primary_archive_html_path(path)]),
        ("baltic", "gas"): len([path for path in (REPORTS_ROOT / "baltic" / "gas").rglob("*.html") if is_primary_archive_html_path(path)]),
        ("baltic", "container"): len([path for path in (REPORTS_ROOT / "baltic" / "container").rglob("*.html") if is_primary_archive_html_path(path)]),
        ("baltic", "ningbo"): len([path for path in (REPORTS_ROOT / "baltic" / "ningbo").rglob("*.html") if is_primary_archive_html_path(path)]),
        ("breakwave_insights", "insights"): len([path for path in (REPORTS_ROOT / "breakwave").rglob("*.html") if is_primary_archive_html_path(path)]),
        ("hellenic", "dry_charter"): len([path for path in (REPORTS_ROOT / "hellenic" / "dry_charter").rglob("*.html") if is_primary_archive_html_path(path)]),
        ("hellenic", "tanker_charter"): len([path for path in (REPORTS_ROOT / "hellenic" / "tanker_charter").rglob("*.html") if is_primary_archive_html_path(path)]),
        ("hellenic", "iron_ore"): len([path for path in (REPORTS_ROOT / "hellenic" / "iron_ore").rglob("*.html") if is_primary_archive_html_path(path)]),
        ("hellenic", "vessel_valuations"): len([path for path in (REPORTS_ROOT / "hellenic" / "vessel_valuations").rglob("*.html") if is_primary_archive_html_path(path)]),
        ("hellenic", "demolition"): len([path for path in (REPORTS_ROOT / "hellenic" / "demolition").rglob("*.html") if is_primary_archive_html_path(path)]),
        ("hellenic", "shipbuilding"): len([path for path in (REPORTS_ROOT / "hellenic" / "shipbuilding").rglob("*.html") if is_primary_archive_html_path(path)]),
        ("book", "book"): len(list(REPORTS_ROOT.glob("*.pdf"))),
    }


def count_processed_documents(documents: list[dict]):
    counts = {}
    for row in documents:
        key = (row.get("source"), row.get("category"))
        counts[key] = counts.get(key, 0) + 1
    return counts


def validate_manifest(documents: list[dict]):
    source_counter = Counter(row.get("source_path") for row in documents if row.get("source_path"))
    doc_counter = Counter(row.get("doc_id") for row in documents if row.get("doc_id"))

    duplicate_source_paths = sorted(path for path, count in source_counter.items() if count > 1)
    duplicate_doc_ids = sorted(doc_id for doc_id, count in doc_counter.items() if count > 1)

    missing_source_files = []
    missing_doc_files = []
    missing_chunk_files = []
    hash_mismatches = []
    hash_version_drifts = []
    compiler_version_mismatches = []

    for row in documents:
        source_path = row.get("source_path")
        doc_path = row.get("doc_path")
        chunk_file = row.get("chunk_file")
        expected_hash = row.get("source_hash")
        expected_hash_version = row.get("source_hash_version")

        if row.get("compiler_version") != COMPILER_VERSION:
            compiler_version_mismatches.append(row.get("doc_id") or source_path or "unknown")

        if source_path:
            source_file = REPO_ROOT / source_path
            if not source_file.exists():
                missing_source_files.append(source_path)
            elif expected_hash:
                actual_hash = source_hash(source_file)
                if actual_hash != expected_hash:
                    if expected_hash_version and expected_hash_version == SOURCE_HASH_VERSION:
                        hash_mismatches.append(source_path)
                    else:
                        hash_version_drifts.append(
                            f"{source_path} (manifest={expected_hash_version or 'missing'}, expected={SOURCE_HASH_VERSION})"
                        )

        if doc_path and not (REPO_ROOT / doc_path).exists():
            missing_doc_files.append(doc_path)

        if chunk_file and not (REPO_ROOT / chunk_file).exists():
            missing_chunk_files.append(chunk_file)

    return {
        "duplicate_source_paths": duplicate_source_paths,
        "duplicate_doc_ids": duplicate_doc_ids,
        "missing_source_files": sorted(set(missing_source_files)),
        "missing_doc_files": sorted(set(missing_doc_files)),
        "missing_chunk_files": sorted(set(missing_chunk_files)),
        "hash_mismatches": sorted(set(hash_mismatches)),
        "hash_version_drifts": sorted(set(hash_version_drifts)),
        "compiler_version_mismatches": sorted(set(compiler_version_mismatches)),
    }


def parse_non_negative_int(value):
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    if parsed < 0:
        return None
    return parsed


def resolve_local_asset_reference(html_path: Path, ref: str) -> Path | None:
    clean = (ref or "").strip()
    if not clean:
        return None
    if looks_like_non_content_link(clean):
        return None
    if ":" in clean and not clean.startswith(("./", "../")) and not clean.startswith("/"):
        parsed_scheme = clean.split(":", 1)[0].lower()
        if parsed_scheme and parsed_scheme not in {"http", "https"}:
            return None
    clean = clean.split("#", 1)[0].split("?", 1)[0]
    if not clean:
        return None
    parsed = urlparse(clean)
    if parsed.scheme in {"http", "https"}:
        return None
    try:
        candidate = (html_path.parent / clean).resolve()
    except OSError:
        return None
    try:
        candidate.relative_to(REPO_ROOT.resolve())
    except ValueError:
        return None
    return candidate


def validate_linked_asset_coverage(documents: list[dict]):
    schema_issues = set()
    consistency_issues = set()
    unresolved_required_local = set()
    external_non_mirrored = set()
    totals = Counter()
    rows_checked = 0
    required_local_markers = ("/assets/", "/pdfs/", "/files/", "/attachments/")

    for row in documents:
        source = row.get("source")
        source_path = row.get("source_path")
        if source not in LINKED_ASSET_SOURCES or not source_path or not source_path.endswith(".html"):
            continue

        source_file = REPO_ROOT / source_path
        if not source_file.exists():
            continue
        rows_checked += 1

        parsed_fields = {}
        for field in LINKED_ASSET_FIELDS:
            parsed = parse_non_negative_int(row.get(field))
            if parsed is None:
                schema_issues.add(f"{source_path} missing-or-invalid {field}")
                parsed = 0
            parsed_fields[field] = parsed
            totals[field] += parsed

        discovered = parsed_fields["linked_assets_discovered"]
        mirrored = parsed_fields["linked_assets_mirrored"]
        ingested = parsed_fields["linked_assets_ingested"]
        skipped = parsed_fields["linked_assets_skipped"]
        failed = parsed_fields["linked_assets_failed"]
        enforce_required_local_links = mirrored > 0

        if mirrored > discovered:
            consistency_issues.add(f"{source_path} mirrored ({mirrored}) > discovered ({discovered})")
        if ingested > mirrored:
            consistency_issues.add(f"{source_path} ingested ({ingested}) > mirrored ({mirrored})")
        if skipped + failed > discovered:
            consistency_issues.add(f"{source_path} skipped+failed ({skipped + failed}) > discovered ({discovered})")

        if discovered <= 0 and mirrored <= 0 and failed <= 0:
            continue

        try:
            soup = BeautifulSoup(source_file.read_text(encoding="utf-8", errors="ignore"), "lxml")
        except OSError:
            continue
        root = soup.select_one("body > section") or soup.select_one("section") or soup.body or soup

        for tag, attr in [("a", "href"), ("img", "src"), ("iframe", "src")]:
            for node in root.find_all(tag):
                raw_ref = (node.get(attr) or "").strip()
                if not raw_ref:
                    continue
                clean_ref = raw_ref.split("#", 1)[0].split("?", 1)[0].strip()
                if not clean_ref:
                    continue
                parsed_ref = urlparse(clean_ref)
                if parsed_ref.scheme in {"http", "https"}:
                    if infer_asset_extension(clean_ref, "") or tag in {"img", "iframe"}:
                        external_non_mirrored.add(f"{source_path} -> {clean_ref}")
                    continue
                local_target = resolve_local_asset_reference(source_file, clean_ref)
                if local_target is None:
                    continue
                normalized_ref = clean_ref.replace("\\", "/")
                if not any(marker in normalized_ref for marker in required_local_markers):
                    external_non_mirrored.add(f"{source_path} -> {normalized_ref}")
                    continue
                if not local_target.exists() or not local_target.is_file():
                    if enforce_required_local_links:
                        unresolved_required_local.add(
                            f"{source_path} -> {Path(clean_ref).as_posix()}"
                        )
                    else:
                        external_non_mirrored.add(f"{source_path} -> {normalized_ref}")

    return {
        "rows_checked": rows_checked,
        "schema_issues": sorted(schema_issues),
        "consistency_issues": sorted(consistency_issues),
        "unresolved_required_local": sorted(unresolved_required_local),
        "external_non_mirrored": sorted(external_non_mirrored),
        "totals": dict(totals),
    }


def inspect_trees(documents: list[dict]):
    section_counts = {}
    section_ids_by_doc = {}
    duplicate_tree_node_ids = set()
    malformed_tree_files = set()
    missing_tree_files = set()
    tree_doc_id_mismatches = set()
    seen_node_ids = set()

    for row in documents:
        doc_id = row.get("doc_id")
        tree_path = row.get("tree_path")
        key = (row.get("source"), row.get("category"))

        if not tree_path:
            missing_tree_files.add(doc_id or row.get("source_path") or "unknown")
            section_ids_by_doc[doc_id] = set()
            continue

        full_path = REPO_ROOT / tree_path
        if not full_path.exists():
            missing_tree_files.add(tree_path)
            section_ids_by_doc[doc_id] = set()
            continue

        try:
            tree = json.loads(full_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            malformed_tree_files.add(tree_path)
            section_ids_by_doc[doc_id] = set()
            continue

        if tree.get("doc_id") != doc_id:
            tree_doc_id_mismatches.add(tree_path)

        section_ids = set()
        valid_tree = True
        for node in iter_tree_nodes(tree):
            node_id = node.get("node_id")
            if not node_id:
                malformed_tree_files.add(tree_path)
                valid_tree = False
                break
            if node_id in seen_node_ids:
                duplicate_tree_node_ids.add(node_id)
            seen_node_ids.add(node_id)
            if node.get("level") != 0:
                section_ids.add(node_id)

        if not valid_tree:
            section_ids_by_doc[doc_id] = set()
            continue

        section_ids_by_doc[doc_id] = section_ids
        section_counts[key] = section_counts.get(key, 0) + len(section_ids)

    return {
        "section_counts": section_counts,
        "section_ids_by_doc": section_ids_by_doc,
        "duplicate_tree_node_ids": sorted(duplicate_tree_node_ids),
        "malformed_tree_files": sorted(malformed_tree_files),
        "missing_tree_files": sorted(missing_tree_files),
        "tree_doc_id_mismatches": sorted(tree_doc_id_mismatches),
    }


def inspect_chunks(documents: list[dict], section_ids_by_doc: dict[str, set[str]]):
    chunk_counts = {}
    duplicate_chunk_ids = set()
    malformed_lines = 0
    missing_section_refs = set()
    invalid_section_refs = set()
    seen_files = set()
    seen_chunk_ids = set()

    for row in documents:
        chunk_file = row.get("chunk_file")
        if not chunk_file or chunk_file in seen_files:
            continue
        seen_files.add(chunk_file)
        path = REPO_ROOT / chunk_file
        count = 0
        first_source = None
        first_category = None

        if path.exists():
            with path.open("r", encoding="utf-8") as handle:
                for line_number, line in enumerate(handle, start=1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                    except json.JSONDecodeError:
                        malformed_lines += 1
                        continue

                    count += 1
                    chunk_id = obj.get("chunk_id") or f"{chunk_file}:{line_number}"
                    if chunk_id in seen_chunk_ids:
                        duplicate_chunk_ids.add(chunk_id)
                    seen_chunk_ids.add(chunk_id)

                    if first_source is None and obj.get("source") and obj.get("category"):
                        first_source = obj.get("source")
                        first_category = obj.get("category")

                    doc_id = obj.get("doc_id")
                    section_id = obj.get("section_id")
                    section_path = obj.get("section_path")
                    if not section_id or not section_path:
                        missing_section_refs.add(chunk_id)
                    elif doc_id in section_ids_by_doc and section_id not in section_ids_by_doc[doc_id]:
                        invalid_section_refs.add(chunk_id)

        if first_source and first_category:
            key = (first_source, first_category)
        elif "books.jsonl" in chunk_file:
            key = ("book", "book")
        else:
            stem = path.stem
            if stem.startswith("breakwave_insights_"):
                key = ("breakwave_insights", stem.split("breakwave_insights_", 1)[1])
            elif stem.startswith("breakwave_"):
                key = ("breakwave", stem.split("_", 1)[1])
            elif stem.startswith("baltic_"):
                key = ("baltic", stem.split("_", 1)[1])
            elif stem.startswith("hellenic_"):
                key = ("hellenic", stem.split("hellenic_", 1)[1])
            else:
                continue
        chunk_counts[key] = count

    return {
        "chunk_counts": chunk_counts,
        "duplicate_chunk_ids": sorted(duplicate_chunk_ids),
        "malformed_chunk_lines": malformed_lines,
        "missing_section_refs": sorted(missing_section_refs),
        "invalid_section_refs": sorted(invalid_section_refs),
    }


def median_of(values):
    ordered = sorted(values)
    count = len(ordered)
    if not count:
        return 0.0
    mid = count // 2
    if count % 2:
        return float(ordered[mid])
    return (ordered[mid - 1] + ordered[mid]) / 2.0


def validate_chunk_content(selected_sources: set[str] | None):
    """Failing gate over recent chunk TEXT per (source, category).

    Groups every chunk in knowledge/chunks/*.jsonl by its embedded
    (source, category), orders by (date, chunk_id), and asserts over the
    trailing CONTENT_GATE_WINDOW chunks: FAILS when the trailing median text
    length is below CONTENT_GATE_MEDIAN_FLOOR, when the stub-rate
    (len < CONTENT_GATE_STUB_CHARS) reaches CONTENT_GATE_STUB_RATE_THRESHOLD,
    or when the boilerplate-marker share reaches
    CONTENT_GATE_BOILERPLATE_RATE_THRESHOLD. Read-only; no shard writes.
    """
    groups: dict[tuple[str, str], list[tuple[str, str, str]]] = {}
    chunks_dir = KNOWLEDGE_ROOT / "chunks"
    if chunks_dir.exists():
        for path in sorted(chunks_dir.glob("*.jsonl")):
            try:
                handle = path.open("r", encoding="utf-8")
            except OSError:
                continue
            with handle:
                for line_number, line in enumerate(handle, start=1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    source = obj.get("source")
                    category = obj.get("category")
                    if not source or not category:
                        continue
                    if selected_sources and source not in selected_sources:
                        continue
                    chunk_id = obj.get("chunk_id") or f"{path.name}:{line_number}"
                    groups.setdefault((source, category), []).append(
                        (obj.get("date") or "", chunk_id, obj.get("text") or "")
                    )

    failures = []
    groups_checked = 0
    for source, category in sorted(groups):
        rows = sorted(groups[(source, category)], key=lambda row: (row[0], row[1]))
        tail = rows[-CONTENT_GATE_WINDOW:]
        if len(tail) < CONTENT_GATE_MIN_SAMPLES:
            continue
        groups_checked += 1
        lengths = [len(text) for _, _, text in tail]
        median_length = median_of(lengths)
        stub_count = sum(1 for length in lengths if length < CONTENT_GATE_STUB_CHARS)
        boilerplate_count = sum(
            1 for _, _, text in tail if any(marker in text for marker in CONTENT_GATE_BOILERPLATE_MARKERS)
        )
        stub_rate = stub_count / len(tail)
        boilerplate_rate = boilerplate_count / len(tail)
        reasons = []
        median_floor = content_gate_median_floor_for(source, category)
        if median_length < median_floor:
            reasons.append(f"median {median_length:.0f} < floor {median_floor}")
        if stub_rate >= CONTENT_GATE_STUB_RATE_THRESHOLD:
            reasons.append(
                f"stub-rate {stub_count}/{len(tail)} ({stub_rate:.0%}) >= {CONTENT_GATE_STUB_RATE_THRESHOLD:.0%}"
            )
        if boilerplate_rate >= CONTENT_GATE_BOILERPLATE_RATE_THRESHOLD:
            reasons.append(
                f"boilerplate-rate {boilerplate_count}/{len(tail)} ({boilerplate_rate:.0%})"
                f" >= {CONTENT_GATE_BOILERPLATE_RATE_THRESHOLD:.0%}"
            )
        if reasons:
            failures.append(
                f"{source}/{category}: tail={len(tail)} median={median_length:.0f}"
                f" stub={stub_count}/{len(tail)} boilerplate={boilerplate_count}/{len(tail)}"
                f" ({'; '.join(reasons)})"
            )

    return {"groups_checked": groups_checked, "failures": sorted(failures)}


def validate_frontmatter(documents: list[dict], section_ids_by_doc: dict[str, set[str]]):
    bad = []
    breakwave_null_signals = 0
    section_count_mismatches = []

    for row in documents:
        doc_path = row.get("doc_path")
        if not doc_path:
            continue
        full_path = REPO_ROOT / doc_path
        if not full_path.exists():
            continue

        post = frontmatter.load(full_path)
        source = post.metadata.get("source")
        category = post.metadata.get("category")
        if not source or not category:
            bad.append(str(full_path))
            continue

        expected_sections = len(section_ids_by_doc.get(row.get("doc_id"), set()))
        if post.metadata.get("section_count") != expected_sections:
            section_count_mismatches.append(doc_path)

        if source == "breakwave":
            signals = post.metadata.get("signals", {}) or {}
            required_key = "bdryff" if category == "drybulk" else "bwetff"
            if signals.get(required_key) is None:
                breakwave_null_signals += 1

    return sorted(set(bad)), breakwave_null_signals, sorted(set(section_count_mismatches))


def validate_section_index(section_ids_by_doc: dict[str, set[str]]):
    rows, malformed_lines = load_jsonl(SECTION_INDEX_PATH)
    expected_node_ids = set()
    for node_ids in section_ids_by_doc.values():
        expected_node_ids.update(node_ids)

    seen_node_ids = set()
    duplicate_node_ids = set()
    unknown_node_ids = set()
    for row in rows:
        node_id = row.get("node_id")
        if not node_id:
            continue
        if node_id in seen_node_ids:
            duplicate_node_ids.add(node_id)
        seen_node_ids.add(node_id)
        if node_id not in expected_node_ids:
            unknown_node_ids.add(node_id)

    return {
        "row_count": len(rows),
        "malformed_lines": malformed_lines,
        "duplicate_node_ids": sorted(duplicate_node_ids),
        "unknown_node_ids": sorted(unknown_node_ids),
        "missing_node_ids": sorted(expected_node_ids - seen_node_ids),
    }


def validate_topic_config():
    if not TOPIC_CONFIG_PATH.exists():
        return {
            "missing_config": True,
            "malformed_config": False,
            "invalid_topics": ["missing wiki topic config"],
            "duplicate_topic_ids": [],
            "unknown_related_topics": [],
            "topic_ids": [],
        }

    try:
        payload = json.loads(TOPIC_CONFIG_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {
            "missing_config": False,
            "malformed_config": True,
            "invalid_topics": ["malformed wiki topic config"],
            "duplicate_topic_ids": [],
            "unknown_related_topics": [],
            "topic_ids": [],
        }

    if not isinstance(payload, list) or not payload:
        return {
            "missing_config": False,
            "malformed_config": True,
            "invalid_topics": ["wiki topic config must be a non-empty JSON array"],
            "duplicate_topic_ids": [],
            "unknown_related_topics": [],
            "topic_ids": [],
        }

    required = {"topic_id", "title", "description"}
    topic_ids = []
    duplicate_topic_ids = set()
    invalid_topics = set()
    for row in payload:
        if not isinstance(row, dict):
            invalid_topics.add("topic rows must be JSON objects")
            continue
        missing = sorted(required - set(row))
        if missing:
            invalid_topics.add(f"{row.get('topic_id') or 'unknown'} missing {', '.join(missing)}")
        topic_id = row.get("topic_id")
        if not topic_id:
            invalid_topics.add("topic_id is required")
            continue
        if topic_id in topic_ids:
            duplicate_topic_ids.add(topic_id)
        topic_ids.append(topic_id)

    topic_id_set = set(topic_ids)
    unknown_related_topics = set()
    for row in payload:
        if not isinstance(row, dict):
            continue
        topic_id = row.get("topic_id") or "unknown"
        for related in row.get("related_topics", []) or []:
            if related not in topic_id_set:
                unknown_related_topics.add(f"{topic_id} -> {related}")

    return {
        "missing_config": False,
        "malformed_config": False,
        "invalid_topics": sorted(invalid_topics),
        "duplicate_topic_ids": sorted(duplicate_topic_ids),
        "unknown_related_topics": sorted(unknown_related_topics),
        "topic_ids": sorted(topic_id_set),
    }


def validate_topic_evidence(topic_ids: list[str], section_ids_by_doc: dict[str, set[str]], known_doc_ids: set[str]):
    rows, malformed_lines = load_jsonl(TOPIC_EVIDENCE_PATH)
    duplicate_refs = set()
    unknown_topic_ids = set()
    missing_doc_ids = set()
    invalid_section_refs = set()
    topic_counts = Counter()
    seen_refs = set()

    for row in rows:
        topic_id = row.get("topic_id")
        doc_id = row.get("doc_id")
        node_id = row.get("node_id")
        ref_key = (topic_id, doc_id, node_id)
        if ref_key in seen_refs:
            duplicate_refs.add("|".join(part or "missing" for part in ref_key))
        seen_refs.add(ref_key)

        if not topic_id or topic_id not in topic_ids:
            unknown_topic_ids.add(topic_id or "missing_topic_id")
        else:
            topic_counts[topic_id] += 1

        if not doc_id or doc_id not in known_doc_ids:
            missing_doc_ids.add(doc_id or "missing_doc_id")
            continue

        if not node_id or node_id not in section_ids_by_doc.get(doc_id, set()):
            invalid_section_refs.add(f"{topic_id or 'missing_topic'}|{doc_id}|{node_id or 'missing_node'}")

    return {
        "row_count": len(rows),
        "malformed_lines": malformed_lines,
        "duplicate_refs": sorted(duplicate_refs),
        "unknown_topic_ids": sorted(unknown_topic_ids),
        "missing_doc_ids": sorted(missing_doc_ids),
        "invalid_section_refs": sorted(invalid_section_refs),
        "missing_topic_ids": sorted(topic_id for topic_id in topic_ids if topic_counts.get(topic_id, 0) == 0),
    }


def validate_wiki_pages(topic_ids: list[str]):
    missing_pages = []
    bad_frontmatter = []
    zero_evidence_pages = []
    missing_citation_pages = []
    unknown_pages = []
    missing_index = not (WIKI_DIR / "index.md").exists()

    if WIKI_DIR.exists():
        for path in WIKI_DIR.glob("*.md"):
            if path.name == "index.md":
                continue
            if path.stem not in topic_ids:
                unknown_pages.append(path.relative_to(REPO_ROOT).as_posix())

    for topic_id in topic_ids:
        path = WIKI_DIR / f"{topic_id}.md"
        rel = path.relative_to(REPO_ROOT).as_posix()
        if not path.exists():
            missing_pages.append(rel)
            continue
        try:
            post = frontmatter.load(path)
        except Exception:
            bad_frontmatter.append(rel)
            continue

        if post.metadata.get("topic_id") != topic_id or post.metadata.get("page_type") != "topic_wiki":
            bad_frontmatter.append(rel)
        if (post.metadata.get("evidence_count") or 0) <= 0 or (post.metadata.get("document_count") or 0) <= 0:
            zero_evidence_pages.append(rel)
        if "doc_id:" not in post.content or "section_id:" not in post.content:
            missing_citation_pages.append(rel)

    return {
        "missing_pages": sorted(missing_pages),
        "bad_frontmatter": sorted(set(bad_frontmatter)),
        "zero_evidence_pages": sorted(set(zero_evidence_pages)),
        "missing_citation_pages": sorted(set(missing_citation_pages)),
        "unknown_pages": sorted(set(unknown_pages)),
        "missing_index": missing_index,
    }


def validate_health_outputs(topic_ids: list[str]):
    missing_files = []
    malformed_files = []
    invalid_payloads = []
    warning_count = 0
    high_severity_count = 0
    divergence_count = 0

    lint_report = None
    coverage_report = None

    if not LINT_REPORT_PATH.exists():
        missing_files.append(LINT_REPORT_PATH.relative_to(REPO_ROOT).as_posix())
    else:
        try:
            lint_report = json.loads(LINT_REPORT_PATH.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            malformed_files.append(LINT_REPORT_PATH.relative_to(REPO_ROOT).as_posix())

    if not COVERAGE_REPORT_PATH.exists():
        missing_files.append(COVERAGE_REPORT_PATH.relative_to(REPO_ROOT).as_posix())
    else:
        try:
            coverage_report = json.loads(COVERAGE_REPORT_PATH.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            malformed_files.append(COVERAGE_REPORT_PATH.relative_to(REPO_ROOT).as_posix())

    if not HEALTH_SUMMARY_PATH.exists():
        missing_files.append(HEALTH_SUMMARY_PATH.relative_to(REPO_ROOT).as_posix())
    else:
        try:
            summary = frontmatter.load(HEALTH_SUMMARY_PATH)
            if summary.metadata.get("page_type") != "knowledge_health_summary":
                invalid_payloads.append(HEALTH_SUMMARY_PATH.relative_to(REPO_ROOT).as_posix())
        except Exception:
            malformed_files.append(HEALTH_SUMMARY_PATH.relative_to(REPO_ROOT).as_posix())

    if lint_report is not None:
        for key in ["generated_at", "current_date", "warning_count", "warnings", "status_counts"]:
            if key not in lint_report:
                invalid_payloads.append(f"lint_report missing {key}")
        warning_count = int(lint_report.get("warning_count") or 0)
        high_severity_count = int(lint_report.get("high_severity_count") or 0)
        warnings = lint_report.get("warnings", [])
        if not isinstance(warnings, list):
            invalid_payloads.append("lint_report warnings must be a list")
        elif warning_count != len(warnings):
            invalid_payloads.append("lint_report warning_count does not match warnings length")
        else:
            for row in warnings:
                if not isinstance(row, dict):
                    invalid_payloads.append("lint_report warning rows must be objects")
                    break
                for key in ["severity", "kind", "key", "message"]:
                    if key not in row:
                        invalid_payloads.append(f"lint_report warning missing {key}")
                        break

    if coverage_report is not None:
        for key in ["generated_at", "current_date", "corpus", "sources", "topics", "divergences"]:
            if key not in coverage_report:
                invalid_payloads.append(f"coverage_report missing {key}")
        topics = coverage_report.get("topics", [])
        divergences = coverage_report.get("divergences", [])
        divergence_count = len(divergences) if isinstance(divergences, list) else 0
        if not isinstance(topics, list):
            invalid_payloads.append("coverage_report topics must be a list")
        else:
            topic_id_set = {row.get("topic_id") for row in topics if isinstance(row, dict)}
            if topic_id_set != set(topic_ids):
                invalid_payloads.append("coverage_report topic ids do not match wiki topic config")
        corpus = coverage_report.get("corpus", {})
        if isinstance(corpus, dict):
            expected_wiki_count = len(topic_ids)
            if corpus.get("topic_count") != len(topic_ids):
                invalid_payloads.append("coverage_report topic_count is inconsistent")
            if corpus.get("wiki_page_count") != expected_wiki_count:
                invalid_payloads.append("coverage_report wiki_page_count is inconsistent")
        else:
            invalid_payloads.append("coverage_report corpus must be an object")

    return {
        "missing_files": sorted(set(missing_files)),
        "malformed_files": sorted(set(malformed_files)),
        "invalid_payloads": sorted(set(invalid_payloads)),
        "warning_count": warning_count,
        "high_severity_count": high_severity_count,
        "divergence_count": divergence_count,
    }


def count_signal_rows():
    rows, malformed = load_jsonl(SIGNALS_PATH)
    counts = {}
    for row in rows:
        key = ("breakwave", row.get("category"))
        counts[key] = counts.get(key, 0) + 1
    return counts, malformed


def print_table(rows):
    header = f"{'Source':24} {'Files':>7} {'Processed':>10} {'Missing':>9} {'Chunks':>9} {'Signals':>8}"
    print(header)
    print("-" * len(header))
    total_files = total_processed = total_missing = total_chunks = total_signals = 0
    for label, files, processed, missing, chunks, signals in rows:
        signals_str = f"{signals}" if signals is not None else "-"
        print(f"{label:24} {files:7} {processed:10} {missing:9} {chunks:9} {signals_str:>8}")
        total_files += files
        total_processed += processed
        total_missing += missing
        total_chunks += chunks
        if signals is not None:
            total_signals += signals
    print("-" * len(header))
    print(f"{'TOTAL':24} {total_files:7} {total_processed:10} {total_missing:9} {total_chunks:9} {total_signals:8}")


def print_sample(title: str, values: list[str], limit: int = 20):
    if not values:
        return
    print(title)
    for value in values[:limit]:
        print(f"- {value}")


def main():
    parser = argparse.ArgumentParser(description="Validate shipping knowledge artifacts")
    parser.add_argument(
        "--source",
        choices=["breakwave", "baltic", "breakwave_insights", "hellenic", "books", "all"],
        default="all",
    )
    args = parser.parse_args()

    selected_sources = normalize_source_filter(args.source)
    scoped_mode = bool(selected_sources)

    all_documents, malformed_manifest_lines = load_jsonl(DOCS_MANIFEST)
    documents = filter_documents_by_source(all_documents, selected_sources)

    source_counts = count_source_files()
    if selected_sources:
        source_counts = {key: value for key, value in source_counts.items() if key[0] in selected_sources}

    processed_counts = count_processed_documents(documents)
    manifest_issues = validate_manifest(documents)
    tree_issues = inspect_trees(documents)
    chunk_issues = inspect_chunks(documents, tree_issues["section_ids_by_doc"])
    signal_counts, malformed_signal_lines = count_signal_rows()
    if selected_sources:
        signal_counts = {key: value for key, value in signal_counts.items() if key[0] in selected_sources}

    if scoped_mode:
        section_index_issues = empty_section_index_issues()
        topic_config_issues = empty_topic_config_issues()
        topic_evidence_issues = empty_topic_evidence_issues()
        wiki_page_issues = empty_wiki_page_issues()
        health_report_issues = empty_health_report_issues()
    else:
        section_index_issues = validate_section_index(tree_issues["section_ids_by_doc"])
        topic_config_issues = validate_topic_config()
        topic_evidence_issues = validate_topic_evidence(
            topic_config_issues["topic_ids"],
            tree_issues["section_ids_by_doc"],
            {row.get("doc_id") for row in documents if row.get("doc_id")},
        )
        wiki_page_issues = validate_wiki_pages(topic_config_issues["topic_ids"])
        health_report_issues = validate_health_outputs(topic_config_issues["topic_ids"])

    linked_asset_issues = validate_linked_asset_coverage(documents)
    content_gate = validate_chunk_content(selected_sources)
    bad_frontmatter, breakwave_null_signals, section_count_mismatches = validate_frontmatter(
        documents,
        tree_issues["section_ids_by_doc"],
    )

    row_order = [row for row in ROW_ORDER if not selected_sources or row[0] in selected_sources]
    rows = []
    coverage_gaps = []
    total_missing = 0
    for source, category, label in row_order:
        files = source_counts.get((source, category), 0)
        processed = processed_counts.get((source, category), 0)
        missing = files - processed
        total_missing += missing
        if missing > 0:
            coverage_gaps.append(f"{label} (files={files}, processed={processed}, missing={missing})")
        chunks = chunk_issues["chunk_counts"].get((source, category), 0)
        signals = signal_counts.get((source, category)) if source == "breakwave" else None
        rows.append((label, files, processed, missing, chunks, signals))

    scope_label = "all sources" if not selected_sources else ", ".join(sorted(selected_sources))
    print(f"Validation scope: {scope_label}")
    if scoped_mode:
        print("Scoped mode: global wiki/health/topic cross-source checks are skipped for this run.")
    print_table(rows)
    print()
    print(f"Malformed manifest lines: {malformed_manifest_lines}")
    print(f"Malformed chunk lines: {chunk_issues['malformed_chunk_lines']}")
    print(f"Malformed signal lines: {malformed_signal_lines}")
    print(f"Malformed tree files: {len(tree_issues['malformed_tree_files'])}")
    print(f"Missing tree files: {len(tree_issues['missing_tree_files'])}")
    print(f"Tree doc id mismatches: {len(tree_issues['tree_doc_id_mismatches'])}")
    print(f"Duplicate source paths: {len(manifest_issues['duplicate_source_paths'])}")
    print(f"Duplicate doc ids: {len(manifest_issues['duplicate_doc_ids'])}")
    print(f"Duplicate chunk ids: {len(chunk_issues['duplicate_chunk_ids'])}")
    print(f"Duplicate tree node ids: {len(tree_issues['duplicate_tree_node_ids'])}")
    print(f"Missing source files in manifest: {len(manifest_issues['missing_source_files'])}")
    print(f"Missing generated docs in manifest: {len(manifest_issues['missing_doc_files'])}")
    print(f"Missing chunk files in manifest: {len(manifest_issues['missing_chunk_files'])}")
    print(f"Source hash mismatches: {len(manifest_issues['hash_mismatches'])}")
    print(f"Source hash version drifts: {len(manifest_issues['hash_version_drifts'])}")
    print(f"Compiler version mismatches: {len(manifest_issues['compiler_version_mismatches'])}")
    print(f"Chunks missing section refs: {len(chunk_issues['missing_section_refs'])}")
    print(f"Chunks with invalid section refs: {len(chunk_issues['invalid_section_refs'])}")
    print(f"Section index rows: {section_index_issues['row_count']}")
    print(f"Malformed section index lines: {section_index_issues['malformed_lines']}")
    print(f"Duplicate section index node ids: {len(section_index_issues['duplicate_node_ids'])}")
    print(f"Unknown section index node ids: {len(section_index_issues['unknown_node_ids'])}")
    print(f"Missing section index node ids: {len(section_index_issues['missing_node_ids'])}")
    print(f"Topic config missing: {int(topic_config_issues['missing_config'])}")
    print(f"Topic config malformed: {int(topic_config_issues['malformed_config'])}")
    print(f"Invalid topic config rows: {len(topic_config_issues['invalid_topics'])}")
    print(f"Duplicate wiki topic ids: {len(topic_config_issues['duplicate_topic_ids'])}")
    print(f"Unknown related wiki topics: {len(topic_config_issues['unknown_related_topics'])}")
    print(f"Topic evidence rows: {topic_evidence_issues['row_count']}")
    print(f"Malformed topic evidence lines: {topic_evidence_issues['malformed_lines']}")
    print(f"Duplicate topic evidence refs: {len(topic_evidence_issues['duplicate_refs'])}")
    print(f"Unknown topic ids in evidence: {len(topic_evidence_issues['unknown_topic_ids'])}")
    print(f"Topic evidence rows with missing docs: {len(topic_evidence_issues['missing_doc_ids'])}")
    print(f"Topic evidence rows with invalid section refs: {len(topic_evidence_issues['invalid_section_refs'])}")
    print(f"Configured topics missing evidence: {len(topic_evidence_issues['missing_topic_ids'])}")
    print(f"Missing wiki pages: {len(wiki_page_issues['missing_pages'])}")
    print(f"Wiki pages with bad frontmatter: {len(wiki_page_issues['bad_frontmatter'])}")
    print(f"Wiki pages with zero evidence: {len(wiki_page_issues['zero_evidence_pages'])}")
    print(f"Wiki pages missing citations: {len(wiki_page_issues['missing_citation_pages'])}")
    print(f"Unknown wiki pages: {len(wiki_page_issues['unknown_pages'])}")
    print(f"Missing wiki index: {int(wiki_page_issues['missing_index'])}")
    print(f"Missing health outputs: {len(health_report_issues['missing_files'])}")
    print(f"Malformed health outputs: {len(health_report_issues['malformed_files'])}")
    print(f"Invalid health payloads: {len(health_report_issues['invalid_payloads'])}")
    print(f"Knowledge health warnings: {health_report_issues['warning_count']}")
    print(f"High-severity health warnings: {health_report_issues['high_severity_count']}")
    print(f"Cross-source divergence flags: {health_report_issues['divergence_count']}")
    print(f"Linked-asset rows checked: {linked_asset_issues['rows_checked']}")
    print(f"Linked assets discovered: {linked_asset_issues['totals'].get('linked_assets_discovered', 0)}")
    print(f"Linked assets mirrored: {linked_asset_issues['totals'].get('linked_assets_mirrored', 0)}")
    print(f"Linked assets ingested: {linked_asset_issues['totals'].get('linked_assets_ingested', 0)}")
    print(f"Linked assets skipped: {linked_asset_issues['totals'].get('linked_assets_skipped', 0)}")
    print(f"Linked assets failed: {linked_asset_issues['totals'].get('linked_assets_failed', 0)}")
    print(f"Linked-asset schema issues: {len(linked_asset_issues['schema_issues'])}")
    print(f"Linked-asset consistency issues: {len(linked_asset_issues['consistency_issues'])}")
    print(f"Unresolved required local linked assets: {len(linked_asset_issues['unresolved_required_local'])}")
    print(f"External linked assets not mirrored (warnings): {len(linked_asset_issues['external_non_mirrored'])}")
    print(f"Frontmatter errors: {len(bad_frontmatter)}")
    print(f"Frontmatter section-count mismatches: {len(section_count_mismatches)}")
    print(f"Breakwave reports with null primary signal: {breakwave_null_signals}")
    print(f"Content-gate groups checked: {content_gate['groups_checked']}")
    print(f"Content-gate failures: {len(content_gate['failures'])}")

    global_failures = 0
    if not scoped_mode:
        global_failures = (
            section_index_issues["malformed_lines"]
            + len(section_index_issues["duplicate_node_ids"])
            + len(section_index_issues["unknown_node_ids"])
            + len(section_index_issues["missing_node_ids"])
            + int(topic_config_issues["missing_config"])
            + int(topic_config_issues["malformed_config"])
            + len(topic_config_issues["invalid_topics"])
            + len(topic_config_issues["duplicate_topic_ids"])
            + len(topic_config_issues["unknown_related_topics"])
            + topic_evidence_issues["malformed_lines"]
            + len(topic_evidence_issues["duplicate_refs"])
            + len(topic_evidence_issues["unknown_topic_ids"])
            + len(topic_evidence_issues["missing_doc_ids"])
            + len(topic_evidence_issues["invalid_section_refs"])
            + len(topic_evidence_issues["missing_topic_ids"])
            + len(wiki_page_issues["missing_pages"])
            + len(wiki_page_issues["bad_frontmatter"])
            + len(wiki_page_issues["zero_evidence_pages"])
            + len(wiki_page_issues["missing_citation_pages"])
            + len(wiki_page_issues["unknown_pages"])
            + int(wiki_page_issues["missing_index"])
            + len(health_report_issues["missing_files"])
            + len(health_report_issues["malformed_files"])
            + len(health_report_issues["invalid_payloads"])
        )

    coverage_failures = total_missing if not scoped_mode else 0

    failures = (
        malformed_manifest_lines
        + chunk_issues["malformed_chunk_lines"]
        + malformed_signal_lines
        + len(tree_issues["malformed_tree_files"])
        + len(tree_issues["missing_tree_files"])
        + len(tree_issues["tree_doc_id_mismatches"])
        + len(manifest_issues["duplicate_source_paths"])
        + len(manifest_issues["duplicate_doc_ids"])
        + len(chunk_issues["duplicate_chunk_ids"])
        + len(tree_issues["duplicate_tree_node_ids"])
        + len(manifest_issues["missing_source_files"])
        + len(manifest_issues["missing_doc_files"])
        + len(manifest_issues["missing_chunk_files"])
        + len(manifest_issues["hash_mismatches"])
        + len(manifest_issues["compiler_version_mismatches"])
        + len(chunk_issues["missing_section_refs"])
        + len(chunk_issues["invalid_section_refs"])
        + len(linked_asset_issues["schema_issues"])
        + len(linked_asset_issues["consistency_issues"])
        + len(linked_asset_issues["unresolved_required_local"])
        + len(bad_frontmatter)
        + len(section_count_mismatches)
        + breakwave_null_signals
        + len(content_gate["failures"])
        + coverage_failures
        + global_failures
    )

    if failures:
        print_sample("Duplicate source paths:", manifest_issues["duplicate_source_paths"])
        print_sample("Duplicate doc ids:", manifest_issues["duplicate_doc_ids"])
        print_sample("Duplicate chunk ids:", chunk_issues["duplicate_chunk_ids"])
        print_sample("Duplicate tree node ids:", tree_issues["duplicate_tree_node_ids"])
        print_sample("Malformed tree files:", tree_issues["malformed_tree_files"])
        print_sample("Missing tree files:", tree_issues["missing_tree_files"])
        print_sample("Tree doc id mismatches:", tree_issues["tree_doc_id_mismatches"])
        print_sample("Source hash mismatches:", manifest_issues["hash_mismatches"])
        print_sample("Source hash version drifts:", manifest_issues["hash_version_drifts"])
        print_sample("Compiler version mismatches:", manifest_issues["compiler_version_mismatches"])
        print_sample("Chunks missing section refs:", chunk_issues["missing_section_refs"])
        print_sample("Chunks with invalid section refs:", chunk_issues["invalid_section_refs"])
        if not scoped_mode:
            print_sample("Unknown section index node ids:", section_index_issues["unknown_node_ids"])
            print_sample("Missing section index node ids:", section_index_issues["missing_node_ids"])
            print_sample("Invalid wiki topic config rows:", topic_config_issues["invalid_topics"])
            print_sample("Duplicate wiki topic ids:", topic_config_issues["duplicate_topic_ids"])
            print_sample("Unknown related wiki topics:", topic_config_issues["unknown_related_topics"])
            print_sample("Duplicate topic evidence refs:", topic_evidence_issues["duplicate_refs"])
            print_sample("Unknown topic ids in evidence:", topic_evidence_issues["unknown_topic_ids"])
            print_sample("Topic evidence rows with missing docs:", topic_evidence_issues["missing_doc_ids"])
            print_sample("Topic evidence rows with invalid section refs:", topic_evidence_issues["invalid_section_refs"])
            print_sample("Configured topics missing evidence:", topic_evidence_issues["missing_topic_ids"])
            print_sample("Missing wiki pages:", wiki_page_issues["missing_pages"])
            print_sample("Wiki pages with bad frontmatter:", wiki_page_issues["bad_frontmatter"])
            print_sample("Wiki pages with zero evidence:", wiki_page_issues["zero_evidence_pages"])
            print_sample("Wiki pages missing citations:", wiki_page_issues["missing_citation_pages"])
            print_sample("Unknown wiki pages:", wiki_page_issues["unknown_pages"])
            print_sample("Missing health outputs:", health_report_issues["missing_files"])
            print_sample("Malformed health outputs:", health_report_issues["malformed_files"])
            print_sample("Invalid health payloads:", health_report_issues["invalid_payloads"])
        print_sample("Linked-asset schema issues:", linked_asset_issues["schema_issues"])
        print_sample("Linked-asset consistency issues:", linked_asset_issues["consistency_issues"])
        print_sample("Unresolved required local linked assets:", linked_asset_issues["unresolved_required_local"])
        print_sample("Invalid frontmatter docs:", bad_frontmatter)
        print_sample("Frontmatter section-count mismatches:", section_count_mismatches)
        print_sample("Chunk content-gate failures:", content_gate["failures"])
        if not scoped_mode:
            print_sample("Source coverage gaps:", coverage_gaps)
        return 1

    print_sample("Source hash version drifts (non-fatal):", manifest_issues["hash_version_drifts"])
    print_sample("External linked assets not mirrored (non-fatal):", linked_asset_issues["external_non_mirrored"])
    if scoped_mode:
        print_sample("Source coverage gaps (scoped non-fatal):", coverage_gaps)

    print("Validation status: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
