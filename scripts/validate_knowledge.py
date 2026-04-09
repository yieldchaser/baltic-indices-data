import hashlib
import json
from collections import Counter
from pathlib import Path

import frontmatter


REPO_ROOT = Path(__file__).parent.parent
REPORTS_ROOT = REPO_ROOT / "reports"
KNOWLEDGE_ROOT = REPO_ROOT / "knowledge"
DOCS_MANIFEST = KNOWLEDGE_ROOT / "manifests" / "documents.jsonl"
SIGNALS_PATH = KNOWLEDGE_ROOT / "derived" / "signals.jsonl"
SECTION_INDEX_PATH = KNOWLEDGE_ROOT / "derived" / "section_index.jsonl"
COMPILER_VERSION = 2


ROW_ORDER = [
    ("breakwave", "drybulk", "breakwave/drybulk"),
    ("breakwave", "tankers", "breakwave/tankers"),
    ("baltic", "dry", "baltic/dry"),
    ("baltic", "tanker", "baltic/tanker"),
    ("baltic", "gas", "baltic/gas"),
    ("baltic", "container", "baltic/container"),
    ("baltic", "ningbo", "baltic/ningbo"),
    ("book", "book", "books"),
]


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
    stat = path.stat()
    payload = f"{path.relative_to(REPO_ROOT).as_posix()}:{stat.st_size}:{int(stat.st_mtime)}"
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def iter_tree_nodes(node: dict):
    yield node
    for child in node.get("children", []) or []:
        yield from iter_tree_nodes(child)


def count_source_files():
    return {
        ("breakwave", "drybulk"): len(list((REPORTS_ROOT / "drybulk").rglob("*.pdf"))),
        ("breakwave", "tankers"): len(list((REPORTS_ROOT / "tankers").rglob("*.pdf"))),
        ("baltic", "dry"): len(list((REPORTS_ROOT / "baltic" / "dry").rglob("*.html"))),
        ("baltic", "tanker"): len(list((REPORTS_ROOT / "baltic" / "tanker").rglob("*.html"))),
        ("baltic", "gas"): len(list((REPORTS_ROOT / "baltic" / "gas").rglob("*.html"))),
        ("baltic", "container"): len(list((REPORTS_ROOT / "baltic" / "container").rglob("*.html"))),
        ("baltic", "ningbo"): len(list((REPORTS_ROOT / "baltic" / "ningbo").rglob("*.html"))),
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
    compiler_version_mismatches = []

    for row in documents:
        source_path = row.get("source_path")
        doc_path = row.get("doc_path")
        chunk_file = row.get("chunk_file")
        expected_hash = row.get("source_hash")

        if row.get("compiler_version") != COMPILER_VERSION:
            compiler_version_mismatches.append(row.get("doc_id") or source_path or "unknown")

        if source_path:
            source_file = REPO_ROOT / source_path
            if not source_file.exists():
                missing_source_files.append(source_path)
            elif expected_hash and source_hash(source_file) != expected_hash:
                hash_mismatches.append(source_path)

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
        "compiler_version_mismatches": sorted(set(compiler_version_mismatches)),
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

                    doc_id = obj.get("doc_id")
                    section_id = obj.get("section_id")
                    section_path = obj.get("section_path")
                    if not section_id or not section_path:
                        missing_section_refs.add(chunk_id)
                    elif doc_id in section_ids_by_doc and section_id not in section_ids_by_doc[doc_id]:
                        invalid_section_refs.add(chunk_id)

        if "books.jsonl" in chunk_file:
            key = ("book", "book")
        else:
            stem = path.stem
            if stem.startswith("breakwave_"):
                key = ("breakwave", stem.split("_", 1)[1])
            elif stem.startswith("baltic_"):
                key = ("baltic", stem.split("_", 1)[1])
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
    documents, malformed_manifest_lines = load_jsonl(DOCS_MANIFEST)
    source_counts = count_source_files()
    processed_counts = count_processed_documents(documents)
    manifest_issues = validate_manifest(documents)
    tree_issues = inspect_trees(documents)
    chunk_issues = inspect_chunks(documents, tree_issues["section_ids_by_doc"])
    signal_counts, malformed_signal_lines = count_signal_rows()
    section_index_issues = validate_section_index(tree_issues["section_ids_by_doc"])
    bad_frontmatter, breakwave_null_signals, section_count_mismatches = validate_frontmatter(
        documents,
        tree_issues["section_ids_by_doc"],
    )

    rows = []
    total_missing = 0
    for source, category, label in ROW_ORDER:
        files = source_counts.get((source, category), 0)
        processed = processed_counts.get((source, category), 0)
        missing = files - processed
        total_missing += missing
        chunks = chunk_issues["chunk_counts"].get((source, category), 0)
        signals = signal_counts.get((source, category)) if source == "breakwave" else None
        rows.append((label, files, processed, missing, chunks, signals))

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
    print(f"Compiler version mismatches: {len(manifest_issues['compiler_version_mismatches'])}")
    print(f"Chunks missing section refs: {len(chunk_issues['missing_section_refs'])}")
    print(f"Chunks with invalid section refs: {len(chunk_issues['invalid_section_refs'])}")
    print(f"Section index rows: {section_index_issues['row_count']}")
    print(f"Malformed section index lines: {section_index_issues['malformed_lines']}")
    print(f"Duplicate section index node ids: {len(section_index_issues['duplicate_node_ids'])}")
    print(f"Unknown section index node ids: {len(section_index_issues['unknown_node_ids'])}")
    print(f"Missing section index node ids: {len(section_index_issues['missing_node_ids'])}")
    print(f"Frontmatter errors: {len(bad_frontmatter)}")
    print(f"Frontmatter section-count mismatches: {len(section_count_mismatches)}")
    print(f"Breakwave reports with null primary signal: {breakwave_null_signals}")

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
        + section_index_issues["malformed_lines"]
        + len(section_index_issues["duplicate_node_ids"])
        + len(section_index_issues["unknown_node_ids"])
        + len(section_index_issues["missing_node_ids"])
        + len(bad_frontmatter)
        + len(section_count_mismatches)
        + breakwave_null_signals
        + total_missing
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
        print_sample("Compiler version mismatches:", manifest_issues["compiler_version_mismatches"])
        print_sample("Chunks missing section refs:", chunk_issues["missing_section_refs"])
        print_sample("Chunks with invalid section refs:", chunk_issues["invalid_section_refs"])
        print_sample("Unknown section index node ids:", section_index_issues["unknown_node_ids"])
        print_sample("Missing section index node ids:", section_index_issues["missing_node_ids"])
        print_sample("Invalid frontmatter docs:", bad_frontmatter)
        print_sample("Frontmatter section-count mismatches:", section_count_mismatches)
        return 1

    print("Validation status: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
