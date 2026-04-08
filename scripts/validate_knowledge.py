import json
from pathlib import Path

import frontmatter


REPO_ROOT = Path(__file__).parent.parent
REPORTS_ROOT = REPO_ROOT / "reports"
KNOWLEDGE_ROOT = REPO_ROOT / "knowledge"
DOCS_MANIFEST = KNOWLEDGE_ROOT / "manifests" / "documents.jsonl"
SIGNALS_PATH = KNOWLEDGE_ROOT / "derived" / "signals.jsonl"


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


def load_jsonl(path: Path) -> list[dict]:
    rows = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


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


def count_chunks(documents: list[dict]):
    chunk_counts = {}
    seen_files = set()
    for row in documents:
        chunk_file = row.get("chunk_file")
        if not chunk_file or chunk_file in seen_files:
            continue
        seen_files.add(chunk_file)
        path = REPO_ROOT / chunk_file
        count = 0
        if path.exists():
            with path.open("r", encoding="utf-8") as handle:
                for index, line in enumerate(handle, start=1):
                    line = line.strip()
                    if not line:
                        continue
                    json.loads(line)
                    count += 1
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
    return chunk_counts


def validate_frontmatter(documents: list[dict]):
    bad = []
    breakwave_null_signals = 0
    for row in documents:
        doc_path = REPO_ROOT / row["doc_path"]
        post = frontmatter.load(doc_path)
        source = post.metadata.get("source")
        category = post.metadata.get("category")
        if not source or not category:
            bad.append(str(doc_path))
            continue
        if source == "breakwave":
            signals = post.metadata.get("signals", {}) or {}
            required_key = "bdryff" if category == "drybulk" else "bwetff"
            if signals.get(required_key) is None:
                breakwave_null_signals += 1
    return bad, breakwave_null_signals


def count_signal_rows():
    rows = load_jsonl(SIGNALS_PATH)
    counts = {}
    for row in rows:
        key = ("breakwave", row.get("category"))
        counts[key] = counts.get(key, 0) + 1
    return counts


def print_table(rows):
    header = f"{'Source':24} {'Files':>7} {'Processed':>10} {'Missing':>9} {'Chunks':>9} {'Signals':>8}"
    print(header)
    print("-" * len(header))
    total_files = total_processed = total_missing = total_chunks = total_signals = 0
    for label, files, processed, missing, chunks, signals in rows:
        signals_str = f"{signals}" if signals is not None else "—"
        print(f"{label:24} {files:7} {processed:10} {missing:9} {chunks:9} {signals_str:>8}")
        total_files += files
        total_processed += processed
        total_missing += missing
        total_chunks += chunks
        if signals is not None:
            total_signals += signals
    print("-" * len(header))
    print(f"{'TOTAL':24} {total_files:7} {total_processed:10} {total_missing:9} {total_chunks:9} {total_signals:8}")


def main():
    documents = load_jsonl(DOCS_MANIFEST)
    source_counts = count_source_files()
    processed_counts = count_processed_documents(documents)
    chunk_counts = count_chunks(documents)
    signal_counts = count_signal_rows()
    bad_frontmatter, breakwave_null_signals = validate_frontmatter(documents)

    rows = []
    for source, category, label in ROW_ORDER:
        files = source_counts.get((source, category), 0)
        processed = processed_counts.get((source, category), 0)
        missing = files - processed
        chunks = chunk_counts.get((source, category), 0)
        signals = signal_counts.get((source, category)) if source == "breakwave" else None
        rows.append((label, files, processed, missing, chunks, signals))

    print_table(rows)
    print()
    print(f"Frontmatter errors: {len(bad_frontmatter)}")
    print(f"Breakwave reports with null primary signal: {breakwave_null_signals}")

    if bad_frontmatter:
        print("Invalid frontmatter docs:")
        for path in bad_frontmatter[:20]:
            print(f"- {path}")


if __name__ == "__main__":
    main()
