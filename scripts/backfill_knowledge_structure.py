import argparse
import shutil
from pathlib import Path

import frontmatter

import process_knowledge as pk


def parse_markdown_sections(body: str, metadata: dict) -> list[dict]:
    sections = []
    current_heading = None
    current_lines = []

    def flush():
        nonlocal current_heading, current_lines
        if current_heading is None:
            current_lines = []
            return
        text = "\n".join(current_lines).strip()
        heading = pk.norm_space(current_heading) or "Main"
        if heading.lower() == "summary":
            if text and not metadata.get("summary"):
                metadata["summary"] = text
            current_lines = []
            return

        section = {
            "heading": heading,
            "text": text,
        }
        if metadata.get("source") == "breakwave":
            lowered = heading.lower()
            if lowered == "overview":
                section["page_start"] = 1
                section["page_end"] = 1
                section["section_type"] = "overview"
            elif lowered == "fundamentals":
                section["page_start"] = 2
                section["page_end"] = 2
                section["section_type"] = "fundamentals"
        elif metadata.get("source") == "book":
            section["section_type"] = "chapter"

        if text:
            sections.append(section)
        current_lines = []

    for line in (body or "").splitlines():
        if line.startswith("## "):
            flush()
            current_heading = line[3:].strip()
            continue
        current_lines.append(line)
    flush()

    if sections:
        return sections

    fallback = body.strip()
    return [{"heading": "Main", "text": fallback}] if fallback else []


def main():
    parser = argparse.ArgumentParser(description="Backfill section trees and section-aware chunks from existing markdown docs")
    parser.add_argument("--snapshot-root", required=True, help="Path to a clean repo snapshot containing the committed knowledge corpus")
    args = parser.parse_args()

    snapshot_root = Path(args.snapshot_root).resolve()
    snapshot_manifest = snapshot_root / "knowledge" / "manifests" / "documents.jsonl"
    if not snapshot_manifest.exists():
        raise SystemExit(f"Missing snapshot manifest: {snapshot_manifest}")

    rows = pk.load_jsonl(snapshot_manifest)
    now = pk.utc_now_iso()

    pk.ensure_layout()
    if pk.TREES_DIR.exists():
        shutil.rmtree(pk.TREES_DIR)
    if pk.CHUNKS_DIR.exists():
        shutil.rmtree(pk.CHUNKS_DIR)
    if pk.DERIVED_DIR.exists():
        shutil.rmtree(pk.DERIVED_DIR)
    pk.ensure_layout()

    chunk_rows_by_file: dict[Path, list[dict]] = {}
    manifest_rows = []

    for row in rows:
        snapshot_doc_path = snapshot_root / row["doc_path"]
        if not snapshot_doc_path.exists():
            raise FileNotFoundError(f"Missing snapshot doc: {snapshot_doc_path}")

        post = frontmatter.load(snapshot_doc_path)
        metadata = dict(post.metadata)
        sections = parse_markdown_sections(post.content, metadata)
        adapted = {
            "text": "\n\n".join(
                f"{section['heading']}\n{section['text']}" if section.get("heading") else section.get("text", "")
                for section in sections
                if section.get("text")
            ).strip(),
            "metadata": metadata,
            "sections": sections,
        }
        adapted, tree = pk.prepare_document_structure(adapted)

        output_doc_path = pk.doc_output_path(metadata)
        tree_path = pk.tree_output_path_from_doc_path(output_doc_path)
        body = pk.build_doc_body(adapted)
        pk.write_markdown_doc(output_doc_path, metadata, body)
        pk.write_tree_file(tree_path, tree)

        chunks = pk.build_chunks(adapted)
        chunk_file = pk.chunk_file_path(metadata["source"], metadata["category"])
        chunk_rows_by_file.setdefault(chunk_file, []).extend(chunks)

        source_file = pk.REPO_ROOT / metadata["source_path"]
        manifest_rows.append({
            "doc_id": metadata["doc_id"],
            "source": metadata["source"],
            "category": metadata["category"],
            "date": metadata.get("date"),
            "title": metadata["title"],
            "source_path": metadata["source_path"],
            "doc_path": pk.relpath(output_doc_path),
            "tree_path": pk.relpath(tree_path),
            "tree_node_count": sum(1 for _ in pk.iter_tree_nodes(tree)),
            "chunk_file": pk.relpath(chunk_file),
            "chunk_count": len(chunks),
            "source_hash": pk.source_hash(source_file),
            "compiler_version": pk.COMPILER_VERSION,
            "processed_at": now,
        })

    for chunk_file, chunk_rows in chunk_rows_by_file.items():
        chunk_file.parent.mkdir(parents=True, exist_ok=True)
        chunk_file.write_text("", encoding="utf-8")
        for chunk in chunk_rows:
            pk.append_jsonl(chunk_file, chunk)

    pk.write_manifest_rows(manifest_rows)
    pk.build_derived()
    print(f"[BACKFILL] docs={len(manifest_rows)} chunk_files={len(chunk_rows_by_file)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
