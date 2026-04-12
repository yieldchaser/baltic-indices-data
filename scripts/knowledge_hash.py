from __future__ import annotations

import hashlib
from pathlib import Path

SOURCE_HASH_VERSION = "content_sha1_v2"

TEXT_NORMALIZED_SUFFIXES = {
    ".html",
    ".htm",
    ".txt",
    ".md",
    ".csv",
    ".tsv",
    ".json",
    ".jsonl",
    ".xml",
    ".svg",
}


def _iter_chunks(path: Path):
    if path.suffix.lower() in TEXT_NORMALIZED_SUFFIXES:
        with path.open("rb") as handle:
            data = handle.read().replace(b"\r\n", b"\n").replace(b"\r", b"\n")
        for index in range(0, len(data), 1024 * 1024):
            yield data[index : index + 1024 * 1024]
        return

    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            yield chunk


def compute_source_hash(path: Path, repo_root: Path) -> str:
    digest = hashlib.sha1()
    root_resolved = repo_root.resolve()
    path_resolved = path.resolve()
    try:
        rel = path_resolved.relative_to(root_resolved).as_posix()
    except ValueError:
        rel = path_resolved.as_posix()
    digest.update(rel.encode("utf-8"))
    for chunk in _iter_chunks(path):
        digest.update(chunk)
    return digest.hexdigest()
