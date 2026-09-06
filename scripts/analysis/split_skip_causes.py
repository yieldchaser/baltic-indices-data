"""Split the ledger's ``linked_assets_skipped`` queue by skip cause.

Read-only cause-split instrumentation for the P0 skipped queue owned by
muse-spark. It parses ``knowledge/manifests/documents.jsonl`` and replays the
skip logic of ``collect_linked_asset_sections`` in
``scripts/process_knowledge.py`` (lines ~2309-2399) per document, attributing
each skipped asset to exactly one of five causes. No extraction is performed:
linked-asset text is never read and no file under ``knowledge/`` is written.

The five skip causes, with source line refs in ``scripts/process_knowledge.py``::

    per_doc_cap            L2326-2327  ``len(sections) >= MAX_LINKED_ASSETS_PER_DOC``
    empty_href             L2337-2340  ``href = norm_space(candidate); if not href``
    non_content_link       L2341-2343  ``if looks_like_non_content_link(href)``
    unresolvable_external  L2346-2349  ``linked_path is None`` + http(s) scheme
    duplicate_path         L2356-2358  ``linked_rel in seen_paths``

The sibling branch of the ``linked_path is None`` fork (non-http(s) scheme,
L2350-2351) counts as *failed*, not skipped, and is therefore not a cause here.

Effective cap note: the code default is ``MAX_LINKED_ASSETS_PER_DOC = 12``
(``process_knowledge.py:87``), but both CI workflows pin ``"28"``
(``.github/workflows/process_knowledge.yml:113``,
``.github/workflows/daily_knowledge_update.yml:89``), and the committed ledger
was generated under that setting (max ``linked_assets_ingested`` per doc is 19,
impossible under a cap of 12). The default ``--max-cap 28`` reproduces the
ledger exactly (8,424 skipped / 3,167 docs, zero per-doc mismatches); replaying
with ``--max-cap 12`` prospectively re-labels 81 resolved-unique assets as
``per_doc_cap`` (total 8,505) and diverges from the ledger.

Stdlib only. Deterministic: fixed cause/source ordering, sorted JSON keys, no
timestamps. HTML is parsed with ``html.parser`` (no bs4/lxml dependency);
candidate order mirrors the original (all ``<a href>`` in document order, then
all ``<img src>``) scoped to the same root
(``body > section`` or ``section`` or ``body``).

Output: ``data/derived/skip_cause_matrix.json`` (POSIX paths only) + a human
summary on stdout. Exit status is nonzero when the replay does not reconcile
with the ledger tallies.
"""

from __future__ import annotations

import argparse
import collections
import html as html_module
import json
import os
import re
import subprocess
import sys
from html.parser import HTMLParser
from pathlib import Path, PurePosixPath
from urllib.parse import urlparse

REPO_ROOT = Path(__file__).resolve().parents[2]
DOCUMENTS_JSONL = "knowledge/manifests/documents.jsonl"
PROCESS_KNOWLEDGE = "scripts/process_knowledge.py"
MATRIX_OUT = "data/derived/skip_cause_matrix.json"

LINKED_ASSET_SOURCES = ("baltic", "breakwave_insights", "hellenic")
CAUSES = (
    "per_doc_cap",
    "empty_href",
    "non_content_link",
    "unresolvable_external",
    "duplicate_path",
)
CAUSE_LINES = {
    "per_doc_cap": "scripts/process_knowledge.py:2326-2327",
    "empty_href": "scripts/process_knowledge.py:2337-2340",
    "non_content_link": "scripts/process_knowledge.py:2341-2343",
    "unresolvable_external": "scripts/process_knowledge.py:2346-2349",
    "duplicate_path": "scripts/process_knowledge.py:2356-2358",
}
CODE_DEFAULT_MAX_CAP = 12  # scripts/process_knowledge.py:87
CI_PINNED_MAX_CAP = 28  # .github/workflows/process_knowledge.yml:113

NON_CONTENT_LINK_HINTS = (
    "mailto:",
    "javascript:",
    "facebook.com/sharer",
    "twitter.com/intent",
    "linkedin.com/share",
    "pinterest.com/pin",
    "whatsapp://",
    "t.me/share",
    "translate.google.com",
    "webcache.googleusercontent",
    "addtoany",
)

VOID_ELEMENTS = frozenset(
    "area base br col embed hr img input link meta param source track wbr".split()
)


# --- Faithful copies of the helpers used by the skip logic -------------------
# (process_knowledge.py:249-254, source_archive_utils_v2.py:106-118/160-166,
#  process_knowledge.py:1952-2012, process_knowledge.py:236-237).


def norm_space(value) -> str:
    if value is None:
        return ""
    text = str(value).replace(" ", " ").replace("​", " ")
    text = (
        text.replace("–", "-")
        .replace("—", "-")
        .replace("’", "'")
        .replace("“", '"')
        .replace("”", '"')
    )
    return re.sub(r"[ \t]+", " ", text).strip()


def repair_text(text) -> str:
    if not text:
        return ""
    value = html_module.unescape(text)
    value = value.replace(" ", " ").replace("Â ", " ").replace("Â", "")
    return re.sub(r"\s+", " ", value).strip()


def looks_like_non_content_link(asset_url: str) -> bool:
    lower = repair_text(asset_url).lower()
    if not lower:
        return True
    if lower.startswith("#") or lower.startswith("data:"):
        return True
    return any(token in lower for token in NON_CONTENT_LINK_HINTS)


def resolve_archive_link_path(html_path: Path, href: str) -> Path | None:
    repo_resolved = REPO_ROOT.resolve()
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
                continue
            try:
                candidate.relative_to(repo_resolved)
            except ValueError:
                continue
            try:
                if candidate.exists() and candidate.is_file():
                    return candidate
            except OSError:
                continue
        return None
    try:
        candidate = (html_path.parent / clean).resolve()
    except OSError:
        return None
    try:
        candidate.relative_to(repo_resolved)
    except ValueError:
        return None
    try:
        if not candidate.exists() or not candidate.is_file():
            return None
    except OSError:
        return None
    return candidate


def relpath_posix(path: Path) -> str:
    return PurePosixPath(path.relative_to(REPO_ROOT).as_posix()).as_posix()


# --- Minimal stdlib DOM: link-candidate enumeration only ----------------------


class _Node:
    __slots__ = ("tag", "attrs", "children", "parent")

    def __init__(self, tag, attrs, parent):
        self.tag = tag
        self.attrs = attrs
        self.children = []
        self.parent = parent


class _DOMBuilder(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=False)
        self.root = _Node("[document]", {}, None)
        self.current = self.root

    def handle_starttag(self, tag, attrs):
        node = _Node(tag, dict(attrs), self.current)
        self.current.children.append(node)
        if tag not in VOID_ELEMENTS:
            self.current = node

    def handle_startendtag(self, tag, attrs):
        node = _Node(tag, dict(attrs), self.current)
        self.current.children.append(node)

    def handle_endtag(self, tag):
        node = self.current
        while node is not None and node.tag != tag:
            node = node.parent
        if node is not None and node.parent is not None:
            self.current = node.parent

    def handle_data(self, data):
        pass


def _iter_descendants(node):
    for child in node.children:
        yield child
        yield from _iter_descendants(child)


def collect_link_candidates(markup: str) -> list:
    """Mirror ``collect_linked_asset_sections`` candidate enumeration.

    Root scoping replicates ``adapt_archive_html`` (``body > section`` first,
    then any ``section``, then ``body``). Order replicates the original loops:
    every ``<a href>`` in document order, then every ``<img src>``.
    ``href=True`` / ``src=True`` mean attribute-present (empty included).
    """
    builder = _DOMBuilder()
    builder.feed(markup)
    builder.close()
    doc = builder.root
    bodies = [n for n in _iter_descendants(doc) if n.tag == "body"]
    body = bodies[0] if bodies else None
    root = None
    if body is not None:
        for child in body.children:
            if child.tag == "section":
                root = child
                break
    if root is None:
        for node in _iter_descendants(doc):
            if node.tag == "section":
                root = node
                break
    if root is None:
        root = body if body is not None else doc
    candidates = []
    for node in _iter_descendants(root):
        if node.tag == "a" and "href" in node.attrs:
            candidates.append(node.attrs["href"])
    for node in _iter_descendants(root):
        if node.tag == "img" and "src" in node.attrs:
            candidates.append(node.attrs["src"])
    return candidates


def split_doc_skips(source: str, html_path: Path, candidates: list, max_cap: int):
    """Replay the five skip branches for one document's candidates.

    ``baltic`` never reaches ``collect_linked_asset_sections`` (``adapt_baltic``
    returns without linked-asset handling), so it always yields zeros here,
    matching the ledger. ``sections_len`` counts resolved-unique candidates as
    ingested: read-only analysis performs no extraction, so extract-time
    failures cannot be positioned; the ledger reconciliation check below keeps
    this assumption honest.
    """
    counts = collections.Counter()
    if source not in LINKED_ASSET_SOURCES or source == "baltic":
        return counts, 0
    seen_paths = set()
    sections_len = 0
    for candidate in candidates:
        if sections_len >= max_cap:  # L2326-2327
            counts["per_doc_cap"] += 1
            continue
        href = norm_space(candidate)
        if not href:  # L2338-2340
            counts["empty_href"] += 1
            continue
        if looks_like_non_content_link(href):  # L2341-2343
            counts["non_content_link"] += 1
            continue
        linked_path = resolve_archive_link_path(html_path, href)
        if linked_path is None:  # L2346-2352
            parsed = urlparse(href)
            if parsed.scheme in {"http", "https"}:  # L2348-2349
                counts["unresolvable_external"] += 1
            # else: failed (L2350-2351) — not a skip cause.
            continue
        linked_rel = relpath_posix(linked_path)
        if linked_rel in seen_paths:  # L2356-2358
            counts["duplicate_path"] += 1
            continue
        seen_paths.add(linked_rel)
        sections_len += 1  # read-only proxy for "ingested" (see docstring)
    return counts, sections_len


def head_hash() -> dict:
    try:
        full = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return {"full": "unknown", "short": "unknown"}
    return {"full": full, "short": full[:9] if len(full) >= 9 else full}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Attribute linked_assets_skipped to the five skip causes."
    )
    parser.add_argument(
        "--max-cap",
        type=int,
        default=None,
        help="Effective MAX_LINKED_ASSETS_PER_DOC (default: env override else 28, "
        "the CI-pinned ledger-effective value).",
    )
    args = parser.parse_args(argv)

    env_cap = os.environ.get("MAX_LINKED_ASSETS_PER_DOC", "").strip()
    if args.max_cap is not None:
        max_cap = args.max_cap
    elif env_cap:
        max_cap = int(env_cap)
    else:
        max_cap = CI_PINNED_MAX_CAP

    docs_path = REPO_ROOT / DOCUMENTS_JSONL
    with docs_path.open(encoding="utf-8") as fh:
        docs = [json.loads(line) for line in fh if line.strip()]

    ledger_skipped = collections.Counter()
    ledger_docs_with_skips = collections.Counter()
    ledger_stats = collections.Counter()
    for doc in docs:
        source = doc.get("source", "?")
        for field in (
            "discovered",
            "mirrored",
            "ingested",
            "skipped",
            "failed",
        ):
            ledger_stats[(source, field)] += doc.get("linked_assets_" + field, 0)
        skipped = doc.get("linked_assets_skipped", 0)
        ledger_skipped[source] += skipped
        if skipped > 0:
            ledger_docs_with_skips[source] += 1

    matrix = {source: {cause: 0 for cause in CAUSES} for source in LINKED_ASSET_SOURCES}
    replay_docs_with_skips = collections.Counter()
    mismatches = []
    replayed = 0
    for doc in docs:
        source = doc.get("source", "?")
        if source not in LINKED_ASSET_SOURCES:
            continue
        source_path = doc.get("source_path", "")
        html_path = REPO_ROOT / PurePosixPath(source_path).as_posix()
        candidates = []
        if source != "baltic" and html_path.is_file():
            markup = html_path.read_text(encoding="utf-8", errors="ignore")
            candidates = collect_link_candidates(markup)
        counts, _ = split_doc_skips(source, html_path, candidates, max_cap)
        replayed += 1
        replay_total = 0
        for cause in CAUSES:
            matrix[source][cause] += counts.get(cause, 0)
            replay_total += counts.get(cause, 0)
        if replay_total > 0:
            replay_docs_with_skips[source] += 1
        if replay_total != doc.get("linked_assets_skipped", 0):
            mismatches.append(
                {
                    "doc_id": doc.get("doc_id"),
                    "source": source,
                    "ledger_skipped": doc.get("linked_assets_skipped", 0),
                    "replay_skipped": replay_total,
                }
            )

    totals = {cause: sum(matrix[s][cause] for s in LINKED_ASSET_SOURCES) for cause in CAUSES}
    total_skipped = sum(totals.values())
    total_docs = sum(replay_docs_with_skips.values())
    ledger_total = sum(ledger_skipped[s] for s in LINKED_ASSET_SOURCES)
    ledger_docs = sum(ledger_docs_with_skips[s] for s in LINKED_ASSET_SOURCES)
    reconciled = (
        total_skipped == ledger_total
        and total_docs == ledger_docs
        and not mismatches
    )

    head = head_hash()
    payload = {
        "method": (
            "Read-only replay of collect_linked_asset_sections skip branches "
            "(scripts/process_knowledge.py:2309-2399) per documents.jsonl row: "
            "candidate order = <a href> in document order then <img src>, scoped to "
            "body>section|section|body; resolved-unique candidates counted as ingested "
            "for cap simulation (no extraction performed). baltic yields zeros by "
            "construction (adapt_baltic never calls the collector). No writes to "
            "knowledge/; no existing files modified."
        ),
        "inputs": {
            "documents": DOCUMENTS_JSONL,
            "code": PROCESS_KNOWLEDGE,
        },
        "output": MATRIX_OUT,
        "head": head,
        "effective_max_cap": max_cap,
        "code_default_max_cap": CODE_DEFAULT_MAX_CAP,
        "ci_pinned_max_cap": CI_PINNED_MAX_CAP,
        "cause_line_refs": dict(CAUSE_LINES),
        "per_source": {
            source: dict(matrix[source]) for source in LINKED_ASSET_SOURCES
        },
        "totals": dict(totals),
        "total_skipped": total_skipped,
        "docs_with_skips": {s: replay_docs_with_skips.get(s, 0) for s in LINKED_ASSET_SOURCES},
        "total_docs_with_skips": total_docs,
        "ledger": {
            "skipped": {s: ledger_skipped.get(s, 0) for s in LINKED_ASSET_SOURCES},
            "total_skipped": ledger_total,
            "docs_with_skips": {
                s: ledger_docs_with_skips.get(s, 0) for s in LINKED_ASSET_SOURCES
            },
            "total_docs_with_skips": ledger_docs,
        },
        "reconciled_with_ledger": reconciled,
        "mismatched_docs": mismatches,
        "docs_replayed": replayed,
    }

    out_path = REPO_ROOT / PurePosixPath(MATRIX_OUT).as_posix()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="\n") as fh:
        json.dump(payload, fh, indent=2, sort_keys=True)
        fh.write("\n")

    width = max(len(c) for c in CAUSES)
    print("skip-cause split (max_cap=%d, HEAD %s)" % (max_cap, head["short"]))
    header = "source".ljust(20) + "".join(c.rjust(width + 2) for c in CAUSES)
    print(header)
    for source in LINKED_ASSET_SOURCES:
        row = source.ljust(20)
        row += "".join(str(matrix[source][c]).rjust(width + 2) for c in CAUSES)
        row += "  (ledger skipped %d, docs>0 %d)" % (
            ledger_skipped.get(source, 0),
            ledger_docs_with_skips.get(source, 0),
        )
        print(row)
    total_row = "TOTAL".ljust(20) + "".join(
        str(totals[c]).rjust(width + 2) for c in CAUSES
    )
    print(total_row)
    print(
        "replay skipped=%d docs_with_skips=%d | ledger skipped=%d docs_with_skips=%d | "
        "mismatches=%d | reconciled=%s"
        % (
            total_skipped,
            total_docs,
            ledger_total,
            ledger_docs,
            len(mismatches),
            reconciled,
        )
    )
    print("wrote %s" % MATRIX_OUT)
    return 0 if reconciled else 1


if __name__ == "__main__":
    sys.exit(main())
