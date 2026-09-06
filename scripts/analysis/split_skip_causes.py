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

Output: ``data/derived/skip_cause_matrix.json`` (POSIX paths only) +
``data/derived/asset_dispositions.jsonl`` (one compact record per discovered
asset: doc_id, source, date, href, asset_kind, disposition, reason,
local_mirror_rel, node_id) + a human summary on stdout. Exit status is
nonzero when the replay does not reconcile with the ledger tallies.

Disposition semantics (X1/M1): ``disposition`` is ``ingested`` for
resolved-unique candidates (the read-only proxy for ledger ingested;
extract-time failures are indistinguishable without extraction and land here
with ``node_id`` null when no tree section matches) and ``skipped`` with one
of the five causes otherwise. ``local_mirror_rel`` is set only when the
candidate resolved to a repo file (ingested + ``duplicate_path`` skips);
true-skipped assets (external / non-content) carry null by construction.
``node_id`` is the tree-shard linked-asset section whose ``Source asset:``
rel matches, or null. Ledger-``failed``-branch candidates (non-http(s)
unresolvable: exactly 1 repo-wide) are emitted as ``skipped`` with null
reason and counted separately as ``unclassified_failed_branch`` so the
five-cause gate stays exact.
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
DISPOSITIONS_OUT = "data/derived/asset_dispositions.jsonl"

DISPOSITIONS = ("ingested", "skipped")
# Failed-branch (non-http unresolvable) records use disposition "skipped" with
# null reason as a documented single exception — see module docstring.

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


def collect_link_candidates_tagged(markup: str) -> list:
    """Mirror ``collect_linked_asset_sections`` candidate enumeration.

    Root scoping replicates ``adapt_archive_html`` (``body > section`` first,
    then any ``section``, then ``body``). Order replicates the original loops:
    every ``<a href>`` in document order, then every ``<img src>``.
    ``href=True`` / ``src=True`` mean attribute-present (empty included).
    Returns ``(raw_value, tag)`` pairs; ``tag`` is ``"a"`` or ``"img"``.
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
            candidates.append((node.attrs["href"], "a"))
    for node in _iter_descendants(root):
        if node.tag == "img" and "src" in node.attrs:
            candidates.append((node.attrs["src"], "img"))
    return candidates


def collect_link_candidates(markup: str) -> list:
    """Raw candidate values in pipeline order (see tagged variant)."""
    return [raw for raw, _tag in collect_link_candidates_tagged(markup)]


def asset_kind_for(tag: str, href: str) -> str:
    """Coarse asset kind: ``img`` for ``<img src>``, else ``pdf``/``link``."""
    if tag == "img":
        return "img"
    clean = norm_space(href).split("#", 1)[0].split("?", 1)[0]
    path = urlparse(clean).path if "://" in clean else clean
    if Path(path).suffix.lower() == ".pdf":
        return "pdf"
    return "link"


def linked_section_index(tree_path: Path) -> dict:
    """Map ``Source asset: {rel}`` -> ``node_id`` for a doc's tree shard.

    Walks shard children recursively in document order; first section wins.
    Returns {} when the shard is missing/unparseable (caller records null).
    """
    index = {}
    try:
        shard = json.loads(tree_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return index
    stack = list(shard.get("children") or [])
    while stack:
        node = stack.pop(0)
        if not isinstance(node, dict):
            continue
        summary = node.get("summary") or node.get("text") or ""
        first_line = summary.split("\n", 1)[0] if summary else ""
        if first_line.startswith("Source asset:"):
            rel = first_line[len("Source asset:"):].strip().split()[0] if first_line[len("Source asset:"):].strip() else ""
            if rel and rel not in index and node.get("node_id"):
                index[rel] = node["node_id"]
        stack.extend(node.get("children") or [])
    return index


def replay_doc_events(source: str, html_path: Path, tagged: list, max_cap: int,
                      section_index: dict) -> tuple:
    """Replay skip branches capturing one disposition event per candidate.

    Branch order mirrors ``split_doc_skips`` exactly (cap -> empty -> non-
    content -> resolve -> duplicate -> ingested-proxy). Events carry the
    candidate's disposition; callers attach doc-level fields. The non-http(s)
    unresolvable fork (ledger-``failed``) yields a ``skipped``/null-reason
    event (documented single exception; counted separately downstream).
    """
    counts = collections.Counter()
    events = []
    if source not in LINKED_ASSET_SOURCES or source == "baltic":
        return counts, events
    seen_paths = set()
    sections_len = 0
    for raw, tag in tagged:
        href = norm_space(raw)
        kind = asset_kind_for(tag, href)
        if sections_len >= max_cap:  # L2326-2327
            counts["per_doc_cap"] += 1
            events.append({"href": href, "asset_kind": kind,
                           "disposition": "skipped", "reason": "per_doc_cap",
                           "local_mirror_rel": None, "node_id": None})
            continue
        if not href:  # L2338-2340
            counts["empty_href"] += 1
            events.append({"href": href, "asset_kind": kind,
                           "disposition": "skipped", "reason": "empty_href",
                           "local_mirror_rel": None, "node_id": None})
            continue
        if looks_like_non_content_link(href):  # L2341-2343
            counts["non_content_link"] += 1
            events.append({"href": href, "asset_kind": kind,
                           "disposition": "skipped", "reason": "non_content_link",
                           "local_mirror_rel": None, "node_id": None})
            continue
        linked_path = resolve_archive_link_path(html_path, href)
        if linked_path is None:  # L2346-2352
            parsed = urlparse(href)
            if parsed.scheme in {"http", "https"}:  # L2348-2349
                counts["unresolvable_external"] += 1
                events.append({"href": href, "asset_kind": kind,
                               "disposition": "skipped",
                               "reason": "unresolvable_external",
                               "local_mirror_rel": None, "node_id": None})
            else:  # L2350-2351 failed branch — single documented exception
                counts["_failed_branch"] += 1
                events.append({"href": href, "asset_kind": kind,
                               "disposition": "skipped", "reason": None,
                               "local_mirror_rel": None, "node_id": None})
            continue
        linked_rel = relpath_posix(linked_path)
        if linked_rel in seen_paths:  # L2356-2358
            counts["duplicate_path"] += 1
            events.append({"href": href, "asset_kind": kind,
                           "disposition": "skipped", "reason": "duplicate_path",
                           "local_mirror_rel": linked_rel, "node_id": None})
            continue
        seen_paths.add(linked_rel)
        sections_len += 1  # read-only proxy for "ingested" (see docstring)
        events.append({"href": href, "asset_kind": kind,
                       "disposition": "ingested", "reason": None,
                       "local_mirror_rel": linked_rel,
                       "node_id": section_index.get(linked_rel)})
    return counts, events


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

    docs_total = len(docs)
    # N3 coverage, derived from documents.jsonl at runtime (no hardcoded
    # counts): docs whose source never enters collect_linked_asset_sections.
    excluded_by_source = collections.Counter(
        doc.get("source", "?")
        for doc in docs
        if doc.get("source", "?") not in LINKED_ASSET_SOURCES
    )
    docs_excluded = sum(excluded_by_source.values())
    breakwave_by_category = collections.Counter(
        doc.get("category", "?") for doc in docs if doc.get("source") == "breakwave"
    )

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
    event_divergences = []
    all_events = []
    replayed = 0
    for doc in docs:
        source = doc.get("source", "?")
        if source not in LINKED_ASSET_SOURCES:
            continue
        source_path = doc.get("source_path", "")
        html_path = REPO_ROOT / PurePosixPath(source_path).as_posix()
        tagged = []
        if source != "baltic" and html_path.is_file():
            markup = html_path.read_text(encoding="utf-8", errors="ignore")
            tagged = collect_link_candidates_tagged(markup)
        candidates = [raw for raw, _tag in tagged]
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
        # Disposition pass: same branches, one event per candidate. Cross-
        # checked against the gate path above; any divergence fails the run.
        tree_rel = doc.get("tree_path", "")
        tree_path = REPO_ROOT / PurePosixPath(tree_rel).as_posix() if tree_rel else None
        section_index = (
            linked_section_index(tree_path)
            if tree_path is not None and source != "baltic"
            else {}
        )
        ev_counts, events = replay_doc_events(
            source, html_path, tagged, max_cap, section_index
        )
        for cause in CAUSES:
            if ev_counts.get(cause, 0) != counts.get(cause, 0):
                event_divergences.append(
                    {
                        "doc_id": doc.get("doc_id"),
                        "cause": cause,
                        "gate_path": counts.get(cause, 0),
                        "event_path": ev_counts.get(cause, 0),
                    }
                )
        for event in events:
            event["doc_id"] = doc.get("doc_id")
            event["source"] = source
            event["date"] = doc.get("date")
            ordered = {
                "doc_id": event["doc_id"],
                "source": event["source"],
                "date": event["date"],
                "href": event["href"],
                "asset_kind": event["asset_kind"],
                "disposition": event["disposition"],
                "reason": event["reason"],
                "local_mirror_rel": event["local_mirror_rel"],
                "node_id": event["node_id"],
            }
            all_events.append(ordered)

    totals = {cause: sum(matrix[s][cause] for s in LINKED_ASSET_SOURCES) for cause in CAUSES}
    total_skipped = sum(totals.values())
    total_docs = sum(replay_docs_with_skips.values())
    ledger_total = sum(ledger_skipped[s] for s in LINKED_ASSET_SOURCES)
    ledger_docs = sum(ledger_docs_with_skips[s] for s in LINKED_ASSET_SOURCES)
    ledger_discovered = sum(
        ledger_stats[(s, "discovered")] for s in LINKED_ASSET_SOURCES
    )
    reconciled = (
        total_skipped == ledger_total
        and total_docs == ledger_docs
        and not mismatches
        and not event_divergences
        and len(all_events) == ledger_discovered
    )

    # N3 exclusion note, assembled from runtime counters only (no hardcoded
    # counts): ordered so the text is deterministic for a given documents.jsonl
    # (excluded sources alphabetically; breakdown lists non-breakwave sources
    # by ascending count, then breakwave expanded by alphabetical category).
    excluded_list = "/".join(sorted(excluded_by_source))
    excluded_non_bw = sorted(
        ((s, c) for s, c in excluded_by_source.items() if s != "breakwave"),
        key=lambda kv: (kv[1], kv[0]),
    )
    bw_cats = sorted(breakwave_by_category.items())
    breakdown_parts = [f"{s} {c}" for s, c in excluded_non_bw]
    if "breakwave" in excluded_by_source:
        breakdown_parts.append(
            "breakwave %d = %s"
            % (
                excluded_by_source["breakwave"],
                " + ".join(f"{k} {v}" for k, v in bw_cats),
            )
        )
    exclusion_reason = (
        "LINKED_ASSET_SOURCES = %s; only these sources are replayed "
        "(docs_replayed %d); %s (%d) excluded by construction, never entering "
        "collect_linked_asset_sections; adapt_baltic never calls the collector "
        "so baltic replays zeros. Verified: %d+%d=%d = documents.jsonl rows; "
        "excluded breakdown verified against documents.jsonl (%s)."
        % (
            "/".join(LINKED_ASSET_SOURCES),
            replayed,
            excluded_list,
            docs_excluded,
            replayed,
            docs_excluded,
            docs_total,
            ", ".join(breakdown_parts),
        )
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
        "docs_total": docs_total,
        "docs_excluded": docs_excluded,
        "docs_excluded_by_source": dict(excluded_by_source),
        "docs_excluded_breakwave_by_category": dict(breakwave_by_category),
        "exclusion_reason": exclusion_reason,
    }

    out_path = REPO_ROOT / PurePosixPath(MATRIX_OUT).as_posix()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="\n") as fh:
        json.dump(payload, fh, indent=2, sort_keys=True)
        fh.write("\n")

    disp_path = REPO_ROOT / PurePosixPath(DISPOSITIONS_OUT).as_posix()
    disp_path.parent.mkdir(parents=True, exist_ok=True)
    with disp_path.open("w", encoding="utf-8", newline="\n") as fh:
        for event in all_events:
            fh.write(json.dumps(event, ensure_ascii=True, separators=(",", ":")) + "\n")

    disp_totals = collections.Counter()
    disp_reasons = collections.Counter()
    disp_mirror_present = collections.Counter()
    disp_node_matched = 0
    for event in all_events:
        disp_totals[event["disposition"]] += 1
        disp_reasons[event["reason"] if event["reason"] else "<null>"] += 1
        mirror = event["local_mirror_rel"]
        if mirror:
            exists = (REPO_ROOT / PurePosixPath(mirror).as_posix()).is_file()
            disp_mirror_present[(event["disposition"], exists)] += 1
        if event["disposition"] == "ingested" and event["node_id"]:
            disp_node_matched += 1

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
    print(
        "dispositions: records=%d (ledger discovered=%d) ingested=%d skipped=%d "
        "| reasons=%s | mirror[skipped,exists]=%d mirror[skipped,missing]=%d "
        "mirror[ingested,exists]=%d mirror[ingested,missing]=%d "
        "| ingested_with_node_id=%d | event_divergences=%d"
        % (
            len(all_events),
            ledger_discovered,
            disp_totals.get("ingested", 0),
            disp_totals.get("skipped", 0),
            dict(sorted(disp_reasons.items())),
            disp_mirror_present.get(("skipped", True), 0),
            disp_mirror_present.get(("skipped", False), 0),
            disp_mirror_present.get(("ingested", True), 0),
            disp_mirror_present.get(("ingested", False), 0),
            disp_node_matched,
            len(event_divergences),
        )
    )
    print("wrote %s" % DISPOSITIONS_OUT)
    return 0 if reconciled else 1


if __name__ == "__main__":
    sys.exit(main())
