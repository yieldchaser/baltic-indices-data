"""Mock validation for the LightRAG layer (offline, no keys, no network).

Proves, against ~20 real tree sections (2 docs x 10) + 5 incremental:
  1. entities/relations extracted incl. a vessel node joined across both docs
     (one entity, source chunks from both doc_ids);
  2. every entity references fixture node_ids via its source chunks;
  3. incremental insert of 5 more sections grows the graph without rebuilding
     (original chunk map + node set preserved, counts grow);
  4. the query path returns node_id citations;
  5. live backends fail closed without keys;
  6. zero writes outside knowledge/graph/lightrag_store (+ temp/allowed paths).

Writes ONLY: knowledge/graph/lightrag_store/** and
scripts/graph/tests/fixtures/validation_log.{json,md}.
"""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from lightrag_build import (  # noqa: E402
    BackendConfigError,
    DEFAULT_WORKING_DIR,
    LIGHTRAG_VERSION_PIN,
    _all_edges,
    _all_nodes,
    create_rag,
    docs_from_fixture,
    graph_counts,
    make_backend,
    query_with_citations,
)

FIXTURES = Path(__file__).resolve().parent / "fixtures"
INITIAL_FIX = FIXTURES / "fixture_sections_initial.json"
INCR_FIX = FIXTURES / "fixture_sections_incremental.json"

ALLOWED_WRITE_PREFIXES = (
    "scripts/graph/",
    "knowledge/graph/",
    "docs/LIGHTRAG_MUSE_SPARK.md",
    "docs/INVENTORY_MUSE_SPARK.md",
)


def sha1_of(path: Path) -> str:
    return hashlib.sha1(path.read_bytes()).hexdigest()


def snapshot_store(wd: Path) -> dict:
    files = {}
    for p in sorted(wd.rglob("*")):
        if p.is_file():
            rel = p.relative_to(wd).as_posix()
            files[rel] = {"bytes": p.stat().st_size, "sha1": sha1_of(p)}
    return files


def git_repo_root() -> Path:
    r = subprocess.run(["git", "rev-parse", "--show-toplevel"],
                       capture_output=True, text=True, cwd=FIXTURES)
    assert r.returncode == 0, "not inside a git checkout"
    return Path(r.stdout.strip())


def check_zero_writes_outside(repo: Path) -> list[str]:
    r = subprocess.run(["git", "status", "--porcelain"], capture_output=True,
                       text=True, cwd=repo)
    assert r.returncode == 0
    bad = []
    for line in r.stdout.splitlines():
        path = line[3:].strip().strip('"').replace(os.sep, "/")
        if not path.startswith(ALLOWED_WRITE_PREFIXES):
            bad.append(line)
    return bad


async def build_phase(backend, docs, fresh: bool):
    from lightrag_build import insert_docs

    rag = create_rag(DEFAULT_WORKING_DIR, backend)
    if fresh and DEFAULT_WORKING_DIR.exists():
        shutil.rmtree(DEFAULT_WORKING_DIR)
        rag = create_rag(DEFAULT_WORKING_DIR, backend)
    inserted = await insert_docs(rag, docs)
    counts = await graph_counts(rag)
    nodes = {n: d for n, d in await _all_nodes(rag)}
    edges = {(s, t): d for s, t, d in await _all_edges(rag)}
    from lightrag_build import _chunk_doc_map

    cmap = await _chunk_doc_map(rag)
    try:
        await rag.finalize_storages()
    except Exception:
        pass
    return {"inserted": inserted, "counts": counts, "nodes": nodes,
            "edges": edges, "chunk_map": cmap}


async def amain() -> dict:
    log: dict = {"lightrag_version": None, "phases": {}}
    import lightrag

    try:
        log["lightrag_version"] = lightrag.__version__
    except Exception:
        import importlib.metadata as md

        log["lightrag_version"] = md.version("lightrag-hku")
    assert log["lightrag_version"] == LIGHTRAG_VERSION_PIN, log["lightrag_version"]

    backend = make_backend("mock")
    initial_docs = docs_from_fixture(INITIAL_FIX)
    incr_docs = docs_from_fixture(INCR_FIX)
    assert len(initial_docs) == 20 and len(incr_docs) == 5
    fixture_ids = {d.stable_id for d in initial_docs} | {d.stable_id for d in incr_docs}
    initial_ids = {d.stable_id for d in initial_docs}
    log["fixture_doc_ids"] = len(fixture_ids)

    # ---- phase 1: initial build ----
    p1 = await build_phase(backend, initial_docs, fresh=True)
    snap_a = snapshot_store(DEFAULT_WORKING_DIR)
    log["phases"]["initial"] = {
        "inserted": len(p1["inserted"]),
        "graph": p1["counts"],
        "store_files": len(snap_a),
        "store_bytes": sum(f["bytes"] for f in snap_a.values()),
    }

    # entities must reference fixture node_ids via source chunks
    cmap = p1["chunk_map"]
    assert len(cmap) == 20, f"expected 20 chunks (1/section, no re-chunk), got {len(cmap)}"
    assert set(cmap.values()) == initial_ids, "chunk->node_id map mismatch"
    unmapped = [k for k, v in cmap.items() if v not in initial_ids]
    assert not unmapped, unmapped
    ent_without_source = [n for n, d in p1["nodes"].items() if not (d or {}).get("source_id")]
    log["phases"]["initial"]["entities_total"] = len(p1["nodes"])
    log["phases"]["initial"]["relations_total"] = len(p1["edges"])
    log["phases"]["initial"]["entities_without_source"] = len(ent_without_source)
    assert not ent_without_source, ent_without_source[:5]

    # vessel joined across both docs: one node, sources from both doc_ids
    doc_of = {d.stable_id: d.doc_id for d in initial_docs}
    joined = []
    for name, data in p1["nodes"].items():
        srcs = str((data or {}).get("source_id") or "").split("<SEP>")
        docs = {doc_of.get(cmap.get(s, ""), "") for s in srcs if s}
        docs.discard("")
        etype = str((data or {}).get("entity_type") or "")
        if len(docs) >= 2:
            joined.append({"entity": name, "type": etype, "docs": sorted(docs),
                           "n_sources": len([s for s in srcs if s])})
    vessels_joined = [j for j in joined if "vessel" in j["type"].lower()]
    log["phases"]["initial"]["entities_spanning_both_docs"] = len(joined)
    log["phases"]["initial"]["vessels_spanning_both_docs"] = vessels_joined[:10]
    assert vessels_joined, "no vessel node joined across both docs"

    # ---- phase 2: incremental insert (fresh instance, same store) ----
    p2 = await build_phase(backend, incr_docs, fresh=False)
    snap_b = snapshot_store(DEFAULT_WORKING_DIR)
    # no rebuild proof: original chunk map preserved (same chunk ids), node set grown
    preserved_chunks = all(p2["chunk_map"].get(k) == v for k, v in cmap.items())
    nodes_before = set(p1["nodes"])
    nodes_after = set(p2["nodes"])
    kept = nodes_before - nodes_after
    log["phases"]["incremental"] = {
        "inserted": len(p2["inserted"]),
        "graph": p2["counts"],
        "chunks_total": len(p2["chunk_map"]),
        "original_chunks_preserved": preserved_chunks,
        "original_nodes_kept": len(nodes_before) - len(kept),
        "original_nodes_lost": sorted(kept)[:10],
        "new_nodes": len(nodes_after - nodes_before),
        "store_files": len(snap_b),
        "store_bytes": sum(f["bytes"] for f in snap_b.values()),
    }
    assert len(p2["chunk_map"]) == 25, p2["chunk_map"].keys()
    assert preserved_chunks, "original chunk ids changed -> rebuild suspected"
    assert not kept, f"nodes lost after incremental insert: {sorted(kept)[:5]}"
    assert p2["counts"]["nodes"] >= p1["counts"]["nodes"], "node count did not grow"
    log["store_hashes_before"] = snap_a
    log["store_hashes_after"] = snap_b

    # ---- phase 3: query with citations (fresh instance) ----
    rag = create_rag(DEFAULT_WORKING_DIR, backend)
    await rag.initialize_storages()
    try:
        res = await query_with_citations(
            rag, "Which vessels were reported sold in week 35 and at what prices?")
    finally:
        try:
            await rag.finalize_storages()
        except Exception:
            pass
    cites = [c["node_id"] for c in res["citations"]]
    log["phases"]["query"] = {
        "question": res["question"],
        "answer_chars": len(res["answer"]),
        "answer": res["answer"][:1200],
        "citations": res["citations"],
    }
    assert res["answer"].strip(), "empty answer"
    assert cites, "no citations returned"
    assert all(c in fixture_ids for c in cites), [c for c in cites if c not in fixture_ids]
    sales_ids = {d.stable_id for d in initial_docs
                 if "sale" in d.title.lower() or d.title.strip().lower() == "tankers"}
    assert any(c in sales_ids for c in cites), "citations miss the sales sections"
    # answer must surface graph content (vessel names from the sales sections)
    known_vessels = {"Mount Dampier", "Efraim A", "Amaryllis", "Hellstugutinden",
                     "Seasenator", "Spar Scorpio", "Arklow Spirit", "African Wagtail",
                     "Lila Mundra", "Marianna", "Princess Eternity", "Hippolyta"}
    assert any(v in res["answer"] for v in known_vessels), res["answer"][:500]

    # ---- phase 4: live backends fail closed (keys scrubbed) ----
    scrubbed = {k: v for k, v in os.environ.items()}
    for v in ("OLLAMA_BASE_URL", "OLLAMA_API_KEY", "OLLAMA_MODEL", "NIM_API_KEY",
              "NVIDIA_API_KEY", "NIM_BASE_URL", "NIM_MODEL", "OPENROUTER_API_KEY",
              "OPENROUTER_BASE_URL", "OPENROUTER_MODEL"):
        os.environ.pop(v, None)
    fail_closed = {}
    try:
        for kind in ("ollama", "nim", "openrouter"):
            try:
                make_backend(kind)
                fail_closed[kind] = "NO-FAILURE (BAD)"
            except BackendConfigError as exc:
                fail_closed[kind] = f"fail-closed OK: {exc}"
    finally:
        os.environ.clear()
        os.environ.update(scrubbed)
    log["phases"]["fail_closed"] = fail_closed
    assert all(v.startswith("fail-closed OK") for v in fail_closed.values()), fail_closed
    return log


def main() -> int:
    repo = git_repo_root()
    before_bad = check_zero_writes_outside(repo)
    log = asyncio.run(amain())
    log["git_status_violations_before"] = before_bad
    log["git_status_violations_after"] = check_zero_writes_outside(repo)
    assert not log["git_status_violations_after"], log["git_status_violations_after"]
    store_bytes = sum(f["bytes"] for f in log["store_hashes_after"].values())
    log["store_total_bytes"] = store_bytes
    FIXTURES.mkdir(parents=True, exist_ok=True)
    (FIXTURES / "validation_log.json").write_text(json.dumps(log, indent=1), encoding="utf-8")

    lines = [
        "# LightRAG mock validation log (muse-spark, Decision 3)",
        "",
        f"- lightrag-hku=={log['lightrag_version']} (pinned, sandbox pip install)",
        f"- fixtures: 20 initial + 5 incremental real tree sections "
        f"(2 docs: Advanced Shipping wk35 + Star Asia wk35)",
        f"- initial: {log['phases']['initial']['inserted']} docs -> "
        f"{log['phases']['initial']['entities_total']} entities / "
        f"{log['phases']['initial']['relations_total']} relations",
        f"- chunks: 20 (exactly 1 per section — no re-chunking); "
        f"entities without source: {log['phases']['initial']['entities_without_source']}",
        f"- entities spanning both docs: {log['phases']['initial']['entities_spanning_both_docs']}",
    ]
    for j in log["phases"]["initial"]["vessels_spanning_both_docs"][:5]:
        lines.append(f"  - vessel `{j['entity']}` ({j['type']}) from {j['n_sources']} chunks, docs: {j['docs']}")
    inc = log["phases"]["incremental"]
    lines += [
        f"- incremental: +{inc['inserted']} docs -> {inc['graph']['nodes']} nodes "
        f"(+{inc['new_nodes']}); original chunks preserved: {inc['original_chunks_preserved']}; "
        f"original nodes kept: {inc['original_nodes_kept']}, lost: {len(inc['original_nodes_lost'])}",
        f"- query citations ({len(log['phases']['query']['citations'])}): "
        + ", ".join("`" + c["node_id"][-60:] + "`" for c in log["phases"]["query"]["citations"][:8]),
        "- fail-closed (keys scrubbed): " + "; ".join(
            f"{k}: {v[:90]}" for k, v in log["phases"]["fail_closed"].items()),
        f"- store: {log['store_hashes_after'] and len(log['store_hashes_after'])} files, "
        f"{store_bytes} bytes under knowledge/graph/lightrag_store",
        f"- git violations before/after: {len(log['git_status_violations_before'])}/"
        f"{len(log['git_status_violations_after'])}",
        "",
        "> Answer sample:",
        ">",
        "> " + log["phases"]["query"]["answer"][:900].replace("\n", " "),
    ]
    (FIXTURES / "validation_log.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("\n".join(lines))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
