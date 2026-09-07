"""LightRAG graph layer over ``knowledge/trees/`` (muse-spark, STATUS BOARD Decision 3).

Join contract
-------------
* Relational core = existing SQLite spine (untouched, not imported here).
* Graph layer = LightRAG built **over** the already-sectioned tree shards in
  ``knowledge/trees/**/*.json``. No re-chunking, no shard replacement:
  exactly **one LightRAG document per tree section**, with the LightRAG
  ``doc_id`` set to the tree section's stable ``node_id``
  (fallback: ``doc_id + '#' + ordinal`` when ``node_id`` is absent).
* Every chunk text is prefixed with a ``[node_id=... doc_id=...]`` provenance
  header, so retrieved context — and therefore query citations — always
  carries the join keys back to the tree section and its parent document.

Backends (same env names as the knowledge pipeline)
---------------------------------------------------
* ``mock`` (default) — fully offline rule-based mock LLM + hash embeddings.
  Used for CI/sandbox validation; exercises entity+relation extraction,
  incremental insert and the citation path with zero network and zero keys.
* ``ollama`` — ``OLLAMA_BASE_URL`` / ``OLLAMA_API_KEY`` / ``OLLAMA_MODEL``.
* ``nim`` — ``NIM_API_KEY`` (or ``NVIDIA_API_KEY`` alias) / ``NIM_BASE_URL`` /
  ``NIM_MODEL``.
* ``openrouter`` — ``OPENROUTER_API_KEY`` / ``OPENROUTER_BASE_URL`` /
  ``OPENROUTER_MODEL``.

No keys are read from anywhere except these env vars, and live backends
**fail closed** with a ``BackendConfigError`` naming the missing variable
when keys/endpoints are absent (the sandbox case).

Storage
-------
``working_dir`` is fixed at ``knowledge/graph/lightrag_store`` — all
LightRAG files (graph, vectors, KV, doc-status, logs) stay inside it.
Nothing is written anywhere else in the repo.

Requires ``lightrag-hku==1.5.7`` (see ``LIGHTRAG_VERSION_PIN``).
"""

from __future__ import annotations

import argparse
import asyncio
import glob
import hashlib
import json
import os
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

LIGHTRAG_VERSION_PIN = "1.5.7"

REPO_ROOT = Path(__file__).resolve().parents[2]
TREES_GLOB = str(REPO_ROOT / "knowledge" / "trees" / "**" / "*.json")
DEFAULT_WORKING_DIR = REPO_ROOT / "knowledge" / "graph" / "lightrag_store"

TUPLE_DELIMITER = "<|#|>"
COMPLETION_DELIMITER = "<|COMPLETE|>"

# ---------------------------------------------------------------------------
# Maritime entity/relation guidance (Decision 3 task 2b)
# ---------------------------------------------------------------------------

MARITIME_ENTITY_TYPES_GUIDANCE = """\
Use the following maritime entity types (in addition to generic ORGANIZATION,
PERSON, GEO and EVENT where they genuinely apply):

- vessel: a named ship (bulk carrier, tanker, container ship, gas carrier, …).
  Record type/dwt/year when stated in the source text.
- owner_charterer: shipowner, operator, charterer or buyer/seller named in a
  fixture or sale & purchase report.
- route_port: a port, berth, anchorage, strait, canal or named trade route
  (e.g. West Australia–China iron ore, NoPac, WCSA).
- rate_index: a freight rate, time-charter equivalent, price, index value or
  bunker price (e.g. USD 18,500 pd, WS 120, BDI 3,186, VLSFO USD/t).
- week_date: a reporting week, publication date or laycan window
  (e.g. Week 35 2026, 21–28 Aug 2026).

Prefer relations SOLD_TO / CHARTERED_BY / FIXED_AT_RATE / CALLED_AT /
REPORTED_IN_WEEK linking vessel ↔ owner_charterer / rate_index / route_port /
week_date. Every entity description must stay factual to the input text and
keep the vessel/port/rate names exactly as written (including the
[node_id=…] provenance header context when present)."""

# ---------------------------------------------------------------------------
# Backend configuration
# ---------------------------------------------------------------------------


class BackendConfigError(RuntimeError):
    """Raised when a live backend is requested without its keys/endpoints."""


def _get(name: str, default: str = "") -> str:
    return (os.environ.get(name) or default).strip()


@dataclass
class BackendSpec:
    kind: str  # mock | ollama | nim | openrouter
    llm_func: Any = None
    embedding_func: Any = None
    embedding_dim: int = 128
    llm_model_name: str = "mock"
    describe: str = ""


def _require(vars_needed: Iterable[str], backend: str) -> dict[str, str]:
    missing = [v for v in vars_needed if not _get(v)]
    if missing:
        raise BackendConfigError(
            f"backend '{backend}' fail-closed: missing required env "
            f"{', '.join(missing)} (no keys in sandbox; refusing to call a "
            f"live LLM without explicit configuration)"
        )
    return {v: _get(v) for v in vars_needed}


# ---------------------------------------------------------------------------
# Mock backend (offline validation)
# ---------------------------------------------------------------------------

MOCK_EMBEDDING_DIM = 128

_KNOWN_PORTS = [
    "Port Hedland", "Dampier", "Singapore", "Rotterdam", "Shanghai",
    "Tubarao", "Richards Bay", "Newcastle", "Santos", "Houston",
    "Fujairah", "Gibraltar", "Panama", "Alang", "Chattogram", "Gaddani",
    "NoPac", "WCSA", "Taiwan", "Japan", "China", "Australia", "Brazil",
    "India", "Pakistan", "Bangladesh", "Turkey", "Korea", "Indonesia",
]

_VESSEL_ROW_RE = re.compile(r"\|\s*([A-Z][A-Za-z0-9 .'-]{2,40}?)\s*\|\s*[\d.,]+\s*\|\s*\d{4}")
_TABLE_NAME_RE = re.compile(r"^[A-Z][A-Z0-9 .'\-&]{2,40}$")
_TABLE_SKIP_NAMES = {"VESSEL", "VESSEL NAME", "SEGMENT (AVG)", "TYPE", "SEGMENT"}
_SHIP_TYPE_TOKENS = {
    "CAPE", "KMAX", "PMAX", "UMAX", "SMAX", "HANDY", "VLCC", "SUEZ", "AFRA",
    "MR", "LR", "LR1", "LR2", "TANKER", "BULKER", "BULK CARRIER", "CAPESIZE",
    "KAMSARMAX", "PANAMAX", "ULTRAMAX", "SUPRAMAX", "HANDYSIZE", "HANDYMAX",
    "CONTAINER", "TEU", "WOOD CHIP CARRIER", "OIL TANKER", "CHEMICAL TANKER",
    "GAS CARRIER", "LPG CARRIER", "LNG CARRIER", "CONTAINER SHIP",
    "GENERAL CARGO",
}


def _canon_vessel(name: str) -> str:
    return re.sub(r"\s+", " ", name).strip(" .-'").title()


def _vessels_from_tables(text: str) -> dict[str, str]:
    """Vessel rows in broker S&P/fixture tables.

    Handles both layouts seen in the corpus, e.g.
    ``| Mount Dampier | 181.469 | 2011 | Imabari …`` (Advanced Shipping) and
    ``PRINCESS ETERNITY | CAPE | 182,263 | 2022 / JAPAN | 78.0 | …``
    (Star Asia: NAME | TYPE | DWT | YEAR / BUILT | PRICE | BUYER).

    Vessel names are canonicalised to title case so the same ship reported by
    two brokers (``MOUNT DAMPIER`` vs ``Mount Dampier``) yields one entity —
    this emulates a production LLM's consistent naming and lets LightRAG's own
    merge produce the cross-document join (proven by source chunks from both
    doc_ids on the merged node).
    """
    out: dict[str, str] = {}

    for m in _VESSEL_ROW_RE.finditer(text):
        name = _canon_vessel(m.group(1))
        if name.upper() in _SHIP_TYPE_TOKENS:
            continue
        if 3 <= len(name) <= 42 and name not in out:
            row = text[max(0, m.start() - 40): m.end() + 120].replace("\n", " ")
            out[name] = row[:200]
    for line in text.splitlines():
        # Star Asia layout: NAME | TYPE | DWT | YEAR / BUILT | PRICE | BUYER.
        # Advanced Shipping tanker tables list TYPE first: TYPE | NAME | DWT | YoB | …
        fields = [f.strip() for f in line.split("|")]
        if len(fields) < 5 or not re.match(r"\d{4}\b", fields[3]):
            continue
        raw = fields[0]
        if raw.upper() in _SHIP_TYPE_TOKENS and len(fields) > 4:
            # TYPE-first row: the name is the next name-like field
            cand = [f for f in fields[1:3]
                    if _TABLE_NAME_RE.match(f) and f not in _TABLE_SKIP_NAMES
                    and f.upper() not in _SHIP_TYPE_TOKENS]
            raw = cand[0] if cand else ""
        if not _TABLE_NAME_RE.match(raw) or raw in _TABLE_SKIP_NAMES:
            continue
        name = _canon_vessel(raw)
        if name.upper() in _SHIP_TYPE_TOKENS:
            continue
        if name not in out:
            out[name] = " | ".join(fields)[:200]
    return out
_PORT_RE = re.compile(r"Port\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?")
_RATE_RE = re.compile(
    r"(?:\bUSD\s*\d[\d,.]*(?:\s*\/\s*(?:pd|day|ldt|t))?|\b(?:WS|BDI|BDTI|BCI|BPI|BSI|BHSI)\b\s*:?\s*\d[\d,.]*)",
    re.IGNORECASE,
)
_WEEK_RE = re.compile(
    r"(?:Week\s*\d{1,2}(?:\s*\(.*?\))?(?:\s*\d{4})?|\d{1,2}\s*(?:Aug|Sep|Oct|Nov|Dec|Jan|Feb|Mar|Apr|May|Jun|Jul)[a-z]*\s*(?:to\s*\d{1,2}\s*[A-Za-z]*\s*)?\d{4}|20\d\d-\d\d-\d\d)",
)
_NODE_ID_RE = re.compile(r"\[node_id=([^\s\]]+)")


def _mock_tokenize_vec(text: str, dim: int = MOCK_EMBEDDING_DIM) -> list[float]:
    import math

    vec = [0.0] * dim
    for tok in re.findall(r"[a-z0-9]+", text.lower()):
        h = int(hashlib.md5(tok.encode()).hexdigest(), 16)
        vec[h % dim] += 1.0
    norm = math.sqrt(sum(v * v for v in vec)) or 1.0
    return [v / norm for v in vec]


async def mock_llm_func(
    prompt: str,
    system_prompt: str | None = None,
    history_messages: list | None = None,
    **kwargs: Any,
) -> str:
    """Deterministic rule-based stand-in for an LLM.

    Dispatches on prompt markers used by lightrag-hku==1.5.7 prompts:
    keyword extraction -> JSON keywords; entity extraction task ->
    ``entity<|#|>...`` / ``relation<|#|>...`` records + ``<|COMPLETE|>``;
    gleaning/continue -> bare ``<|COMPLETE|>``; summarisation -> truncation;
    anything else (query answering) -> canned maritime answer built ONLY from
    entity/relation/chunk content present in the retrieved context.

    NOTE: LightRAG passes the retrieved context as ``system_prompt`` and the
    user question as ``prompt`` on the answer path, so both are scanned.
    """
    prompt = prompt or ""
    system_prompt = system_prompt or ""
    text = f"{system_prompt}\n{prompt}"
    # 1. keyword extraction (JSON)
    if "high_level_keywords" in text and "low_level_keywords" in text:
        qm = re.search(r"[Qq]uery:\s*(.+?)(?:\n\n|\Z)", text, re.S)
        query = (qm.group(1) if qm else text)[-800:]
        words = [w for w in re.findall(r"[A-Za-z]{4,}", query)]
        hl = [w for w in words if w[0].isupper()][:5] or ["shipping market"]
        ll = [w.lower() for w in words if w[0].islower()][:5] or ["vessel", "rate"]
        ports = [p for p in _KNOWN_PORTS if p.lower() in query.lower()][:3]
        ll = ports + ll
        return json.dumps({"high_level_keywords": hl[:5], "low_level_keywords": ll[:5]})
    # 2. entity/relation extraction
    if "Extract entities and relationships" in text:
        return _mock_extraction_records(text)
    # 3. gleaning continue prompt -> nothing more
    if "MANY entities" in text or ("continue" in text.lower() and "missing" in text.lower()):
        return COMPLETION_DELIMITER
    # 4. description-merge summarisation (distinctive template marker)
    if "synthesize a list of descriptions" in text or "Description List" in text:
        core = text[-1500:]
        return (core[:600] + COMPLETION_DELIMITER) if COMPLETION_DELIMITER not in core else core[:600]
    # 5. query answering (rag_response and friends): build the answer ONLY
    # from entity JSON + chunk content present in the retrieved context.
    vessels = _vessels_from_tables(text)
    ent_rows = re.findall(
        r'\{"entity":\s*"([^"]+)",\s*"type":\s*"([^"]+)",\s*"description":\s*"((?:[^"\\]|\\.){0,300})',
        text,
    )
    seen: dict[str, str] = {}
    for name, etype, desc in ent_rows:
        if etype == "vessel" and name not in seen:
            seen[name] = desc.replace("\\n", " ").strip()
    for v in vessels:
        seen.setdefault(v, vessels[v][:160])
    node_ids = sorted(set(_NODE_ID_RE.findall(text)))
    ports = sorted({p for p in _KNOWN_PORTS if p in text})[:6]
    if seen:
        lines = ["Reported vessels in the retrieved graph context:"]
        for name, desc in list(seen.items())[:10]:
            frag = re.sub(r"\s+", " ", desc)[:160]
            lines.append(f"- {name}" + (f": {frag}" if frag else ""))
    else:
        lines = ["No vessel entities in the retrieved context for this query."]
    if ports:
        lines.append("Trade touchpoints include " + ", ".join(ports) + ".")
    if node_ids:
        lines.append("Sources: " + ", ".join(f"[node_id={n}]" for n in node_ids[:10]) + ".")
    return " ".join(lines)


def _mock_extraction_records(prompt_text: str) -> str:
    """Canned-but-content-aware extraction: entities/relations are emitted only
    for vessel names, ports, rates and week/dates literally present in the
    extraction prompt (which carries the chunk text)."""
    vessels: dict[str, str] = _vessels_from_tables(prompt_text)
    # also catch Title Case vessel mentions outside tables (e.g. LYRA TANKER rows)
    for m in re.finditer(r"\b([A-Z][A-Z .'-]{3,30}?)\s+(TANKER|BULKER|BULK CARRIER|VLCC|AFRAMAX|SUEZMAX|MR1?|LR2?|CAPESIZE|KAMSARMAX|PANAMAX|ULTRAMAX|SUPRAMAX|HANDYSIZE)\b", prompt_text):
        name = _canon_vessel(m.group(0))
        if name not in vessels and len(vessels) < 14:
            vessels[name] = m.group(0)[:120]
    ports = sorted({p for p in _KNOWN_PORTS if p in prompt_text})
    ports += sorted(set(_PORT_RE.findall(prompt_text)))
    ports = list(dict.fromkeys(ports))[:8]
    rates = list(dict.fromkeys(r.strip() for r in _RATE_RE.findall(prompt_text)))[:6]
    weeks = list(dict.fromkeys(w.strip() for w in _WEEK_RE.findall(prompt_text)))[:4]

    td = TUPLE_DELIMITER
    recs: list[str] = []
    vessel_names = list(vessels)[:12]
    for v in vessel_names:
        desc = re.sub(r"\s+", " ", vessels[v])[:220].replace(td, " ")
        recs.append(f"entity{td}{v}{td}vessel{td}{desc}")
    for p in ports:
        recs.append(f"entity{td}{p}{td}route_port{td}Port/route mentioned in market report text")
    for r in rates:
        recs.append(f"entity{td}{r[:60]}{td}rate_index{td}Freight/price/index level quoted in report text")
    for w in weeks:
        recs.append(f"entity{td}{w[:60]}{td}week_date{td}Reporting week or date in report text")
    # relations: vessel x rate / week / port (endpoints co-occur in this chunk)
    rels = 0
    for v in vessel_names:
        for r in rates[:2]:
            recs.append(f"relation{td}{v}{td}{r[:60]}{td}REPORTED_RATE{td}{v} reported at {r[:60]}")
            rels += 1
        for w in weeks[:1]:
            recs.append(f"relation{td}{v}{td}{w[:60]}{td}REPORTED_IN_WEEK{td}{v} reported in {w[:60]}")
            rels += 1
        for p in ports[:1]:
            recs.append(f"relation{td}{v}{td}{p}{td}TRADE_CALL{td}{v} trading via {p}")
            rels += 1
        if rels >= 10:
            break
    if not recs:
        return COMPLETION_DELIMITER
    return "\n".join(recs) + "\n" + COMPLETION_DELIMITER


async def mock_embedding_func(texts: list[str], **kwargs: Any) -> Any:
    import numpy as np

    return np.array([_mock_tokenize_vec(t) for t in texts])


def make_mock_backend() -> BackendSpec:
    from lightrag.utils import EmbeddingFunc

    return BackendSpec(
        kind="mock",
        llm_func=mock_llm_func,
        embedding_func=EmbeddingFunc(
            embedding_dim=MOCK_EMBEDDING_DIM,
            max_token_size=8192,
            func=mock_embedding_func,
        ),
        embedding_dim=MOCK_EMBEDDING_DIM,
        llm_model_name="mock-rule-based-v1",
        describe="offline rule-based mock (no network, no keys)",
    )


def make_live_backend(kind: str) -> BackendSpec:
    """Build an Ollama / NIM / OpenRouter backend. Fail-closed without keys."""
    if kind == "ollama":
        cfg = _require(("OLLAMA_BASE_URL", "OLLAMA_MODEL"), "ollama")
        try:
            from lightrag.llm.ollama import ollama_embed, ollama_model_complete
        except Exception as exc:  # pragma: no cover - import-time only
            raise BackendConfigError(f"backend 'ollama': lightrag ollama provider import failed: {exc}")
        from functools import partial

        from lightrag.utils import EmbeddingFunc

        dim = int(_get("LIGHTRAG_EMBEDDING_DIM", "1024"))
        base = cfg["OLLAMA_BASE_URL"]
        model = cfg["OLLAMA_MODEL"]
        embed_base = getattr(ollama_embed, "func", ollama_embed)
        return BackendSpec(
            kind="ollama",
            llm_func=partial(ollama_model_complete, host=base, model=model),
            embedding_func=EmbeddingFunc(
                embedding_dim=dim, max_token_size=8192,
                func=partial(embed_base, model=_get("OLLAMA_EMBED_MODEL", "nomic-embed-text"), host=base),
            ),
            embedding_dim=dim,
            llm_model_name=model,
            describe=f"ollama {model} @ {base}",
        )
    if kind in ("nim", "openrouter"):
        if kind == "nim":
            if not _get("NIM_API_KEY") and not _get("NVIDIA_API_KEY"):
                raise BackendConfigError(
                    "backend 'nim' fail-closed: missing required env NIM_API_KEY "
                    "(or NVIDIA_API_KEY alias) (no keys in sandbox; refusing to "
                    "call a live LLM without explicit configuration)"
                )
            cfg = {
                "key": _get("NIM_API_KEY") or _get("NVIDIA_API_KEY"),
                "base": _get("NIM_BASE_URL", "https://integrate.api.nvidia.com/v1"),
                "model": _get("NIM_MODEL", "meta/llama-3.3-70b-instruct"),
            }
        else:
            cfg = _require(("OPENROUTER_API_KEY",), "openrouter")
            cfg = {
                "key": cfg["OPENROUTER_API_KEY"],
                "base": _get("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1"),
                "model": _get("OPENROUTER_MODEL", "meta-llama/llama-3.3-70b-instruct"),
            }
        try:
            from lightrag.llm.openai import openai_complete_if_cache, openai_embed
        except Exception as exc:  # pragma: no cover - import-time only
            raise BackendConfigError(f"backend '{kind}': lightrag openai provider import failed: {exc}")
        from functools import partial

        from lightrag.utils import EmbeddingFunc

        dim = int(_get("LIGHTRAG_EMBEDDING_DIM", "1024"))
        embed_base = getattr(openai_embed, "func", openai_embed)
        return BackendSpec(
            kind=kind,
            llm_func=partial(
                openai_complete_if_cache, model=cfg["model"],
                base_url=cfg["base"], api_key=cfg["key"],
            ),
            embedding_func=EmbeddingFunc(
                embedding_dim=dim, max_token_size=8192,
                func=partial(embed_base, model=_get("LIGHTRAG_EMBED_MODEL", cfg["model"]),
                             base_url=cfg["base"], api_key=cfg["key"]),
            ),
            embedding_dim=dim,
            llm_model_name=cfg["model"],
            describe=f"{kind} {cfg['model']} @ {cfg['base']}",
        )
    raise BackendConfigError(f"unknown backend '{kind}' (want mock|ollama|nim|openrouter)")


def make_backend(kind: str = "mock") -> BackendSpec:
    kind = (kind or "mock").lower()
    if kind == "mock":
        return make_mock_backend()
    return make_live_backend(kind)


# ---------------------------------------------------------------------------
# Loader: tree sections -> LightRAG docs (no re-chunking)
# ---------------------------------------------------------------------------

_DATE_RE = re.compile(r"(20\d\d-\d\d-\d\d)")


@dataclass
class TreeDoc:
    stable_id: str  # == node_id (fallback doc_id#ordinal)
    doc_id: str
    node_id: str
    title: str
    text: str  # single chunk text (title + path + summary + keywords)
    metadata: dict[str, Any] = field(default_factory=dict)


def _section_text(section: dict[str, Any], tree_file: str) -> str:
    node_id = section.get("node_id") or ""
    doc_id = section.get("doc_id") or ""
    title = section.get("title") or ""
    path = section.get("section_path_text") or " / ".join(section.get("section_path") or [])
    summary = section.get("summary") or ""
    keywords = ", ".join(section.get("keywords") or [])
    header = f"[node_id={node_id} doc_id={doc_id}]"
    parts = [header, f"Title: {title}", f"Section: {path}", summary.strip()]
    if keywords:
        parts.append(f"Keywords: {keywords}")
    return "\n".join(p for p in parts if p)


def load_tree_sections(tree_files: Iterable[str] | None = None) -> list[TreeDoc]:
    """Flatten tree shards to one TreeDoc per section (root included).

    Stable id = existing ``node_id``; fallback ``doc_id + '#' + ordinal``
    when ``node_id`` is absent. No re-chunking is performed here or
    downstream (each TreeDoc is inserted as exactly one LightRAG chunk).
    """
    files = sorted(tree_files) if tree_files else sorted(glob.glob(TREES_GLOB, recursive=True))
    docs: list[TreeDoc] = []
    for tf in files:
        with open(tf, encoding="utf-8") as fh:
            root = json.load(fh)
        rel = os.path.relpath(tf, REPO_ROOT).replace(os.sep, "/")
        date_m = _DATE_RE.search(rel)
        stack = [root]
        sections: list[dict] = []
        while stack:
            node = stack.pop(0)
            sections.append(node)
            stack.extend(node.get("children") or [])
        root_source = root.get("source_path") or root.get("source_url") or ""
        for sec in sections:
            node_id = sec.get("node_id") or f"{sec.get('doc_id')}#{sec.get('ordinal')}"
            docs.append(TreeDoc(
                stable_id=node_id,
                doc_id=sec.get("doc_id") or "",
                node_id=node_id,
                title=sec.get("title") or "",
                text=_section_text(sec, rel),
                metadata={
                    "doc_id": sec.get("doc_id") or "",
                    "tree_file": rel,
                    "source": root_source,
                    "date": date_m.group(1) if date_m else None,
                    "section_path": sec.get("section_path_text") or "",
                    "level": sec.get("level"),
                    "ordinal": sec.get("ordinal"),
                    "token_count": sec.get("token_count"),
                },
            ))
    return docs


def docs_from_fixture(fixture_path: str | Path) -> list[TreeDoc]:
    """Build TreeDocs from a verbatim-sections fixture file (mock validation)."""
    recs = json.load(open(fixture_path, encoding="utf-8"))
    docs: list[TreeDoc] = []
    for r in recs:
        node_id = r.get("node_id") or f"{r.get('doc_id')}#{r.get('ordinal')}"
        docs.append(TreeDoc(
            stable_id=node_id,
            doc_id=r.get("doc_id") or "",
            node_id=node_id,
            title=r.get("title") or "",
            text=_section_text(r, str(fixture_path)),
            metadata={
                "doc_id": r.get("doc_id") or "",
                "tree_file": str(fixture_path),
                "source": "",
                "date": None,
                "section_path": r.get("section_path_text") or "",
                "level": r.get("level"),
                "ordinal": r.get("ordinal"),
                "token_count": r.get("token_count"),
                "batch": r.get("batch", ""),
            },
        ))
    return docs


# ---------------------------------------------------------------------------
# LightRAG wiring
# ---------------------------------------------------------------------------


def _check_lightrag_version() -> str:
    try:
        from lightrag import __version__ as installed
    except Exception:
        try:
            import importlib.metadata as md

            installed = md.version("lightrag-hku")
        except Exception:
            installed = "unknown"
    if installed != LIGHTRAG_VERSION_PIN:
        print(
            f"WARNING: lightrag-hku {installed} != pinned {LIGHTRAG_VERSION_PIN}",
            file=sys.stderr,
        )
    return installed


def create_rag(working_dir: str | Path, backend: BackendSpec):
    """Create (not yet initialised) LightRAG with the maritime guidance."""
    from lightrag import LightRAG

    wd = Path(working_dir)
    if Path(wd).resolve() != (DEFAULT_WORKING_DIR).resolve() and os.environ.get(
        "LIGHTRAG_ALLOW_CUSTOM_DIR", ""
    ) != "1":
        # Working dir is fixed by design; custom dirs need an explicit opt-in
        # (used by tests via temp dirs if ever needed).
        raise RuntimeError(
            f"refusing working_dir {wd}: LightRAG store must live at "
            f"{DEFAULT_WORKING_DIR} (set LIGHTRAG_ALLOW_CUSTOM_DIR=1 to override)"
        )
    wd.mkdir(parents=True, exist_ok=True)
    rag = LightRAG(
        working_dir=str(wd),
        llm_model_func=backend.llm_func,
        embedding_func=backend.embedding_func,
        addon_params={"entity_types_guidance": MARITIME_ENTITY_TYPES_GUIDANCE},
        entity_extract_max_gleaning=0,
        llm_model_name=backend.llm_model_name,
    )
    return rag


async def insert_docs(rag, docs: list[TreeDoc]) -> list[str]:
    """Insert one LightRAG document per tree section (single-chunk each).

    Uses ``ainsert_custom_chunks`` with exactly one chunk per document, so
    LightRAG performs no re-chunking of tree sections. Re-inserting an
    already-PROCESSED ``doc_id`` is idempotent (no rebuild); unseen ids are
    appended incrementally.
    """
    await rag.initialize_storages()
    inserted: list[str] = []
    for d in docs:
        await rag.ainsert_custom_chunks(d.text, [d.text], doc_id=d.stable_id)
        inserted.append(d.stable_id)
    await rag.finalize_storages()
    return inserted


def _graph_storage(rag):
    """Graph storage instance (lightrag-hku>=1.5 names it chunk_entity_relation_graph)."""
    for attr in ("chunk_entity_relation_graph", "graph_storage"):
        store = getattr(rag, attr, None)
        if store is not None and not isinstance(store, str):
            return store
    raise RuntimeError("no graph storage instance found on LightRAG object")


async def _all_nodes(rag) -> list[tuple[str, dict]]:
    raw = await _graph_storage(rag).get_all_nodes()
    return _pairs(raw)


async def _all_edges(rag) -> list[tuple[str, str, dict]]:
    raw = await _graph_storage(rag).get_all_edges()
    out = []
    for item in raw or []:
        if isinstance(item, (list, tuple)) and len(item) == 3:
            out.append((item[0], item[1], item[2] or {}))
        elif isinstance(item, dict):
            out.append((item.get("source", ""), item.get("target", ""), item))
    return out


def _pairs(raw) -> list[tuple[str, dict]]:
    out = []
    if isinstance(raw, dict):
        return [(k, v or {}) for k, v in raw.items()]
    for item in raw or []:
        if isinstance(item, (list, tuple)) and len(item) == 2:
            out.append((item[0], item[1] or {}))
        elif isinstance(item, dict):
            key = item.get("entity_name", item.get("id", ""))
            out.append((key, item))
    return out


async def graph_counts(rag) -> dict[str, int]:
    nodes = await _all_nodes(rag)
    edges = await _all_edges(rag)
    return {"nodes": len(nodes), "edges": len(edges)}


async def query_with_citations(
    rag, question: str, mode: str = "mix", top_k: int = 20
) -> dict[str, Any]:
    """Answer a question and return ``(answer, citations)``.

    Citations are ``node_id``/``doc_id`` join keys recovered from the
    retrieved context's ``[node_id=...]`` provenance headers and cross-checked
    against graph entities' source chunks (each source chunk's ``full_doc_id``
    is the section ``node_id``, since doc_id == node_id and chunks are 1:1
    with sections).
    """
    from lightrag import QueryParam

    param = QueryParam(mode=mode, top_k=top_k, only_need_context=False)
    answer = await rag.aquery(question, param=param)
    if not isinstance(answer, str):
        parts = [c async for c in answer]
        answer = "".join(parts)
    ctx_param = QueryParam(mode=mode, top_k=top_k, only_need_context=True)
    context = await rag.aquery(question, param=ctx_param)
    if not isinstance(context, str):
        parts = [c async for c in context]
        context = "".join(parts)
    cited = sorted(set(_NODE_ID_RE.findall(context or "")))
    # cross-check: every cited node_id should back ≥1 graph entity source
    nodes = await _all_nodes(rag)
    chunks = await _chunk_doc_map(rag)
    backed: list[str] = []
    for nid in cited:
        for _name, data in nodes:
            srcs = str((data or {}).get("source_id") or "")
            chunk_ids = [s for s in re.split(r"<SEP>", srcs) if s]
            if any(chunks.get(c) == nid for c in chunk_ids):
                backed.append(nid)
                break
    return {
        "question": question,
        "answer": answer,
        "citations": [{"node_id": n, "graph_backed": n in backed} for n in cited],
    }


async def _chunk_doc_map(rag) -> dict[str, str]:
    """chunk_id -> full_doc_id (== section node_id).

    Primary path: the ``text_chunks`` KV store (``get_by_id`` per chunk id
    seen in entity ``source_id`` fields). Fallback: read the JsonKVStorage
    chunk files directly from the working dir.
    """
    out: dict[str, str] = {}
    store = getattr(rag, "text_chunks", None)
    if store is not None and not isinstance(store, str):
        try:
            nodes = await _all_nodes(rag)
            cids: set[str] = set()
            for _n, data in nodes:
                cids.update(s for s in re.split(r"<SEP>", str((data or {}).get("source_id") or "")) if s)
            if cids:
                recs = await store.get_by_ids(list(cids))
                for cid, rec in zip(cids, recs or []):
                    if isinstance(rec, dict) and rec.get("full_doc_id"):
                        out[cid] = rec["full_doc_id"]
                if out:
                    return out
        except Exception:
            pass
    # fallback: read JsonKVStorage files directly from the working dir
    wd = Path(getattr(rag, "working_dir", "."))
    for jf in wd.glob("kv_store_*chunk*.json"):
        try:
            data = json.loads(jf.read_text(encoding="utf-8"))
        except Exception:
            continue
        for k, v in data.items():
            if isinstance(v, dict) and v.get("full_doc_id"):
                out[k] = v["full_doc_id"]
    return out


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def cmd_build(args: argparse.Namespace) -> None:
    backend = make_backend(args.backend)
    _check_lightrag_version()
    docs = load_tree_sections(args.trees and glob.glob(args.trees, recursive=True))
    if args.limit:
        docs = docs[: args.limit]
    rag = create_rag(args.working_dir, backend)
    inserted = asyncio.run(insert_docs(rag, docs))
    print(f"inserted {len(inserted)} section-docs via {backend.describe}")


def cmd_query(args: argparse.Namespace) -> None:
    from lightrag import LightRAG

    backend = make_backend(args.backend)
    rag = LightRAG(
        working_dir=str(Path(args.working_dir)),
        llm_model_func=backend.llm_func,
        embedding_func=backend.embedding_func,
        addon_params={"entity_types_guidance": MARITIME_ENTITY_TYPES_GUIDANCE},
        entity_extract_max_gleaning=0,
        llm_model_name=backend.llm_model_name,
    )

    async def _run():
        await rag.initialize_storages()
        try:
            return await query_with_citations(rag, args.question, mode=args.mode)
        finally:
            await rag.finalize_storages()

    res = asyncio.run(_run())
    print(json.dumps(res, indent=1)[:4000])


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="LightRAG layer over knowledge/trees/")
    ap.add_argument("--working-dir", default=str(DEFAULT_WORKING_DIR))
    ap.add_argument("--backend", default="mock", help="mock|ollama|nim|openrouter")
    sub = ap.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("build", help="build graph from tree sections")
    b.add_argument("--trees", default=None)
    b.add_argument("--limit", type=int, default=0)
    b.set_defaults(fn=cmd_build)
    q = sub.add_parser("query", help="query with node_id citations")
    q.add_argument("question")
    q.add_argument("--mode", default="mix")
    q.set_defaults(fn=cmd_query)
    args = ap.parse_args(argv)
    args.fn(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
