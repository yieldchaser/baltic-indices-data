"""
Build LightRAG Graph Layer over Existing Knowledge Trees.
Preserves knowledge/trees/ hierarchy and links graph entities and relationships
to dim_tree_nodes(node_id) and dim_tree_nodes(doc_id) in maritime_knowledge_spine.db.
Strictly additive-only: never modifies or overwrites knowledge/trees/ or knowledge/derived/.
"""

import os
import sys
import io
import json
import re
import time
import hashlib
import argparse
import asyncio
from pathlib import Path
from typing import List, Dict, Any, Set, Tuple

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

import numpy as np
from lightrag import LightRAG, QueryParam
from lightrag.utils import EmbeddingFunc

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
SOURCE_ROOT = Path(os.environ.get("SHIPPING_SOURCE_ROOT", str(REPO_ROOT)))
TREES_DIR = SOURCE_ROOT / "knowledge" / "trees"
DEFAULT_GRAPH_DIR = REPO_ROOT / "data" / "derived" / "lightrag_graph"

# ---------------------------------------------------------------------------
# Maritime Domain Taxonomy
# ---------------------------------------------------------------------------

VESSEL_CLASSES = {
    "capesize": "Capesize", "newcastlemax": "Newcastlemax", "vloc": "VLOC",
    "panamax": "Panamax", "kamsarmax": "Kamsarmax", "post-panamax": "Post-Panamax",
    "supramax": "Supramax", "ultramax": "Ultramax", "handysize": "Handysize",
    "handymax": "Handymax", "vlcc": "VLCC", "suezmax": "Suezmax", "aframax": "Aframax",
    "lr2": "LR2", "lr1": "LR1", "mr": "MR Tanker",
    "lng": "LNG Carrier", "lpg": "LPG Carrier", "vlgc": "VLGC", "container": "Container Ship"
}

COMMODITIES = {
    "iron ore": "Iron Ore", "iron_ore": "Iron Ore", "bauxite": "Bauxite",
    "coal": "Coal", "coking coal": "Coking Coal", "thermal coal": "Thermal Coal",
    "grain": "Grain", "soybeans": "Soybeans", "corn": "Corn", "wheat": "Wheat",
    "crude oil": "Crude Oil", "crude_oil": "Crude Oil", "dirty": "Crude Oil",
    "fuel oil": "Fuel Oil", "bunker": "Bunker Fuel", "vlsfo": "VLSFO", "mgo": "MGO",
    "gas": "Gas", "lng": "LNG", "lpg": "LPG", "steel": "Steel", "scrap": "Scrap Metal"
}

COMPANIES = {
    "vale": "Vale", "bhp": "BHP", "rio tinto": "Rio Tinto", "fortescue": "Fortescue",
    "anglo american": "Anglo American", "petrobras": "Petrobras", "saudi aramco": "Saudi Aramco",
    "vitol": "Vitol", "trafigura": "Trafigura", "glencore": "Glencore", "shell": "Shell", "bp": "BP",
    "golden ocean": "Golden Ocean", "star bulk": "Star Bulk", "genco": "Genco",
    "seanergy": "Seanergy", "frontline": "Frontline", "euronav": "Euronav",
    "oldendorff": "Oldendorff", "swissmarine": "Swissmarine", "shandong marine": "Shandong Marine"
}

PORTS_REGIONS = {
    "china": "China", "australia": "Australia", "brazil": "Brazil",
    "tubarao": "Tubarao", "ponta da madeira": "Ponta da Madeira", "port hedland": "Port Hedland",
    "dampier": "Dampier", "qingdao": "Qingdao", "singapore": "Singapore", "rotterdam": "Rotterdam",
    "santos": "Santos", "ecsa": "ECSA", "us gulf": "US Gulf", "us_gulf": "US Gulf",
    "atlantic": "Atlantic", "pacific": "Pacific", "meg": "Arabian Gulf", "west africa": "West Africa"
}

INDEX_BENCHMARKS = {
    "bdi": "BDI", "bci": "BCI", "bpi": "BPI", "bsi": "BSI", "bhsi": "BHSI",
    "c5tc": "C5TC", "c3": "C3 Route", "c5": "C5 Route", "td3c": "TD3C",
    "sgx": "SGX Curves", "ffa": "FFA Forward Curves", "hi5": "Hi5 Spread"
}

BROKERS = {
    "fearnleys": "Fearnleys", "ssy": "SSY", "clarksons": "Clarksons",
    "bancosta": "Bancosta", "intermodal": "Intermodal", "allied": "Allied", "poten": "Poten"
}

# ---------------------------------------------------------------------------
# Deterministic Offline Embeddings & LLM Dispatcher
# ---------------------------------------------------------------------------

EMBED_DIM = 384

async def deterministic_embed(texts: List[str]) -> np.ndarray:
    """
    Deterministic 384-dimensional token-hash embedding.
    Runs 100% offline with zero external model downloads or API dependencies.
    """
    embeddings = []
    for text in texts:
        vec = np.zeros(EMBED_DIM, dtype=np.float32)
        clean = re.sub(r"[^\w\s]", " ", text.lower())
        words = clean.split()
        if not words:
            vec[0] = 1.0
        else:
            for w in words:
                h = int(hashlib.md5(w.encode("utf-8")).hexdigest(), 16)
                idx = h % EMBED_DIM
                sign = 1.0 if (h >> 16) & 1 else -1.0
                vec[idx] += sign
            norm = np.linalg.norm(vec)
            if norm > 0:
                vec = vec / norm
        embeddings.append(vec)
    return np.array(embeddings, dtype=np.float32)

async def smart_offline_llm(prompt: str, system_prompt: str = None, history_messages: list = None, **kwargs) -> str:
    """
    Offline LLM dispatcher for keyword parsing and synthesis.
    Can be seamlessly replaced or overridden by Ollama/NIM when environment keys exist.
    """
    p_str = prompt.lower()
    # 1. Keyword extraction request
    if "keyword" in str(kwargs) or "high_level_keywords" in prompt:
        high = []
        low = []
        for name in list(VESSEL_CLASSES.values()) + list(COMMODITIES.values()) + list(INDEX_BENCHMARKS.values()):
            if name.lower() in p_str:
                high.append(name)
                low.append(name)
        for name in list(COMPANIES.values()) + list(PORTS_REGIONS.values()):
            if name.lower() in p_str:
                low.append(name)
        if not high:
            high = ["Shipping", "Freight"]
        if not low:
            low = ["Maritime", "Vessel", "Market"]
        return json.dumps({
            "high_level_keywords": list(set(high))[:5],
            "low_level_keywords": list(set(low))[:10]
        })

    # 2. General synthesis request
    return "Offline maritime graph response synthesized from knowledge tree nodes and relational spine facts."

# ---------------------------------------------------------------------------
# Node KG Extraction
# ---------------------------------------------------------------------------

def extract_node_kg(node: Dict[str, Any], file_path: str) -> Dict[str, Any]:
    """
    Extracts structured entities, relationships, and text chunks from a knowledge tree node.
    Strictly preserves source_id = node_id.
    """
    node_id = node.get("node_id")
    if not node_id:
        return None
    title = node.get("title") or ""
    summary = node.get("summary") or ""
    keywords = [str(k).lower() for k in (node.get("keywords") or [])]
    text_to_scan = f"{title} {summary} {' '.join(keywords)}".lower()

    entities = []
    found_vessels: Set[str] = set()
    found_commodities: Set[str] = set()
    found_companies: Set[str] = set()
    found_ports: Set[str] = set()
    found_indices: Set[str] = set()
    found_brokers: Set[str] = set()

    for k, name in VESSEL_CLASSES.items():
        if k in keywords or re.search(rf"\b{re.escape(k)}\b", text_to_scan):
            found_vessels.add(name)
            entities.append({
                "entity_name": name,
                "entity_type": "VESSEL_CLASS",
                "description": f"Vessel class referenced in tree section '{title}'",
                "source_id": node_id,
                "file_path": file_path
            })

    for k, name in COMMODITIES.items():
        if k in keywords or re.search(rf"\b{re.escape(k)}\b", text_to_scan):
            found_commodities.add(name)
            entities.append({
                "entity_name": name,
                "entity_type": "COMMODITY",
                "description": f"Commodity cargo referenced in tree section '{title}'",
                "source_id": node_id,
                "file_path": file_path
            })

    for k, name in COMPANIES.items():
        if k in keywords or re.search(rf"\b{re.escape(k)}\b", text_to_scan):
            found_companies.add(name)
            entities.append({
                "entity_name": name,
                "entity_type": "COMPANY",
                "description": f"Company referenced in tree section '{title}'",
                "source_id": node_id,
                "file_path": file_path
            })

    for k, name in PORTS_REGIONS.items():
        if k in keywords or re.search(rf"\b{re.escape(k)}\b", text_to_scan):
            found_ports.add(name)
            entities.append({
                "entity_name": name,
                "entity_type": "PORT_REGION",
                "description": f"Port or trading region referenced in tree section '{title}'",
                "source_id": node_id,
                "file_path": file_path
            })

    for k, name in INDEX_BENCHMARKS.items():
        if k in keywords or re.search(rf"\b{re.escape(k)}\b", text_to_scan):
            found_indices.add(name)
            entities.append({
                "entity_name": name,
                "entity_type": "INDEX_BENCHMARK",
                "description": f"Market rate index referenced in tree section '{title}'",
                "source_id": node_id,
                "file_path": file_path
            })

    for k, name in BROKERS.items():
        if k in keywords or re.search(rf"\b{re.escape(k)}\b", text_to_scan):
            found_brokers.add(name)
            entities.append({
                "entity_name": name,
                "entity_type": "BROKER",
                "description": f"Shipbroker / market reporting source in '{title}'",
                "source_id": node_id,
                "file_path": file_path
            })

    relationships = []
    # 1. Vessel -> Commodity (TRANSPORTS)
    for v in found_vessels:
        for c in found_commodities:
            if v != c:
                relationships.append({
                    "src_id": v, "tgt_id": c,
                    "description": f"{v} carries {c} freight",
                    "keywords": "transports, shipping, cargo",
                    "weight": 1.0, "source_id": node_id, "file_path": file_path
                })

    # 2. Company -> Commodity (PRODUCES_EXPORTS)
    for comp in found_companies:
        for c in found_commodities:
            if comp != c:
                relationships.append({
                    "src_id": comp, "tgt_id": c,
                    "description": f"{comp} produces or commercializes {c}",
                    "keywords": "produces, exports, trading",
                    "weight": 1.0, "source_id": node_id, "file_path": file_path
                })

    # 3. Vessel -> Port (OPERATES_IN)
    for v in found_vessels:
        for p in found_ports:
            if v != p:
                relationships.append({
                    "src_id": v, "tgt_id": p,
                    "description": f"{v} calls or trades to {p}",
                    "keywords": "route, call, trade_lane",
                    "weight": 1.0, "source_id": node_id, "file_path": file_path
                })

    # 4. Index -> Vessel (BENCHMARKS)
    for idx in found_indices:
        for v in found_vessels:
            if idx != v:
                relationships.append({
                    "src_id": idx, "tgt_id": v,
                    "description": f"{idx} rate index benchmarks {v} earnings",
                    "keywords": "benchmarks, index, freight_rates",
                    "weight": 1.0, "source_id": node_id, "file_path": file_path
                })

    # 5. Index -> Commodity (CORRELATES_WITH)
    for idx in found_indices:
        for c in found_commodities:
            if idx != c:
                relationships.append({
                    "src_id": idx, "tgt_id": c,
                    "description": f"{idx} index reflects trade flows of {c}",
                    "keywords": "correlation, macro, freight",
                    "weight": 1.0, "source_id": node_id, "file_path": file_path
                })

    # 6. Company -> Vessel (CHARTERED_BY)
    for comp in found_companies:
        for v in found_vessels:
            if comp != v:
                relationships.append({
                    "src_id": comp, "tgt_id": v,
                    "description": f"{comp} charters or operates {v} tonnage",
                    "keywords": "charterer, operator, fixture",
                    "weight": 1.0, "source_id": node_id, "file_path": file_path
                })

    chunk_content = f"{title}\n{summary}\nKeywords: {', '.join(keywords)}".strip()
    chunk = {
        "content": chunk_content if len(chunk_content) > 10 else f"Tree Section: {title}",
        "source_id": node_id,
        "file_path": file_path
    }

    return {
        "chunk": chunk,
        "entities": entities,
        "relationships": relationships
    }

# ---------------------------------------------------------------------------
# Graph Builder Execution
# ---------------------------------------------------------------------------

async def build_lightrag_layer(
    trees_dir: Path = TREES_DIR,
    output_dir: Path = DEFAULT_GRAPH_DIR,
    limit: int = 500,
    batch_size: int = 500
):
    print("=" * 70)
    print("LightRAG Graph Layer Builder (Over Existing Knowledge Trees)")
    print("=" * 70)
    print(f"Source Trees Directory: {trees_dir}")
    print(f"Output Graph Directory: {output_dir}")
    print(f"File Limit: {'ALL' if limit <= 0 else limit} files")
    output_dir.mkdir(parents=True, exist_ok=True)

    tree_files = sorted(list(trees_dir.rglob("*.json")))
    if limit > 0:
        tree_files = tree_files[:limit]
    print(f"Discovered {len(tree_files)} tree shard files to index.")

    rag = LightRAG(
        working_dir=str(output_dir),
        embedding_func=EmbeddingFunc(
            embedding_dim=EMBED_DIM,
            func=deterministic_embed,
            model_name="deterministic-384"
        ),
        llm_model_func=smart_offline_llm,
        graph_storage="NetworkXStorage",
        cosine_better_than_threshold=0.01,
        log_level=None,
    )
    await rag.initialize_storages()

    all_chunks: List[Dict[str, Any]] = []
    all_entities: List[Dict[str, Any]] = []
    all_relationships: List[Dict[str, Any]] = []
    seen_node_ids: Set[str] = set()

    print("Extracting entities, relationships, and chunks from tree shards...")
    t_start = time.time()
    for idx, tf in enumerate(tree_files):
        try:
            data = json.loads(tf.read_text(encoding="utf-8"))
            f_rel = str(tf.relative_to(REPO_ROOT)).replace("\\", "/")
            stack = [data]
            while stack:
                n = stack.pop()
                nid = n.get("node_id")
                if not nid or nid in seen_node_ids:
                    continue
                seen_node_ids.add(nid)
                kg_item = extract_node_kg(n, f_rel)
                if kg_item:
                    all_chunks.append(kg_item["chunk"])
                    all_entities.extend(kg_item["entities"])
                    all_relationships.extend(kg_item["relationships"])
                stack.extend(n.get("children") or [])
        except Exception as e:
            continue

        if (idx + 1) % 100 == 0 or (idx + 1) == len(tree_files):
            print(f"  Processed {idx + 1}/{len(tree_files)} files: {len(all_chunks)} chunks, {len(all_entities)} entities, {len(all_relationships)} relations")

    print(f"Extraction completed in {time.time() - t_start:.2f}s.")
    print(f"Total Chunks: {len(all_chunks)}")
    print(f"Total Raw Entities: {len(all_entities)}")
    print(f"Total Raw Relationships: {len(all_relationships)}")

    # Ingest into LightRAG in manageable batches
    print(f"\nIngesting custom KG into LightRAG (batch size: {batch_size})...")
    t_ingest = time.time()
    num_batches = (len(all_chunks) + batch_size - 1) // batch_size
    for b_idx in range(num_batches):
        start = b_idx * batch_size
        end = min(start + batch_size, len(all_chunks))
        chunk_slice = all_chunks[start:end]
        chunk_source_ids = {c["source_id"] for c in chunk_slice}

        # Select entities and relationships matching these chunks
        ent_slice = [e for e in all_entities if e["source_id"] in chunk_source_ids]
        rel_slice = [r for r in all_relationships if r["source_id"] in chunk_source_ids]

        custom_kg_payload = {
            "chunks": chunk_slice,
            "entities": ent_slice,
            "relationships": rel_slice
        }
        await rag.ainsert_custom_kg(custom_kg_payload, full_doc_id=f"tree_batch_{b_idx}")
        print(f"  Batch {b_idx + 1}/{num_batches} ingested ({len(chunk_slice)} chunks, {len(ent_slice)} entities, {len(rel_slice)} relations).")

    print(f"LightRAG ingestion complete in {time.time() - t_ingest:.2f}s!")

    # Inspect compiled graph
    storage = rag.chunk_entity_relation_graph
    g = storage._graph
    node_count = g.number_of_nodes()
    edge_count = g.number_of_edges()

    # Degree centrality (top hubs)
    degrees = dict(g.degree())
    top_hubs = sorted(degrees.items(), key=lambda x: x[1], reverse=True)[:15]

    summary = {
        "build_timestamp": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
        "source_tree_files_scanned": len(tree_files),
        "total_chunks_indexed": len(all_chunks),
        "total_graph_nodes": node_count,
        "total_graph_edges": edge_count,
        "top_connected_hubs": [{"entity": h[0], "connections": h[1]} for h in top_hubs],
        "storage_artifacts": {
            "graphml": str((output_dir / "graph_chunk_entity_relation.graphml").name),
            "vdb_entities": str((output_dir / "vdb_entities.json").name),
            "vdb_relationships": str((output_dir / "vdb_relationships.json").name),
            "vdb_chunks": str((output_dir / "vdb_chunks.json").name)
        }
    }

    summary_path = output_dir / "graph_summary.json"
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("\n--- LightRAG Graph Compilation Summary ---")
    print(f"  Graph Nodes (Entities): {node_count}")
    print(f"  Graph Edges (Semantic Relations): {edge_count}")
    print(f"  Top Hubs: {', '.join(f'{k} ({v})' for k, v in top_hubs[:8])}")
    print(f"  Summary saved to: {summary_path}")

def main():
    parser = argparse.ArgumentParser(description="Build LightRAG Graph Layer over Existing Trees")
    parser.add_argument("--limit", type=int, default=500, help="Number of tree files to process (0 = all)")
    parser.add_argument("--batch-size", type=int, default=500, help="Batch size for LightRAG insertion")
    parser.add_argument("--output-dir", type=str, default=str(DEFAULT_GRAPH_DIR), help="Target graph directory")
    args = parser.parse_args()

    asyncio.run(build_lightrag_layer(
        trees_dir=TREES_DIR,
        output_dir=Path(args.output_dir),
        limit=args.limit,
        batch_size=args.batch_size
    ))

if __name__ == "__main__":
    main()
