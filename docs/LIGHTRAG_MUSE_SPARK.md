# LightRAG graph layer — muse-spark (STATUS BOARD Decision 3)

Scaffold + mock validation only. No production batch has been run and none is
authorized by this work (pilot scope ends at scaffold + validation).

## 0. Decision

- Graph layer = **LightRAG** (`lightrag-hku==1.5.7`, sandbox pip install) built
  **over** `knowledge/trees/`, joined on `node_id`/`doc_id`.
- Relational core = existing SQLite spine — **kept as-is, untouched, not
  imported** by this layer.
- No shard replacement, no re-chunking, no overwrites of
  `knowledge/trees/`, `knowledge/derived/`, `knowledge/chunks/`,
  `process_knowledge.py`, workflows, or requirements.

## 1. Architecture

```
reports/*.html/md ──process_knowledge.py──▶ knowledge/trees/**/*.json (sections, stable node_id)
                                                    │
                                                    │  loader: 1 section = 1 doc (NO re-chunking)
                                                    ▼
                                    knowledge/graph/lightrag_store/  (LightRAG working_dir)
                                     ├─ graph_chunk_entity_relation.graphml (entities/relations + source chunk ids)
                                     ├─ kv_store_{full_docs,text_chunks,full_entities,full_relations,…}.json
                                     ├─ vdb_{chunks,entities,relationships}.json (NanoVectorDB)
                                     ├─ kv_store_doc_status.json (incremental bookkeeping)
                                     └─ kv_store_llm_response_cache.json
                                                    │
                                        chunk.full_doc_id == tree node_id
                                                    │
                                                    ▼
                                      query helper ─▶ (answer, [{node_id, graph_backed}])
```

Join contract:

- LightRAG `doc_id` = tree section `node_id` (fallback `doc_id + '#' + ordinal`
  when `node_id` is absent — not observed in the corpus, 8849/8849 files carry
  `node_id`).
- One chunk per document via `ainsert_custom_chunks(full_text, [chunk],
  doc_id=node_id)` — LightRAG's own splitter never runs on tree text.
- Chunk text header `[node_id=… doc_id=…]` keeps provenance inside retrieved
  context; entity `source_id` chunk ids resolve through the `text_chunks` KV
  store back to `full_doc_id == node_id`. Citations are cross-checked:
  `graph_backed=true` means ≥1 graph entity sources a chunk of that node.

Code: `scripts/graph/lightrag_build.py`
(`loader` = `load_tree_sections`/`docs_from_fixture`, `insert_docs`,
`query_with_citations`, backends `make_backend`).

## 2. Maritime extraction guidance

`MARITIME_ENTITY_TYPES_GUIDANCE` (injected via LightRAG `addon_params`
`entity_types_guidance`, the documented 1.5.x override point) declares:

- `vessel` (named ship; type/dwt/year kept), `owner_charterer`,
  `route_port`, `rate_index` (rate/TCE/price/index/bunker), `week_date`;
- preferred relations `SOLD_TO / CHARTERED_BY / FIXED_AT_RATE / CALLED_AT /
  REPORTED_IN_WEEK`;
- descriptions stay factual to the input text, names verbatim.

`entity_extract_max_gleaning=0` (single extraction pass; deterministic mock).

## 3. Incremental-update design (weekly corpus growth)

- LightRAG tracks per-`doc_id` status in `kv_store_doc_status.json`.
  Inserting unseen section `node_id`s **appends** (create mode with full
  bookkeeping); re-inserting a PROCESSED id is idempotent (patch/no-op).
- Weekly flow: new/changed tree sections (ledger-diff on `documents.jsonl`
  `source_hash` + `compiler_version`, same binding as §2 of the inventory) →
  `insert_docs(new_sections)` on the existing store. No rebuild.
- Proof artefact: `scripts/graph/tests/run_mock_validation.py` phase 2
  asserts original chunk-id map preserved, original node set ⊆ new node set
  (0 lost), node/edge counts grow (see §6).

## 4. Backend wiring (no new vendor, CI secrets)

Same env names the pipeline already uses (`.github/workflows/`
`process_knowledge.yml:90-99`, `daily_knowledge_update.yml:66-79`,
`daily_brief.yml:64-89`; client defaults mirror `scripts/generate_brief.py`):

| backend | env |
|---|---|
| `mock` (default) | none — offline rule-based LLM + hash embeddings (dim 128) |
| `ollama` | `OLLAMA_BASE_URL`, `OLLAMA_MODEL` (+ optional `OLLAMA_API_KEY`, `OLLAMA_EMBED_MODEL`, `LIGHTRAG_EMBEDDING_DIM` default 1024) |
| `nim` | `NIM_API_KEY` (or `NVIDIA_API_KEY` alias), `NIM_BASE_URL` (default `https://integrate.api.nvidia.com/v1`), `NIM_MODEL` (default `meta/llama-3.3-70b-instruct`) |
| `openrouter` | `OPENROUTER_API_KEY`, `OPENROUTER_BASE_URL` (default `https://openrouter.ai/api/v1`), `OPENROUTER_MODEL` (default `meta-llama/llama-3.3-70b-instruct`) |

No keys in sandbox: live backends raise `BackendConfigError` naming the exact
missing variable and perform zero network I/O (validated with scrubbed env,
see §6). Live paths are implemented but **not exercised** — first live call
happens in CI with secrets, per §7.

LightRAG 1.5.7 API notes (pinned; recorded from the installed package):
`LightRAG(working_dir, llm_model_func, embedding_func, addon_params,
entity_extract_max_gleaning, llm_model_name)`; `await
ainsert_custom_chunks(text, [text], doc_id=…)`; `await aquery(q,
QueryParam(mode, top_k, only_need_context))`; graph via
`chunk_entity_relation_graph.get_all_nodes/get_all_edges()` (list-of-dict
rows); chunks via `text_chunks.get_by_id*`. Mock LLM emits
`entity<|#|>name<|#|>type<|#|>desc` / `relation<|#|>…` + `<|COMPLETE|>`,
keyword JSON `{high_level_keywords, low_level_keywords}`.

## 5. What runs where

- **CI, manual/dispatch only (not yet run):** full-tree build
  `python scripts/graph/lightrag_build.py build --backend ollama|nim`
  (defaults to `knowledge/graph/lightrag_store`); weekly append of new
  sections the same way. No workflow file added in this slice.
- **Query usage:** `query_with_citations(rag, question, mode="mix")` →
  `{"answer", "citations": [{"node_id", "graph_backed"}]}` (CLI:
  `lightrag_build.py query "…" --backend …`).
- **Sandbox/CI offline gate:** `python
  scripts/graph/tests/run_mock_validation.py` (cwd-independent; writes only
  the store dir + `scripts/graph/tests/fixtures/validation_log.{json,md}`).

## 6. Validation results (mock, 2026-09-07, `lightrag-hku==1.5.7`)

Fixtures (`scripts/graph/tests/fixtures/`, verbatim tree sections):
`fixture_sections_initial.json` (20 sections: 10 × Advanced Shipping wk35 +
10 × Star Asia wk35) + `fixture_sections_incremental.json` (5 sections) +
`fixtures_manifest.json` (source tree SHAs).

| check | result |
|---|---|
| build 20 section-docs | 20 chunks (exactly 1/section — no re-chunk), 55 entities / 44 relations, 0 entities without `source_id` |
| cross-doc vessel join | 17 entities span both docs, incl. **7 vessels** (`Mount Dampier`, `Efraim A`, `Amaryllis`, `Spar Scorpio`, `Arklow Spirit`, `Hellstugutinden`, `Seasenator`) — one node each, source chunks from both broker docs (same wk35 S&P deals, two reporters) |
| incremental +5 | 57 nodes (+2), 0 original nodes lost, original chunk-id map 100% preserved (no rebuild); store 12 files / 674,101 B on disk (669,239 B at build snapshot; drift = query-phase appends to kv_store_llm_response_cache.json) |
| query `"Which vessels were reported sold in week 35 and at what prices?"` | answer names real vessels with prices/buyers; 20 citations, all ∈ fixture `node_id`s, all `graph_backed`, incl. both sales sections |
| fail-closed (keys scrubbed) | `ollama`/`nim`/`openrouter` all raise `BackendConfigError` naming the missing var |
| zero stray writes | `git status --porcelain` before/after shows only `scripts/graph/`, `knowledge/graph/` (this doc + inventory line are the remaining allowed paths) |

Mock-fidelity notes (documented limits, not defects of the layer): the mock
canonicalises vessel names to title case to emulate a production LLM's
consistent naming (LightRAG merge itself is case-sensitive — observed, not
worked around in layer code); broker table layouts (Advanced Shipping
TYPE-first rows vs Star Asia NAME-first) are parsed by the mock's table
reader; a real LLM needs no such reader.

Store-size decision: mock store = 12 files, **674,101 B total on disk** (sum of `knowledge/graph/lightrag_store/*` bytes; build-phase snapshot was 669,239 B — drift is `kv_store_llm_response_cache.json` appended at query phase after the build snapshot) — small, so
the blobs are kept in-tree under `knowledge/graph/lightrag_store/` as the
validation artefact (a production full-corpus store will be GB-scale and must
live in CI artefacts/object storage, never in git — see §7).

## 7. Production rollout steps (EXPLICITLY NOT RUN)

No batch authorization exists beyond pilot scope. When authorized, in order:

1. Provision CI secrets (`NIM_*` / `OLLAMA_*` — already wired for the
   knowledge pipeline; no new vendor).
2. Add a manual/dispatch workflow calling `lightrag_build.py build` on a
   runner with secrets; store backend = CI artefact/object storage (full
   corpus ≈ 8.8k sections; do NOT commit the production store to git).
3. Pilot full-build on a 200-section slice; check entity precision on vessel
   names against S&P tables; tune `entity_types_guidance` if needed.
4. Schedule weekly incremental append driven by the `documents.jsonl`
   ledger diff; monitor `doc_status` FAILED counts and node-growth deltas.
5. Wire `query_with_citations` into the brief/research path as an optional
   context source (citations mandatory in output).

Out of scope for this slice: workflow file, production store, live LLM spend.
