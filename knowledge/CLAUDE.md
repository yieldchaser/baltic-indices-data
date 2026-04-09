# Shipping Intelligence Knowledge Base - Schema & Query Contract

## Purpose
Unified corpus of Breakwave Advisors bi-weekly reports (dry bulk + tankers),
Baltic Exchange weekly roundups (dry, tanker, gas, container, ningbo), and
shipping reference books - for market intelligence, historical context, and Q&A.

## Document Schema (frontmatter in every `knowledge/docs/**/*.md`)
```yaml
---
doc_id: breakwave_drybulk_2024-03-05
source: breakwave          # breakwave | baltic | book
category: drybulk          # drybulk | tankers | dry | tanker | gas | container | ningbo | book
date: 2024-03-05           # ISO date; null for books
title: "Dry Bulk Shipping - March 5, 2024"
source_path: reports/drybulk/2024/2024-03-05_Breakwave_Dry_Bulk.pdf
source_url: null           # original page URL when available (mainly Baltic HTML)
document_type: biweekly_report   # biweekly_report | weekly_roundup | reference_book
section_count: 2
vessel_classes: [capesize, panamax, supramax]
regions: [china, brazil, australia, atlantic, pacific]
commodities: [iron_ore, coal, grain, bauxite]
signals:
  bdryff: 1016
  bdryff_30d_pct: -0.1
  bdryff_ytd_pct: -1.1
  bdryff_yoy_pct: -58.3
  bdi_spot: 602
  bdi_30d_pct: -45.1
  bdi_ytd_pct: -60.3
  bdi_yoy_pct: -69.0
  momentum: neutral
  sentiment: negative
  fundamentals: positive
---
```

## Tree Schema (`knowledge/trees/**/*.json`)
Each document now has a section tree. Current trees are shallow (root + natural sections),
but the schema is hierarchical so later phases can add nested nodes without changing the contract.

```json
{
  "node_id": "breakwave_drybulk_2024-03-05__root",
  "doc_id": "breakwave_drybulk_2024-03-05",
  "title": "Dry Bulk Shipping - March 5, 2024",
  "summary": "Short document summary...",
  "keywords": ["capesize", "china", "iron_ore"],
  "page_start": 1,
  "page_end": 2,
  "children": [
    {
      "node_id": "breakwave_drybulk_2024-03-05__s01_overview",
      "parent_id": "breakwave_drybulk_2024-03-05__root",
      "title": "Overview",
      "section_path": ["Overview"],
      "section_path_text": "Overview",
      "level": 1,
      "ordinal": 1,
      "summary": "Section summary...",
      "keywords": ["capesize", "atlantic"],
      "page_start": 1,
      "page_end": 1,
      "token_count": 212,
      "children": []
    }
  ]
}
```

## Chunk Schema (one JSON object per line in `knowledge/chunks/*.jsonl`)
```json
{
  "chunk_id": "breakwave_drybulk_2024-03-05_001",
  "doc_id": "breakwave_drybulk_2024-03-05",
  "source": "breakwave",
  "category": "drybulk",
  "date": "2024-03-05",
  "section": "overview",
  "section_id": "breakwave_drybulk_2024-03-05__s01_overview",
  "section_title": "Overview",
  "section_path": ["Overview"],
  "section_path_text": "Overview",
  "section_level": 1,
  "section_chunk_index": 1,
  "page_start": 1,
  "page_end": 1,
  "text": "...",
  "token_count": 380,
  "keywords": ["capesize", "iron_ore", "china"]
}
```

## Derived Retrieval Artifacts
- `knowledge/derived/signals.jsonl` - structured Breakwave signal history
- `knowledge/derived/themes.jsonl` - themes, entities, and tone per document
- `knowledge/derived/section_index.jsonl` - flattened section-level retrieval index
- `knowledge/derived/topic_evidence.jsonl` - scored section-to-topic evidence rows for the wiki layer
- `knowledge/derived/timelines.json` - ISO-week alignment across Breakwave and Baltic

## Topic Wiki Layer
- `knowledge/config/wiki_topics.json` - topic definitions, scoring hints, and related-topic links
- `knowledge/wiki/*.md` - generated evergreen topic pages with citations back to `doc_id` and `section_id`

## Chunking Rules
- Breakwave reports: chunk by natural sections (`Overview`, `Fundamentals`) at ~450 tokens with 60-token overlap
- Baltic HTML: chunk by natural section heading; split if section > 600 tokens
- Books: 500 tokens, 100-token overlap, while respecting detected chapter boundaries

## Query Instructions for AI Agents
1. Always read this file first.
2. Load `knowledge/manifests/documents.jsonl` for document inventory and source metadata.
3. For section-first retrieval, scan `knowledge/derived/section_index.jsonl` before reading raw chunks.
4. Open the matching `knowledge/trees/**/*.json` file when you need structure, page spans, or explainable navigation.
5. Only fall back to `knowledge/chunks/*.jsonl` once the relevant document/section is identified.
6. Topic overviews: start with `knowledge/wiki/*.md`, then inspect the linked evidence rows in `knowledge/derived/topic_evidence.jsonl`.
7. Signal queries: use `knowledge/derived/signals.jsonl`.
8. Timeline queries: use `knowledge/derived/timelines.json`.
9. Cross-source synthesis: retrieve from both Breakwave and Baltic, then align by ISO week.
10. Always cite: source, date, doc_id, and section title or page span when available.

## Source Registry
- breakwave/drybulk: 2018-present, bi-weekly (~26/year)
- breakwave/tankers: 2023-present, bi-weekly (~26/year)
- baltic/dry, tanker, gas, container, ningbo: 2015-present, weekly
- books: reference titles stored in `reports/` root
