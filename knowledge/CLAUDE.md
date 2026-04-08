# Shipping Intelligence Knowledge Base — Schema & Query Contract

## Purpose
Unified corpus of Breakwave Advisors bi-weekly reports (dry bulk + tankers),
Baltic Exchange weekly roundups (dry, tanker, gas, container, ningbo), and
shipping reference books — for market intelligence, historical context, and Q&A.

## Document Schema (frontmatter in every knowledge/docs/**/*.md)
```yaml
---
doc_id: breakwave_drybulk_2024-03-05
source: breakwave          # breakwave | baltic | book
category: drybulk          # drybulk | tankers | dry | tanker | gas | container | ningbo | book
date: 2024-03-05           # ISO date; null for books
title: "Dry Bulk Shipping — March 5, 2024"
source_path: reports/drybulk/2024/2024-03-05_Breakwave_Dry_Bulk.pdf
document_type: biweekly_report   # biweekly_report | weekly_roundup | reference_book
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

## Chunk Schema (one JSON object per line in knowledge/chunks/*.jsonl)
```json
{
  "chunk_id": "breakwave_drybulk_2024-03-05_001",
  "doc_id": "breakwave_drybulk_2024-03-05",
  "source": "breakwave",
  "category": "drybulk",
  "date": "2024-03-05",
  "section": "main",
  "text": "...",
  "token_count": 380,
  "keywords": ["capesize", "iron_ore", "china"]
}
```

## Chunk Size Rules
- Breakwave reports: 400 tokens, 50-token overlap, max 3 chunks per report
- Baltic HTML: one chunk per vessel-class/commodity section; split if section > 600 tokens
- Books: 500 tokens, 100-token overlap, respect heading boundaries

## Query Instructions for AI Agents
1. Always read this file first.
2. Load `knowledge/manifests/documents.jsonl` for document inventory.
3. Keyword search: scan chunk `keywords` arrays first, then full-text.
4. Signal queries: use `knowledge/derived/signals.jsonl`.
5. Timeline queries: use `knowledge/derived/timelines.json`.
6. Cross-source synthesis: retrieve from both breakwave and baltic, date-align by ISO week.
7. Always cite: source, date, doc_id.
8. Market outlook: retrieve 3 most recent Breakwave reports + matching Baltic week, reason from those.

## Source Registry
- breakwave/drybulk: 2018–present, bi-weekly (~26/year)
- breakwave/tankers: 2023–present, bi-weekly (~26/year)
- baltic/dry, tanker, gas, container, ningbo: 2015–present, weekly
- books: 12 reference titles (shipping economics, maritime history, fleet analysis)
