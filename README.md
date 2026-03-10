# The Brownfield Cartographer

Multi-agent codebase and data lineage analysis. Clone repos and analyze with the Surveyor and Hydrologist agents.

## Setup

```bash
uv sync
```

## Structure

- `src/models/` — Pydantic schemas (Node / Edge / Graph)
- `src/analyzers/` — Extraction (AST, SQL, YAML)
- `src/agents/` — Surveyor & Hydrologist
- `src/graph/` — Knowledge graph
- `src/tools/` — Repo tools (clone, analyze)
- `src/cli.py` — Entry point
- `src/orchestrator.py` — Pipeline wiring
