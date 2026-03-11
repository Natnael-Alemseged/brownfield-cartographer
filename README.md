# The Brownfield Cartographer

Multi-agent codebase and data lineage analysis. Clone repos and analyze with the Surveyor and Hydrologist agents.

## Install

```bash
uv sync
```

## Run analysis

From the project root:

```bash
# Full analysis (Surveyor + Hydrologist): writes .cartography/module_graph.json, lineage_graph.json, lineage_summary.md
uv run python main.py analyze <path-or-git-url>

# Survey only: module graph, PageRank, git velocity, dead-code candidates
uv run python main.py survey <path-or-git-url>

# Lineage only: data lineage graph (SQL, Python, dbt, notebooks)
uv run python main.py lineage <path-or-git-url>
```

`<path-or-git-url>` can be a local directory or a GitHub URL (e.g. `https://github.com/dbt-labs/jaffle_shop.git`). For URLs, the repo is cloned to a temp directory, analyzed, and outputs are written to `.cartography/`.

## Cartography artifacts

Running analysis on at least one target codebase produces:

- `.cartography/module_graph.json` — module dependency graph (Surveyor)
- `.cartography/lineage_graph.json` — data lineage (at minimum SQL lineage via sqlglot; Hydrologist)

## Structure

- `src/models/` — Pydantic schemas (Node / Edge / Graph)
- `src/analyzers/` — Extraction (AST, SQL, YAML)
- `src/agents/` — Surveyor & Hydrologist
- `src/graph/` — Knowledge graph
- `src/tools/` — Repo tools (clone, analyze)
- `src/cli.py` — Entry point
- `src/orchestrator.py` — Pipeline wiring

## Graph schema versioning

Stored JSON graphs include a `schema_version` field. See [docs/SCHEMA_VERSIONING.md](docs/SCHEMA_VERSIONING.md) for versioning and migration strategy as the schema evolves.
