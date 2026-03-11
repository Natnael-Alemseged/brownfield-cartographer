"""
SQL lineage extraction via sqlglot (PostgreSQL, BigQuery, Snowflake, DuckDB).

Extracts table dependencies from SELECT/FROM/JOIN/WITH/CTE and dbt ref()/source().
Unparseable SQL is logged and skipped. Output includes per-query source/target mappings
with source file and line range. Distinguishes read (FROM/JOIN) vs write (CREATE/INSERT).
"""

import logging
import re
from pathlib import Path
from typing import Any

try:
    import sqlglot
except ImportError:
    sqlglot = None

logger = logging.getLogger(__name__)

SUPPORTED_DIALECTS = ("postgres", "bigquery", "snowflake", "duckdb")


def extract_sql_lineage(
    source: str,
    rel_path: str = "",
    dialect: str = "postgres",
) -> dict[str, Any]:
    """
    Parse SQL and return structured lineage:
    - tables_in, tables_out, refs, sources (aggregate)
    - queries: list of per-query mappings with source_file, line_start, line_end, tables_in, tables_out
    Tries 3+ dialects. Handles dbt ref() and source(). Logs and returns best-effort on parse failure.
    """
    result: dict[str, Any] = {
        "tables_in": [],
        "tables_out": [],
        "refs": [],
        "sources": [],
        "queries": [],
        "source_file": rel_path,
    }
    # dbt ref/source (work even with Jinja)
    for m in re.finditer(r"ref\s*\(\s*['\"]([^'\"]+)['\"]", source):
        result["refs"].append(m.group(1))
    for m in re.finditer(
        r"source\s*\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]", source
    ):
        result["sources"].append(f"{m.group(1)}.{m.group(2)}")

    if not sqlglot:
        result["tables_in"] = list(dict.fromkeys(result["refs"] + result["sources"]))
        result["tables_out"] = [Path(rel_path).stem] if rel_path else []
        return result

    parsed = None
    last_error = None
    for d in (dialect,) + tuple(d for d in SUPPORTED_DIALECTS if d != dialect):
        try:
            parsed = sqlglot.parse_one(source, dialect=d)
            break
        except Exception as e:
            last_error = e
            continue

    if parsed is None:
        logger.warning(
            "SQL lineage: unparseable SQL in %s (tried %s): %s",
            rel_path or "<string>",
            SUPPORTED_DIALECTS,
            last_error,
        )
        result["tables_in"] = list(dict.fromkeys(result["refs"] + result["sources"]))
        result["tables_out"] = [Path(rel_path).stem] if rel_path else []
        return result

    # Line range from root (approximate for whole statement)
    line_start = 1
    line_end = 1
    if hasattr(parsed, "start_line") and parsed.start_line:
        line_start = parsed.start_line
    if hasattr(parsed, "end_line") and parsed.end_line:
        line_end = parsed.end_line

    tables_in = []
    tables_out = []
    for table in parsed.find_all(sqlglot.exp.Table):
        name = table.name
        if table.db:
            name = f"{table.db}.{name}"
        tables_in.append(name)
    for create in parsed.find_all(sqlglot.exp.Create):
        if create.this:
            tables_out.append(create.this.name)
    for insert in parsed.find_all(sqlglot.exp.Insert):
        if insert.this:
            tables_out.append(insert.this.name)

    # Filter out CTE aliases so internal CTE names are not treated as external datasets
    cte_aliases: set[str] = set()
    try:
        for cte in parsed.find_all(sqlglot.exp.CTE):
            alias = getattr(cte, "alias", None)
            name = getattr(alias, "name", None) if alias is not None else None
            if isinstance(name, str):
                cte_aliases.add(name)
    except Exception:
        # Best-effort; if CTE traversal fails, fall back to unfiltered tables_in
        cte_aliases = set()
    tables_in = [t for t in tables_in if t not in cte_aliases]

    if not tables_out and rel_path:
        tables_out = [Path(rel_path).stem]

    result["tables_in"] = list(dict.fromkeys(tables_in + result["refs"] + result["sources"]))
    result["tables_out"] = list(dict.fromkeys(tables_out))
    result["queries"].append({
        "source_file": rel_path,
        "line_start": line_start,
        "line_end": line_end,
        "tables_in": list(dict.fromkeys(tables_in)),
        "tables_out": list(dict.fromkeys(tables_out)),
    })
    return result
