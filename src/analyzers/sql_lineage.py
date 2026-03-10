"""
SQL lineage extraction via sqlglot (PostgreSQL, BigQuery, Snowflake, DuckDB).

Extracts table dependencies from SELECT/FROM/JOIN/WITH and dbt ref()/source().
"""

import re
from pathlib import Path
from typing import Any

try:
    import sqlglot
except ImportError:
    sqlglot = None


def extract_sql_lineage(
    source: str,
    rel_path: str = "",
    dialect: str = "postgres",
) -> dict[str, Any]:
    """
    Parse SQL and return { "tables_in": [...], "tables_out": [...], "refs": [...], "sources": [...] }.
    Tries dialect then falls back to others. Handles dbt ref('x') and source('s','t').
    """
    result: dict[str, Any] = {"tables_in": [], "tables_out": [], "refs": [], "sources": []}
    for m in re.finditer(r"ref\s*\(\s*['\"]([^'\"]+)['\"]", source):
        result["refs"].append(m.group(1))
    for m in re.finditer(r"source\s*\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]", source):
        result["sources"].append(f"{m.group(1)}.{m.group(2)}")
    if not sqlglot:
        result["tables_in"] = result["refs"] + result["sources"]
        result["tables_out"] = [Path(rel_path).stem] if rel_path else []
        return result
    for d in (dialect, "postgres", "bigquery", "snowflake", "duckdb"):
        try:
            parsed = sqlglot.parse_one(source, dialect=d)
            for table in parsed.find_all(sqlglot.exp.Table):
                name = table.name
                if table.db:
                    name = f"{table.db}.{name}"
                result["tables_in"].append(name)
            for create in parsed.find_all(sqlglot.exp.Create):
                if create.this:
                    result["tables_out"].append(create.this.name)
            for insert in parsed.find_all(sqlglot.exp.Insert):
                if insert.this:
                    result["tables_out"].append(insert.this.name)
            break
        except Exception:
            continue
    if not result["tables_out"] and rel_path:
        result["tables_out"] = [Path(rel_path).stem]
    result["tables_in"] = list(dict.fromkeys(result["tables_in"] + result["refs"] + result["sources"]))
    result["tables_out"] = list(dict.fromkeys(result["tables_out"]))
    return result
