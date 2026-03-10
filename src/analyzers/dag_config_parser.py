"""
Airflow / dbt YAML config parsing for DAG and source definitions.
"""

import re
from pathlib import Path
from typing import Any


def parse_dag_config(source: str, rel_path: str = "") -> dict[str, Any]:
    """
    Parse YAML/config content for Airflow task_ids and dbt sources/exposures.
    Returns { "sources": [...], "exposures": [...], "task_ids": [...] }.
    """
    result: dict[str, Any] = {"sources": [], "exposures": [], "task_ids": []}
    if "sources:" in source or "exposures:" in source:
        for m in re.finditer(r"name:\s*['\"]?([\w\.]+)['\"]?", source):
            result["sources"].append(m.group(1))
    if "task_id" in source or "task_id=" in source:
        for m in re.finditer(r"task_id\s*=\s*['\"]([^'\"]+)['\"]", source):
            result["task_ids"].append(m.group(1))
    return result
