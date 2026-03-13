"""
Hydrologist agent: data flow and lineage analysis.

Four analyzers: PythonDataFlow, SQLLineage, DAGConfig, Notebook.
Outputs: lineage graph (PRODUCES/CONSUMES), lineage_summary.md.
"""

import logging
import os
import re
from pathlib import Path
from typing import Optional

from src.cartographer.core.language_router import LanguageRouter
from src.cartographer.knowledge_graph import LineageGraph
from src.models import TransformationNode
from src.tracing.cartography_trace import CartographyTrace

logger = logging.getLogger(__name__)

SKIP_DIRS = {".git", ".venv", "venv", "env", "node_modules", "__pycache__"}


def _get_node_text(source_bytes: bytes, node) -> str:
    return source_bytes[node.start_byte : node.end_byte].decode("utf-8", errors="replace")


# ---- 1. PythonDataFlowAnalyzer ----
def _collect_string_args_python(source_bytes: bytes, tree, call_patterns: list[tuple[str, str]]) -> list[tuple[str, str, int, int]]:
    """
    Use tree-sitter to find call patterns (module.method) and extract first string literal arg.
    Returns (dataset_name_or_dynamic, kind, line_start, line_end).
    """
    from tree_sitter import Node
    results = []
    def walk(node: Node) -> None:
        if node.type != "call":
            for i in range(node.child_count):
                walk(node.child(i))
            return
        # call: function (maybe attribute) + argument_list
        fn = node.child_by_field_name("function")
        if not fn or fn.type != "attribute":
            for i in range(node.child_count):
                walk(node.child(i))
            return
        obj = fn.child_by_field_name("object")
        attr = fn.child_by_field_name("attribute")
        if not obj or not attr:
            for i in range(node.child_count):
                walk(node.child(i))
            return
        obj_text = _get_node_text(source_bytes, obj).strip()
        attr_text = _get_node_text(source_bytes, attr).strip()
        key = (obj_text, attr_text)
        for (mod, method) in call_patterns:
            if mod in obj_text and attr_text == method:
                args = node.child_by_field_name("arguments")
                if args and args.child_count >= 2:
                    first_arg = args.child(1)
                    if first_arg.type == "string":
                        val = _get_node_text(source_bytes, first_arg).strip().strip("'\"").strip('"\'')
                        if val:
                            results.append((val, f"{mod}.{method}", node.start_point[0] + 1, node.end_point[0] + 1))
                else:
                    results.append(("dynamic_reference", f"{mod}.{method}", node.start_point[0] + 1, node.end_point[0] + 1))
                break
        for i in range(node.child_count):
            walk(node.child(i))
    walk(tree.root_node)
    return results


def python_data_flow_analyzer(
    repo_path: Path, rel_path: str, source_bytes: bytes, language_router: LanguageRouter
) -> list[TransformationNode]:
    """
    Extract pandas/SQLAlchemy/PySpark read/write from Python files.
    Logs dynamic_reference for f-strings/variables; does not crash.
    """
    from tree_sitter import Language, Parser
    from tree_sitter_python import language as python_lang
    lang = language_router.get_language(rel_path)
    if lang is None:
        return []
    try:
        parser = Parser(Language(python_lang()))
        tree = parser.parse(source_bytes)
    except Exception as e:
        logger.warning("PythonDataFlow: parse failed %s: %s", rel_path, e)
        return []
    read_patterns = [
        ("pd", "read_csv"), ("pandas", "read_csv"), ("pd", "read_sql"), ("pandas", "read_sql"),
        ("spark", "read"), ("df", "read"), ("session", "execute"), ("engine", "execute"),
        ("conn", "execute"), ("connection", "execute"),
    ]
    write_patterns = [
        ("df", "to_csv"), ("df", "to_sql"), ("df", "write"), ("table", "to_sql"),
        ("spark", "write"), ("session", "execute"),
    ]
    nodes = []
    try:
        # 1. Static AST analysis (tree-sitter)
        reads = _collect_string_args_python(source_bytes, tree, read_patterns)
        for (dataset, kind, start, end) in reads:
            if dataset == "dynamic_reference":
                logger.warning("PythonDataFlow: dynamic_reference in %s (line %d)", rel_path, start)
                continue
            nodes.append(TransformationNode(
                source_datasets=[dataset],
                target_datasets=[],
                transformation_type="code",
                source_file=rel_path,
                line_range=(start, end),
            ))
        
        writes = _collect_string_args_python(source_bytes, tree, write_patterns)
        for (dataset, kind, start, end) in writes:
            if dataset == "dynamic_reference":
                continue
            nodes.append(TransformationNode(
                source_datasets=[],
                target_datasets=[dataset],
                transformation_type="code",
                source_file=rel_path,
                line_range=(start, end),
            ))

        # 2. Add fallback for execute() with raw SQL string literals if not captured by tree-sitter
        # (Already handled by reads pattern "session.execute" etc. above if it matches call structure)
    except Exception as e:
        logger.warning("PythonDataFlow: %s: %s", rel_path, e)
    return nodes


# ---- 2. SQLLineageAnalyzer ----
def sql_lineage_analyzer(
    repo_path: Path, rel_path: str, source_bytes: bytes
) -> list[TransformationNode]:
    """
    Use sqlglot (via extract_sql_lineage) to parse SQL and extract table dependencies.
    Handles dbt ref('model') and source('schema','table'). Supports 4 dialects.
    Unparseable SQL is logged by extract_sql_lineage; returns empty list or best-effort.
    """
    try:
        source = source_bytes.decode("utf-8", errors="replace")
    except Exception:
        return []
    from src.analyzers.sql_lineage import extract_sql_lineage

    result = extract_sql_lineage(source, rel_path=rel_path)
    tables_in = result.get("tables_in", [])
    tables_out = result.get("tables_out", [])
    if not tables_in and not tables_out:
        return []
    # Use line range from first query if available
    line_start, line_end = 0, 0
    queries = result.get("queries", [])
    if queries:
        q = queries[0]
        line_start = q.get("line_start", 0)
        line_end = q.get("line_end", 0)
    return [TransformationNode(
        source_datasets=list(dict.fromkeys(tables_in)),
        target_datasets=list(dict.fromkeys(tables_out)),
        transformation_type="sql",
        source_file=rel_path,
        line_range=(line_start, line_end),
        sql_query_if_applicable=source[:2000],
    )]


# ---- 3. DAGConfigAnalyzer ----
def dag_config_analyzer(
    repo_path: Path, rel_path: str, source_bytes: bytes, language_router: LanguageRouter
) -> list[TransformationNode]:
    """
    Airflow: task deps; dbt: schema.yml sources/exposures/tests; Prefect if present.
    """
    nodes = []
    ext = Path(rel_path).suffix.lower()
    try:
        source = source_bytes.decode("utf-8", errors="replace")
    except Exception:
        return []
    if ext in (".yaml", ".yml"):
        # dbt schema.yml: sources, exposures, tests
        if "sources:" in source or "exposures:" in source:
            for m in re.finditer(r"name:\s*['\"]?([\w\.]+)['\"]?", source):
                nodes.append(TransformationNode(
                    source_datasets=[],
                    target_datasets=[m.group(1)],
                    transformation_type="config_defined",
                    source_file=rel_path,
                    line_range=(0, 0),
                ))
    if ext == ".py" and ("airflow" in source.lower() or "DAG" in source):
        # Simple: task_id and set_downstream/set_upstream or >> <<
        for m in re.finditer(r"task_id\s*=\s*['\"]([^'\"]+)['\"]", source):
            nodes.append(TransformationNode(
                source_datasets=[],
                target_datasets=[m.group(1)],
                transformation_type="config_defined",
                source_file=rel_path,
                line_range=(0, 0),
            ))
    return nodes


# ---- 4. NotebookAnalyzer ----
def notebook_analyzer(
    repo_path: Path, rel_path: str, source_bytes: bytes
) -> list[TransformationNode]:
    """
    Parse .ipynb JSON; extract read/write from cell source (pandas, SQLAlchemy).
    Return TransformationNode with notebook_cell metadata (cell_index, cell_type).
    """
    try:
        import nbformat
        nb = nbformat.reads(source_bytes.decode("utf-8", errors="replace"), as_version=4)
    except Exception as e:
        logger.warning("NotebookAnalyzer: %s: %s", rel_path, e)
        return []
    nodes = []
    for idx, cell in enumerate(nb.cells):
        src = cell.get("source", "")
        if isinstance(src, list):
            src = "".join(src)
        for m in re.finditer(r"(?:read_csv|read_sql|to_csv|to_sql)\s*\(\s*['\"]([^'\"]+)['\"]", src):
            path_or_table = m.group(1)
            if "read" in m.group(0).lower():
                nodes.append(TransformationNode(
                    source_datasets=[path_or_table],
                    target_datasets=[],
                    transformation_type="notebook",
                    source_file=rel_path,
                    line_range=(0, 0),
                    notebook_cell={"cell_index": idx, "cell_type": cell.get("cell_type", "code")},
                ))
            else:
                nodes.append(TransformationNode(
                    source_datasets=[],
                    target_datasets=[path_or_table],
                    transformation_type="notebook",
                    source_file=rel_path,
                    line_range=(0, 0),
                    notebook_cell={"cell_index": idx, "cell_type": cell.get("cell_type", "code")},
                ))
    return nodes


# ---- Hydrologist ----
class Hydrologist:
    """
    Runs all four analyzers on a repo and builds the lineage graph.
    """

    def __init__(self, output_dir: Optional[Path] = None) -> None:
        self.output_dir = Path(output_dir) if output_dir else Path(".cartography")
        self._language_router = LanguageRouter()
        self._lineage = LineageGraph()

    def analyze_repository(
        self,
        repo_path: str,
        output_dir: Optional[Path] = None,
        run_id: Optional[str] = None,
    ) -> tuple[Path, Path]:
        """
        Run all analyzers, build lineage graph, write lineage_graph.json and lineage_summary.md.
        Returns (path_to_json, path_to_summary).
        """
        out = Path(output_dir) if output_dir else self.output_dir
        out.mkdir(parents=True, exist_ok=True)
        trace = CartographyTrace(out, agent="hydrologist", run_id=run_id)
        repo = Path(repo_path)
        self._lineage = LineageGraph()

        for root, dirs, files in os.walk(repo):
            dirs[:] = [d for d in dirs if d not in SKIP_DIRS and not d.startswith(".")]
            for f in files:
                full = Path(root) / f
                try:
                    rel = full.relative_to(repo)
                except ValueError:
                    continue
                rel_str = str(rel).replace("\\", "/")
                try:
                    source_bytes = full.read_bytes()
                except Exception as e:
                    logger.warning("Skip file (read error): %s — %s", rel_str, e)
                    continue
                ext = Path(rel_str).suffix.lower()
                if ext == ".py":
                    for t in python_data_flow_analyzer(repo, rel_str, source_bytes, self._language_router):
                        self._lineage.add_transformation(t)
                elif ext in (".sql", ".sqlx"):
                    for t in sql_lineage_analyzer(repo, rel_str, source_bytes):
                        self._lineage.add_transformation(t)
                elif ext in (".yaml", ".yml"):
                    for t in dag_config_analyzer(repo, rel_str, source_bytes, self._language_router):
                        self._lineage.add_transformation(t)
                elif ext == ".ipynb":
                    for t in notebook_analyzer(repo, rel_str, source_bytes):
                        self._lineage.add_transformation(t)

        json_path = out / "lineage_graph.json"
        self._lineage.write_json(json_path)
        summary_path = out / "lineage_summary.md"
        self._write_summary(summary_path)
        trace.log(
            "lineage_complete",
            evidence_source="lineage_graph.json",
            status="success",
        )
        return json_path, summary_path

    def _write_summary(self, path: Path) -> None:
        sources = self._lineage.find_sources()
        sinks = self._lineage.find_sinks()
        lines = [
            "# Lineage Summary",
            "",
            f"## Sources (in_degree=0)\n{len(sources)} dataset(s)",
            "",
        ]
        for s in sorted(sources)[:50]:
            lines.append(f"- {s}")
        if len(sources) > 50:
            lines.append(f"- ... and {len(sources) - 50} more")
        lines.extend(["", "## Sinks (out_degree=0)", ""])
        for s in sorted(sinks)[:50]:
            lines.append(f"- {s}")
        if len(sinks) > 50:
            lines.append(f"- ... and {len(sinks) - 50} more")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("\n".join(lines), encoding="utf-8")
        logger.info("Wrote %s", path)

    def trace_lineage(self, dataset_name: str, direction: str = "upstream") -> tuple[list[str], list[tuple[str, str, str]]]:
        return self._lineage.trace_lineage(dataset_name, direction)

    def blast_radius(self, node_name: str, direction: str = "downstream") -> dict[str, list[str]]:
        return self._lineage.blast_radius(node_name, direction)

    def blast_radius_filtered(
        self,
        node_name: str,
        direction: str = "downstream",
        transformation_type: Optional[str] = None,
    ) -> dict[str, list[str]]:
        """Blast radius considering only edges with the given transformation_type."""
        return self._lineage.blast_radius_filtered(node_name, direction, transformation_type)

    def paths_between(self, source: str, target: str, max_paths: int = 50) -> list[list[str]]:
        """Enumerate simple paths from source to target (lineage explanation)."""
        return self._lineage.paths_between(source, target, max_paths)

    def rank_datasets(self, top_n: int = 20) -> list[dict]:
        """Rank dataset nodes by fan-in + fan-out (most critical first)."""
        return self._lineage.rank_datasets(top_n)

    def find_sources(self) -> list[str]:
        return self._lineage.find_sources()

    def find_sinks(self) -> list[str]:
        return self._lineage.find_sinks()
