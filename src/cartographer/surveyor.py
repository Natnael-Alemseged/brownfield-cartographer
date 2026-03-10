"""
Surveyor agent: static structure analysis of a codebase.

Uses tree-sitter for AST parsing, builds module import graph, git velocity,
PageRank, SCC, and dead-code candidate detection. Writes .cartography/module_graph.json.
"""

import logging
import os
import re
import subprocess
from pathlib import Path
from typing import Optional

import networkx as nx
from networkx.readwrite import json_graph

from src.cartographer.core.language_router import LanguageRouter
from src.models import ClassInfo, FunctionInfo, ModuleNode
from src.tools.repo_tools import analyze_git_progression, extract_git_history

logger = logging.getLogger(__name__)

# Default extensions we consider as "modules" for the graph
MODULE_EXTENSIONS = {".py", ".sql", ".yaml", ".yml", ".js", ".jsx", ".ts", ".tsx"}

# Directories to skip when walking repo (same as common ignore patterns)
SKIP_DIRS = {".git", ".venv", "venv", "env", "node_modules", "__pycache__", ".mypy_cache", ".ruff_cache"}

# Entry points: exclude from dead-code candidate when in_degree == 0
ENTRY_POINT_NAMES = {"main.py", "app.py", "__main__.py"}
ENTRY_POINT_SUBSTR = "dag"


def _resolve_python_relative_import(
    from_module: str, repo_root: Path, file_path: Path
) -> Optional[str]:
    """
    Resolve a relative Python import (e.g. '.foo' or '..bar') to a repo-relative module path.
    Returns None if resolution fails or is outside repo.
    """
    if not from_module or from_module == ".":
        # Same package: directory of current file as module path
        return str(file_path.parent).replace(os.sep, ".") if file_path.parent.name else ""
    if from_module.startswith("."):
        # Relative: count dots and walk up from file_path's parent
        parts = from_module.split(".")
        level = 0
        for p in parts:
            if p == "":
                level += 1
            else:
                break
        rel_parts = [p for p in parts if p]
        try:
            current = file_path.resolve().parent
            for _ in range(level - 1):
                current = current.parent
            for p in rel_parts:
                current = current / p
            if not current.is_dir():
                current = current.parent  # it's a file
            try:
                rel = current.relative_to(repo_root.resolve())
                return str(rel).replace(os.sep, ".")
            except ValueError:
                return None
        except Exception:
            return None
    return from_module


def _get_node_text(source_bytes: bytes, node) -> str:
    """Extract text for a tree-sitter node from source bytes."""
    return source_bytes[node.start_byte : node.end_byte].decode("utf-8", errors="replace")


def _collect_python_imports(source_bytes: bytes, tree, file_path: Path, repo_root: Path) -> list[str]:
    """Extract import targets from Python AST (tree-sitter), resolved to module paths."""
    imports: list[str] = []
    try:
        from tree_sitter import Node
        def walk(node: Node) -> None:
            if node.type == "import_statement":
                # import foo [as bar] -> foo
                name_node = node.child_by_field_name("name")
                if name_node:
                    imp = _get_node_text(source_bytes, name_node).strip()
                    if imp:
                        imports.append(imp)
            elif node.type == "import_from_statement":
                # from x import ... -> x
                module_node = node.child_by_field_name("module_name")
                if module_node:
                    mod = _get_node_text(source_bytes, module_node).strip()
                else:
                    # from . import ...
                    mod = "."
                rel_node = node.child_by_field_name("relative_import")
                if rel_node:
                    mod = "." + mod if mod else "."
                resolved = _resolve_python_relative_import(mod, repo_root, file_path)
                if resolved is not None:
                    imports.append(resolved)
            for i in range(node.child_count):
                walk(node.child(i))
        walk(tree.root_node)
    except Exception as e:
        logger.warning("Error collecting Python imports from %s: %s", file_path, e)
    return imports


def _collect_python_functions_and_classes(source_bytes: bytes, tree) -> tuple[list[FunctionInfo], list[ClassInfo]]:
    """Extract public functions and classes from Python AST."""
    functions: list[FunctionInfo] = []
    classes: list[ClassInfo] = []
    try:
        from tree_sitter import Node
        def walk(node: Node) -> None:
            if node.type == "function_definition":
                name_node = node.child_by_field_name("name")
                if name_node:
                    name = _get_node_text(source_bytes, name_node)
                    if not name.startswith("_"):
                        sig = _get_node_text(source_bytes, node)
                        functions.append(FunctionInfo(
                            name=name,
                            signature=sig.split("\n")[0][:200],
                            line_start=node.start_point[0] + 1,
                            line_end=node.end_point[0] + 1,
                        ))
            elif node.type == "class_definition":
                name_node = node.child_by_field_name("name")
                if name_node:
                    name = _get_node_text(source_bytes, name_node)
                    bases: list[str] = []
                    arg_list = node.child_by_field_name("superclasses")
                    if arg_list:
                        for i in range(arg_list.child_count):
                            c = arg_list.child(i)
                            if c.type == "identifier":
                                bases.append(_get_node_text(source_bytes, c))
                    # parent_classes = full inheritance chain (here we only have direct bases)
                    parent_classes = list(bases)
                    if not name.startswith("_"):
                        classes.append(ClassInfo(
                            name=name,
                            bases=bases,
                            parent_classes=parent_classes,
                            line_start=node.start_point[0] + 1,
                            line_end=node.end_point[0] + 1,
                        ))
            for i in range(node.child_count):
                walk(node.child(i))
        walk(tree.root_node)
    except Exception as e:
        logger.warning("Error collecting functions/classes: %s", e)
    return functions, classes


def _cyclomatic_python(source_bytes: bytes, tree) -> int:
    """Count decision points in Python AST for cyclomatic complexity."""
    from tree_sitter import Node
    count = 0
    decision_types = {
        "if_statement", "elif_clause", "else_clause",
        "for_statement", "while_statement",
        "conditional_expression",
        "and_operator", "or_operator",
        "try_statement", "except_clause",
        "match_statement", "case_clause",
    }
    def walk(node: Node) -> None:
        nonlocal count
        if node.type in decision_types:
            count += 1
        for i in range(node.child_count):
            walk(node.child(i))
    walk(tree.root_node)
    return max(0, count)


def _collect_cross_language_refs(
    source_bytes: bytes, rel_path: str, path_to_node: dict[str, ModuleNode]
) -> list[tuple[str, str]]:
    """
    Detect SQL/YAML references in Python or JS source (e.g. open('query.sql'), dbt.ref()).
    Returns list of (target_path, edge_type) where edge_type is REFERENCES_SQL or REFERENCES_CONFIG.
    """
    results: list[tuple[str, str]] = []
    try:
        source = source_bytes.decode("utf-8", errors="replace")
    except Exception:
        return results
    # String literals: '...sql', "...sql", '...yaml', "...yml", etc.
    for m in re.finditer(r"""['"]([^'"]*\.(?:sql|yaml|yml))['"]""", source, re.IGNORECASE):
        candidate = m.group(1).replace("\\", "/").lstrip("/")
        # Resolve relative to current file's directory
        base = str(Path(rel_path).parent) if Path(rel_path).parent != Path(".") else ""
        if base:
            resolved = (Path(base) / candidate).as_posix()
        else:
            resolved = candidate
        if resolved in path_to_node:
            ext = Path(resolved).suffix.lower()
            edge_type = "REFERENCES_SQL" if ext == ".sql" else "REFERENCES_CONFIG"
            results.append((resolved, edge_type))
        else:
            # Try candidate as-is
            if candidate in path_to_node:
                ext = Path(candidate).suffix.lower()
                edge_type = "REFERENCES_SQL" if ext == ".sql" else "REFERENCES_CONFIG"
                results.append((candidate, edge_type))
    # dbt.ref('model_name') or ref('model_name') -> look for models/<name>.sql
    for m in re.finditer(r"(?:dbt\.)?ref\s*\(\s*['\"]([^'\"]+)['\"]", source):
        name = m.group(1).strip()
        for p in path_to_node:
            if p.endswith(f"{name}.sql") or p.endswith(f"/{name}.sql"):
                results.append((p, "REFERENCES_SQL"))
                break
    return results


def _analyze_python_file(
    file_path: Path, repo_root: Path, rel_path: str, source_bytes: bytes,
    language_router: LanguageRouter, parser
) -> Optional[ModuleNode]:
    """Build ModuleNode for a Python file using tree-sitter."""
    lang = language_router.get_language(rel_path)
    if lang is None:
        return None
    parser.language = lang
    tree = parser.parse(source_bytes)
    if tree.root_node.has_error:
        logger.warning("Parse errors in %s", rel_path)
    imports = _collect_python_imports(source_bytes, tree, file_path, repo_root)
    functions, classes = _collect_python_functions_and_classes(source_bytes, tree)
    lines = source_bytes.count(b"\n") + (1 if source_bytes else 0)
    comment_lines = source_bytes.count(b"\n#") + (1 if source_bytes.strip().startswith(b"#") else 0)
    comment_ratio = comment_lines / lines if lines else 0.0
    cyclomatic = _cyclomatic_python(source_bytes, tree)
    complexity = float(lines) + cyclomatic  # combine LOC and cyclomatic
    return ModuleNode(
        path=rel_path,
        language="python",
        imports=imports,
        public_functions=functions,
        classes=classes,
        lines_of_code=lines,
        comment_ratio=comment_ratio,
        cyclomatic_complexity=cyclomatic,
        complexity_score=complexity,
    )


def _analyze_generic_file(
    file_path: Path, repo_root: Path, rel_path: str, source_bytes: bytes,
    language_router: LanguageRouter, parser
) -> Optional[ModuleNode]:
    """Build minimal ModuleNode for SQL/YAML/JS/TS (no import extraction for now)."""
    lang = language_router.get_language(rel_path)
    if lang is None:
        return None
    lines = source_bytes.count(b"\n") + (1 if source_bytes else 0)
    ext = Path(rel_path).suffix.lower()
    if ext in (".yaml", ".yml"):
        language = "yaml"
    elif ext in (".js", ".jsx"):
        language = "javascript"
    elif ext in (".ts", ".tsx"):
        language = "typescript"
    else:
        language = "sql"
    return ModuleNode(
        path=rel_path,
        language=language,
        lines_of_code=lines,
        complexity_score=float(lines),
        cyclomatic_complexity=0,
    )


class Surveyor:
    """
    Static structure analyst: builds module graph, git velocity, and dead-code candidates.
    Accepts only local filesystem paths; CLI handles cloning.
    """

    def __init__(self, output_dir: Optional[Path] = None) -> None:
        self.output_dir = Path(output_dir) if output_dir else Path(".cartography")
        self._language_router = LanguageRouter()
        self._git_velocity_cache: dict[tuple[str, int], tuple[dict[str, int], set[str]]] = {}

    def analyze_module(self, path: str) -> Optional[ModuleNode]:
        """
        Analyze a single file and return a ModuleNode, or None if unsupported/unparseable.
        """
        from tree_sitter import Language, Parser

        path_obj = Path(path)
        if not path_obj.is_file():
            logger.warning("Not a file: %s", path)
            return None
        try:
            source_bytes = path_obj.read_bytes()
        except (OSError, UnicodeDecodeError) as e:
            logger.warning("Could not read %s: %s", path, e)
            return None
        # Assume repo root is parent of path for single-file; for repo-wide use analyze_repository
        repo_root = path_obj.parent
        rel_path = path_obj.name
        ext = path_obj.suffix.lower()
        if ext == ".py":
            lang = self._language_router.get_language(rel_path)
            if lang is None:
                return None
            parser = Parser(lang)
            return _analyze_python_file(
                path_obj, repo_root, rel_path, source_bytes,
                self._language_router, parser
            )
        if ext in (".sql", ".yaml", ".yml"):
            lang = self._language_router.get_language(rel_path)
            if lang is None:
                return None
            parser = Parser(lang)
            return _analyze_generic_file(
                path_obj, repo_root, rel_path, source_bytes,
                self._language_router, parser
            )
        return None

    def extract_git_velocity(self, repo_path: str, days: int = 30) -> tuple[dict[str, int], set[str]]:
        """
        Return (path -> change_count, set of top 20% high-churn file paths).
        """
        repo = Path(repo_path)
        if not repo.is_dir():
            return {}, set()
        try:
            result = subprocess.run(
                ["git", "log", f"--since={days} days ago", "--name-only", "--pretty=format:"],
                cwd=repo,
                capture_output=True,
                text=True,
                timeout=60,
            )
        except (subprocess.TimeoutExpired, FileNotFoundError) as e:
            logger.warning("Git velocity failed: %s", e)
            return {}, set()
        if result.returncode != 0:
            return {}, set()
        count: dict[str, int] = {}
        for line in result.stdout.splitlines():
            line = line.strip()
            if not line or line.startswith("commit") or line.startswith("Author"):
                continue
            count[line] = count.get(line, 0) + 1
        if not count:
            return count, set()
        sorted_paths = sorted(count.keys(), key=lambda p: count[p], reverse=True)
        top_20_percent_count = max(1, (len(sorted_paths) * 20 + 99) // 100)
        high_churn = set(sorted_paths[:top_20_percent_count])
        return count, high_churn

    def build_module_graph(self, repo_path: str) -> nx.DiGraph:
        """
        Build a NetworkX DiGraph of modules (nodes = path, edges = imports).
        Populates node attributes with ModuleNode data and adds PageRank and SCC info.
        """
        from tree_sitter import Language, Parser
        from tree_sitter_python import language as python_lang

        repo = Path(repo_path)
        if not repo.is_dir():
            return nx.DiGraph()
        logger.info("Building module graph for repository at %s", repo)
        G = nx.DiGraph()
        path_to_node: dict[str, ModuleNode] = {}
        # Collect all relevant files
        for root, dirs, files in os.walk(repo):
            dirs[:] = [d for d in dirs if d not in SKIP_DIRS and not d.startswith(".")]
            for f in files:
                ext = Path(f).suffix.lower()
                if ext not in MODULE_EXTENSIONS:
                    continue
                full = Path(root) / f
                try:
                    rel = full.relative_to(repo)
                except ValueError:
                    continue
                rel_str = str(rel).replace("\\", "/")
                if self._language_router.get_language(rel_str) is None:
                    continue
                try:
                    source_bytes = full.read_bytes()
                except (OSError, UnicodeDecodeError) as e:
                    logger.warning("Skip %s: %s", rel_str, e)
                    continue
                if ext == ".py":
                    parser = Parser(Language(python_lang()))
                    node = _analyze_python_file(
                        full, repo, rel_str, source_bytes,
                        self._language_router, parser
                    )
                else:
                    parser = Parser(self._language_router.get_language(rel_str))
                    node = _analyze_generic_file(
                        full, repo, rel_str, source_bytes,
                        self._language_router, parser
                    )
                if node:
                    path_to_node[rel_str] = node
                    G.add_node(rel_str, module_node=node.model_dump(mode="json"))

        logger.info("Found %d modules (nodes)", len(path_to_node))
        # Edges: imports (only Python imports for now)
        import_edge_count = 0
        for rel_str, node in path_to_node.items():
            for imp in node.imports:
                # imp might be "foo.bar" or "foo"; we store edges to same-repo modules
                target = imp
                if target not in path_to_node:
                    # Try as path: foo/bar -> foo/bar.py
                    candidate = target.replace(".", "/") + ".py"
                    if candidate in path_to_node:
                        target = candidate
                    else:
                        continue
                G.add_edge(rel_str, target, edge_type="IMPORTS")
                import_edge_count += 1
        logger.info("Added %d IMPORTS edges", import_edge_count)

        # Cross-language edges: REFERENCES_SQL, REFERENCES_CONFIG (from Python/JS/TS)
        ref_edge_count = 0
        code_extensions = {".py", ".js", ".jsx", ".ts", ".tsx"}
        for rel_str in path_to_node:
            if Path(rel_str).suffix.lower() not in code_extensions:
                continue
            full = repo / rel_str
            if not full.is_file():
                continue
            try:
                source_bytes = full.read_bytes()
            except Exception:
                continue
            for target_path, edge_type in _collect_cross_language_refs(source_bytes, rel_str, path_to_node):
                if G.has_node(target_path) and target_path != rel_str:
                    G.add_edge(rel_str, target_path, edge_type=edge_type)
                    ref_edge_count += 1
        logger.info("Added %d cross-language REFERENCES_SQL/REFERENCES_CONFIG edges", ref_edge_count)

        # PageRank
        if G.number_of_nodes() > 0:
            pr = nx.pagerank(G)
            for n, v in pr.items():
                if G.has_node(n):
                    G.nodes[n]["pagerank"] = v
            logger.info("Computed PageRank for %d nodes", G.number_of_nodes())

        # SCC (strongly connected components)
        sccs = list(nx.strongly_connected_components(G))
        for i, comp in enumerate(sccs):
            for n in comp:
                if G.has_node(n):
                    G.nodes[n]["scc_id"] = i
                    G.nodes[n]["scc_size"] = len(comp)
        logger.info("Found %d strongly connected component(s)", len(sccs))

        # Dead-code candidates: in_degree == 0 excluding entry points (main.py, app.py, DAGs)
        def _is_entry_point(path_str: str) -> bool:
            path_lower = path_str.lower()
            if path_str.endswith("/main.py") or path_str == "main.py":
                return True
            if path_str.endswith("/app.py") or path_str == "app.py":
                return True
            if path_str.endswith("/__main__.py") or path_str == "__main__.py":
                return True
            if ENTRY_POINT_SUBSTR in path_lower:
                return True
            return False

        for rel_str, node in path_to_node.items():
            in_degree = G.in_degree(rel_str)
            has_exports = bool(node.public_functions or node.classes) or rel_str.endswith(".py")
            if has_exports and in_degree == 0 and not _is_entry_point(rel_str):
                node.is_dead_code_candidate = True
            G.nodes[rel_str]["module_node"] = node.model_dump(mode="json")
        dead_count = sum(1 for n in path_to_node.values() if n.is_dead_code_candidate)
        logger.info("Marked %d dead-code candidate(s) (in_degree=0, excluding entry points)", dead_count)

        # Git velocity (run once per repo, cached)
        cache_key = (repo_path, 30)
        if cache_key not in self._git_velocity_cache:
            self._git_velocity_cache[cache_key] = self.extract_git_velocity(repo_path, 30)
        velocity, high_churn = self._git_velocity_cache[cache_key]
        logger.info("Computed git velocity: %d file(s) changed in last 30 days (cached)", len(velocity))
        for rel_str in path_to_node:
            path_to_node[rel_str].change_velocity_30d = velocity.get(rel_str, 0)
            if G.has_node(rel_str):
                G.nodes[rel_str]["module_node"] = path_to_node[rel_str].model_dump(mode="json")

        return G

    def analyze_repository(
        self, repo_path: str, output_dir: Optional[Path] = None
    ) -> Path:
        """
        Run full survey: build module graph and write .cartography/module_graph.json.
        Returns the path to the written file.
        """
        out = Path(output_dir) if output_dir else self.output_dir
        out.mkdir(parents=True, exist_ok=True)
        logger.info("Output directory: %s", out)
        G = self.build_module_graph(repo_path)
        # Serialize for JSON: use node-link format; module_node is a dict (Pydantic model_dump)
        data = json_graph.node_link_data(G)
        out_file = out / "module_graph.json"
        import json
        logger.info("Writing %s ...", out_file)
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        logger.info("Wrote %s", out_file)

        # Git commit history and progression analysis (from repo_tools)
        try:
            logger.info("Extracting git commit history ...")
            history_text = extract_git_history(repo_path)
            if history_text and not history_text.strip().startswith("Error"):
                history_file = out / "git_history.txt"
                history_file.write_text(history_text, encoding="utf-8")
                logger.info("Wrote %s (%d commit lines)", history_file, len(history_text.strip().splitlines()))
                logger.info("Analyzing git progression (atomic commits, phases, conventional commits) ...")
                progression = analyze_git_progression(history_text)
                progression_file = out / "git_progression.txt"
                progression_file.write_text(progression, encoding="utf-8")
                logger.info("Wrote %s", progression_file)
            else:
                logger.warning("No git history extracted (not a git repo or error): %s", (history_text or "")[:200])
        except Exception as e:
            logger.warning("Git history/progression analysis failed: %s", e)

        return out_file
