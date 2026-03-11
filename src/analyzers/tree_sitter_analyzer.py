"""
Multi-language AST parsing with LanguageRouter (tree-sitter, no build_library).

Analyzes a single file and returns typed results per language (PythonAnalysisResult,
SqlAnalysisResult, YamlAnalysisResult) or a generic dict. Handles star imports and
dynamic import hints. Unparseable files are logged and skipped (returns None).
"""

import logging
import os
import re
from pathlib import Path
from typing import Any, Optional

from src.cartographer.core.language_router import LanguageRouter
from src.models import (
    ClassDefResult,
    FunctionDefResult,
    KeyHierarchyEntry,
    PythonAnalysisResult,
    QueryStructureResult,
    SqlAnalysisResult,
    TableRefResult,
    YamlAnalysisResult,
)

logger = logging.getLogger(__name__)


def _resolve_python_relative_import(
    from_module: str, repo_root: Optional[Path], file_path: Path
) -> Optional[str]:
    """Resolve relative Python import to repo-relative module path, or None."""
    if not from_module or from_module == ".":
        return str(file_path.parent).replace(os.sep, ".") if file_path.parent.name else ""
    if from_module.startswith("."):
        parts = from_module.split(".")
        level = sum(1 for p in parts if p == "")
        rel_parts = [p for p in parts if p]
        try:
            current = file_path.resolve().parent
            for _ in range(level - 1):
                current = current.parent
            for p in rel_parts:
                current = current / p
            if not current.is_dir():
                current = current.parent
            if repo_root is not None:
                return str(current.relative_to(repo_root.resolve())).replace(os.sep, ".")
            return str(current).replace(os.sep, ".")
        except (ValueError, OSError):
            return None
    return from_module


def analyze_file(
    file_path: str | Path,
    language_router: Optional[LanguageRouter] = None,
    repo_root: Optional[Path] = None,
) -> Optional[dict[str, Any]]:
    """
    Parse a file with tree-sitter and return a structured AST summary.

    - Python: imports (with relative path resolution when repo_root is set), function
      definitions with signatures and decorators, class definitions with bases.
    - SQL: table references and query structure (FROM/JOIN/WITH) at AST level.
    - YAML: key hierarchies relevant to pipeline config.

    Returns None if unsupported or parse error (logged, not raised).
    """
    path = Path(file_path)
    if not path.is_file():
        return None
    router = language_router or LanguageRouter()
    lang = router.get_language(str(path))
    if lang is None:
        return None
    try:
        source_bytes = path.read_bytes()
    except Exception as e:
        logger.warning("tree_sitter_analyzer: could not read %s: %s", path, e)
        return None
    try:
        from tree_sitter import Parser

        parser = Parser(lang)
        tree = parser.parse(source_bytes)
    except Exception as e:
        logger.warning("tree_sitter_analyzer: parse failed %s: %s", path, e)
        return None

    suffix = path.suffix.lower()
    rel_path = str(path)
    if repo_root:
        try:
            rel_path = str(path.relative_to(repo_root))
        except ValueError:
            pass

    def get_text(node: Any) -> str:
        return source_bytes[node.start_byte : node.end_byte].decode("utf-8", errors="replace")

    # Python: imports (with resolution), star/dynamic import handling, functions, classes
    if suffix == ".py":
        imports: list[str] = []
        star_imports: list[str] = []
        dynamic_imports: list[str] = []
        functions: list[dict[str, Any]] = []
        classes: list[dict[str, Any]] = []
        try:
            from tree_sitter import Node

            def walk_py(node: Node) -> None:
                if node.type == "import_statement":
                    name_node = node.child_by_field_name("name")
                    if name_node:
                        imports.append(get_text(name_node).strip())
                elif node.type == "import_from_statement":
                    mod_node = node.child_by_field_name("module_name")
                    rel_node = node.child_by_field_name("relative_import")
                    if mod_node:
                        mod = get_text(mod_node).strip()
                    else:
                        mod = "."
                    if rel_node:
                        mod = "." + mod if mod else "."
                    resolved = _resolve_python_relative_import(mod, repo_root, path)
                    star_node = node.child_by_field_name("wildcard_import")
                    if star_node is not None:
                        star_imports.append(resolved if resolved is not None else mod)
                    elif resolved is not None:
                        imports.append(resolved)
                elif node.type == "function_definition":
                    name_node = node.child_by_field_name("name")
                    if name_node:
                        name = get_text(name_node)
                        sig = get_text(node).split("\n")[0][:200]
                        decorators = []
                        for i in range(node.child_count):
                            c = node.child(i)
                            if c.type == "decorator":
                                dec_inner = c.child_by_field_name("decorator")
                                if dec_inner:
                                    decorators.append(get_text(dec_inner).strip())
                        functions.append({
                            "name": name,
                            "signature": sig,
                            "line_start": node.start_point[0] + 1,
                            "line_end": node.end_point[0] + 1,
                            "decorators": decorators,
                        })
                elif node.type == "class_definition":
                    name_node = node.child_by_field_name("name")
                    if name_node:
                        name = get_text(name_node)
                        bases = []
                        superclasses = node.child_by_field_name("superclasses")
                        if superclasses:
                            for i in range(superclasses.child_count):
                                c = superclasses.child(i)
                                if c.type == "identifier":
                                    bases.append(get_text(c))
                        classes.append({
                            "name": name,
                            "bases": bases,
                            "parent_classes": list(bases),
                            "line_start": node.start_point[0] + 1,
                            "line_end": node.end_point[0] + 1,
                        })
                for i in range(node.child_count):
                    walk_py(node.child(i))

            walk_py(tree.root_node)
            # Dynamic import hints: __import__(...) or importlib.import_module(...)
            try:
                source_str = source_bytes.decode("utf-8", errors="replace")
                for m in re.finditer(r"(__import__|importlib\.import_module)\s*\(\s*['\"]([^'\"]+)['\"]", source_str):
                    dynamic_imports.append(m.group(2).strip())
            except Exception:
                pass
        except Exception as e:
            logger.warning("tree_sitter_analyzer: Python walk failed %s: %s", path, e)
        out = PythonAnalysisResult(
            path=rel_path,
            language="python",
            imports=imports,
            star_imports=star_imports,
            dynamic_imports=dynamic_imports,
            functions=[FunctionDefResult(**f) for f in functions],
            classes=[ClassDefResult(**c) for c in classes],
        )
        return out.model_dump(mode="json")

    # SQL: table references and query structure at AST level
    if suffix == ".sql":
        tables: list[dict[str, Any]] = []
        query_structures: list[dict[str, Any]] = []
        try:
            from tree_sitter import Node

            def walk_sql(node: Node) -> None:
                if getattr(node, "type", None) == "identifier":
                    text = get_text(node).strip()
                    if text and text.lower() not in ("select", "from", "join", "where", "and", "or", "as", "on", "inner", "left", "right", "outer", "cross"):
                        tables.append({"name": text, "line": node.start_point[0] + 1})
                t = getattr(node, "type", None)
                if t and (t.endswith("_statement") or t in ("join_clause", "with_clause", "table_expression")):
                    query_structures.append({
                        "type": t,
                        "line_start": node.start_point[0] + 1,
                        "line_end": node.end_point[0] + 1,
                    })
                for i in range(getattr(node, "child_count", 0)):
                    walk_sql(node.child(i))

            walk_sql(tree.root_node)
            tables = list({t["name"]: t for t in tables}.values())
        except Exception as e:
            logger.warning("tree_sitter_analyzer: SQL walk failed %s: %s", path, e)
        out = SqlAnalysisResult(
            path=rel_path,
            language="sql",
            tables=[TableRefResult(**t) for t in tables],
            query_structures=[QueryStructureResult(**q) for q in query_structures],
        )
        return out.model_dump(mode="json")

    # YAML: key hierarchies for pipeline config
    if suffix in (".yaml", ".yml"):
        keys: list[str] = []
        key_hierarchy: list[dict[str, Any]] = []
        try:
            from tree_sitter import Node

            def walk_yaml(node: Node, prefix: str = "") -> None:
                t = getattr(node, "type", None)
                if t == "block_mapping_pair":
                    key_node = node.child_by_field_name("key")
                    if key_node:
                        key_text = get_text(key_node).strip().strip("'\"").strip()
                        full_key = f"{prefix}.{key_text}" if prefix else key_text
                        keys.append(full_key)
                        key_hierarchy.append({"key": full_key, "line": node.start_point[0] + 1})
                        for i in range(node.child_count):
                            walk_yaml(node.child(i), full_key)
                        return
                for i in range(getattr(node, "child_count", 0)):
                    walk_yaml(node.child(i), prefix)

            walk_yaml(tree.root_node)
        except Exception as e:
            logger.warning("tree_sitter_analyzer: YAML walk failed %s: %s", path, e)
        out = YamlAnalysisResult(
            path=rel_path,
            language="yaml",
            keys=keys,
            key_hierarchy=[KeyHierarchyEntry(**e) for e in key_hierarchy],
        )
        return out.model_dump(mode="json")

    # Generic fallback: minimal result for other languages (JS/TS)
    result = {"path": rel_path, "imports": [], "functions": [], "classes": []}
    try:
        for i in range(getattr(tree.root_node, "child_count", 0)):
            n = tree.root_node.child(i)
            t = getattr(n, "type", None)
            if t == "import_statement":
                name_node = getattr(n, "child_by_field_name", lambda _: None)("name")
                if name_node:
                    result["imports"].append(get_text(name_node).strip())
            elif t == "import_from_statement":
                mod = getattr(n, "child_by_field_name", lambda _: None)("module_name")
                if mod:
                    result["imports"].append(get_text(mod).strip())
            elif t == "function_definition":
                name_node = getattr(n, "child_by_field_name", lambda _: None)("name")
                if name_node:
                    result["functions"].append(get_text(name_node).strip())
            elif t == "class_definition":
                name_node = getattr(n, "child_by_field_name", lambda _: None)("name")
                if name_node:
                    result["classes"].append(get_text(name_node).strip())
    except Exception as e:
        logger.warning("tree_sitter_analyzer: walk failed %s: %s", path, e)
    return result
