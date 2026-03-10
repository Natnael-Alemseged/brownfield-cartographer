"""
Multi-language AST parsing with LanguageRouter (tree-sitter, no build_library).

Analyzes a single file and returns imports, function names, and class names.
"""

import logging
from pathlib import Path
from typing import Any, Optional

from src.cartographer.core.language_router import LanguageRouter

logger = logging.getLogger(__name__)


def analyze_file(
    file_path: str | Path,
    language_router: Optional[LanguageRouter] = None,
) -> Optional[dict[str, Any]]:
    """
    Parse a file with tree-sitter and return a minimal AST summary: imports, functions, classes.
    Uses LanguageRouter for .py, .sql, .yaml, .js, .ts, etc. Returns None if unsupported or parse error.
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

    result: dict[str, Any] = {"path": str(path), "imports": [], "functions": [], "classes": []}

    def get_text(node: Any) -> str:
        return source_bytes[node.start_byte : node.end_byte].decode("utf-8", errors="replace")

    def walk(node: Any) -> None:
        if getattr(node, "type", None) == "import_statement":
            name_node = getattr(node, "child_by_field_name", lambda _: None)("name")
            if name_node:
                result["imports"].append(get_text(name_node).strip())
        elif getattr(node, "type", None) == "import_from_statement":
            mod = getattr(node, "child_by_field_name", lambda _: None)("module_name")
            if mod:
                result["imports"].append(get_text(mod).strip())
        elif getattr(node, "type", None) == "function_definition":
            name_node = getattr(node, "child_by_field_name", lambda _: None)("name")
            if name_node:
                result["functions"].append(get_text(name_node).strip())
        elif getattr(node, "type", None) == "class_definition":
            name_node = getattr(node, "child_by_field_name", lambda _: None)("name")
            if name_node:
                result["classes"].append(get_text(name_node).strip())
        for i in range(getattr(node, "child_count", 0)):
            walk(node.child(i))

    try:
        walk(tree.root_node)
    except Exception as e:
        logger.warning("tree_sitter_analyzer: walk failed %s: %s", path, e)
    return result
