"""
Language router: map file paths to tree-sitter Language instances.

Uses pre-compiled language packages (tree-sitter-python, etc.). Does not use
Language.build_library() or vendors/ folders.
"""

import logging
from pathlib import Path
from typing import Optional

from tree_sitter import Language

logger = logging.getLogger(__name__)

# Extension -> (package_import_name, language_getter_name)
# JS/TS: tree-sitter-javascript has language(); tree-sitter-typescript has language() and language_tsx()
_EXTENSION_MAP = {
    ".py": ("tree_sitter_python", "language"),
    ".sql": ("tree_sitter_sql", "language"),
    ".yaml": ("tree_sitter_yaml", "language"),
    ".yml": ("tree_sitter_yaml", "language"),
    ".js": ("tree_sitter_javascript", "language"),
    ".jsx": ("tree_sitter_javascript", "language"),
    ".ts": ("tree_sitter_typescript", "language_typescript"),
    ".tsx": ("tree_sitter_typescript", "language_tsx"),
}


def _load_language(module_name: str, attr: str = "language"):
    """Load a tree-sitter Language from a pre-compiled package. Returns None if missing."""
    try:
        mod = __import__(module_name, fromlist=[attr])
        getter = getattr(mod, attr)
        lang_ptr = getter() if callable(getter) else getter
        return Language(lang_ptr)
    except ImportError as e:
        logger.warning("Tree-sitter language package not available: %s (%s)", module_name, e)
        return None
    except Exception as e:
        logger.warning("Failed to load tree-sitter language %s: %s", module_name, e)
        return None


# Cache Language instances per (module_name, attr)
_language_cache: dict[tuple[str, str], Optional[Language]] = {}


class LanguageRouter:
    """
    Maps file paths to tree-sitter Language instances using file extension.

    Uses pre-compiled packages only. Missing packages are logged and the file
    is skipped (returns None).
    """

    def __init__(self) -> None:
        self._cache: dict[str, Optional[Language]] = {}

    def get_language(self, file_path: str) -> Optional[Language]:
        """
        Return the tree-sitter Language for the given file path, or None if
        the extension is unsupported or the language package is missing.

        :param file_path: Path or filename (e.g. "src/foo.py" or "bar.sql")
        :return: Language instance or None (caller should skip file)
        """
        path = Path(file_path)
        suffix = path.suffix.lower()
        if suffix not in _EXTENSION_MAP:
            return None
        module_name, attr = _EXTENSION_MAP[suffix]
        cache_key = (module_name, attr)
        if cache_key not in _language_cache:
            _language_cache[cache_key] = _load_language(module_name, attr)
        return _language_cache[cache_key]

    def supported_extensions(self) -> set[str]:
        """Return the set of file extensions that can be routed."""
        return set(_EXTENSION_MAP.keys())
