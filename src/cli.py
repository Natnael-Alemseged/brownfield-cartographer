"""
CLI entry point for Brownfield Cartographer.

Commands:
  survey <input>  Run Surveyor on a local path or git URL (clones to temp, analyzes, writes .cartography/)
"""

import re
import sys
from pathlib import Path

# Ensure project root is on path when running as main.py
if __name__ == "__main__":
    _root = Path(__file__).resolve().parent
    if str(_root) not in sys.path:
        sys.path.insert(0, str(_root))

from src.cartographer.surveyor import Surveyor
from src.tools.repo_tools import RepoSandbox, is_safe_url

GIT_URL_PATTERN = re.compile(r"^(https?://|git@)")


def _is_git_url(input_str: str) -> bool:
    """True if input looks like a git URL (https, http, or git@)."""
    return bool(GIT_URL_PATTERN.match(input_str.strip()))


def cmd_survey(input_path: str, output_dir: Path | None = None) -> int:
    """
    Run Surveyor on a local path or git URL.
    If URL: clone to temp dir, analyze, write to output_dir (default .cartography), cleanup.
    If local path: analyze in place, write to output_dir.
    """
    output_dir = output_dir or Path.cwd() / ".cartography"
    surveyor = Surveyor(output_dir=output_dir)

    if _is_git_url(input_path):
        url = input_path.strip()
        if not is_safe_url(url):
            print("Error: URL not allowed (only https/git GitHub URLs).", file=sys.stderr)
            return 1
        try:
            with RepoSandbox(url) as temp_path:
                out_file = surveyor.analyze_repository(temp_path, output_dir=output_dir)
                print(f"Survey complete. Output: {out_file}")
        except (ValueError, RuntimeError) as e:
            print(f"Error: {e}", file=sys.stderr)
            return 1
    else:
        path = Path(input_path.strip()).resolve()
        if not path.exists():
            print(f"Error: path does not exist: {path}", file=sys.stderr)
            return 1
        if not path.is_dir():
            print("Error: survey expects a directory (repo root) or a git URL.", file=sys.stderr)
            return 1
        try:
            out_file = surveyor.analyze_repository(str(path), output_dir=output_dir)
            print(f"Survey complete. Output: {out_file}")
        except Exception as e:
            print(f"Error: {e}", file=sys.stderr)
            return 1

    return 0


def main() -> int:
    """Dispatch subcommands. Usage: python main.py survey <input>"""
    args = sys.argv[1:]
    if not args or args[0] != "survey":
        print("Usage: python main.py survey <path-or-git-url>", file=sys.stderr)
        print("  <path-or-git-url>: local directory or https://... / git@... repo URL", file=sys.stderr)
        return 2
    if len(args) < 2:
        print("Error: survey requires an input path or URL.", file=sys.stderr)
        return 2
    return cmd_survey(args[1])


if __name__ == "__main__":
    sys.exit(main())
