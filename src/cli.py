"""
CLI entry point for Brownfield Cartographer.

Commands:
  survey <input>  Run Surveyor on a local path or git URL (clones to temp, analyzes, writes .cartography/)
"""

import logging
import re
import sys
from pathlib import Path

# Ensure project root is on path when running as main.py
if __name__ == "__main__":
    _root = Path(__file__).resolve().parent
    if str(_root) not in sys.path:
        sys.path.insert(0, str(_root))

from src.agents.hydrologist import Hydrologist
from src.agents.surveyor import Surveyor
from src.orchestrator import run_analysis
from src.tools.repo_tools import RepoSandbox, is_safe_url

GIT_URL_PATTERN = re.compile(r"^(https?://|git@)")


def _configure_survey_logging() -> None:
    """Configure detailed logging to terminal for survey runs."""
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    if not any(getattr(h, "stream", None) and getattr(h.stream, "name", "") == "<stdout>" for h in root.handlers):
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.INFO)
        handler.setFormatter(logging.Formatter("[%(name)s] %(message)s"))
        root.addHandler(handler)


def _is_git_url(input_str: str) -> bool:
    """True if input looks like a git URL (https, http, or git@)."""
    return bool(GIT_URL_PATTERN.match(input_str.strip()))


def cmd_survey(input_path: str, output_dir: Path | None = None) -> int:
    """
    Run Surveyor on a local path or git URL.
    If URL: clone to temp dir, analyze, write to output_dir (default .cartography), cleanup.
    If local path: analyze in place, write to output_dir.
    """
    _configure_survey_logging()
    output_dir = output_dir or Path.cwd() / ".cartography"
    surveyor = Surveyor(output_dir=output_dir)

    if _is_git_url(input_path):
        url = input_path.strip()
        if not is_safe_url(url):
            print("Error: URL not allowed (only https/git GitHub URLs).", file=sys.stderr)
            return 1
        try:
            print(f"[CLI] Cloning {url} ...")
            with RepoSandbox(url) as temp_path:
                print(f"[CLI] Clone complete. Analyzing repository at {temp_path}")
                out_file = surveyor.analyze_repository(temp_path, output_dir=output_dir)
                print(f"[CLI] Survey complete. Output: {out_file}")
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
            print(f"[CLI] Analyzing repository at {path}")
            out_file = surveyor.analyze_repository(str(path), output_dir=output_dir)
            print(f"[CLI] Survey complete. Output: {out_file}")
        except Exception as e:
            print(f"Error: {e}", file=sys.stderr)
            return 1

    return 0


def cmd_lineage(input_path: str, output_dir: Path | None = None) -> int:
    """
    Run Hydrologist (lineage) on a local path or git URL.
    Writes .cartography/lineage_graph.json and .cartography/lineage_summary.md.
    """
    _configure_survey_logging()
    output_dir = output_dir or Path.cwd() / ".cartography"
    hydrologist = Hydrologist(output_dir=output_dir)

    if _is_git_url(input_path):
        url = input_path.strip()
        if not is_safe_url(url):
            print("Error: URL not allowed (only https/git GitHub URLs).", file=sys.stderr)
            return 1
        try:
            print(f"[CLI] Cloning {url} ...")
            with RepoSandbox(url) as temp_path:
                print(f"[CLI] Clone complete. Running lineage at {temp_path}")
                json_path, summary_path = hydrologist.analyze_repository(temp_path, output_dir=output_dir)
                print(f"[CLI] Lineage complete. Output: {json_path}, {summary_path}")
        except (ValueError, RuntimeError) as e:
            print(f"Error: {e}", file=sys.stderr)
            return 1
    else:
        path = Path(input_path.strip()).resolve()
        if not path.exists():
            print(f"Error: path does not exist: {path}", file=sys.stderr)
            return 1
        if not path.is_dir():
            print("Error: lineage expects a directory (repo root) or a git URL.", file=sys.stderr)
            return 1
        try:
            print(f"[CLI] Running lineage at {path}")
            json_path, summary_path = hydrologist.analyze_repository(str(path), output_dir=output_dir)
            print(f"[CLI] Lineage complete. Output: {json_path}, {summary_path}")
        except Exception as e:
            print(f"Error: {e}", file=sys.stderr)
            return 1

    return 0


def cmd_analyze(input_path: str, output_dir: Path | None = None) -> int:
    """
    Run full analysis: Surveyor then Hydrologist, write all artifacts to .cartography/.
    (module_graph.json, lineage_graph.json, lineage_summary.md)
    """
    _configure_survey_logging()
    output_dir = output_dir or Path.cwd() / ".cartography"
    if _is_git_url(input_path):
        url = input_path.strip()
        if not is_safe_url(url):
            print("Error: URL not allowed (only https/git GitHub URLs).", file=sys.stderr)
            return 1
        try:
            print(f"[CLI] Cloning {url} ...")
            with RepoSandbox(url) as temp_path:
                print(f"[CLI] Running full analysis at {temp_path}")
                mg, lg, ls = run_analysis(temp_path, output_dir=output_dir)
                print(f"[CLI] Analysis complete. Outputs: {mg}, {lg}, {ls}")
        except (ValueError, RuntimeError) as e:
            print(f"Error: {e}", file=sys.stderr)
            return 1
    else:
        path = Path(input_path.strip()).resolve()
        if not path.exists():
            print(f"Error: path does not exist: {path}", file=sys.stderr)
            return 1
        if not path.is_dir():
            print("Error: analyze expects a directory (repo root) or a git URL.", file=sys.stderr)
            return 1
        try:
            print(f"[CLI] Running full analysis at {path}")
            mg, lg, ls = run_analysis(str(path), output_dir=output_dir)
            print(f"[CLI] Analysis complete. Outputs: {mg}, {lg}, {ls}")
        except Exception as e:
            print(f"Error: {e}", file=sys.stderr)
            return 1
    return 0


def main() -> int:
    """Dispatch subcommands. Usage: python main.py survey|lineage|analyze <input>"""
    args = sys.argv[1:]
    if not args or args[0] not in ("survey", "lineage", "analyze"):
        print("Usage: python main.py survey <path-or-git-url>", file=sys.stderr)
        print("       python main.py lineage <path-or-git-url>", file=sys.stderr)
        print("       python main.py analyze <path-or-git-url>", file=sys.stderr)
        print("  <path-or-git-url>: local directory or https://... / git@... repo URL", file=sys.stderr)
        return 2
    if len(args) < 2:
        print("Error: command requires an input path or URL.", file=sys.stderr)
        return 2
    cmd, input_path = args[0], args[1]
    if cmd == "survey":
        return cmd_survey(input_path)
    if cmd == "lineage":
        return cmd_lineage(input_path)
    if cmd == "analyze":
        return cmd_analyze(input_path)
    return 2


if __name__ == "__main__":
    sys.exit(main())
