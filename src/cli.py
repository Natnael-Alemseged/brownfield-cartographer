"""
CLI entry point for Brownfield Cartographer.

Commands (subset modes):
  survey   — structure-only: module graph, PageRank, git velocity, dead-code (writes module_graph.json, survey_summary.md)
  lineage  — lineage-only: data lineage from SQL/Python/dbt/notebooks (writes lineage_graph.json, lineage_summary.md)
  analyze  — full pipeline: survey then lineage (all artifacts)
"""

import argparse
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


def _configure_survey_logging(verbose: bool = False) -> None:
    """Configure logging to terminal. If verbose, use DEBUG for more per-file detail."""
    root = logging.getLogger()
    root.setLevel(logging.DEBUG if verbose else logging.INFO)
    if not any(getattr(h, "stream", None) and getattr(h.stream, "name", "") == "<stdout>" for h in root.handlers):
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.DEBUG if verbose else logging.INFO)
        handler.setFormatter(logging.Formatter("[%(name)s] %(message)s"))
        root.addHandler(handler)


def _is_git_url(input_str: str) -> bool:
    """True if input looks like a git URL (https, http, or git@)."""
    return bool(GIT_URL_PATTERN.match(input_str.strip()))


def cmd_survey(input_path: str, output_dir: Path | None = None, verbose: bool = False) -> int:
    """
    Run Surveyor (structure-only): module graph, PageRank, git velocity, dead-code.
    Writes module_graph.json and survey_summary.md.
    """
    _configure_survey_logging(verbose=verbose)
    output_dir = output_dir or Path.cwd() / ".cartography"
    surveyor = Surveyor(output_dir=output_dir)

    if _is_git_url(input_path):
        url = input_path.strip()
        if not is_safe_url(url):
            print("Error: URL not allowed (only https/git GitHub URLs).", file=sys.stderr)
            return 1
        try:
            print("[CLI] Cloning repository ...")
            with RepoSandbox(url) as temp_path:
                print(f"[CLI] Clone complete. Running survey (structure-only) at {temp_path}")
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
            print(f"[CLI] Running survey (structure-only) at {path}")
            out_file = surveyor.analyze_repository(str(path), output_dir=output_dir)
            print(f"[CLI] Survey complete. Output: {out_file}")
        except Exception as e:
            print(f"Error: {e}", file=sys.stderr)
            return 1

    return 0


def cmd_lineage(input_path: str, output_dir: Path | None = None, verbose: bool = False) -> int:
    """
    Run Hydrologist (lineage-only): SQL, Python, dbt, notebooks.
    Writes lineage_graph.json and lineage_summary.md.
    """
    _configure_survey_logging(verbose=verbose)
    output_dir = output_dir or Path.cwd() / ".cartography"
    hydrologist = Hydrologist(output_dir=output_dir)

    if _is_git_url(input_path):
        url = input_path.strip()
        if not is_safe_url(url):
            print("Error: URL not allowed (only https/git GitHub URLs).", file=sys.stderr)
            return 1
        try:
            print("[CLI] Cloning repository ...")
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


def cmd_analyze(input_path: str, output_dir: Path | None = None, verbose: bool = False) -> int:
    """
    Run full pipeline: Surveyor then Hydrologist (all artifacts).
    """
    _configure_survey_logging(verbose=verbose)
    output_dir = output_dir or Path.cwd() / ".cartography"
    if _is_git_url(input_path):
        url = input_path.strip()
        if not is_safe_url(url):
            print("Error: URL not allowed (only https/git GitHub URLs).", file=sys.stderr)
            return 1
        try:
            print("[CLI] Cloning repository ...")
            with RepoSandbox(url) as temp_path:
                print(f"[CLI] Clone complete. Running full analysis at {temp_path}")
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
    """Dispatch subcommands. Supports structure-only (survey), lineage-only (lineage), or full (analyze)."""
    parser = argparse.ArgumentParser(
        description="Brownfield Cartographer: codebase and data lineage analysis.",
        epilog="Modes: survey=structure-only, lineage=lineage-only, analyze=full pipeline.",
    )
    parser.add_argument("command", choices=["survey", "lineage", "analyze"], help="survey | lineage | analyze")
    parser.add_argument("input", help="Local directory or GitHub repo URL")
    parser.add_argument("-o", "--output", type=Path, default=None, help="Output directory (default: .cartography)")
    parser.add_argument("-v", "--verbose", action="store_true", help="Per-file / verbose logging")
    args = parser.parse_args()
    output_dir = args.output or Path.cwd() / ".cartography"

    if args.command == "survey":
        return cmd_survey(args.input, output_dir=output_dir, verbose=args.verbose)
    if args.command == "lineage":
        return cmd_lineage(args.input, output_dir=output_dir, verbose=args.verbose)
    if args.command == "analyze":
        return cmd_analyze(args.input, output_dir=output_dir, verbose=args.verbose)
    return 2


if __name__ == "__main__":
    sys.exit(main())
