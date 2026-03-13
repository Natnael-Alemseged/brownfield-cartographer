"""
CLI entry point for Brownfield Cartographer.

Commands (subset modes):
  survey   — structure-only: module graph, PageRank, git velocity, dead-code (writes module_graph.json, survey_summary.md)
  lineage  — lineage-only: data lineage from SQL/Python/dbt/notebooks (writes lineage_graph.json, lineage_summary.md)
  semantic — LLM purpose extraction, domain clustering, Day-One brief (requires survey + optional lineage outputs)
  analyze  — full pipeline: survey then lineage (all artifacts)
  full     — full pipeline: survey -> lineage -> semantic -> archivist (CODEBASE.md)
  query    — run Navigator query on .cartography/ (e.g. "What produces X?")
  living-context — regenerate CODEBASE.md from existing .cartography/ (run full first)
  self-audit — compare Week 1 doc with CODEBASE.md; write self_audit_report.md
"""

import argparse
import logging
import os
import re
import sys
from pathlib import Path

# Ensure project root is on path when running as main.py
if __name__ == "__main__":
    _root = Path(__file__).resolve().parent.parent
    if str(_root) not in sys.path:
        sys.path.insert(0, str(_root))

from src.agents.archivist import generate_CODEBASE_md
from src.agents.hydrologist import Hydrologist
from src.agents.navigator import run_query as navigator_run_query
from src.agents.semanticist import Semanticist
from src.agents.surveyor import Surveyor
from src.orchestrator import run_analysis, run_full_pipeline, run_full_pipeline_incremental
from src.self_audit import run_self_audit
from src.tracing.cartography_trace import CartographyTrace
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


def cmd_semantic(input_path: str, output_dir: Path | None = None, verbose: bool = False) -> int:
    """
    Run Semanticist on existing .cartography: purpose statements, domain clustering, Day-One brief.
    Expects module_graph.json (run survey first). Optionally uses lineage_graph.json and summaries.
    """
    _configure_survey_logging(verbose=verbose)
    output_dir = output_dir or Path.cwd() / ".cartography"
    if _is_git_url(input_path):
        print("Error: semantic command expects a local repo path (run survey/analyze on clone first).", file=sys.stderr)
        return 1
    path = Path(input_path.strip()).resolve()
    if not path.exists() or not path.is_dir():
        print(f"Error: path does not exist or is not a directory: {path}", file=sys.stderr)
        return 1
    if not (output_dir / "module_graph.json").exists():
        print("Error: module_graph.json not found. Run 'survey' or 'analyze' first.", file=sys.stderr)
        return 1
    try:
        semanticist = Semanticist(output_dir=output_dir, repo_path=str(path))
        print(f"[CLI] Running Semanticist at {path} (output: {output_dir})")
        mg, day_one_json, onboarding_md = semanticist.analyze_repository(str(path), output_dir=output_dir)
        print(f"[CLI] Semanticist complete. Outputs: {mg}, {day_one_json}, {onboarding_md}")
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


def cmd_full(
    input_path: str,
    output_dir: Path | None = None,
    verbose: bool = False,
    incremental: bool = False,
) -> int:
    """Run full pipeline: Surveyor -> Hydrologist -> Semanticist -> Archivist. Use --incremental to re-run only changed (git)."""
    _configure_survey_logging(verbose=verbose)
    output_dir = output_dir or Path.cwd() / ".cartography"
    run_fn = run_full_pipeline_incremental if incremental else run_full_pipeline
    if _is_git_url(input_path):
        url = input_path.strip()
        if not is_safe_url(url):
            print("Error: URL not allowed (only https/git GitHub URLs).", file=sys.stderr)
            return 1
        try:
            print("[CLI] Cloning repository ...")
            with RepoSandbox(url) as temp_path:
                print(f"[CLI] Clone complete. Running full pipeline at {temp_path}" + (" (incremental)" if incremental else ""))
                mg, lg, ls, day_one_json, onboarding_md = run_fn(temp_path, output_dir=output_dir)
                print(f"[CLI] Full pipeline complete. Outputs: {mg}, {lg}, {ls}, {day_one_json}, {onboarding_md}; CODEBASE.md in {output_dir}")
        except (ValueError, RuntimeError) as e:
            print(f"Error: {e}", file=sys.stderr)
            return 1
    else:
        path = Path(input_path.strip()).resolve()
        if not path.exists() or not path.is_dir():
            print(f"Error: path does not exist: {path}", file=sys.stderr)
            return 1
        try:
            print(f"[CLI] Running full pipeline at {path}" + (" (incremental)" if incremental else ""))
            mg, lg, ls, day_one_json, onboarding_md = run_fn(str(path), output_dir=output_dir)
            print(f"[CLI] Full pipeline complete. Outputs: {mg}, {lg}, {ls}, {day_one_json}, {onboarding_md}; CODEBASE.md in {output_dir}")
        except Exception as e:
            print(f"Error: {e}", file=sys.stderr)
            return 1
    return 0


def cmd_query(question: str, output_dir: Path | None = None, verbose: bool = False) -> int:
    """Run Navigator query on existing .cartography/ (run full first)."""
    _configure_survey_logging(verbose=verbose)
    output_dir = output_dir or Path.cwd() / ".cartography"
    if not (output_dir / "module_graph.json").exists():
        print("Error: module_graph.json not found. Run 'survey' or 'full' first.", file=sys.stderr)
        return 1
    try:
        answer, evidence = navigator_run_query(output_dir, question)
        print(answer)
        if verbose and evidence:
            print("\n[Evidence]")
            for e in evidence:
                print(f"  {e.file_path}:{e.line_start}-{e.line_end} [{e.evidence_type}] {e.confidence or ''}")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    return 0


def cmd_self_audit(week1_repo_path: str, output_dir: Path | None = None, verbose: bool = False) -> int:
    """Compare Week 1 doc (ARCHITECTURE_NOTES.md or RECONNAISSANCE.md) with CODEBASE.md; write .cartography/self_audit_report.md."""
    _configure_survey_logging(verbose=verbose)
    path = Path(week1_repo_path).resolve()
    if not path.exists() or not path.is_dir():
        print(f"Error: path does not exist or is not a directory: {path}", file=sys.stderr)
        return 1
    out = output_dir or path / ".cartography"
    try:
        report_path = run_self_audit(path, output_dir=out)
        print(f"[CLI] Self-audit report written: {report_path}")
    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    return 0


def cmd_living_context(output_dir: Path | None = None, repo_path: str | None = None, verbose: bool = False) -> int:
    """Regenerate CODEBASE.md from existing .cartography/ artifacts (run survey+lineage+semantic first)."""
    _configure_survey_logging(verbose=verbose)
    output_dir = output_dir or Path.cwd() / ".cartography"
    if not (output_dir / "module_graph.json").exists():
        print("Error: module_graph.json not found. Run 'survey' or 'full' first.", file=sys.stderr)
        return 1
    try:
        trace = CartographyTrace(output_dir, agent="archivist")
        path_obj = Path(repo_path).resolve() if repo_path else None
        codebase_path = generate_CODEBASE_md(
            output_dir,
            repo_name=path_obj.name if path_obj else "repository",
            repo_path=path_obj,
            run_id=trace.run_id,
            trace=trace,
        )
        print(f"[CLI] CODEBASE.md written: {codebase_path}")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    return 0


def main() -> int:
    """Dispatch subcommands: survey, lineage, semantic, analyze, full, living-context."""
    parser = argparse.ArgumentParser(
        description="Brownfield Cartographer: codebase and data lineage analysis.",
        epilog="Modes: survey, lineage, semantic, analyze (survey+lineage), full (survey+lineage+semantic+archivist), living-context (regenerate CODEBASE.md).",
    )
    parser.add_argument(
        "command",
        choices=["survey", "lineage", "semantic", "analyze", "full", "query", "living-context", "self-audit"],
        help="survey | lineage | semantic | analyze | full | query | living-context | self-audit",
    )
    parser.add_argument("input", help="Local path, GitHub URL, or query string (for query command)")
    parser.add_argument("-o", "--output", type=Path, default=None, help="Output directory (default: .cartography)")
    parser.add_argument("-v", "--verbose", action="store_true", help="Per-file / verbose logging")
    parser.add_argument("--incremental", action="store_true", help="Incremental run (only for 'full': re-run Semanticist on changed files)")
    args = parser.parse_args()
    output_dir = args.output or Path.cwd() / ".cartography"

    if args.command == "survey":
        return cmd_survey(args.input, output_dir=output_dir, verbose=args.verbose)
    if args.command == "lineage":
        return cmd_lineage(args.input, output_dir=output_dir, verbose=args.verbose)
    if args.command == "semantic":
        return cmd_semantic(args.input, output_dir=output_dir, verbose=args.verbose)
    if args.command == "analyze":
        return cmd_analyze(args.input, output_dir=output_dir, verbose=args.verbose)
    if args.command == "full":
        return cmd_full(args.input, output_dir=output_dir, verbose=args.verbose, incremental=args.incremental)
    if args.command == "query":
        return cmd_query(args.input.strip(), output_dir=output_dir, verbose=args.verbose)
    if args.command == "living-context":
        return cmd_living_context(output_dir=output_dir, repo_path=args.input or None, verbose=args.verbose)
    if args.command == "self-audit":
        return cmd_self_audit(args.input, output_dir=output_dir, verbose=args.verbose)
    return 2


if __name__ == "__main__":
    sys.exit(main())
