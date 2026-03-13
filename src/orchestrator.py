"""
Orchestrator: wires Surveyor + Hydrologist (+ optional Semanticist + Archivist) and serializes outputs to .cartography/.

Usage:
  run_analysis(repo_path, output_dir) — Surveyor then Hydrologist.
  run_full_pipeline(repo_path, output_dir) — Surveyor then Hydrologist then Semanticist then Archivist.
  run_full_pipeline_incremental(repo_path, output_dir) — Use git diff since last run; re-run only changed (Semanticist incremental).
"""

import logging
import shutil
import subprocess
from pathlib import Path
from typing import Optional

from src.agents.archivist import generate_CODEBASE_md
from src.agents.hydrologist import Hydrologist
from src.agents.semanticist import Semanticist
from src.agents.surveyor import Surveyor
from src.tracing.cartography_trace import CartographyTrace, new_run_id

logger = logging.getLogger(__name__)

LAST_RUN_COMMIT_FILE = "last_run_commit.txt"
BACKUP_DIR = ".backup"


def _read_last_run_commit(output_dir: Path) -> Optional[str]:
    """Return the commit SHA from last run, or None."""
    path = Path(output_dir) / LAST_RUN_COMMIT_FILE
    if not path.exists():
        return None
    try:
        return path.read_text(encoding="utf-8").strip() or None
    except Exception:
        return None


def _write_last_run_commit(output_dir: Path, commit_sha: str) -> None:
    """Write last run commit SHA for incremental mode."""
    path = Path(output_dir) / LAST_RUN_COMMIT_FILE
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(commit_sha, encoding="utf-8")
    logger.info("Wrote %s", path)


def _get_head_sha(repo_path: str) -> Optional[str]:
    """Return current HEAD SHA or None."""
    try:
        r = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_path,
            capture_output=True,
            text=True,
            timeout=5,
        )
        if r.returncode == 0 and r.stdout:
            return r.stdout.strip()
    except Exception as e:
        logger.debug("Could not get HEAD: %s", e)
    return None


def _get_changed_files(repo_path: str, since_sha: str) -> tuple[list[str], list[str]]:
    """Return (changed_paths, deleted_paths) between since_sha and HEAD. Paths normalized to /."""
    changed = []
    deleted = []
    try:
        r = subprocess.run(
            ["git", "diff", "--name-only", since_sha, "HEAD"],
            cwd=repo_path,
            capture_output=True,
            text=True,
            timeout=10,
        )
        if r.returncode != 0:
            return changed, deleted
        for line in (r.stdout or "").strip().splitlines():
            path = line.strip().replace("\\", "/")
            if path:
                changed.append(path)
        r2 = subprocess.run(
            ["git", "diff", "--name-only", "--diff-filter=D", since_sha, "HEAD"],
            cwd=repo_path,
            capture_output=True,
            text=True,
            timeout=10,
        )
        if r2.returncode == 0 and r2.stdout:
            for line in r2.stdout.strip().splitlines():
                path = line.strip().replace("\\", "/")
                if path:
                    deleted.append(path)
    except Exception as e:
        logger.warning("Could not get changed files: %s", e)
    return changed, deleted


def _backup_cartography(output_dir: Path) -> bool:
    """Copy output_dir to output_dir/.backup. Return True on success."""
    backup = Path(output_dir) / BACKUP_DIR
    try:
        if output_dir.exists():
            if backup.exists():
                shutil.rmtree(backup)
            shutil.copytree(output_dir, backup, ignore=shutil.ignore_patterns(BACKUP_DIR))
        return True
    except Exception as e:
        logger.warning("Backup failed: %s", e)
        return False


def _restore_cartography_from_backup(output_dir: Path) -> bool:
    """Restore output_dir from output_dir/.backup. Return True on success."""
    backup = Path(output_dir) / BACKUP_DIR
    if not backup.exists():
        return False
    try:
        for item in output_dir.iterdir():
            if item.name == BACKUP_DIR:
                continue
            if item.is_file():
                item.unlink()
            else:
                shutil.rmtree(item)
        for item in backup.iterdir():
            if item.name == BACKUP_DIR:
                continue
            dest = output_dir / item.name
            if item.is_file():
                shutil.copy2(item, dest)
            else:
                shutil.copytree(item, dest)
        return True
    except Exception as e:
        logger.warning("Restore from backup failed: %s", e)
        return False


def run_analysis(
    repo_path: str,
    output_dir: Optional[Path] = None,
    run_id: Optional[str] = None,
) -> tuple[Optional[Path], Optional[Path], Optional[Path]]:
    """
    Run Surveyor then Hydrologist on the given repo path and write all artifacts to output_dir.

    Per-file errors inside each agent are logged and skipped; partial results are produced.
    If run_id is provided, all agents use it for trace correlation; otherwise a new one is generated.
    Returns:
        (module_graph_path, lineage_graph_path, lineage_summary_path)
        Any may be None if that step failed entirely.
    """
    out = Path(output_dir) if output_dir else Path(".cartography")
    out.mkdir(parents=True, exist_ok=True)
    run_id = run_id or new_run_id()
    logger.info("Orchestrator: run_id=%s", run_id)

    module_graph_path = None
    lineage_graph_path = None
    lineage_summary_path = None

    logger.info("Orchestrator: running Surveyor (module graph, git velocity, dead-code)...")
    try:
        surveyor = Surveyor(output_dir=out)
        module_graph_path = surveyor.analyze_repository(repo_path, output_dir=out, run_id=run_id)
        logger.info("Orchestrator: Surveyor complete. Output: %s", module_graph_path)
    except Exception as e:
        logger.exception("Orchestrator: Surveyor failed (partial results may exist): %s", e)

    logger.info("Orchestrator: running Hydrologist (lineage, sources/sinks)...")
    try:
        hydrologist = Hydrologist(output_dir=out)
        lineage_graph_path, lineage_summary_path = hydrologist.analyze_repository(
            repo_path, output_dir=out, run_id=run_id
        )
        logger.info(
            "Orchestrator: Hydrologist complete. Outputs: %s, %s",
            lineage_graph_path,
            lineage_summary_path,
        )
    except Exception as e:
        logger.exception("Orchestrator: Hydrologist failed (partial results may exist): %s", e)

    return module_graph_path, lineage_graph_path, lineage_summary_path


def run_full_pipeline(
    repo_path: str,
    output_dir: Optional[Path] = None,
) -> tuple[
    Optional[Path], Optional[Path], Optional[Path], Optional[Path], Optional[Path]
]:
    """
    Run Surveyor then Hydrologist then Semanticist then Archivist. Returns all artifact paths including Day-One brief and CODEBASE.md.
    Returns:
        (module_graph_path, lineage_graph_path, lineage_summary_path, day_one_answers_json_path, onboarding_brief_md_path)
    """
    run_id = new_run_id()
    mg, lg, ls = run_analysis(repo_path, output_dir=output_dir, run_id=run_id)
    out = Path(output_dir) if output_dir else Path(".cartography")
    day_one_json = None
    onboarding_md = None
    if mg is not None:
        logger.info("Orchestrator: running Semanticist (purpose extraction, domain clustering, Day-One brief)...")
        try:
            semanticist = Semanticist(output_dir=out)
            _, day_one_json, onboarding_md = semanticist.analyze_repository(
                repo_path, output_dir=out, run_id=run_id
            )
            logger.info("Orchestrator: Semanticist complete. Outputs: %s, %s", day_one_json, onboarding_md)
        except Exception as e:
            logger.exception("Orchestrator: Semanticist failed (partial results may exist): %s", e)

    logger.info("Orchestrator: running Archivist (CODEBASE.md)...")
    codebase_md_path = None
    try:
        archivist_trace = CartographyTrace(out, agent="archivist", run_id=run_id)
        repo_path_obj = Path(repo_path) if repo_path else None
        codebase_md_path = generate_CODEBASE_md(
            out,
            repo_name=repo_path_obj.name if repo_path_obj else "repository",
            repo_path=repo_path_obj,
            run_id=run_id,
            trace=archivist_trace,
        )
        logger.info("Orchestrator: Archivist complete. Output: %s", codebase_md_path)
    except Exception as e:
        logger.exception("Orchestrator: Archivist failed: %s", e)

    head_sha = _get_head_sha(repo_path)
    if head_sha:
        _write_last_run_commit(out, head_sha)

    return mg, lg, ls, day_one_json, onboarding_md


def run_full_pipeline_incremental(
    repo_path: str,
    output_dir: Optional[Path] = None,
) -> tuple[
    Optional[Path], Optional[Path], Optional[Path], Optional[Path], Optional[Path]
]:
    """
    Incremental run: if git shows new commits since last run, re-run with changed_files for Semanticist.
    Backs up .cartography before run; on failure restores from backup.
    Surveyor and Hydrologist run full for now; Semanticist uses changed_files when available.
    """
    out = Path(output_dir) if output_dir else Path(".cartography")
    out.mkdir(parents=True, exist_ok=True)
    last_sha = _read_last_run_commit(out)
    head_sha = _get_head_sha(repo_path)

    if not head_sha:
        logger.info("Orchestrator: not a git repo or no HEAD; running full pipeline.")
        return run_full_pipeline(repo_path, output_dir=output_dir)

    if not last_sha or last_sha == head_sha:
        logger.info("Orchestrator: no previous run or same commit; running full pipeline.")
        return run_full_pipeline(repo_path, output_dir=output_dir)

    changed, deleted = _get_changed_files(repo_path, last_sha)
    if not changed and not deleted:
        logger.info("Orchestrator: no file changes since %s; skipping run.", last_sha[:8])
        _write_last_run_commit(out, head_sha)
        mg = out / "module_graph.json"
        lg = out / "lineage_graph.json"
        ls = out / "lineage_summary.md"
        dj = out / "day_one_answers.json"
        ob = out / "onboarding_brief.md"
        return (mg if mg.exists() else None, lg if lg.exists() else None, ls if ls.exists() else None, dj if dj.exists() else None, ob if ob.exists() else None)

    logger.info("Orchestrator: incremental run; changed=%d, deleted=%d", len(changed), len(deleted))
    if not _backup_cartography(out):
        logger.warning("Orchestrator: backup failed; proceeding without backup.")

    try:
        run_id = new_run_id()
        mg, lg, ls = run_analysis(repo_path, output_dir=out, run_id=run_id)
        day_one_json = None
        onboarding_md = None
        if mg is not None:
            try:
                semanticist = Semanticist(output_dir=out)
                _, day_one_json, onboarding_md = semanticist.analyze_repository(
                    repo_path, output_dir=out, changed_files=changed, run_id=run_id
                )
            except Exception as e:
                logger.exception("Orchestrator: Semanticist failed: %s", e)
        try:
            archivist_trace = CartographyTrace(out, agent="archivist", run_id=run_id)
            generate_CODEBASE_md(out, repo_name=Path(repo_path).name, repo_path=Path(repo_path), run_id=run_id, trace=archivist_trace)
        except Exception as e:
            logger.exception("Orchestrator: Archivist failed: %s", e)
        _write_last_run_commit(out, head_sha)
        return mg, lg, ls, day_one_json, onboarding_md
    except Exception as e:
        logger.exception("Orchestrator: incremental pipeline failed: %s", e)
        if _restore_cartography_from_backup(out):
            logger.info("Orchestrator: restored .cartography from backup.")
        raise
