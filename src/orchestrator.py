"""
Orchestrator: wires Surveyor + Hydrologist in sequence and serializes outputs to .cartography/.

Usage: run_analysis(repo_path, output_dir) runs module graph (Surveyor) then lineage (Hydrologist).
Per-file errors are isolated (logged and skipped) so partial results are always produced.
Progress is logged to console. Writes module_graph.json and lineage_graph.json to output_dir.
"""

import logging
from pathlib import Path
from typing import Optional

from src.agents.hydrologist import Hydrologist
from src.agents.surveyor import Surveyor

logger = logging.getLogger(__name__)


def run_analysis(
    repo_path: str,
    output_dir: Optional[Path] = None,
) -> tuple[Optional[Path], Optional[Path], Optional[Path]]:
    """
    Run Surveyor then Hydrologist on the given repo path and write all artifacts to output_dir.

    Per-file errors inside each agent are logged and skipped; partial results are produced.
    Returns:
        (module_graph_path, lineage_graph_path, lineage_summary_path)
        Any may be None if that step failed entirely.
    """
    out = Path(output_dir) if output_dir else Path(".cartography")
    out.mkdir(parents=True, exist_ok=True)

    module_graph_path = None
    lineage_graph_path = None
    lineage_summary_path = None

    logger.info("Orchestrator: running Surveyor (module graph, git velocity, dead-code)...")
    try:
        surveyor = Surveyor(output_dir=out)
        module_graph_path = surveyor.analyze_repository(repo_path, output_dir=out)
        logger.info("Orchestrator: Surveyor complete. Output: %s", module_graph_path)
    except Exception as e:
        logger.exception("Orchestrator: Surveyor failed (partial results may exist): %s", e)

    logger.info("Orchestrator: running Hydrologist (lineage, sources/sinks)...")
    try:
        hydrologist = Hydrologist(output_dir=out)
        lineage_graph_path, lineage_summary_path = hydrologist.analyze_repository(
            repo_path, output_dir=out
        )
        logger.info(
            "Orchestrator: Hydrologist complete. Outputs: %s, %s",
            lineage_graph_path,
            lineage_summary_path,
        )
    except Exception as e:
        logger.exception("Orchestrator: Hydrologist failed (partial results may exist): %s", e)

    return module_graph_path, lineage_graph_path, lineage_summary_path
