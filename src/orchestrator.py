"""
Orchestrator: wires Surveyor + Hydrologist in sequence and serializes outputs to .cartography/.

Usage: run_analysis(repo_path, output_dir) runs module graph (Surveyor) then lineage (Hydrologist).
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
) -> tuple[Path, Path, Path]:
    """
    Run Surveyor then Hydrologist on the given repo path and write all artifacts to .cartography/.

    Returns:
        (module_graph_path, lineage_graph_path, lineage_summary_path)
    """
    out = Path(output_dir) if output_dir else Path(".cartography")
    out.mkdir(parents=True, exist_ok=True)

    logger.info("Orchestrator: running Surveyor (module graph, git velocity, dead-code)...")
    surveyor = Surveyor(output_dir=out)
    module_graph_path = surveyor.analyze_repository(repo_path, output_dir=out)

    logger.info("Orchestrator: running Hydrologist (lineage, sources/sinks)...")
    hydrologist = Hydrologist(output_dir=out)
    lineage_graph_path, lineage_summary_path = hydrologist.analyze_repository(repo_path, output_dir=out)

    return module_graph_path, lineage_graph_path, lineage_summary_path
