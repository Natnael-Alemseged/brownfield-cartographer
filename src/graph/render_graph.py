"""
Render module and lineage graphs to PNG images for the scanned repo.
Produces .cartography/dependency_graph.png (module imports) and
.cartography/lineage_graph.png (data flow) when artifacts exist.
"""

import logging
from pathlib import Path
from typing import Optional

import networkx as nx

logger = logging.getLogger(__name__)


def _render_nx_to_png(
    G: nx.DiGraph,
    out_path: Path,
    title: str = "Dependency Graph",
    node_labels: Optional[dict] = None,
    layout: str = "spring",
) -> bool:
    """Render a NetworkX DiGraph to PNG. Returns True on success."""
    if G.number_of_nodes() == 0:
        logger.debug("Empty graph, skipping render")
        return False
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError as e:
        logger.warning("matplotlib not available, skipping graph image: %s", e)
        return False

    try:
        fig, ax = plt.subplots(figsize=(12, 8))
        labels = node_labels or {n: n for n in G}
        # Shorten labels for display (use last path component if path-like)
        display_labels = {}
        for n, lb in labels.items():
            if isinstance(lb, str) and "/" in lb:
                display_labels[n] = lb.split("/")[-1]
            else:
                display_labels[n] = str(lb)[:30]

        if layout == "spring":
            pos = nx.spring_layout(G, k=1.5, iterations=50, seed=42)
        elif layout == "shell":
            pos = nx.shell_layout(G)
        else:
            pos = nx.spring_layout(G, seed=42)

        nx.draw_networkx_nodes(G, pos, node_color="lightblue", node_size=800, ax=ax)
        nx.draw_networkx_edges(G, pos, edge_color="gray", arrows=True, arrowsize=15, ax=ax)
        nx.draw_networkx_labels(G, pos, display_labels, font_size=8, ax=ax)
        ax.set_title(title)
        ax.axis("off")
        plt.tight_layout()
        out_path = Path(out_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(out_path, dpi=120, bbox_inches="tight")
        plt.close()
        logger.info("Wrote %s", out_path)
        return True
    except Exception as e:
        logger.warning("Could not render graph to image: %s", e)
        return False


def render_module_dependency_graph(module_graph_path: Path, output_dir: Path) -> Optional[Path]:
    """
    Load module_graph.json and render the module dependency graph to dependency_graph.png.
    Returns path to the PNG file or None if failed/skipped.
    """
    if not module_graph_path.exists():
        return None
    try:
        from src.cartographer.knowledge_graph import ModuleGraphStorage
        storage = ModuleGraphStorage.load(module_graph_path)
        G = storage.graph
        out_path = Path(output_dir) / "dependency_graph.png"
        if _render_nx_to_png(G, out_path, title="Module dependencies (imports)"):
            return out_path
    except Exception as e:
        logger.warning("Could not render module dependency graph: %s", e)
    return None


def render_lineage_graph(lineage_graph_path: Path, output_dir: Path) -> Optional[Path]:
    """
    Load lineage_graph.json and render the data lineage graph to lineage_graph.png.
    Returns path to the PNG file or None if failed/skipped.
    """
    if not lineage_graph_path.exists():
        return None
    try:
        from src.cartographer.knowledge_graph import LineageGraph
        lg = LineageGraph.load(lineage_graph_path)
        G = lg.graph
        out_path = Path(output_dir) / "lineage_graph.png"
        if _render_nx_to_png(G, out_path, title="Data lineage (sources and sinks)"):
            return out_path
    except Exception as e:
        logger.warning("Could not render lineage graph: %s", e)
    return None


def render_all_graphs(output_dir: Path) -> list[Path]:
    """
    Render both dependency_graph.png and lineage_graph.png from .cartography/ artifacts.
    Returns list of paths that were written.
    """
    output_dir = Path(output_dir)
    written = []
    mg_path = output_dir / "module_graph.json"
    p = render_module_dependency_graph(mg_path, output_dir)
    if p:
        written.append(p)
    lg_path = output_dir / "lineage_graph.json"
    p = render_lineage_graph(lg_path, output_dir)
    if p:
        written.append(p)
    return written
