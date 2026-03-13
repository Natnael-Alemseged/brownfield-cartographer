"""
Archivist agent: produces and maintains CODEBASE.md (living context file).

Reads from .cartography/ artifacts; writes CODEBASE.md with provenance header,
Critical Path (top 5 PageRank hubs with purpose + import_count), Data Sources & Sinks,
Known Debt, High-Velocity Files, and Module Purpose Index.
"""

import json
import logging
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import networkx as nx

from src.cartographer.knowledge_graph import LineageGraph, ModuleGraphStorage
from src.tracing.cartography_trace import CartographyTrace

logger = logging.getLogger(__name__)


def _get_repo_commit_sha(repo_path: Optional[Path]) -> str:
    """Return current HEAD SHA or empty string if not a git repo."""
    if not repo_path or not repo_path.is_dir():
        return ""
    try:
        r = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_path,
            capture_output=True,
            text=True,
            timeout=5,
        )
        if r.returncode == 0 and r.stdout:
            return r.stdout.strip()[:40]
    except Exception as e:
        logger.debug("Could not get git HEAD: %s", e)
    return ""


def _get_analysis_cost_from_trace(trace_path: Path, run_id: Optional[str]) -> int:
    """Sum input_tokens + output_tokens from trace lines for the given run_id (or all if no run_id)."""
    if not trace_path.exists():
        return 0
    total = 0
    try:
        with open(trace_path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                    if run_id and rec.get("run_id") != run_id:
                        continue
                    total += rec.get("input_tokens", 0) + rec.get("output_tokens", 0)
                except json.JSONDecodeError:
                    continue
    except Exception as e:
        logger.debug("Could not compute analysis cost from trace: %s", e)
    return total


def _get_cartographer_version() -> str:
    """Return version from package metadata or pyproject."""
    try:
        from importlib.metadata import version
        return version("brownfield-cartographer")
    except Exception:
        pass
    try:
        import tomllib
        with open(Path(__file__).resolve().parent.parent.parent / "pyproject.toml", "rb") as f:
            data = tomllib.load(f)
            return data.get("project", {}).get("version", "0.1.0")
    except Exception:
        pass
    return "0.1.0"


def _top_pagerank_hubs(storage: ModuleGraphStorage, top_n: int = 5) -> list[dict]:
    """Top N modules by PageRank with path, pagerank_score, purpose_statement, import_count (in-degree)."""
    G = storage.graph
    if G.number_of_nodes() == 0:
        return []
    try:
        pr = nx.pagerank(G)
    except Exception:
        return []
    nodes = [
        (path, pr.get(path, 0.0), G.nodes[path].get("module_node", {}))
        for path in G
        if G.nodes[path].get("node_type") == "module"
    ]
    nodes.sort(key=lambda x: -x[1])
    out = []
    for path, score, mn in nodes[:top_n]:
        mn = mn or {}
        in_degree = G.in_degree(path) if hasattr(G, "in_degree") else len(mn.get("imports") or [])
        out.append({
            "path": path,
            "pagerank_score": round(score, 6),
            "purpose_statement": (mn.get("purpose_statement") or "").strip() or "(no purpose)",
            "import_count": in_degree,
        })
    return out


def generate_CODEBASE_md(
    output_dir: Path,
    repo_name: str = "repository",
    repo_path: Optional[Path] = None,
    run_id: Optional[str] = None,
    trace: Optional[CartographyTrace] = None,
) -> Path:
    """
    Generate CODEBASE.md from existing .cartography/ artifacts.
    Sections: provenance header, Architecture Overview, Critical Path, Data Sources & Sinks,
    Known Debt, High-Velocity Files, Module Purpose Index.
    Returns path to written CODEBASE.md.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Archivist: generating CODEBASE.md in %s", output_dir)

    # Provenance
    generated_at = datetime.now(timezone.utc).isoformat()
    repo_commit_sha = _get_repo_commit_sha(repo_path) if repo_path else _get_repo_commit_sha(output_dir.parent)
    trace_path = output_dir / "cartography_trace.jsonl"
    analysis_cost = _get_analysis_cost_from_trace(trace_path, run_id)
    cartographer_version = _get_cartographer_version()

    lines = [
        "---",
        f"generated_at: {generated_at}",
        f"repo_commit_sha: {repo_commit_sha}",
        f"analysis_cost: {analysis_cost}",
        f"cartographer_version: {cartographer_version}",
        "---",
        "",
        f"# Living context: {repo_name}",
        "",
    ]

    # Architecture Overview: one paragraph from onboarding_brief or survey + lineage summaries
    overview_path = output_dir / "onboarding_brief.md"
    if overview_path.exists():
        text = overview_path.read_text(encoding="utf-8").strip()
        first_para = text.split("\n\n")[0] if text else ""
        if first_para:
            lines.extend(["## Architecture Overview", "", first_para, ""])
    if not any("Architecture Overview" in l for l in lines):
        survey_path = output_dir / "survey_summary.md"
        lineage_path = output_dir / "lineage_summary.md"
        parts = []
        if survey_path.exists():
            parts.append(survey_path.read_text(encoding="utf-8").strip()[:500])
        if lineage_path.exists():
            parts.append(lineage_path.read_text(encoding="utf-8").strip()[:500])
        if parts:
            lines.extend(["## Architecture Overview", "", " ".join(parts)[:800].strip(), ""])

    # Critical Path: top 5 PageRank with path, score, purpose, import_count
    module_graph_path = output_dir / "module_graph.json"
    if module_graph_path.exists():
        try:
            storage = ModuleGraphStorage.load(module_graph_path)
            hubs = _top_pagerank_hubs(storage, 5)
            lines.extend(["## Critical Path (top 5 modules by PageRank)", ""])
            for h in hubs:
                lines.append(f"- **{h['path']}** (score={h['pagerank_score']}, in_degree={h['import_count']})")
                lines.append(f"  - {h['purpose_statement']}")
            lines.append("")
        except Exception as e:
            logger.warning("Could not load module graph for Critical Path: %s", e)

    # Diagrams (dependency and lineage images)
    dep_png = output_dir / "dependency_graph.png"
    lineage_png = output_dir / "lineage_graph.png"
    if dep_png.exists() or lineage_png.exists():
        lines.extend(["## Diagrams", ""])
        if dep_png.exists():
            lines.append("- **Module dependencies:** `dependency_graph.png` (import graph)")
        if lineage_png.exists():
            lines.append("- **Data lineage:** `lineage_graph.png` (sources and sinks)")
        lines.append("")

    # Data Sources & Sinks
    lineage_graph_path = output_dir / "lineage_graph.json"
    if lineage_graph_path.exists():
        try:
            lg = LineageGraph.load(lineage_graph_path)
            sources = lg.find_sources()
            sinks = lg.find_sinks()
            lines.extend(["## Data Sources & Sinks", ""])
            lines.append("### Sources (in_degree=0)")
            for s in sorted(sources)[:30]:
                lines.append(f"- {s}")
            if len(sources) > 30:
                lines.append(f"- ... and {len(sources) - 30} more")
            lines.append("")
            lines.append("### Sinks (out_degree=0)")
            for s in sorted(sinks)[:30]:
                lines.append(f"- {s}")
            if len(sinks) > 30:
                lines.append(f"- ... and {len(sinks) - 30} more")
            lines.append("")
        except Exception as e:
            logger.warning("Could not load lineage for sources/sinks: %s", e)

    # Known Debt: circular deps + doc drift
    if module_graph_path.exists():
        try:
            storage = ModuleGraphStorage.load(module_graph_path)
            G = storage.graph
            lines.extend(["## Known Debt", ""])
            sccs = list(nx.strongly_connected_components(G))
            cycles = [c for c in sccs if len(c) > 1]
            if cycles:
                lines.append("### Circular dependencies (SCCs size > 1)")
                for i, comp in enumerate(cycles[:15], 1):
                    nodes = sorted(comp)
                    lines.append(f"- Cycle {i} ({len(comp)} nodes): " + ", ".join(f"`{n}`" for n in nodes[:8]))
                    if len(nodes) > 8:
                        lines.append(f"  ... and {len(nodes) - 8} more")
                lines.append("")
            drift_nodes = []
            for n in G:
                if G.nodes[n].get("node_type") != "module":
                    continue
                mn = G.nodes[n].get("module_node") or {}
                if mn.get("doc_drift_severity") or mn.get("doc_drift_type"):
                    drift_nodes.append((n, mn.get("doc_drift_severity"), mn.get("doc_drift_type")))
            if drift_nodes:
                lines.append("### Documentation drift")
                for path, sev, typ in drift_nodes[:20]:
                    lines.append(f"- `{path}`: {sev or '?'} / {typ or '?'}")
                lines.append("")
            if not cycles and not drift_nodes:
                lines.append("(No circular dependencies or doc drift flagged.)")
                lines.append("")
        except Exception as e:
            logger.warning("Could not compute Known Debt: %s", e)

    # High-Velocity Files
    if module_graph_path.exists():
        try:
            storage = ModuleGraphStorage.load(module_graph_path)
            G = storage.graph
            nodes_vel = []
            for n in G:
                if G.nodes[n].get("node_type") != "module":
                    continue
                mn = G.nodes[n].get("module_node") or {}
                vel = mn.get("change_velocity_30d", 0)
                if vel > 0:
                    nodes_vel.append((n, vel))
            nodes_vel.sort(key=lambda x: -x[1])
            lines.extend(["## High-Velocity Files (most changed in last 30 days)", ""])
            for path, count in nodes_vel[:15]:
                lines.append(f"- `{path}` — {count} change(s)")
            if not nodes_vel:
                lines.append("(No change velocity data.)")
            lines.append("")
        except Exception as e:
            logger.warning("Could not compute high-velocity files: %s", e)

    # Module Purpose Index
    if module_graph_path.exists():
        try:
            storage = ModuleGraphStorage.load(module_graph_path)
            G = storage.graph
            lines.extend(["## Module Purpose Index", ""])
            for n in sorted(G):
                if G.nodes[n].get("node_type") != "module":
                    continue
                mn = G.nodes[n].get("module_node") or {}
                purpose = (mn.get("purpose_statement") or "").strip() or "(no purpose)"
                domain = (mn.get("domain_cluster") or "").strip()
                if domain:
                    lines.append(f"- **{n}** [{domain}]")
                else:
                    lines.append(f"- **{n}**")
                lines.append(f"  {purpose}")
            lines.append("")
        except Exception as e:
            logger.warning("Could not build Module Purpose Index: %s", e)

    out_path = output_dir / "CODEBASE.md"
    out_path.write_text("\n".join(lines), encoding="utf-8")
    logger.info("Wrote %s", out_path)

    if trace:
        trace.log(
            "generate_CODEBASE_md",
            evidence_source="CODEBASE.md",
            status="success",
        )
    return out_path
