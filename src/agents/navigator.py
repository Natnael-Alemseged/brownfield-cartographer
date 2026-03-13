"""
Navigator agent: LangGraph-backed query interface with four evidence-citing tools.

Tools: find_implementation(concept), trace_lineage(dataset, direction),
blast_radius(module_path), explain_module(path). Every answer cites EvidenceEntry
with file_path, line range, evidence_type, and confidence (high/medium/low).
"""

import json
import logging
from pathlib import Path
from typing import Any, Optional

import networkx as nx

from src.cartographer.knowledge_graph import LineageGraph, ModuleGraphStorage
from src.graph.semantic_index import SemanticIndexStore
from src.models import EvidenceEntry

logger = logging.getLogger(__name__)


class NavigatorArtifacts:
    """Loaded .cartography/ artifacts for the four tools."""

    def __init__(
        self,
        module_graph: ModuleGraphStorage,
        lineage_graph: LineageGraph,
        semantic_index: SemanticIndexStore,
    ) -> None:
        self.module_graph = module_graph
        self.lineage_graph = lineage_graph
        self.semantic_index = semantic_index

    @classmethod
    def load(cls, output_dir: Path) -> Optional["NavigatorArtifacts"]:
        """Load from .cartography/; return None if any required file is missing."""
        output_dir = Path(output_dir)
        mg_path = output_dir / "module_graph.json"
        lg_path = output_dir / "lineage_graph.json"
        si_path = output_dir / "semantic_index"
        if not mg_path.exists():
            logger.warning("module_graph.json not found at %s", mg_path)
            return None
        try:
            module_graph = ModuleGraphStorage.load(mg_path)
        except Exception as e:
            logger.warning("Could not load module graph: %s", e)
            return None
        lineage_graph = LineageGraph()
        if lg_path.exists():
            try:
                lineage_graph = LineageGraph.load(lg_path)
            except Exception as e:
                logger.warning("Could not load lineage graph: %s", e)
        semantic_index = SemanticIndexStore()
        if si_path.exists() and (si_path / "manifest.jsonl").exists():
            try:
                semantic_index = SemanticIndexStore.load(si_path)
            except Exception as e:
                logger.warning("Could not load semantic index: %s", e)
        return cls(module_graph, lineage_graph, semantic_index)


def _tool_find_implementation(artifacts: NavigatorArtifacts, concept: str, k: int = 5) -> tuple[str, list[EvidenceEntry]]:
    """Semantic search; confidence from similarity (high >= 0.8, medium 0.5-0.8, low < 0.5)."""
    evidence = artifacts.semantic_index.search_to_evidence(concept, k=k)
    if not evidence:
        return "No matching modules found for that concept.", []
    lines = [f"- `{e.file_path}` [{e.confidence or 'N/A'}]: {e.description[:80]}..." for e in evidence]
    return "Possible implementations:\n" + "\n".join(lines), evidence


def _tool_trace_lineage(
    artifacts: NavigatorArtifacts,
    dataset: str,
    direction: str = "upstream",
) -> tuple[str, list[EvidenceEntry]]:
    """Lineage graph traversal; confidence always high (static)."""
    nodes, edges = artifacts.lineage_graph.trace_lineage(dataset, direction)
    if not nodes and not edges:
        return f"Dataset '{dataset}' not found in lineage graph or has no {direction} nodes.", []
    G = artifacts.lineage_graph.graph
    evidence = []
    seen = set()
    for u, v, etype in edges:
        for nid in (u, v):
            if nid in seen:
                continue
            node_data = G.nodes.get(nid, {})
            source_file = node_data.get("source_file")
            if source_file:
                seen.add(nid)
                line_range = node_data.get("line_range", (0, 0))
                if isinstance(line_range, list):
                    line_range = (line_range[0], line_range[1]) if len(line_range) >= 2 else (0, 0)
                evidence.append(
                    EvidenceEntry(
                        file_path=source_file,
                        line_start=line_range[0] if isinstance(line_range, (list, tuple)) else 0,
                        line_end=line_range[1] if isinstance(line_range, (list, tuple)) and len(line_range) > 1 else 0,
                        description=f"{direction} lineage",
                        evidence_type="lineage_graph",
                        confidence="high",
                    )
                )
    if not evidence:
        for n in nodes[:5]:
            evidence.append(
                EvidenceEntry(
                    file_path=n,
                    line_start=0,
                    line_end=0,
                    description=f"Node in {direction} lineage",
                    evidence_type="lineage_graph",
                    confidence="high",
                )
            )
    result = f"Found {len(nodes)} nodes, {len(edges)} edges ({direction})."
    return result, evidence


def _tool_blast_radius(
    artifacts: NavigatorArtifacts,
    module_path: str,
    direction: str = "downstream",
) -> tuple[str, list[EvidenceEntry]]:
    """Module-level: descendants in module graph. Confidence high if depth <= 3 else medium."""
    G = artifacts.module_graph.graph
    if module_path not in G:
        return f"Module '{module_path}' not found in module graph.", []
    if direction == "downstream":
        targets = nx.descendants(G, module_path)
    else:
        targets = nx.ancestors(G, module_path)
    paths = {}
    for t in list(targets)[:50]:
        try:
            if direction == "downstream":
                path = nx.shortest_path(G, module_path, t)
            else:
                path = nx.shortest_path(G, t, module_path)
            depth = len(path) - 1
            paths[t] = (path, depth)
        except Exception:
            continue
    max_depth = max((d for _, d in paths.values()), default=0)
    conf = "high" if max_depth <= 3 else "medium"
    evidence = [
        EvidenceEntry(
            file_path=target,
            line_start=0,
            line_end=0,
            description=f"{direction} dependent (depth {d})",
            evidence_type="static_analysis",
            confidence=conf,
        )
        for target, (_, d) in list(paths.items())[:20]
    ]
    result = f"Blast radius ({direction}): {len(paths)} modules. Max depth: {max_depth}."
    return result, evidence


def _tool_explain_module(artifacts: NavigatorArtifacts, path: str) -> tuple[str, list[EvidenceEntry]]:
    """Purpose from module graph; confidence high if from graph else medium if LLM."""
    G = artifacts.module_graph.graph
    if path not in G:
        return f"Module '{path}' not found.", []
    mn = G.nodes[path].get("module_node") or {}
    purpose = (mn.get("purpose_statement") or "").strip() or "(no purpose statement)"
    domain = (mn.get("domain_cluster") or "").strip()
    evidence = [
        EvidenceEntry(
            file_path=path,
            line_start=0,
            line_end=0,
            description=purpose[:200],
            evidence_type="static_analysis",
            confidence="high",
        )
    ]
    result = f"**{path}**"
    if domain:
        result += f" [{domain}]"
    result += f"\n{purpose}"
    return result, evidence


def run_query(
    output_dir: Path,
    question: str,
    tool_hint: Optional[str] = None,
) -> tuple[str, list[EvidenceEntry]]:
    """
    Run a single query against loaded artifacts. If tool_hint is set, use that tool;
    otherwise infer from keywords: lineage/trace -> trace_lineage, blast/depend -> blast_radius,
    explain/what does -> explain_module, else find_implementation.
    Returns (answer_text, evidence_list).
    """
    artifacts = NavigatorArtifacts.load(Path(output_dir))
    if not artifacts:
        return "Could not load .cartography/ artifacts. Run 'full' or 'survey' + 'lineage' + 'semantic' first.", []

    q = question.lower().strip()
    if tool_hint:
        tool = tool_hint.lower()
    elif "lineage" in q or "produce" in q or "source" in q or "upstream" in q or "downstream" in q:
        tool = "trace_lineage"
    elif "blast" in q or "break" in q or "depend" in q or "radius" in q:
        tool = "blast_radius"
    elif "explain" in q or "what does" in q or "purpose" in q:
        tool = "explain_module"
    else:
        tool = "find_implementation"

    logger.info("Navigator: using tool=%s for query", tool)
    evidence: list[EvidenceEntry] = []

    if tool == "find_implementation":
        answer, evidence = _tool_find_implementation(artifacts, question, k=5)
    elif tool == "trace_lineage":
        # Simple heuristic: look for a dataset name (word in quotes or last token)
        parts = question.replace('"', "'").split("'")
        dataset = parts[1].strip() if len(parts) > 1 else question.split()[-1]
        direction = "upstream" if "upstream" in q or "source" in q or "produce" in q else "downstream"
        answer, evidence = _tool_trace_lineage(artifacts, dataset, direction)
    elif tool == "blast_radius":
        # Use last path-like token or quoted string
        parts = question.replace('"', "'").split("'")
        module_path = parts[1].strip() if len(parts) > 1 else question.split()[-1]
        answer, evidence = _tool_blast_radius(artifacts, module_path, "downstream")
    elif tool == "explain_module":
        parts = question.replace('"', "'").split("'")
        path = parts[1].strip() if len(parts) > 1 else question.split()[-1]
        answer, evidence = _tool_explain_module(artifacts, path)
    else:
        answer, evidence = _tool_find_implementation(artifacts, question, k=5)

    # Append evidence summary to answer
    if evidence:
        answer += "\n\n**Evidence:**\n"
        for e in evidence[:10]:
            answer += f"- `{e.file_path}`"
            if e.line_start or e.line_end:
                answer += f" (lines {e.line_start}-{e.line_end})"
            answer += f" [{e.evidence_type}]"
            if e.confidence:
                answer += f" ({e.confidence})"
            answer += f": {e.description[:60]}...\n"
    return answer, evidence
