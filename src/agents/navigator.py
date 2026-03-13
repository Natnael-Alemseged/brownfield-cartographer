"""
Navigator agent: LangGraph-backed query interface with four evidence-citing tools.

Uses a StateGraph to orchestrate: route (select tools) -> execute_tools -> synthesize.
Tools are bound @tool definitions (StructuredTool) invoked dynamically before answering.
Every answer cites EvidenceEntry with file_path, line range, evidence_type, confidence.
"""

import json
import logging
from pathlib import Path
from typing import Annotated, Any, Literal, Optional, TypedDict

import networkx as nx
from langchain_core.tools import StructuredTool, tool
from langgraph.graph import END, START, StateGraph

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


# ---- Internal tool implementations (return text + evidence) ----

def _tool_find_implementation(
    artifacts: NavigatorArtifacts, concept: str, k: int = 5
) -> tuple[str, list[EvidenceEntry]]:
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
                        line_end=(
                            line_range[1]
                            if isinstance(line_range, (list, tuple)) and len(line_range) > 1
                            else 0
                        ),
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


def _tool_explain_module(
    artifacts: NavigatorArtifacts, path: str
) -> tuple[str, list[EvidenceEntry]]:
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


# ---- State and bound @tool definitions for LangGraph ----

ToolCallSpec = dict[str, Any]  # {"tool": str, "args": dict}


class NavigatorState(TypedDict, total=False):
    """State for the Navigator StateGraph."""

    question: str
    tool_hint: Optional[str]
    artifacts: Any  # NavigatorArtifacts (not serialized across steps)
    next_tools: list[ToolCallSpec]
    tool_results: list[str]
    evidence: list[dict[str, Any]]  # EvidenceEntry.model_dump()
    answer: str


def _create_bound_tools(artifacts: NavigatorArtifacts) -> dict[str, StructuredTool]:
    """Create four StructuredTools with artifacts bound (for dynamic invocation)."""

    @tool
    def find_implementation(concept: str, k: int = 5) -> dict[str, Any]:
        """Search the codebase for modules that implement or relate to a concept. Use for 'where is X', 'find Y', 'how is Z done'."""
        text, ev = _tool_find_implementation(artifacts, concept, k=k)
        return {"result": text, "evidence": [e.model_dump() for e in ev]}

    @tool
    def trace_lineage(
        dataset: str,
        direction: Annotated[str, Literal["upstream", "downstream"]] = "upstream",
    ) -> dict[str, Any]:
        """Trace data lineage for a dataset: upstream (sources) or downstream (sinks). Use for 'what produces X', 'where does Y go'."""
        text, ev = _tool_trace_lineage(artifacts, dataset, direction=direction)
        return {"result": text, "evidence": [e.model_dump() for e in ev]}

    @tool
    def blast_radius(
        module_path: str,
        direction: Annotated[str, Literal["upstream", "downstream"]] = "downstream",
    ) -> dict[str, Any]:
        """Compute impact: which modules depend on this one (downstream) or it depends on (upstream). Use for 'if I change X, what breaks'."""
        text, ev = _tool_blast_radius(artifacts, module_path, direction=direction)
        return {"result": text, "evidence": [e.model_dump() for e in ev]}

    @tool
    def explain_module(path: str) -> dict[str, Any]:
        """Get the purpose and domain of a module from the graph. Use for 'what does X do', 'explain Y'."""
        text, ev = _tool_explain_module(artifacts, path)
        return {"result": text, "evidence": [e.model_dump() for e in ev]}

    return {
        "find_implementation": find_implementation,
        "trace_lineage": trace_lineage,
        "blast_radius": blast_radius,
        "explain_module": explain_module,
    }


def _route_node(state: NavigatorState) -> dict[str, Any]:
    """Select which tool(s) to run and with what args (dynamic orchestration)."""
    question = (state.get("question") or "").strip()
    tool_hint = state.get("tool_hint")
    q = question.lower()

    if tool_hint:
        tool_name = tool_hint.lower().strip()
    elif any(k in q for k in ("lineage", "produce", "source", "upstream", "downstream")):
        tool_name = "trace_lineage"
    elif any(k in q for k in ("blast", "break", "depend", "radius")):
        tool_name = "blast_radius"
    elif any(k in q for k in ("explain", "what does", "purpose")):
        tool_name = "explain_module"
    else:
        tool_name = "find_implementation"

    # Build tool call spec with parsed args
    parts = question.replace('"', "'").split("'")
    quoted = parts[1].strip() if len(parts) > 1 else None
    last_token = question.split()[-1] if question.split() else ""

    if tool_name == "trace_lineage":
        dataset = quoted or last_token
        direction = "upstream" if any(x in q for x in ("upstream", "source", "produce")) else "downstream"
        next_tools = [{"tool": "trace_lineage", "args": {"dataset": dataset, "direction": direction}}]
    elif tool_name == "blast_radius":
        module_path = quoted or last_token
        next_tools = [{"tool": "blast_radius", "args": {"module_path": module_path, "direction": "downstream"}}]
    elif tool_name == "explain_module":
        path = quoted or last_token
        next_tools = [{"tool": "explain_module", "args": {"path": path}}]
    else:
        next_tools = [{"tool": "find_implementation", "args": {"concept": question, "k": 5}}]

    logger.info("Navigator StateGraph: route -> %s", next_tools)
    return {"next_tools": next_tools}


def _execute_tools_node(state: NavigatorState, tools: dict[str, StructuredTool]) -> dict[str, Any]:
    """Invoke each selected tool and merge results and evidence into state."""
    next_tools: list[ToolCallSpec] = state.get("next_tools") or []
    tool_results: list[str] = []
    evidence: list[dict[str, Any]] = list(state.get("evidence") or [])

    for spec in next_tools:
        name = spec.get("tool")
        args = spec.get("args") or {}
        tool_fn = tools.get(name) if isinstance(name, str) else None
        if not tool_fn:
            tool_results.append(f"(Unknown tool: {name})")
            continue
        try:
            out = tool_fn.invoke(args)
            if isinstance(out, dict):
                tool_results.append(out.get("result", str(out)))
                evidence.extend(out.get("evidence") or [])
            else:
                tool_results.append(str(out))
        except Exception as e:
            logger.exception("Tool %s failed: %s", name, e)
            tool_results.append(f"Error: {e}")

    return {"tool_results": tool_results, "evidence": evidence}


def _synthesize_node(state: NavigatorState) -> dict[str, Any]:
    """Format final answer from tool results and evidence."""
    tool_results: list[str] = state.get("tool_results") or []
    evidence: list[dict[str, Any]] = state.get("evidence") or []

    answer = "\n\n".join(tool_results) if tool_results else "No results."
    if evidence:
        answer += "\n\n**Evidence:**\n"
        for e in evidence[:10]:
            entry = e if isinstance(e, dict) else {}
            fp = entry.get("file_path", "")
            ls = entry.get("line_start", 0)
            le = entry.get("line_end", 0)
            desc = (entry.get("description") or "")[:60]
            etype = entry.get("evidence_type", "")
            conf = entry.get("confidence", "")
            line_part = f" (lines {ls}-{le})" if ls or le else ""
            answer += f"- `{fp}`{line_part} [{etype}]"
            if conf:
                answer += f" ({conf})"
            answer += f": {desc}...\n"

    return {"answer": answer}


def _build_navigator_graph(tools: dict[str, StructuredTool]) -> Any:
    """Build and compile the StateGraph: route -> execute_tools -> synthesize."""
    builder = StateGraph(NavigatorState)

    def execute_tools(state: NavigatorState) -> dict[str, Any]:
        return _execute_tools_node(state, tools)

    builder.add_node("route", _route_node)
    builder.add_node("execute_tools", execute_tools)
    builder.add_node("synthesize", _synthesize_node)

    builder.add_edge(START, "route")
    builder.add_edge("route", "execute_tools")
    builder.add_edge("execute_tools", "synthesize")
    builder.add_edge("synthesize", END)

    return builder.compile()


def run_query(
    output_dir: Path,
    question: str,
    tool_hint: Optional[str] = None,
) -> tuple[str, list[EvidenceEntry]]:
    """
    Run a single query via the LangGraph StateGraph. Tools are bound to loaded
    artifacts and orchestrated dynamically (route -> execute_tools -> synthesize).
    Returns (answer_text, evidence_list).
    """
    artifacts = NavigatorArtifacts.load(Path(output_dir))
    if not artifacts:
        return (
            "Could not load .cartography/ artifacts. Run 'full' or 'survey' + 'lineage' + 'semantic' first.",
            [],
        )

    tools = _create_bound_tools(artifacts)
    graph = _build_navigator_graph(tools)

    initial: NavigatorState = {
        "question": question,
        "tool_hint": tool_hint,
        "artifacts": artifacts,
        "next_tools": [],
        "tool_results": [],
        "evidence": [],
        "answer": "",
    }

    result = graph.invoke(initial)
    answer = result.get("answer") or ""
    evidence_dicts = result.get("evidence") or []
    evidence_list: list[EvidenceEntry] = []
    for d in evidence_dicts:
        if isinstance(d, dict):
            try:
                evidence_list.append(EvidenceEntry.model_validate(d))
            except Exception:
                pass

    return answer, evidence_list
