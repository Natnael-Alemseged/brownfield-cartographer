"""
Lineage knowledge graph: datasets, transformations, PRODUCES/CONSUMES edges.
Module graph storage: ModuleNode-based graph with typed edges and JSON ser/deser.

Integrates with Phase 1 module graph; adds DatasetNode and TransformationNode layers.
"""

import json
import logging
from pathlib import Path
from typing import Optional

import networkx as nx
from networkx.readwrite import json_graph

from src.models import DatasetNode, EdgeType, ModuleNode, TransformationNode

logger = logging.getLogger(__name__)


class LineageGraph:
    """
    Directed graph for data lineage: dataset nodes, transformation nodes,
    PRODUCES (transformation -> dataset) and CONSUMES (dataset -> transformation) edges.
    """

    def __init__(self) -> None:
        self._G = nx.DiGraph()
        self._datasets: dict[str, dict] = {}  # name -> DatasetNode attributes
        self._transformations: dict[str, dict] = {}  # id -> TransformationNode attributes
        self._transformation_counter = 0

    def _transformation_id(self, source_file: str, line_range: tuple[int, int]) -> str:
        return f"transformation:{source_file}:{line_range[0]}-{line_range[1]}"

    def add_dataset(self, name: str, **attrs: object) -> None:
        """Ensure a dataset node exists (merge optional attributes)."""
        if name not in self._datasets:
            self._datasets[name] = {"name": name, **attrs}
            self._G.add_node(name, node_type="dataset", **self._datasets[name])
        else:
            for k, v in attrs.items():
                if v is not None:
                    self._datasets[name][k] = v
            self._G.add_node(name, node_type="dataset", **self._datasets[name])

    def add_transformation(self, t: TransformationNode) -> str:
        """
        Add a transformation and PRODUCES/CONSUMES edges.
        Returns the transformation node id.
        """
        self._transformation_counter += 1
        base_id = self._transformation_id(t.source_file, t.line_range)
        tid = f"{base_id}#{self._transformation_counter}"
        self._transformations[tid] = t.model_dump(mode="json")
        self._G.add_node(tid, node_type="transformation", **self._transformations[tid])

        for src in t.source_datasets:
            self.add_dataset(src)
            self._G.add_edge(
                src, tid,
                edge_type=EdgeType.CONSUMES.value,
                transformation_type=t.transformation_type,
                source_file=t.source_file,
                line_range=t.line_range,
            )
        for tgt in t.target_datasets:
            self.add_dataset(tgt)
            self._G.add_edge(
                tid, tgt,
                edge_type=EdgeType.PRODUCES.value,
                transformation_type=t.transformation_type,
                source_file=t.source_file,
                line_range=t.line_range,
            )

        return tid

    def blast_radius(self, node_name: str, direction: str = "downstream") -> list[str]:
        """
        Return all nodes affected in the given direction (downstream or upstream).
        """
        if node_name not in self._G:
            return []
        if direction == "downstream":
            return list(nx.descendants(self._G, node_name))
        return list(nx.ancestors(self._G, node_name))

    def find_sources(self) -> list[str]:
        """Nodes with in_degree 0 (entry points)."""
        return [n for n in self._G.nodes() if self._G.in_degree(n) == 0 and self._G.nodes[n].get("node_type") == "dataset"]

    def find_sinks(self) -> list[str]:
        """Dataset nodes with out_degree 0 (final outputs)."""
        return [n for n in self._G.nodes() if self._G.out_degree(n) == 0 and self._G.nodes[n].get("node_type") == "dataset"]

    def trace_lineage(
        self, dataset_name: str, direction: str = "upstream"
    ) -> tuple[list[str], list[tuple[str, str, str]]]:
        """
        Return (list of node ids, list of (source, target, edge_type)) in lineage order.
        direction: 'upstream' (sources of this dataset) or 'downstream' (consumers/outputs).
        """
        if dataset_name not in self._G:
            return [], []
        nodes = []
        edges = []
        if direction == "upstream":
            for pred in nx.ancestors(self._G, dataset_name):
                nodes.append(pred)
                for u, v in self._G.in_edges(pred):
                    edges.append((u, v, self._G.edges[u, v].get("edge_type", "")))
            nodes.append(dataset_name)
            for u, v in self._G.in_edges(dataset_name):
                edges.append((u, v, self._G.edges[u, v].get("edge_type", "")))
        else:
            for succ in nx.descendants(self._G, dataset_name):
                nodes.append(succ)
                for u, v in self._G.out_edges(succ):
                    edges.append((u, v, self._G.edges[u, v].get("edge_type", "")))
            nodes.append(dataset_name)
            for u, v in self._G.out_edges(dataset_name):
                edges.append((u, v, self._G.edges[u, v].get("edge_type", "")))
        return list(dict.fromkeys(nodes)), list(dict.fromkeys(edges))

    def to_dict(self) -> dict:
        """Serialize for JSON (node_link_data style with edges)."""
        return json_graph.node_link_data(self._G)

    def write_json(self, path: Path) -> None:
        """Write lineage graph to JSON."""
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(self.to_dict(), f, indent=2)
        logger.info("Wrote %s", path)

    @classmethod
    def load(cls, path: str | Path) -> "LineageGraph":
        """Deserialize lineage graph from JSON (node_link_data format). Returns a new LineageGraph."""
        path = Path(path)
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        G = json_graph.node_link_graph(data)
        inst = cls()
        inst._G = G
        # Rebuild _datasets and _transformations from node attributes for API consistency
        for n, attrs in G.nodes(data=True):
            if attrs.get("node_type") == "dataset":
                inst._datasets[n] = dict(attrs)
            elif attrs.get("node_type") == "transformation":
                inst._transformations[n] = dict(attrs)
        return inst

    @property
    def graph(self) -> nx.DiGraph:
        return self._G


class ModuleGraphStorage:
    """
    Shared storage for the module import graph. Wraps NetworkX with typed methods
    for adding ModuleNodes and edges (EdgeType), with JSON serialization and deserialization.
    Multiple agents can write to and read from this layer.
    """

    def __init__(self) -> None:
        self._G = nx.DiGraph()

    def add_module_node(self, node: ModuleNode) -> None:
        """Add or update a module node. Enforces alignment with ModuleNode schema."""
        attrs = node.model_dump(mode="json")
        self._G.add_node(node.path, node_type="module", module_node=attrs)

    def add_edge(self, u: str, v: str, edge_type: EdgeType) -> None:
        """Add a typed edge between module nodes."""
        self._G.add_edge(u, v, edge_type=edge_type.value)

    def to_dict(self) -> dict:
        """Serialize graph to node_link_data format for JSON."""
        return json_graph.node_link_data(self._G)

    def write_json(self, path: Path) -> None:
        """Write module graph to JSON."""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(self.to_dict(), f, indent=2)
        logger.info("Wrote %s", path)

    @classmethod
    def load(cls, path: str | Path) -> "ModuleGraphStorage":
        """Deserialize module graph from JSON. Returns a new ModuleGraphStorage."""
        path = Path(path)
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        G = json_graph.node_link_graph(data)
        inst = cls()
        inst._G = G
        return inst

    @property
    def graph(self) -> nx.DiGraph:
        return self._G


class KnowledgeGraph:
    """
    Wrapper for loading a saved lineage graph from JSON and querying it.
    trace_lineage / find_sources / find_sinks return list of node dicts with \"name\" key.
    """

    def __init__(self, G: nx.DiGraph) -> None:
        self._G = G

    @classmethod
    def load(cls, path: str | Path) -> "KnowledgeGraph":
        """Load a lineage graph from JSON (node_link_data format)."""
        path = Path(path)
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        G = json_graph.node_link_graph(data)
        return cls(G)

    def trace_lineage(self, dataset_name: str, direction: str = "upstream") -> list[dict]:
        """
        Return list of node dicts (each with \"name\" key) upstream or downstream of dataset_name.
        """
        if dataset_name not in self._G:
            return []
        if direction == "upstream":
            nodes = list(nx.ancestors(self._G, dataset_name)) + [dataset_name]
        else:
            nodes = [dataset_name] + list(nx.descendants(self._G, dataset_name))
        result = []
        for n in nodes:
            attrs = dict(self._G.nodes[n])
            attrs.setdefault("name", n)
            attrs.setdefault("id", n)
            result.append(attrs)
        return result

    def find_sources(self) -> list[dict]:
        """Return list of source node dicts (in_degree 0, dataset type) with \"name\" key."""
        sources = [
            n for n in self._G.nodes()
            if self._G.in_degree(n) == 0 and self._G.nodes[n].get("node_type") == "dataset"
        ]
        return [{"name": n, "id": n, **dict(self._G.nodes[n])} for n in sources]

    def find_sinks(self) -> list[dict]:
        """Return list of sink node dicts (out_degree 0, dataset type) with \"name\" key."""
        sinks = [
            n for n in self._G.nodes()
            if self._G.out_degree(n) == 0 and self._G.nodes[n].get("node_type") == "dataset"
        ]
        return [{"name": n, "id": n, **dict(self._G.nodes[n])} for n in sinks]
