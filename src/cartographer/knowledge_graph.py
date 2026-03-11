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

# Max paths to return from paths_between to avoid explosion
MAX_PATHS_BETWEEN = 50


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
        """Ensure a dataset node exists. Validates attributes against DatasetNode schema."""
        if not name or not str(name).strip():
            raise ValueError("dataset name must be non-empty")
        allowed = set(DatasetNode.model_fields)
        safe_attrs = {k: v for k, v in attrs.items() if k in allowed and v is not None}
        try:
            node = DatasetNode(name=name, **safe_attrs)
            validated = node.model_dump(mode="json")
        except Exception as e:
            raise ValueError(f"dataset attributes invalid for '{name}': {e}") from e
        if name not in self._datasets:
            self._datasets[name] = dict(validated)
            self._G.add_node(name, node_type="dataset", **self._datasets[name])
        else:
            for k, v in validated.items():
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

    def blast_radius(self, node_name: str, direction: str = "downstream") -> dict[str, list[str]]:
        """
        Return all affected nodes and their shortest paths from/to node_name.

        direction="downstream": paths from node_name -> target (data consumers).
        direction="upstream": paths from source -> node_name (data producers).
        """
        if node_name not in self._G:
            return {}
        if direction == "downstream":
            targets = nx.descendants(self._G, node_name)
        else:
            targets = nx.ancestors(self._G, node_name)
        paths: dict[str, list[str]] = {}
        for t in targets:
            try:
                if direction == "downstream":
                    path = nx.shortest_path(self._G, node_name, t)
                else:
                    path = nx.shortest_path(self._G, t, node_name)
            except Exception:
                continue
            paths[t] = path
        return paths

    def blast_radius_filtered(
        self,
        node_name: str,
        direction: str = "downstream",
        transformation_type: Optional[str] = None,
    ) -> dict[str, list[str]]:
        """
        Like blast_radius but only traverses edges whose transformation_type matches.
        If transformation_type is None, same as blast_radius.
        """
        if node_name not in self._G:
            return {}
        if transformation_type is None:
            return self.blast_radius(node_name, direction)
        # Build subgraph with only matching edges
        if direction == "downstream":
            edges_ok = [(u, v) for u, v in self._G.edges()
                        if self._G.edges[u, v].get("transformation_type") == transformation_type]
        else:
            edges_ok = [(v, u) for u, v in self._G.edges()
                        if self._G.edges[u, v].get("transformation_type") == transformation_type]
        sub = nx.DiGraph()
        sub.add_edges_from(edges_ok)
        if node_name not in sub:
            return {}
        targets = nx.descendants(sub, node_name)
        paths: dict[str, list[str]] = {}
        for t in targets:
            try:
                path = nx.shortest_path(sub, node_name, t)
                paths[t] = path
            except Exception:
                continue
        return paths

    def paths_between(
        self,
        source: str,
        target: str,
        max_paths: int = MAX_PATHS_BETWEEN,
    ) -> list[list[str]]:
        """
        Enumerate simple paths from source to target (for lineage explanation).
        Capped at max_paths to avoid combinatorial explosion.
        """
        if source not in self._G or target not in self._G:
            return []
        out: list[list[str]] = []
        try:
            for i, path in enumerate(nx.all_simple_paths(self._G, source, target)):
                if i >= max_paths:
                    break
                out.append(path)
        except Exception:
            pass
        return out

    def rank_datasets(
        self,
        top_n: int = 20,
    ) -> list[dict]:
        """
        Rank dataset nodes by criticality: fan-in + fan-out (and optionally transformation density).
        Returns list of dicts with name, fan_in, fan_out, score (fan_in + fan_out), node_type=dataset.
        """
        datasets = [n for n in self._G.nodes() if self._G.nodes[n].get("node_type") == "dataset"]
        rows: list[dict] = []
        for n in datasets:
            fan_in = self._G.in_degree(n)
            fan_out = self._G.out_degree(n)
            score = fan_in + fan_out
            rows.append({
                "name": n,
                "fan_in": fan_in,
                "fan_out": fan_out,
                "score": score,
                "node_type": "dataset",
            })
        rows.sort(key=lambda r: -r["score"])
        return rows[:top_n]

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
        """Serialize for JSON (node_link_data style with edges). Includes schema_version for migrations."""
        data = json_graph.node_link_data(self._G)
        data["schema_version"] = 1
        return data

    def write_json(self, path: Path) -> None:
        """Write lineage graph to JSON."""
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(self.to_dict(), f, indent=2)
        logger.info("Wrote %s", path)

    @classmethod
    def load(cls, path: str | Path) -> "LineageGraph":
        """Deserialize lineage graph from JSON (node_link_data format). Supports schema_version for future migrations."""
        path = Path(path)
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        version = data.pop("schema_version", None)
        if version is not None and version != 1:
            logger.warning("Lineage graph schema_version=%s; current is 1. Migration not implemented.", version)
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
        """Serialize graph to node_link_data format for JSON. Includes schema_version for migrations."""
        data = json_graph.node_link_data(self._G)
        data["schema_version"] = 1
        return data

    def write_json(self, path: Path) -> None:
        """Write module graph to JSON."""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(self.to_dict(), f, indent=2)
        logger.info("Wrote %s", path)

    @classmethod
    def load(cls, path: str | Path) -> "ModuleGraphStorage":
        """Deserialize module graph from JSON. Handles schema_version for future migrations."""
        path = Path(path)
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        data.pop("schema_version", None)
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
        data.pop("schema_version", None)
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
