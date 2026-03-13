"""
Semanticist agent: LLM-powered purpose extraction, domain clustering, and FDE Day-One synthesis.

Uses ContextWindowBudget and Trace Manager; supports Ollama 3.2 (default), incremental runs,
smart code truncation, and structured Day-One answers with evidence types.
"""

import hashlib
import json
import logging
import os
import re
from pathlib import Path
from typing import Any, Literal, Optional

import networkx as nx
import numpy as np
from scipy.cluster.vq import kmeans2

from src.cartographer.knowledge_graph import LineageGraph, ModuleGraphStorage
from src.models import (
    DayOneAnswer,
    EvidenceEntry,
    EvidenceType,
    ModuleNode,
)

logger = logging.getLogger(__name__)

# Trace action types
TraceAction = Literal["purpose_extraction", "drift_detection", "cluster_labeling", "day_one_synthesis"]
TraceStatus = Literal["success", "error", "skipped"]

# Default models (Ollama 3.2 for token conservation)
DEFAULT_MODEL_BULK = "ollama/llama3.2"
DEFAULT_MODEL_SYNTHESIS = "ollama/llama3.2"

TRUNCATION_MARKER = "# ... [truncated for context window] ..."

# Approximate chars per token for heuristic when tiktoken not available
CHARS_PER_TOKEN_HEURISTIC = 4


def _extract_first_json_object(text: str) -> str:
    """Extract the first complete top-level JSON object from text (handles trailing content or multiple objects)."""
    text = text.strip()
    if not text or text[0] != "{":
        return "{}"
    depth = 0
    in_string = False
    escape = False
    quote = None
    i = 0
    for i, c in enumerate(text):
        if escape:
            escape = False
            continue
        if in_string:
            if c == "\\":
                escape = True
                continue
            if c == quote:
                in_string = False
            continue
        if c in ("'", '"'):
            in_string = True
            quote = c
            continue
        if c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                return text[: i + 1]
    return text


# -----------------------------------------------------------------------------
# ContextWindowBudget
# -----------------------------------------------------------------------------


class ContextWindowBudget:
    """
    Token budget: estimate tokens before each LLM call, track cumulative usage,
    and select model by task (bulk vs synthesis).
    """

    def __init__(
        self,
        max_tokens: Optional[int] = None,
        model_bulk: str = DEFAULT_MODEL_BULK,
        model_synthesis: Optional[str] = None,
    ) -> None:
        self.max_tokens = max_tokens  # None = no hard cap
        self.model_bulk = model_bulk
        self.model_synthesis = model_synthesis or model_bulk
        self._input_tokens = 0
        self._output_tokens = 0

    def estimate_tokens(self, text: str) -> int:
        """Estimate token count for text (heuristic for Ollama/local models)."""
        try:
            import tiktoken
            enc = tiktoken.get_encoding("cl100k_base")
            return len(enc.encode(text))
        except Exception:
            return max(1, len(text) // CHARS_PER_TOKEN_HEURISTIC)

    def add_usage(self, input_tokens: int, output_tokens: int = 0) -> None:
        self._input_tokens += input_tokens
        self._output_tokens += output_tokens

    @property
    def cumulative_input_tokens(self) -> int:
        return self._input_tokens

    @property
    def cumulative_output_tokens(self) -> int:
        return self._output_tokens

    def can_afford(self, estimated_input: int, max_output: int = 1024) -> bool:
        """True if we can still afford this call (within max_tokens if set)."""
        if self.max_tokens is None:
            return True
        projected = self._input_tokens + self._output_tokens + estimated_input + max_output
        return projected <= self.max_tokens

    def model_for(self, task: Literal["bulk", "synthesis"]) -> str:
        return self.model_bulk if task == "bulk" else self.model_synthesis


# -----------------------------------------------------------------------------
# Trace Manager (Pillar 1: Auditability)
# -----------------------------------------------------------------------------


class TraceManager:
    """Appends every LLM call and budget update to .cartography/cartography_trace.jsonl."""

    def __init__(self, output_dir: Path) -> None:
        self._output_dir = Path(output_dir)
        self._path = self._output_dir / "cartography_trace.jsonl"
        self._path.parent.mkdir(parents=True, exist_ok=True)

    def log(
        self,
        action: TraceAction,
        *,
        target_module: Optional[str] = None,
        model_used: Optional[str] = None,
        input_tokens: int = 0,
        output_tokens: int = 0,
        cumulative_budget: Optional[dict] = None,
        confidence_score: Optional[float] = None,
        status: TraceStatus = "success",
        error_message: Optional[str] = None,
    ) -> None:
        import time
        record: dict[str, Any] = {
            "timestamp": time.time(),
            "agent": "semanticist",
            "action": action,
            "target_module": target_module,
            "model_used": model_used,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "cumulative_budget": cumulative_budget,
            "confidence_score": confidence_score,
            "status": status,
        }
        if error_message:
            record["error_message"] = error_message
        with open(self._path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, default=str) + "\n")


# -----------------------------------------------------------------------------
# Smart truncation (Pillar 2)
# -----------------------------------------------------------------------------


def _smart_truncate(code: str, max_tokens: int) -> str:
    """
    Preserve (1) all import statements, (2) all class/function signatures,
    (3) fill remaining budget with body lines; append truncation marker if truncated.
    """
    def est(s: str) -> int:
        return max(1, len(s) // CHARS_PER_TOKEN_HEURISTIC)

    lines = code.splitlines()
    if est(code) <= max_tokens:
        return code

    # Partition: import block, then signature lines (def/class), then body
    import_lines: list[str] = []
    signature_lines: list[str] = []
    body_lines: list[str] = []
    in_imports = True
    import_re = re.compile(r"^(\s*)(import |from .+ import )")
    def_class_re = re.compile(r"^\s*(def |class )")

    i = 0
    while i < len(lines):
        line = lines[i]
        stripped = line.strip()
        if in_imports and (import_re.match(line) or (stripped.startswith("from ") and " import " in stripped)):
            import_lines.append(line)
            i += 1
            continue
        in_imports = False
        if def_class_re.match(line):
            signature_lines.append(line)
            i += 1
            continue
        body_lines.append(line)
        i += 1

    out_parts: list[str] = []
    budget_used = 0
    marker_tokens = est(TRUNCATION_MARKER) + 1

    for L in (import_lines, signature_lines):
        for ln in L:
            t = est(ln) + 1
            if budget_used + t + marker_tokens > max_tokens:
                break
            out_parts.append(ln)
            budget_used += t
    remaining = max_tokens - budget_used - marker_tokens
    for ln in body_lines:
        if remaining <= 0:
            break
        t = est(ln) + 1
        if t > remaining:
            break
        out_parts.append(ln)
        remaining -= t
        budget_used += t

    result = "\n".join(out_parts)
    if budget_used < est(code):
        result += "\n" + TRUNCATION_MARKER
    return result


# -----------------------------------------------------------------------------
# Docstring extraction (for drift detection)
# -----------------------------------------------------------------------------


def _extract_module_docstring_python(source: str) -> Optional[str]:
    """Extract first module-level string literal (docstring) from Python source."""
    # Skip shebang and encoding
    lines = source.strip().splitlines()
    start = 0
    if lines and (lines[0].startswith("#!") or "coding" in lines[0].lower()):
        start = 1
    # Find first triple-quoted string at module level
    pattern = r'^("""[\s\S]*?"""|\'\'\'[\s\S]*?\'\'\')'
    m = re.search(pattern, "\n".join(lines[start:]), re.MULTILINE)
    if m:
        return m.group(1).strip('"\'').strip()
    return None


# -----------------------------------------------------------------------------
# generate_purpose_statement (with resilience)
# -----------------------------------------------------------------------------


def generate_purpose_statement(
    repo_path: str,
    module_node: ModuleNode,
    budget: ContextWindowBudget,
    trace: TraceManager,
) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Return (purpose_statement, doc_drift_severity, doc_drift_type).
    On failure returns (None, None, None) and caller should set analysis_error.
    """
    import litellm

    file_path = Path(repo_path) / module_node.path
    if not file_path.is_file():
        return None, None, None
    try:
        source_bytes = file_path.read_bytes()
        source = source_bytes.decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        trace.log("purpose_extraction", target_module=module_node.path, status="error", error_message="encoding_error")
        return None, None, None
    except OSError as e:
        logger.warning("Could not read %s: %s", module_node.path, e)
        trace.log("purpose_extraction", target_module=module_node.path, status="error", error_message="read_error")
        return None, None, None

    # Binary / empty check
    if not source.strip():
        return None, None, None

    max_tokens_in = 2000
    max_tokens_out = 256
    if not budget.can_afford(max_tokens_in, max_tokens_out):
        trace.log("purpose_extraction", target_module=module_node.path, status="skipped", error_message="budget_exceeded")
        return None, None, None

    code_for_prompt = _smart_truncate(source, max_tokens_in - 200)

    prompt = f"""You are a code analyst. Based only on the implementation below (not docstrings), write a 2-3 sentence purpose statement: what this module does in business/functional terms, not implementation detail. Be concise.

File: {module_node.path}

``` 
{code_for_prompt}
```

Purpose statement:"""

    model = budget.model_for("bulk")
    try:
        response = litellm.completion(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=max_tokens_out,
        )
    except Exception as e:
        logger.warning("LLM call failed for %s: %s", module_node.path, e)
        trace.log(
            "purpose_extraction",
            target_module=module_node.path,
            model_used=model,
            status="error",
            error_message=str(e),
        )
        return None, None, None

    content = (response.choices[0].message.content or "").strip()
    in_tok = response.usage.prompt_tokens if response.usage else budget.estimate_tokens(prompt)
    out_tok = response.usage.completion_tokens if response.usage else budget.estimate_tokens(content)
    budget.add_usage(in_tok, out_tok)
    trace.log(
        "purpose_extraction",
        target_module=module_node.path,
        model_used=model,
        input_tokens=in_tok,
        output_tokens=out_tok,
        cumulative_budget={"input": budget.cumulative_input_tokens, "output": budget.cumulative_output_tokens},
        status="success",
    )

    # Drift detection: compare to module docstring (Python only)
    doc_drift_severity: Optional[str] = None
    doc_drift_type: Optional[str] = None
    if module_node.path.endswith(".py"):
        docstring = _extract_module_docstring_python(source)
        if docstring and content:
            # Simple heuristic: if docstring and purpose disagree strongly, flag
            doc_lower = docstring.lower()
            purpose_lower = content.lower()
            if doc_lower and purpose_lower and not any(w in purpose_lower for w in doc_lower.split()[:5] if len(w) > 3):
                doc_drift_severity = "major"
                doc_drift_type = "STALE_DESCRIPTION"

    return content or None, doc_drift_severity, doc_drift_type


# -----------------------------------------------------------------------------
# cluster_into_domains (TF-IDF + k-means, then LLM or heuristic labels)
# -----------------------------------------------------------------------------


def _tfidf_embed(texts: list[str], max_features: int = 128) -> np.ndarray:
    """Simple TF-IDF style embedding (term frequency, no IDF for simplicity)."""
    from collections import Counter
    vocab: dict[str, int] = {}
    tokenize = re.compile(r"\b\w+\b").findall
    doc_freq: list[Counter] = []
    for t in texts:
        tokens = [x.lower() for x in tokenize(t) if len(x) > 1]
        doc_freq.append(Counter(tokens))
        for w in tokens:
            vocab.setdefault(w, len(vocab))
            if len(vocab) >= max_features:
                break
        if len(vocab) >= max_features:
            break
    # Limit vocab to max_features
    vocab = dict(list(vocab.items())[:max_features])
    inv_vocab = {v: k for k, v in vocab.items()}
    rows = []
    for cf in doc_freq:
        row = [0.0] * len(vocab)
        total = sum(cf.values()) or 1
        for idx, w in inv_vocab.items():
            row[idx] = cf.get(w, 0) / total
        rows.append(row)
    return np.array(rows, dtype=np.float64)


def cluster_into_domains(
    storage: ModuleGraphStorage,
    budget: ContextWindowBudget,
    trace: TraceManager,
    k: int = 6,
) -> None:
    """Assign domain_cluster to each module from k-means on purpose statement embeddings."""
    G = storage.graph
    nodes_with_purpose = [
        (path, G.nodes[path].get("module_node", {}))
        for path in G
        if G.nodes[path].get("node_type") == "module"
        and (G.nodes[path].get("module_node") or {}).get("purpose_statement")
    ]
    if not nodes_with_purpose:
        logger.info("No modules with purpose_statement; skipping domain clustering.")
        return
    paths = [p for p, _ in nodes_with_purpose]
    texts = [m.get("purpose_statement", "") or "" for _, m in nodes_with_purpose]
    X = _tfidf_embed(texts)
    if X.shape[0] < k:
        k = max(1, X.shape[0])
    try:
        centroids, labels = kmeans2(X, k, minit="points")
    except Exception as e:
        logger.warning("k-means failed: %s; assigning single domain", e)
        labels = np.zeros(len(paths), dtype=int)
        k = 1

    # Label clusters by most common tokens in purpose statements of that cluster
    cluster_labels: dict[int, str] = {}
    for c in range(k):
        indices = [i for i, lb in enumerate(labels) if lb == c]
        if not indices:
            cluster_labels[c] = "other"
            continue
        combined = " ".join(texts[i] for i in indices)
        tokens = re.findall(r"\b\w{4,}\b", combined.lower())
        from collections import Counter
        top = Counter(tokens).most_common(3)
        cluster_labels[c] = "_".join(t[0] for t in top) if top else "other"

    for path, label_idx in zip(paths, labels):
        label = cluster_labels.get(int(label_idx), "other")
        if G.has_node(path) and G.nodes[path].get("node_type") == "module":
            mn = dict(G.nodes[path].get("module_node", {}))
            mn["domain_cluster"] = label
            G.nodes[path]["module_node"] = mn
    trace.log("cluster_labeling", status="success", cumulative_budget={"input": budget.cumulative_input_tokens, "output": budget.cumulative_output_tokens})


# -----------------------------------------------------------------------------
# answer_day_one_questions (PageRank hub + DayOneAnswer JSON + onboarding_brief.md)
# -----------------------------------------------------------------------------


FDE_QUESTIONS = [
    "What is the primary data ingestion path?",
    "What are the 3-5 most critical output datasets/endpoints?",
    "What is the blast radius if the most critical module fails?",
    "Where is the business logic concentrated vs. distributed?",
    "What has changed most frequently in the last 90 days (git velocity)?",
]


def _top_pagerank_modules(storage: ModuleGraphStorage, top_n: int = 5) -> list[dict]:
    """Return top N modules by PageRank with path, pagerank_score, purpose_statement, import_count."""
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
        out.append({
            "path": path,
            "pagerank_score": round(score, 6),
            "purpose_statement": mn.get("purpose_statement") or "",
            "import_count": len(mn.get("imports") or []),
        })
    return out


def answer_day_one_questions(
    storage: ModuleGraphStorage,
    lineage_path: Optional[Path],
    survey_summary_path: Optional[Path],
    lineage_summary_path: Optional[Path],
    output_dir: Path,
    budget: ContextWindowBudget,
    trace: TraceManager,
) -> tuple[Path, Path]:
    """
    Synthesize Day-One answers; write day_one_answers.json and onboarding_brief.md.
    Returns (path_to_json, path_to_md).
    """
    import litellm

    # Build context: PageRank hubs (structured JSON), summaries, sources/sinks
    pagerank_hubs = _top_pagerank_modules(storage, 5)
    context_parts = ["## Top 5 modules by PageRank (critical path)\n" + json.dumps(pagerank_hubs, indent=2)]

    if survey_summary_path and survey_summary_path.exists():
        context_parts.append("\n## Survey summary\n" + survey_summary_path.read_text(encoding="utf-8"))
    if lineage_summary_path and lineage_summary_path.exists():
        context_parts.append("\n## Lineage summary\n" + lineage_summary_path.read_text(encoding="utf-8"))

    # Blast radius for top module if we have lineage
    if lineage_path and lineage_path.exists():
        try:
            lg = LineageGraph.load(lineage_path)
            if pagerank_hubs:
                top_path = pagerank_hubs[0]["path"]
                # Map module path to dataset if needed; for now use top_path as context
                context_parts.append(f"\n## Top critical module (for blast radius)\n{top_path}")
        except Exception as e:
            logger.warning("Could not load lineage for blast radius: %s", e)

    full_context = "\n".join(context_parts)
    prompt = f"""You are an FDE (Forward Deployed Engineer) briefing a new team member. Using ONLY the evidence below, answer each of the following five questions. For each answer, cite evidence with file paths and line numbers where possible. Reply with valid JSON only: {{ "answers": [ {{ "question_id": 1, "answer_text": "...", "evidence_list": [ {{ "file_path": "...", "line_start": 0, "line_end": 0, "description": "...", "evidence_type": "static_analysis" or "semantic_inference" or "lineage_graph" or "git_history" }} ], "confidence": 0.9 }} ] }}

Questions:
1. {FDE_QUESTIONS[0]}
2. {FDE_QUESTIONS[1]}
3. {FDE_QUESTIONS[2]}
4. {FDE_QUESTIONS[3]}
5. {FDE_QUESTIONS[4]}

Evidence:
{full_context[:12000]}
"""

    model = budget.model_for("synthesis")
    try:
        response = litellm.completion(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=4096,
        )
    except Exception as e:
        logger.warning("Day-One synthesis LLM failed: %s", e)
        trace.log("day_one_synthesis", model_used=model, status="error", error_message=str(e))
        # Write placeholder
        answers = [
            DayOneAnswer(question_id=i + 1, answer_text="(Synthesis failed: " + str(e) + ")", evidence_list=[], confidence=0.0)
            for i in range(5)
        ]
    else:
        content = (response.choices[0].message.content or "").strip()
        in_tok = response.usage.prompt_tokens if response.usage else budget.estimate_tokens(prompt)
        out_tok = response.usage.completion_tokens if response.usage else budget.estimate_tokens(content)
        budget.add_usage(in_tok, out_tok)
        trace.log(
            "day_one_synthesis",
            model_used=model,
            input_tokens=in_tok,
            output_tokens=out_tok,
            cumulative_budget={"input": budget.cumulative_input_tokens, "output": budget.cumulative_output_tokens},
            status="success",
        )
        # Parse JSON from response (may be wrapped in markdown code block or have trailing text)
        json_str = content
        if "```json" in content:
            json_str = content.split("```json")[1].split("```")[0].strip()
        elif "```" in content:
            json_str = content.split("```")[1].split("```")[0].strip()
        # Extract first complete JSON object to avoid "Extra data" when LLM returns multiple objects or trailing text
        json_str = _extract_first_json_object(json_str)
        try:
            data = json.loads(json_str)
            raw_answers = data.get("answers", data) if isinstance(data, dict) else data
            if not isinstance(raw_answers, list):
                raw_answers = [data]
            answers = []
            for i, a in enumerate(raw_answers[:5]):
                if isinstance(a, dict):
                    ev = a.get("evidence_list", [])
                    valid_types = ("static_analysis", "semantic_inference", "lineage_graph", "git_history")
                    evidence_list = []
                    for e in ev:
                        et = e.get("evidence_type", "semantic_inference")
                        if et not in valid_types:
                            et = "semantic_inference"
                        try:
                            evidence_list.append(
                                EvidenceEntry(
                                    file_path=e.get("file_path", ""),
                                    line_start=int(e.get("line_start", 0)),
                                    line_end=int(e.get("line_end", 0)),
                                    description=e.get("description", ""),
                                    evidence_type=et,
                                )
                            )
                        except Exception:
                            pass
                    answers.append(
                        DayOneAnswer(
                            question_id=int(a.get("question_id", i + 1)),
                            answer_text=a.get("answer_text", ""),
                            evidence_list=evidence_list,
                            confidence=float(a.get("confidence", 0.5)),
                        )
                    )
                else:
                    answers.append(DayOneAnswer(question_id=i + 1, answer_text="", evidence_list=[], confidence=0.0))
            while len(answers) < 5:
                answers.append(DayOneAnswer(question_id=len(answers) + 1, answer_text="", evidence_list=[], confidence=0.0))
        except (json.JSONDecodeError, KeyError) as e:
            logger.warning("Failed to parse Day-One JSON: %s", e)
            answers = [
                DayOneAnswer(question_id=i + 1, answer_text=content[:500] if i == 0 else "", evidence_list=[], confidence=0.0)
                for i in range(5)
            ]

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "day_one_answers.json"
    json_path.write_text(
        json.dumps([a.model_dump(mode="json") for a in answers], indent=2),
        encoding="utf-8",
    )
    md_path = output_dir / "onboarding_brief.md"
    md_lines = ["# FDE Day-One Brief", ""]
    for a in answers:
        md_lines.append(f"## Question {a.question_id}")
        q_idx = max(0, min(a.question_id - 1, len(FDE_QUESTIONS) - 1))
        md_lines.append(FDE_QUESTIONS[q_idx] if FDE_QUESTIONS else "")
        md_lines.append("")
        md_lines.append(a.answer_text)
        md_lines.append("")
        if a.evidence_list:
            md_lines.append("**Evidence:**")
            for e in a.evidence_list:
                md_lines.append(f"- `{e.file_path}` (lines {e.line_start}-{e.line_end}) [{e.evidence_type}]: {e.description}")
            md_lines.append("")
    md_path.write_text("\n".join(md_lines), encoding="utf-8")
    logger.info("Wrote %s and %s", json_path, md_path)
    return json_path, md_path


# -----------------------------------------------------------------------------
# Semanticist class and analyze_repository
# -----------------------------------------------------------------------------


class Semanticist:
    """
    LLM-powered purpose extraction, domain clustering, and FDE Day-One synthesis.
    Reads Surveyor + Hydrologist outputs from .cartography; writes updated module graph,
    domain map, day_one_answers.json, onboarding_brief.md, and cartography_trace.jsonl.
    """

    def __init__(
        self,
        output_dir: Optional[Path] = None,
        repo_path: Optional[str] = None,
        *,
        model_bulk: Optional[str] = None,
        model_synthesis: Optional[str] = None,
        max_tokens_budget: Optional[int] = None,
    ) -> None:
        self.output_dir = Path(output_dir) if output_dir else Path(".cartography")
        self.repo_path = Path(repo_path) if repo_path else None
        self.model_bulk = model_bulk or os.environ.get("SEMANTICIST_MODEL_BULK", DEFAULT_MODEL_BULK)
        self.model_synthesis = model_synthesis or os.environ.get("SEMANTICIST_MODEL_SYNTHESIS", "") or self.model_bulk
        self.max_tokens_budget = max_tokens_budget
        self._purpose_cache: dict[str, str] = {}  # path -> content hash for incremental

    def analyze_repository(
        self,
        repo_path: str,
        output_dir: Optional[Path] = None,
        changed_files: Optional[list[str]] = None,
    ) -> tuple[Optional[Path], Optional[Path], Optional[Path]]:
        """
        Run Semanticist: load module graph, generate purposes, cluster domains, answer Day-One questions.
        Returns (module_graph_path, day_one_json_path, onboarding_brief_path).
        """
        out = Path(output_dir) if output_dir else self.output_dir
        out.mkdir(parents=True, exist_ok=True)
        repo = Path(repo_path)
        self.repo_path = repo

        # Load purpose cache for incremental (path -> content_hash)
        cache_path = out / ".semanticist_purpose_cache.json"
        if cache_path.exists():
            try:
                self._purpose_cache = json.loads(cache_path.read_text(encoding="utf-8"))
            except Exception:
                self._purpose_cache = {}

        module_graph_path = out / "module_graph.json"
        if not module_graph_path.exists():
            logger.warning("Module graph not found at %s; run Surveyor first.", module_graph_path)
            return None, None, None

        storage = ModuleGraphStorage.load(module_graph_path)
        G = storage.graph
        budget = ContextWindowBudget(
            max_tokens=self.max_tokens_budget,
            model_bulk=self.model_bulk,
            model_synthesis=self.model_synthesis,
        )
        trace = TraceManager(out)

        # Determine which modules to process (incremental vs full)
        def module_scope() -> list[str]:
            node_list = [n for n in G if G.nodes[n].get("node_type") == "module"]
            if changed_files is None:
                return node_list
            changed_set = set(changed_files)
            return [n for n in node_list if n in changed_set or self._file_hash(repo / n) != self._purpose_cache.get(n)]

        scope = module_scope()
        logger.info("Semanticist: processing %d module(s)", len(scope))

        for path in scope:
            try:
                node_attrs = G.nodes[path].get("module_node", {})
                node = ModuleNode.model_validate(node_attrs) if node_attrs else None
                if not node:
                    continue
                content_hash = self._file_hash(repo / path)
                if changed_files is not None and content_hash == self._purpose_cache.get(path):
                    continue
                purpose, drift_sev, drift_type = generate_purpose_statement(str(repo), node, budget, trace)
                if purpose is not None:
                    node_attrs = dict(G.nodes[path].get("module_node", {}))
                    node_attrs["purpose_statement"] = purpose
                    node_attrs["doc_drift_severity"] = drift_sev
                    node_attrs["doc_drift_type"] = drift_type
                    node_attrs.pop("analysis_error", None)
                    G.nodes[path]["module_node"] = node_attrs
                    self._purpose_cache[path] = content_hash
                else:
                    # Failure: set analysis_error (reason from trace is implicit)
                    node_attrs = dict(G.nodes[path].get("module_node", {}))
                    node_attrs["purpose_statement"] = None
                    node_attrs["analysis_error"] = "encoding_error_or_budget"
                    G.nodes[path]["module_node"] = node_attrs
            except Exception as e:
                logger.exception("Semanticist failed for module %s: %s", path, e)
                trace.log("purpose_extraction", target_module=path, status="error", error_message=str(e))
                node_attrs = dict(G.nodes[path].get("module_node", {}))
                node_attrs["analysis_error"] = "syntax_error_or_exception"
                G.nodes[path]["module_node"] = node_attrs

        # Persist purpose cache
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(json.dumps(self._purpose_cache, indent=0), encoding="utf-8")

        # Cluster domains
        cluster_into_domains(storage, budget, trace, k=6)
        # Domain architecture map
        domain_map_path = out / "domain_architecture_map.json"
        domain_map: dict[str, list[str]] = {}
        for n in G:
            if G.nodes[n].get("node_type") != "module":
                continue
            mn = G.nodes[n].get("module_node", {})
            d = mn.get("domain_cluster") or "other"
            domain_map.setdefault(d, []).append(n)
        domain_map_path.write_text(json.dumps(domain_map, indent=2), encoding="utf-8")
        (out / "domain_architecture_map.md").write_text(
            "\n".join([f"## {d}\n" + "\n".join(f"- `{p}`" for p in sorted(paths)) for d, paths in sorted(domain_map.items())]),
            encoding="utf-8",
        )

        # Semantic index stub (Phase 4 prep): populate and save
        try:
            from src.graph.semantic_index import SemanticIndexStore
            index_store = SemanticIndexStore(embedding_model="tfidf_stub")
            for n in G:
                if G.nodes[n].get("node_type") != "module":
                    continue
                mn = G.nodes[n].get("module_node", {})
                purpose = mn.get("purpose_statement") or ""
                if purpose:
                    index_store.add_module(n, purpose, mn.get("domain_cluster") or "")
            if index_store._module_paths:
                index_store.save(out / "semantic_index")
        except Exception as e:
            logger.warning("Could not write semantic index stub: %s", e)

        # Write updated module graph
        storage.write_json(module_graph_path)

        # Day-One synthesis
        lineage_path = out / "lineage_graph.json"
        survey_summary = out / "survey_summary.md"
        lineage_summary = out / "lineage_summary.md"
        day_one_json, onboarding_md = answer_day_one_questions(
            storage, lineage_path, survey_summary, lineage_summary, out, budget, trace
        )

        return module_graph_path, day_one_json, onboarding_md

    def _file_hash(self, file_path: Path) -> str:
        if not file_path.is_file():
            return ""
        try:
            return hashlib.sha256(file_path.read_bytes()).hexdigest()[:16]
        except Exception:
            return ""
