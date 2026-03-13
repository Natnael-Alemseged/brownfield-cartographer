"""
Semantic index stub (Phase 4 prep): vector store of module purpose statements for semantic search.

Schema: manifest.jsonl per line: module_path, purpose_text, domain_cluster, embedding_model, generated_at, embedding_index.
embeddings.npy: shape (N, D); D variable for TF-IDF stub, optional 384 for all-MiniLM-L6-v2.
search() returns (module_path, score); search_to_evidence() returns list[EvidenceEntry] for Navigator.
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import numpy as np

from src.models import EvidenceEntry

logger = logging.getLogger(__name__)


class SemanticIndexStore:
    """
    Stub store: add_module(), search(query, k), save(), load().
    Uses simple TF-IDF style vectors (same as cluster_into_domains) for consistency without extra deps.
    """

    def __init__(self, embedding_model: str = "tfidf_stub") -> None:
        self.embedding_model = embedding_model
        self._module_paths: list[str] = []
        self._purpose_texts: list[str] = []
        self._domain_clusters: list[str] = []
        self._embeddings: Optional[np.ndarray] = None

    def add_module(
        self,
        module_path: str,
        purpose_text: str,
        domain_cluster: str = "",
        purpose_embedding: Optional[np.ndarray] = None,
    ) -> None:
        """Add a module to the index. If purpose_embedding is None, compute from purpose_text (stub)."""
        self._module_paths.append(module_path)
        self._purpose_texts.append(purpose_text or "")
        self._domain_clusters.append(domain_cluster or "")
        if purpose_embedding is not None:
            if self._embeddings is None:
                self._embeddings = purpose_embedding.reshape(1, -1)
            else:
                self._embeddings = np.vstack([self._embeddings, purpose_embedding.reshape(1, -1)])
        else:
            # Defer embedding build until save/search
            self._embeddings = None

    def _build_embeddings(self) -> np.ndarray:
        """Build TF-IDF style embeddings from purpose_texts if not set."""
        if self._embeddings is not None and len(self._embeddings) == len(self._purpose_texts):
            return self._embeddings
        import re
        from collections import Counter
        max_features = 128
        vocab: dict[str, int] = {}
        tokenize = re.compile(r"\b\w+\b").findall
        doc_freq: list[Counter] = []
        for t in self._purpose_texts:
            tokens = [x.lower() for x in tokenize(t) if len(x) > 1]
            doc_freq.append(Counter(tokens))
            for w in tokens:
                vocab.setdefault(w, len(vocab))
                if len(vocab) >= max_features:
                    break
            if len(vocab) >= max_features:
                break
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

    def search(self, query: str, k: int = 10) -> list[tuple[str, float]]:
        """Return top-k (module_path, score) by cosine similarity of query to purpose embeddings."""
        if not self._purpose_texts:
            return []
        emb = self._build_embeddings()
        # Query embedding (same TF-IDF style, vocab from existing texts)
        import re
        from collections import Counter
        tokenize = re.compile(r"\b\w+\b").findall
        q_tokens = [x.lower() for x in tokenize(query) if len(x) > 1]
        q_cf = Counter(q_tokens)
        vocab_size = emb.shape[1]
        # We don't have stored vocab here; use dot product with first row's shape
        q_vec = np.zeros(emb.shape[1])
        for i, (path, text) in enumerate(zip(self._module_paths, self._purpose_texts)):
            tokens = tokenize(text.lower())
            for w in q_tokens:
                if w in tokens:
                    idx = list(tokens).index(w) if isinstance(tokens, list) else 0
                    if idx < vocab_size:
                        q_vec[idx] = q_cf.get(w, 0)
            break
        # Simpler: just dot product query token counts with each doc (approximate)
        q_arr = np.zeros(emb.shape[1])
        for j in range(emb.shape[1]):
            q_arr[j] = 1.0 / (1 + j)  # placeholder; real impl would use shared vocab
        scores = np.dot(emb, q_arr)
        if scores.size == 0:
            return []
        top_idx = np.argsort(-scores)[:k]
        return [(self._module_paths[i], float(scores[i])) for i in top_idx if i < len(self._module_paths)]

    def search_to_evidence(
        self,
        query: str,
        k: int = 10,
    ) -> list[EvidenceEntry]:
        """
        Return top-k search results as EvidenceEntry list for Navigator.
        confidence: high if similarity >= 0.8, medium if 0.5-0.8, low if < 0.5.
        Scores are normalized to [0, 1] by max for stub (cosine would be 0-1 naturally).
        """
        pairs = self.search(query, k=k)
        if not pairs:
            return []
        scores = [s for _, s in pairs]
        max_s = max(scores) if scores else 1.0
        norm = max_s if max_s > 0 else 1.0
        out = []
        for i, (module_path, score) in enumerate(pairs):
            norm_score = score / norm
            if norm_score >= 0.8:
                conf = "high"
            elif norm_score >= 0.5:
                conf = "medium"
            else:
                conf = "low"
            purpose = self._purpose_texts[self._module_paths.index(module_path)] if module_path in self._module_paths else ""
            out.append(
                EvidenceEntry(
                    file_path=module_path,
                    line_start=0,
                    line_end=0,
                    description=purpose[:200] if purpose else "semantic match",
                    evidence_type="semantic_inference",
                    confidence=conf,
                )
            )
        return out

    def save(self, path: Path) -> None:
        """Write manifest.jsonl (with generated_at) and embeddings.npy to path (directory)."""
        path = Path(path)
        path.mkdir(parents=True, exist_ok=True)
        emb = self._build_embeddings()
        np.save(path / "embeddings.npy", emb)
        generated_at = datetime.now(timezone.utc).isoformat()
        manifest_path = path / "manifest.jsonl"
        with open(manifest_path, "w", encoding="utf-8") as f:
            for i, (mp, pt, dc) in enumerate(zip(self._module_paths, self._purpose_texts, self._domain_clusters)):
                rec = {
                    "module_path": mp,
                    "purpose_text": pt,
                    "domain_cluster": dc,
                    "embedding_model": self.embedding_model,
                    "generated_at": generated_at,
                    "embedding_index": i,
                }
                f.write(json.dumps(rec, default=str) + "\n")
        logger.info("Wrote %s and %s", manifest_path, path / "embeddings.npy")

    @classmethod
    def load(cls, path: Path) -> "SemanticIndexStore":
        """Load from .cartography/semantic_index/."""
        path = Path(path)
        inst = cls(embedding_model="tfidf_stub")
        inst._embeddings = np.load(path / "embeddings.npy", allow_pickle=False)
        inst._module_paths = []
        inst._purpose_texts = []
        inst._domain_clusters = []
        with open(path / "manifest.jsonl", encoding="utf-8") as f:
            for line in f:
                rec = json.loads(line)
                inst._module_paths.append(rec["module_path"])
                inst._purpose_texts.append(rec.get("purpose_text", ""))
                inst._domain_clusters.append(rec.get("domain_cluster", ""))
        # Backward compat: manifest may lack generated_at
        return inst
