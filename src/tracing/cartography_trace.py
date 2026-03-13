"""
Unified cartography trace: single audit log for all agents.

Schema: timestamp, agent, action, run_id, parent_action_id, evidence_source,
confidence, evidence_verified, plus optional target_module, model_used, tokens, status.
All agents append to .cartography/cartography_trace.jsonl.
"""

import json
import logging
import time
import uuid
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)


def new_run_id() -> str:
    """Return a new UUID for a pipeline invocation."""
    return str(uuid.uuid4())


class CartographyTrace:
    """
    Appends every agent action to .cartography/cartography_trace.jsonl.
    Supports run_id (per pipeline run), parent_action_id (tool chaining),
    evidence_source, confidence, evidence_verified.
    """

    def __init__(
        self,
        output_dir: Path,
        agent: str = "cartographer",
        run_id: Optional[str] = None,
    ) -> None:
        self._output_dir = Path(output_dir)
        self._path = self._output_dir / "cartography_trace.jsonl"
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._agent = agent
        self._run_id = run_id or new_run_id()

    @property
    def run_id(self) -> str:
        return self._run_id

    def log(
        self,
        action: str,
        *,
        parent_action_id: Optional[str] = None,
        evidence_source: Optional[str] = None,
        confidence: Optional[str] = None,
        evidence_verified: Optional[bool] = None,
        target_module: Optional[str] = None,
        model_used: Optional[str] = None,
        input_tokens: int = 0,
        output_tokens: int = 0,
        cumulative_budget: Optional[dict] = None,
        confidence_score: Optional[float] = None,
        status: str = "success",
        error_message: Optional[str] = None,
        **extra: Any,
    ) -> None:
        record: dict[str, Any] = {
            "timestamp": time.time(),
            "agent": self._agent,
            "action": action,
            "run_id": self._run_id,
            "target_module": target_module,
            "model_used": model_used,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "cumulative_budget": cumulative_budget,
            "confidence_score": confidence_score,
            "status": status,
        }
        if parent_action_id is not None:
            record["parent_action_id"] = parent_action_id
        if evidence_source is not None:
            record["evidence_source"] = evidence_source
        if confidence is not None:
            record["confidence"] = confidence
        if evidence_verified is not None:
            record["evidence_verified"] = evidence_verified
        if error_message:
            record["error_message"] = error_message
        record.update(extra)
        with open(self._path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, default=str) + "\n")
        logger.debug("Trace: %s %s", self._agent, action)
