"""
Evidence verification: validate EvidenceEntry file paths and line ranges.
Log evidence_verified to trace; used after Day-One synthesis and optionally in Archivist.
"""

import logging
from pathlib import Path
from typing import Optional

from src.models import EvidenceEntry

logger = logging.getLogger(__name__)


def verify_evidence_entry(entry: EvidenceEntry, repo_path: Path) -> bool:
    """
    Verify a single EvidenceEntry: file exists and is readable, line range within bounds.
    Returns True if verified, False otherwise.
    """
    if not repo_path or not repo_path.is_dir():
        return False
    raw = entry.file_path
    if not raw:
        return False
    full = (repo_path / raw).resolve()
    try:
        if not full.exists() or not full.is_file():
            return False
        content = full.read_text(encoding="utf-8", errors="replace")
        lines = content.splitlines()
        max_line = len(lines)
        if entry.line_end > 0 or entry.line_start > 0:
            if entry.line_start < 1 or entry.line_start > max_line:
                return False
            if entry.line_end > 0 and (entry.line_end < entry.line_start or entry.line_end > max_line):
                return False
        return True
    except Exception as e:
        logger.debug("Evidence verification failed for %s: %s", entry.file_path, e)
        return False


def verify_evidence_list(
    entries: list[EvidenceEntry],
    repo_path: Path,
    trace: Optional[object] = None,
) -> list[tuple[EvidenceEntry, bool]]:
    """
    Verify each EvidenceEntry under repo_path. Return list of (entry, verified).
    If trace is provided (CartographyTrace), log each result with evidence_verified.
    """
    results = []
    for entry in entries:
        verified = verify_evidence_entry(entry, repo_path)
        results.append((entry, verified))
        if trace is not None and hasattr(trace, "log"):
            trace.log(
                "evidence_verification",
                evidence_source=entry.file_path,
                evidence_verified=verified,
                status="success" if verified else "unverified",
            )
    return results
