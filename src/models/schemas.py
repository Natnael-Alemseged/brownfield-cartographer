"""Pydantic schemas for the knowledge graph (nodes and edges)."""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class FunctionInfo(BaseModel):
    """Public function or method signature info from static analysis."""

    name: str
    signature: str = ""
    line_start: int = 0
    line_end: int = 0


class ClassInfo(BaseModel):
    """Class definition with inheritance chain."""

    name: str
    bases: list[str] = Field(default_factory=list, description="Direct base class names")
    parent_classes: list[str] = Field(
        default_factory=list,
        description="Full inheritance chain (ordered list of base names)",
    )
    line_start: int = 0
    line_end: int = 0


class ModuleNode(BaseModel):
    """Node representing a source file/module in the module graph."""

    path: str = Field(description="Relative path from repo root")
    language: str = Field(description="e.g. python, sql, yaml")
    purpose_statement: Optional[str] = Field(default=None, description="LLM-generated (Phase 3)")
    domain_cluster: Optional[str] = Field(default=None, description="Inferred domain (Phase 3)")
    complexity_score: float = Field(default=0.0, ge=0, description="e.g. cyclomatic or LOC-based")
    change_velocity_30d: int = Field(default=0, ge=0, description="Number of changes in last 30 days")
    is_dead_code_candidate: bool = Field(default=False, description="Exported but no inbound imports")
    last_modified: Optional[datetime] = None

    # Phase 1 static analysis
    imports: list[str] = Field(default_factory=list, description="Resolved import targets (module paths)")
    public_functions: list[FunctionInfo] = Field(default_factory=list)
    classes: list[ClassInfo] = Field(default_factory=list)
    lines_of_code: int = 0
    comment_ratio: float = 0.0
    cyclomatic_complexity: int = Field(default=0, ge=0, description="Decision points (if/else/loop/etc.)")
