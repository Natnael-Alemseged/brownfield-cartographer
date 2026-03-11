"""Pydantic schemas for the knowledge graph (nodes and edges)."""

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, field_validator


class EdgeType(str, Enum):
    """Typed edge types for module and lineage graphs."""

    IMPORTS = "IMPORTS"  # module -> module (imports)
    REFERENCES_SQL = "REFERENCES_SQL"  # module -> sql file
    REFERENCES_CONFIG = "REFERENCES_CONFIG"  # module -> config file
    PRODUCES = "PRODUCES"  # transformation -> dataset
    CONSUMES = "CONSUMES"  # dataset -> transformation


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

    @field_validator("path")
    @classmethod
    def path_not_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("path must be non-empty")
        return v.strip()


class FunctionNode(BaseModel):
    """Node representing a function/symbol in the module graph (for fine-grained analysis)."""

    name: str = Field(description="Function or symbol name")
    module_path: str = Field(description="Relative path of the containing module")
    signature: str = Field(default="", description="Full or truncated signature")
    line_start: int = Field(default=0, ge=0)
    line_end: int = Field(default=0, ge=0)
    is_method: bool = Field(default=False, description="True if defined inside a class")
    class_name: Optional[str] = Field(default=None, description="Containing class if is_method")


class DatasetNode(BaseModel):
    """Node representing a dataset (table, file, stream, API) in the lineage graph."""

    name: str = Field(description="Dataset identifier (table name, path, or stream name)")
    storage_type: str = Field(
        default="table",
        description="One of: table | file | stream | api",
    )
    schema_snapshot: Optional[str] = Field(default=None, description="Optional schema description")
    freshness_sla: Optional[str] = Field(default=None)
    owner: Optional[str] = Field(default=None)
    is_source_of_truth: bool = Field(default=False)


class TransformationNode(BaseModel):
    """Node representing a transformation (code/SQL/config) in the lineage graph."""

    source_datasets: list[str] = Field(default_factory=list, description="Upstream dataset names")
    target_datasets: list[str] = Field(default_factory=list, description="Downstream dataset names")
    transformation_type: str = Field(
        default="code",
        description="e.g. code | sql | config_defined | notebook",
    )
    source_file: str = Field(default="", description="Relative path to source file")
    line_range: tuple[int, int] = Field(default=(0, 0), description="(line_start, line_end)")
    sql_query_if_applicable: Optional[str] = Field(default=None)
    notebook_cell: Optional[dict] = Field(
        default=None,
        description="If from notebook: {cell_index, cell_type}",
    )
