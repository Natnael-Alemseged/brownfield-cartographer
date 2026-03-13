"""Pydantic schemas for the knowledge graph (nodes and edges)."""

from datetime import datetime
from enum import Enum
from typing import Literal, Optional

from pydantic import BaseModel, Field, field_validator

# Phase 3: Documentation drift taxonomy (Semanticist)
DocDriftSeverity = Literal["critical", "major", "minor"]
DocDriftType = Literal[
    "STALE_DESCRIPTION",
    "MISSING_PARAMETERS",
    "SIDE_EFFECTS_UNDOCUMENTED",
    "RAISES_UNDOCUMENTED",
]

# Phase 3: Evidence provenance for Day-One answers (Semanticist / Navigator)
EvidenceType = Literal["static_analysis", "semantic_inference", "lineage_graph", "git_history"]


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
    doc_drift_severity: Optional[str] = Field(default=None, description="critical|major|minor (Phase 3)")
    doc_drift_type: Optional[str] = Field(default=None, description="STALE_DESCRIPTION|MISSING_PARAMETERS|etc (Phase 3)")
    analysis_error: Optional[str] = Field(
        default=None,
        description="If purpose extraction failed: binary_file|encoding_error|syntax_error|budget_exceeded (Phase 3)",
    )
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


# ---- Typed AST analysis results (per-language, for downstream consumption) ----


class FunctionDefResult(BaseModel):
    """Single function from AST extraction."""

    name: str
    signature: str = ""
    line_start: int = 0
    line_end: int = 0
    decorators: list[str] = Field(default_factory=list)


class ClassDefResult(BaseModel):
    """Single class from AST extraction."""

    name: str
    bases: list[str] = Field(default_factory=list)
    parent_classes: list[str] = Field(default_factory=list)
    line_start: int = 0
    line_end: int = 0


class PythonAnalysisResult(BaseModel):
    """Structured result of Python file AST analysis."""

    path: str
    language: str = "python"
    imports: list[str] = Field(default_factory=list)
    star_imports: list[str] = Field(default_factory=list, description="from x import * (module names)")
    dynamic_imports: list[str] = Field(default_factory=list, description="Unresolved or dynamic import hints")
    functions: list[FunctionDefResult] = Field(default_factory=list)
    classes: list[ClassDefResult] = Field(default_factory=list)


class TableRefResult(BaseModel):
    """Table reference from SQL AST."""

    name: str
    line: int = 0


class QueryStructureResult(BaseModel):
    """Query structure hint from SQL AST."""

    type: str
    line_start: int = 0
    line_end: int = 0


class SqlAnalysisResult(BaseModel):
    """Structured result of SQL file AST analysis."""

    path: str
    language: str = "sql"
    tables: list[TableRefResult] = Field(default_factory=list)
    query_structures: list[QueryStructureResult] = Field(default_factory=list)


class KeyHierarchyEntry(BaseModel):
    """Key entry in YAML hierarchy."""

    key: str
    line: int = 0


class YamlAnalysisResult(BaseModel):
    """Structured result of YAML file AST analysis."""

    path: str
    language: str = "yaml"
    keys: list[str] = Field(default_factory=list)
    key_hierarchy: list[KeyHierarchyEntry] = Field(default_factory=list)


# ---- Phase 3: Semanticist Day-One answers (structured evidence for Navigator) ----


class EvidenceEntry(BaseModel):
    """Single evidence citation for a Day-One answer."""

    file_path: str = Field(description="Relative path to source file")
    line_start: int = Field(default=0, ge=0)
    line_end: int = Field(default=0, ge=0)
    description: str = Field(default="", description="Short description of what this evidence shows")
    evidence_type: EvidenceType = Field(
        default="static_analysis",
        description="Provenance: static_analysis | semantic_inference | lineage_graph | git_history",
    )


class DayOneAnswer(BaseModel):
    """One of the Five FDE Day-One answers with evidence citations."""

    question_id: int = Field(description="1-5 for the five questions")
    answer_text: str = Field(default="", description="Answer body")
    evidence_list: list[EvidenceEntry] = Field(default_factory=list)
    confidence: float = Field(default=0.0, ge=0, le=1, description="0-1 confidence")
