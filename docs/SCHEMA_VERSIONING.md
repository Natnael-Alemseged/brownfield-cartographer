# Graph schema versioning and migration

Stored JSON graphs (module graph and lineage graph) include a **schema_version** field so that future schema changes can be detected and, if needed, migrated.

## Current version

- **schema_version: 1** — Initial version. Module graph: node_link_data with `node_type`, `module_node`. Lineage graph: node_link_data with `node_type` (dataset | transformation), edge attributes `edge_type`, `transformation_type`, `source_file`, `line_range`.

## Strategy

1. **When writing**: Always set `schema_version` in the root of the serialized JSON (e.g. `to_dict()` adds it). Use an integer that increments when the stored shape or semantics change in a breaking way.

2. **When reading**: `LineageGraph.load()` and `ModuleGraphStorage.load()` read `schema_version` and remove it before calling NetworkX’s `node_link_graph()`. If the version is unknown or newer than the code supports, log a warning and still attempt to load; add migration logic when required.

3. **Migration**: For a new version (e.g. 2), add a function that takes a v1 payload and returns a v2 payload (e.g. renaming attributes, adding defaults). In `load()`, if `schema_version == 1`, run the migration to v2, then proceed with the normal v2 load. Old files without `schema_version` are treated as v1.

## Adding a new version

- Bump the version in `to_dict()` (e.g. to 2).
- In `load()`, after reading JSON, if `data.get("schema_version") == 1`, call a `_migrate_v1_to_v2(data)` that returns the new structure (with `schema_version: 2`), then use that for `node_link_graph`.
- Document the change in this file (e.g. “v2: added X field to nodes”).
