# Catalog metadata and publication (wire 3.0)

`catalog_contents` returns six nullable Arrow IPC streams: `schemas`, `tables`, `views`, `functions`, `macros`, and `indexes`. A stream can contain multiple rows and record batches spanning multiple schemas. Empty categories are null. Functions and macros retain their subtype fields. Per-schema listing RPCs keep their existing item shapes, except `SchemaInfo` no longer carries attachment bytes; real attach request/result and function state remain intact.

The three binary `FunctionInfo` fields `arguments`, `output_schema`, and `parameter_default_values` use `dictionary<int32, binary>`. Author-facing byte values stay unchanged. Equal content shares a dictionary entry; null and empty byte values remain distinct, and readers accept dictionary replacements between batches.


Publication is opt-in. Configure `SnapshotIdentity` with a catalog namespace, a stable source revision, the reported catalog version, and optionally a shared visibility scope. Omitted visibility scope isolates each authenticated attachment. A shared scope is an author declaration that every included caller sees exactly the same metadata. The revision must change when metadata, configuration, or the relevant authorization view changes; do not derive it by serializing the catalog on every request.

The framework opens/authenticates the attachment before consulting publication state. A cold publication collects and validates the catalog, serializes the RPC envelope once, uploads it, fetches it back through the ordinary external-result safety policy, validates its hash/envelopes/all metadata, and conditionally commits its manifest. Warm requests reuse the small descriptor; matching ETags return an inline `not_modified` response. Cold recovery discovers and validates a durable manifest once without rebuilding the catalog. Concurrent requests for the same identity share one publication operation; unrelated identities proceed independently.

`CatalogPublicationRepository` provides storage, discovery, conditional commit/invalidation, location lifetime guarantees, and optional persisted URL renewal. The in-memory repository only survives while its instance survives. `S3CatalogPublicationRepository` provides durable S3/R2 manifests and immutable objects through an injected SDK client: bounded GET, conditional PUT (`If-None-Match`/`If-Match`), and presigned GET. The adapter must propagate authentication/network errors rather than treating them as missing objects or conflicts. Configure bucket retention beyond issued URL lifetimes. Invalidating discovery does not delete objects that existing clients may still fetch.

Defaults reject publication failures, limit raw and stored payloads to 128 MiB each, retain at most 256 validated descriptors, and stop serving a signed URL 60 seconds before expiry. Ordinary-response fallback is an explicit policy option. URL renewal may change only URL and URL expiry for the same immutable manifest. The framework validates a changed location before serving it. Treat manifests and signed URLs as trusted, sensitive server configuration; never accept them directly from RPC callers or log URL credentials.

The manager exposes startup warming, trusted manifest import, and conditional invalidation. Keep identity/revision callbacks cheap and independent of collection serialization. Externalized responses use the existing vgi-rpc result-pointer contract; there is no schema-level publication RPC.

Java: call `Worker.catalogSnapshot(policy)`. Use `policy.manager.warm`, `importPublication`, and `invalidate`. This implementation requires `farm.query:vgirpc:0.33.2`, including scalar dictionary annotations, its collection row codec and idempotent IPC stream termination.

Version 0.45.0 requires native clients and workers on wire 3.0.x under `vgi.v3`; `vgi.v2` peers are incompatible. CI and release builds resolve RPC 0.33.2 from Maven Central.

RPC 0.33.2 includes the scalar `DICT_INT16_UTF8` record fix required by macro and
constraint metadata. Release qualification disables the composite source override
and resolves that published dependency from Maven Central. No local repository
fallback is part of the SDK build.

## Constraints, nullability, generated columns and macros (vgi.v3)

Constraints are canonical `ConstraintInfo` records the worker attests: build them with `ConstraintRecord.builder(schema, table, id)` (`notNull`, `primaryKey`, `unique`, `check`, `foreignKey`), state `ENFORCED` + `ALL_ROWS`, and attach them with `CatalogTable.Builder#constraints(records, revision)`. The pre-v3 `primaryKey`/`unique`/`check`/`foreignKeys` builders are `@Deprecated` and refused when the table is served. Every catalog path (main, auxiliary, hosted) advertises `vgi.constraint.records.read.v1` + `vgi.expression.duckdb.scalar.v1` and selects what the client offers automatically.

Before serving a table the framework applies the rules DuckDB applies at ATTACH and refuses, naming the table, constraint id and column path: records owned by another table or a multi-component schema path, duplicate ids or declared names (case-insensitive), a second PRIMARY KEY, column paths that do not resolve (nested paths only on NOT_NULL, through ordinary STRUCTs), foreign keys to another schema, with an action other than NO_ACTION/RESTRICT or whose referenced key the catalog does not declare, and CHECK expressions outside the one-numeric-comparison grammar (`budget >= 0`: unqualified signed-integer/float columns and numeric literals).

Nullability needs no author work: the served Arrow schema is rewritten so a field is non-nullable exactly when a NOT_NULL/PRIMARY_KEY record names it (descending through ordinary STRUCTs; the row-id column and logical-type storage are untouched). A non-nullable field no record names is served nullable.

Generated columns: `CatalogColumns.virtualGenerated(name, type, sql, schemaPath)` builds a VIRTUAL column (nullable; BOOLEAN, signed integer or VARCHAR; the extension's deterministic scalar grammar, no division or float literals). Legacy `generated_expression` metadata is translated to that form when served.

Capabilities: `CatalogProfiles` encodes, decodes and negotiates profiles (`negotiate`, `select`, `clientOffer`, `defaultClientOffer`). A hosted catalog that enforces constraints on its own write paths selects `vgi.constraint.create.v1` + `vgi.constraint.enforce.v1` from `CatalogInterface#catalogCapabilities`; only then does `CREATE TABLE` with constraint definitions reach its `tableCreate`. Auxiliary catalogs use `ExtraCatalog#withCapabilities`. `InMemoryCatalog` stores no rows and enforces nothing, so it refuses constrained, NOT NULL, logical-type and generated columns on `CREATE TABLE`.

Macros: hosted catalogs build `MacroInfo` from a `Macro` descriptor with `MacroInfo.of(macro)`. Parameter defaults are literal values only (`NULL`, booleans, numbers, `'strings'`); an expression default such as `current_date` is refused by `registerMacro` at startup.
