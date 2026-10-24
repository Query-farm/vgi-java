package farm.query.vgi.catalog.snapshot;
/** A stable revision and explicit metadata visibility boundary. Null scope is attachment-local. */
public record SnapshotIdentity(String catalog_namespace, String source_revision, String visibility_scope, long catalog_version) {}
