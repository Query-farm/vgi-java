package farm.query.vgi.catalog.snapshot;
import farm.query.vgirpc.CallContext;
/** Created only after attachment authentication. attachment_scope identifies the original sealed handle. */
public record SnapshotRequest(byte[] attach_opaque_data, String attachment_scope, long catalog_version, CallContext context) {}
