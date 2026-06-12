// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.storage;

import java.nio.charset.StandardCharsets;

/**
 * Framework-reserved storage namespaces, all under the {@code _vgi/} prefix
 * that {@link BoundStorage} refuses for user-supplied namespaces. Byte-for-byte
 * identical to vgi-python's {@code FrameworkNS} so storage rows stay portable
 * across SDKs. The names are persisted in storage rows — never rename a member
 * value.
 */
public enum FrameworkNs {

    /** Init-time metadata for buffered table functions. */
    BUFFERING_INIT("_vgi/buffering_init"),

    /** Cursor-into-storage finalize stream batches. */
    STREAMING_FINALIZE("_vgi/streaming_finalize"),

    /** Table-in/out per-execution exchange state. */
    TIO_STATE("_vgi/tio_state"),

    /** Aggregate per-group partial state (keys are packed group ids). */
    AGGREGATE_STATE("_vgi/aggregate_state"),

    /** Aggregate per-window-partition state. */
    AGGREGATE_WINDOW_PARTITION("_vgi/aggregate_window_partition"),

    /** Session-scoped state for streaming operators. */
    STREAMING_SESSION("_vgi/streaming_session");

    private final byte[] bytes;

    FrameworkNs(String name) {
        this.bytes = name.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * The namespace's wire bytes (UTF-8 of the {@code _vgi/...} name).
     *
     * @return a fresh copy of the namespace bytes
     */
    public byte[] bytes() {
        return bytes.clone();
    }
}
