// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.types;

import farm.query.vgi.internal.SchemaUtil;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Wire-portable IPC-encoded {@link Schema} with on-demand deserialisation
 * caching. Used by table-producer states that hold an output schema across
 * exchange ticks — the IPC bytes survive HTTP state-token round-trips
 * (Jackson serialises the public {@link #ipc} field), and the cached Schema
 * is reused per process to avoid re-decoding on every emit.
 *
 * <p>The class is intentionally a mutable POJO with a public no-arg
 * constructor so the {@code StateSerializer} reflection path can both
 * instantiate it and populate {@code ipc} from JSON.
 */
public final class CachedSchema {

    /** IPC-encoded schema bytes; public so Jackson can serialise it through state tokens. */
    public byte[] ipc;

    private transient Schema cached;

    /** No-arg constructor for the reflection-based state deserialisation path. */
    public CachedSchema() {}

    /**
     * Wrap pre-encoded IPC schema bytes.
     *
     * @param ipc IPC-encoded schema bytes
     */
    public CachedSchema(byte[] ipc) { this.ipc = ipc; }

    /**
     * Encode a {@link Schema} to IPC bytes for storage.
     *
     * @param schema the schema to encode
     */
    public CachedSchema(Schema schema) { this.ipc = SchemaUtil.serializeSchema(schema); }

    /**
     * Deserialise on first call; cached for the lifetime of this instance.
     *
     * @return the decoded {@link Schema}
     */
    public Schema get() {
        if (cached == null) cached = SchemaUtil.deserializeSchema(ipc);
        return cached;
    }
}
