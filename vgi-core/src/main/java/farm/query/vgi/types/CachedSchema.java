// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

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

    public byte[] ipc;

    private transient Schema cached;

    public CachedSchema() {}

    public CachedSchema(byte[] ipc) { this.ipc = ipc; }

    public CachedSchema(Schema schema) { this.ipc = SchemaUtil.serializeSchema(schema); }

    /** Deserialise on first call; cached for the lifetime of this instance. */
    public Schema get() {
        if (cached == null) cached = SchemaUtil.deserializeSchema(ipc);
        return cached;
    }
}
