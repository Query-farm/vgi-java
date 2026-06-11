// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.table;

import farm.query.vgi.internal.SchemaUtil;
import farm.query.vgi.protocol.BindResponse;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Base for table functions with a <em>fixed</em> output schema. Handles
 * schema → IPC-bytes caching and the trivial {@link #onBind} that ships them.
 *
 * <p>Subclasses implement {@link #outputSchema()} (returns a constant) and
 * {@link #createProducer(TableInitParams)}; everything else is inherited from
 * {@link TableFunction} with sensible defaults.
 *
 * <p>For sequence-like fixtures that emit {@code count} rows in
 * {@code count}/{@code batch_size} loops, extend
 * {@link CountdownTableFunction} instead — it adds {@code argumentSpecs},
 * {@code cardinality}, and a default {@code statistics} on top of this
 * contract.
 */
public abstract class SimpleTableFunction implements TableFunction {

    private volatile byte[] schemaIpcCache;

    /** Sole constructor; the schema IPC cache is populated lazily on first bind. */
    protected SimpleTableFunction() {}

    /**
     * The fixed output schema. Called once and the result is cached.
     *
     * @return the constant output schema
     */
    protected abstract Schema outputSchema();

    private byte[] schemaIpc() {
        byte[] cached = schemaIpcCache;
        if (cached == null) {
            cached = SchemaUtil.serializeSchema(outputSchema());
            schemaIpcCache = cached;
        }
        return cached;
    }

    /** {@return a bind response carrying the cached IPC-serialized {@link #outputSchema()}} */
    @Override
    public BindResponse onBind(TableBindParams params) {
        return BindResponse.forSchema(schemaIpc());
    }
}
