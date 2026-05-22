// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.example.table;

import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.internal.SchemaUtil;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.table.TableBindParams;
import farm.query.vgi.table.TableFunction;
import farm.query.vgi.table.TableInitParams;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.table.TransactionStorage;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * {@code tx_cached_value(key, seed)} — a single-row table function whose value
 * is cached per {@code (transaction_opaque_data, key)} via
 * {@link TableBindParams#transactionStorage()}.
 *
 * <p>First call within a transaction for a given {@code key} stores and emits
 * {@code seed}; later calls for the same key emit the cached value and ignore
 * their {@code seed}. Outside an explicit {@code BEGIN}/{@code COMMIT} block the
 * storage handle is {@code null}, so every call uses its own {@code seed}.
 * The resolved value is shipped bind→producer via {@code BindResponse.opaque_data}.
 * Mirrors vgi-python's {@code transaction_storage.py}.
 */
public final class TxCachedValueFunction implements TableFunction {

    private static final Schema OUTPUT = Schemas.of(Schemas.nullable("v", Schemas.INT64));
    private static final byte[] OUTPUT_IPC = SchemaUtil.serializeSchema(OUTPUT);

    private static byte[] encode(long v) { return ByteBuffer.allocate(8).putLong(v).array(); }

    private static long decode(byte[] b) { return ByteBuffer.wrap(b).getLong(); }

    private static byte[] storageKey(String userKey) {
        return ("vgi-fixture:tx_cached_value:" + userKey).getBytes(StandardCharsets.UTF_8);
    }

    private static final FunctionSpec SPEC = FunctionSpec.builder("tx_cached_value")
            .metadata(FunctionMetadata.describe(
                    "Return a value cached per (transaction_opaque_data, key) via transaction_storage.")
                    .withCategories("test", "transaction-storage"))
            .constArg("key", Schemas.UTF8)
            .constArg("seed", Schemas.INT64)
            .build();

    @Override public FunctionSpec spec() { return SPEC; }

    @Override public BindResponse onBind(TableBindParams params) {
        String key = params.arguments().positionalString(0, "");
        long seed = params.arguments().positionalLong(1, 0L);
        long value;
        TransactionStorage storage = params.transactionStorage();
        if (storage != null) {
            byte[] cached = storage.getOne(storageKey(key));
            if (cached != null) {
                value = decode(cached);
            } else {
                value = seed;
                storage.putOne(storageKey(key), encode(value));
            }
        } else {
            // No transaction → no caching possible; every call uses its seed.
            value = seed;
        }
        return new BindResponse(OUTPUT_IPC, encode(value), List.of(), List.of(), List.of());
    }

    @Override public long cardinality(TableBindParams params) { return 1L; }

    @Override public long maxWorkers() { return 1L; }

    @Override public TableProducerState createProducer(TableInitParams params) {
        byte[] opaque = params.bindOpaqueData();
        long value = opaque != null && opaque.length == 8 ? decode(opaque) : 0L;
        return new State(value);
    }

    public static final class State extends TableProducerState {
        public long value;
        public boolean emitted = false;

        public State() {}

        State(long value) { this.value = value; }

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            if (emitted) { out.finish(); return; }
            VectorSchemaRoot root = VectorSchemaRoot.create(OUTPUT, Allocators.root());
            root.allocateNew();
            BigIntVector v = (BigIntVector) root.getVector("v");
            v.setSafe(0, value);
            root.setRowCount(1);
            out.emit(root);
            emitted = true;
        }
    }
}
