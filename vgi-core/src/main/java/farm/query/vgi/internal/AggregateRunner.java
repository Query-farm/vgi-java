// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.internal;

import farm.query.vgi.aggregate.AggregateFunction;
import farm.query.vgi.protocol.AggregateBindResponse;
import farm.query.vgi.protocol.AggregateFinalizeResponse;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Drives the {@code aggregate_bind / update / combine / finalize / destructor}
 * lifecycle for one or more registered {@link AggregateFunction}s.
 *
 * <p>Per-execution state is persisted in a shared {@link AggregateStateStore}
 * (SQLite-backed) so partial states accumulated by one DuckDB worker
 * subprocess are visible to combine/finalize calls dispatched to a sibling
 * subprocess. Each lifecycle method follows the load → mutate → save pattern.
 */
public final class AggregateRunner {

    private final Map<String, AggregateFunction<?>> registry;
    private final AggregateStateStore store = AggregateStateStore.get();

    public AggregateRunner(Map<String, AggregateFunction<?>> registry) {
        this.registry = registry;
    }

    public boolean knows(String functionName) { return registry.containsKey(functionName); }

    public AggregateBindResponse bind(String functionName, byte[] inputSchemaIpc, byte[] argumentsIpc) {
        AggregateFunction<?> fn = registry.get(functionName);
        if (fn == null) throw new IllegalArgumentException("Unknown aggregate: " + functionName);
        Schema inputSchema = inputSchemaIpc == null ? null : SchemaUtil.deserializeSchema(inputSchemaIpc);
        byte[] outputSchemaIpc = SchemaUtil.serializeSchema(fn.bindOutputSchema(inputSchema));
        byte[] executionId = newExecutionId();
        if (argumentsIpc != null && argumentsIpc.length > 0) {
            store.saveArgs(executionId, functionName, argumentsIpc);
        }
        return new AggregateBindResponse(outputSchemaIpc, executionId);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public void update(String functionName, byte[] executionId, byte[] inputBatch) {
        AggregateFunction fn = lookup(functionName);
        BufferAllocator alloc = Allocators.root();
        BatchUtil.withReadBatch(inputBatch, alloc, root -> {
            if (root == null || root.getRowCount() == 0) return null;
            int gidIdx = -1;
            for (int i = 0; i < root.getSchema().getFields().size(); i++) {
                if ("__vgi_group_id".equals(root.getSchema().getFields().get(i).getName())) {
                    gidIdx = i;
                    break;
                }
            }
            if (gidIdx < 0) throw new IllegalStateException("aggregate_update: missing __vgi_group_id column");
            BigIntVector gidVec = (BigIntVector) root.getVector(gidIdx);
            int rows = root.getRowCount();
            long[] gids = new long[rows];
            LinkedHashSet<Long> uniq = new LinkedHashSet<>();
            for (int i = 0; i < rows; i++) {
                gids[i] = gidVec.get(i);
                uniq.add(gids[i]);
            }
            long[] uniqArr = uniq.stream().mapToLong(Long::longValue).toArray();

            // Build a child VSR without the group_id column for fn.update().
            List<FieldVector> argVectors = new java.util.ArrayList<>();
            List<Field> argFields = new java.util.ArrayList<>();
            for (int i = 0; i < root.getFieldVectors().size(); i++) {
                if (i == gidIdx) continue;
                argVectors.add(root.getFieldVectors().get(i));
                argFields.add(root.getSchema().getFields().get(i));
            }
            VectorSchemaRoot args = new VectorSchemaRoot(new Schema(argFields), argVectors, rows);

            Map<Long, byte[]> raw = store.loadStates(executionId, functionName, uniqArr);
            Map<Long, Object> states = new HashMap<>(raw.size());
            for (Map.Entry<Long, byte[]> e : raw.entrySet()) {
                states.put(e.getKey(), fn.deserializeState(e.getValue()));
            }
            byte[] argsIpc = store.loadArgs(executionId, functionName);
            farm.query.vgi.function.Arguments bindArgs = argsIpc == null
                    ? farm.query.vgi.function.Arguments.empty()
                    : ArgumentsParser.parse(argsIpc);
            fn.update((Map) states, gids, args, bindArgs);

            Map<Long, byte[]> dirty = new LinkedHashMap<>(states.size());
            for (Map.Entry<Long, Object> e : states.entrySet()) {
                dirty.put(e.getKey(), fn.serializeState(e.getValue()));
            }
            store.saveStates(executionId, functionName, dirty);
            return null;
        });
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public void combine(String functionName, byte[] executionId, byte[] mergeBatch) {
        AggregateFunction fn = lookup(functionName);
        BufferAllocator alloc = Allocators.root();
        BatchUtil.withReadBatch(mergeBatch, alloc, root -> {
            if (root == null || root.getRowCount() == 0) return null;
            int srcIdx = -1, tgtIdx = -1;
            for (int i = 0; i < root.getSchema().getFields().size(); i++) {
                String name = root.getSchema().getFields().get(i).getName();
                if ("source_group_id".equals(name)) srcIdx = i;
                else if ("target_group_id".equals(name)) tgtIdx = i;
            }
            if (srcIdx < 0 || tgtIdx < 0) {
                throw new IllegalStateException(
                        "aggregate_combine: merge_batch missing source_group_id/target_group_id");
            }
            BigIntVector srcVec = (BigIntVector) root.getVector(srcIdx);
            BigIntVector tgtVec = (BigIntVector) root.getVector(tgtIdx);
            int n = root.getRowCount();

            LinkedHashSet<Long> needed = new LinkedHashSet<>();
            long[] srcs = new long[n];
            long[] tgts = new long[n];
            for (int i = 0; i < n; i++) {
                srcs[i] = srcVec.get(i);
                tgts[i] = tgtVec.get(i);
                needed.add(srcs[i]);
                needed.add(tgts[i]);
            }
            long[] neededArr = needed.stream().mapToLong(Long::longValue).toArray();

            Map<Long, byte[]> raw = store.loadStates(executionId, functionName, neededArr);
            Map<Long, Object> states = new HashMap<>();
            for (Map.Entry<Long, byte[]> e : raw.entrySet()) {
                states.put(e.getKey(), fn.deserializeState(e.getValue()));
            }
            LinkedHashSet<Long> toDelete = new LinkedHashSet<>();
            for (int i = 0; i < n; i++) {
                long src = srcs[i];
                long tgt = tgts[i];
                Object srcState = states.get(src);
                Object tgtState = states.get(tgt);
                if (srcState == null) continue;
                if (tgtState == null) {
                    states.put(tgt, srcState);
                    states.remove(src);
                    toDelete.add(src);
                } else {
                    fn.combine(tgtState, srcState);
                    states.remove(src);
                    toDelete.add(src);
                }
            }
            // Persist remaining states + delete consumed sources.
            Map<Long, byte[]> dirty = new LinkedHashMap<>();
            for (Map.Entry<Long, Object> e : states.entrySet()) {
                dirty.put(e.getKey(), fn.serializeState(e.getValue()));
            }
            store.saveStates(executionId, functionName, dirty);
            if (!toDelete.isEmpty()) {
                store.deleteStates(executionId, functionName,
                        toDelete.stream().mapToLong(Long::longValue).toArray());
            }
            return null;
        });
    }

    public AggregateFinalizeResponse finalizeRequest(
            String functionName, byte[] executionId, byte[] groupIdsBatch, byte[] outputSchemaIpc) {
        @SuppressWarnings({"unchecked", "rawtypes"})
        AggregateFunction fn = lookup(functionName);
        Schema outputSchema = SchemaUtil.deserializeSchema(outputSchemaIpc);

        long[] gids = readGroupIds(groupIdsBatch);
        BufferAllocator alloc = Allocators.root();
        byte[] argsIpc = store.loadArgs(executionId, functionName);
        farm.query.vgi.function.Arguments bindArgs = argsIpc == null
                ? farm.query.vgi.function.Arguments.empty()
                : ArgumentsParser.parse(argsIpc);
        try (VectorSchemaRoot output = VectorSchemaRoot.create(outputSchema, alloc)) {
            output.allocateNew();
            Map<Long, byte[]> raw = store.loadStates(executionId, functionName, gids);
            for (int i = 0; i < gids.length; i++) {
                byte[] bytes = raw.get(gids[i]);
                if (bytes == null) {
                    fn.finalizeEmpty(output, i);
                } else {
                    Object state = fn.deserializeState(bytes);
                    fn.finalize(output, i, state, bindArgs);
                }
            }
            output.setRowCount(gids.length);
            return new AggregateFinalizeResponse(BatchUtil.writeSingleBatch(output));
        }
    }

    public void destructor(String functionName, byte[] executionId, byte[] groupIdsBatch) {
        long[] gids = readGroupIds(groupIdsBatch);
        if (gids.length == 0) return;
        store.deleteStates(executionId, functionName, gids);
    }

    @SuppressWarnings("rawtypes")
    private AggregateFunction lookup(String functionName) {
        AggregateFunction<?> fn = registry.get(functionName);
        if (fn == null) throw new IllegalStateException("aggregate: unknown function " + functionName);
        return fn;
    }

    private static long[] readGroupIds(byte[] batch) {
        BufferAllocator alloc = Allocators.root();
        try (VectorSchemaRoot root = BatchUtil.readSingleBatch(batch, alloc)) {
            if (root == null) return new long[0];
            int idx = -1;
            for (int i = 0; i < root.getSchema().getFields().size(); i++) {
                if ("group_id".equals(root.getSchema().getFields().get(i).getName())) { idx = i; break; }
            }
            if (idx < 0) idx = 0;
            BigIntVector v = (BigIntVector) root.getVector(idx);
            int rows = root.getRowCount();
            long[] gids = new long[rows];
            for (int i = 0; i < rows; i++) gids[i] = v.get(i);
            return gids;
        }
    }

    private static byte[] newExecutionId() {
        UUID u = UUID.randomUUID();
        byte[] out = new byte[16];
        long msb = u.getMostSignificantBits();
        long lsb = u.getLeastSignificantBits();
        for (int i = 0; i < 8; i++) out[i] = (byte) (msb >>> (8 * (7 - i)));
        for (int i = 0; i < 8; i++) out[i + 8] = (byte) (lsb >>> (8 * (7 - i)));
        return out;
    }
}
