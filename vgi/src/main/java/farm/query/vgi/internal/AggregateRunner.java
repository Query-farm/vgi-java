// Copyright 2026 Query Farm LLC - https://query.farm

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

/**
 * Drives the {@code aggregate_bind / update / combine / finalize / destructor}
 * lifecycle for one or more registered {@link AggregateFunction}s.
 *
 * <p>Per-execution state is persisted in the worker's shared
 * {@link farm.query.vgi.storage.FunctionStorage} (via {@link AggregateStateStore})
 * so partial states accumulated by one DuckDB worker subprocess are visible to
 * combine/finalize calls dispatched to a sibling subprocess. Each lifecycle
 * method follows the load → mutate → save pattern.
 */
public final class AggregateRunner {

    private final Map<String, AggregateFunction<?>> registry;
    private final AggregateStateStore store;

    /**
     * Creates a runner over a fixed aggregate registry.
     *
     * @param registry registered aggregates keyed by function name
     * @param storage  shared worker storage backing per-execution state
     */
    public AggregateRunner(Map<String, AggregateFunction<?>> registry,
                           farm.query.vgi.storage.FunctionStorage storage) {
        this.registry = registry;
        this.store = new AggregateStateStore(storage);
    }

    /**
     * Handle {@code aggregate_bind}: resolve the function, compute its output
     * schema, mint an execution id, and persist bind-time arguments.
     *
     * @param functionName  the aggregate to bind
     * @param inputSchemaIpc IPC-encoded input schema, or {@code null}
     * @param argumentsIpc   IPC-encoded bind arguments, or {@code null}/empty
     * @param secretsIpc     IPC-encoded pre-resolved secret values, or {@code null}/empty
     * @return the bind response carrying the output schema and execution id
     */
    public AggregateBindResponse bind(String functionName, byte[] inputSchemaIpc, byte[] argumentsIpc,
                                       byte[] secretsIpc) {
        AggregateFunction<?> fn = registry.get(functionName);
        if (fn == null) throw new IllegalArgumentException("Unknown aggregate: " + functionName);
        Schema inputSchema = inputSchemaIpc == null ? null : SchemaUtil.deserializeSchema(inputSchemaIpc);
        farm.query.vgi.function.Arguments bindArgs = (argumentsIpc == null || argumentsIpc.length == 0)
                ? farm.query.vgi.function.Arguments.empty()
                : ArgumentsParser.parse(argumentsIpc);
        farm.query.vgi.Secrets secrets = farm.query.vgi.Secrets.parse(secretsIpc);
        farm.query.vgi.function.ConstraintEnforcer.enforce(bindArgs, fn.argumentSpecs());
        byte[] outputSchemaIpc = SchemaUtil.serializeSchema(
                fn.bindOutputSchema(inputSchema, bindArgs, secrets));
        byte[] executionId = newExecutionId();
        if (argumentsIpc != null && argumentsIpc.length > 0) {
            store.saveArgs(executionId, functionName, argumentsIpc);
        }
        return new AggregateBindResponse(outputSchemaIpc, executionId);
    }

    /**
     * Handle {@code aggregate_update}: load each group's state, fold the input
     * batch (split by the {@code __vgi_group_id} column) into it, and save.
     *
     * @param functionName the aggregate
     * @param executionId  the execution scope from {@code bind}
     * @param inputBatch   IPC batch of argument columns plus {@code __vgi_group_id}
     */
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

    /**
     * Handle {@code aggregate_combine}: merge each {@code source_group_id}'s
     * partial state into its {@code target_group_id}. Sources are left intact
     * (freed later by the destructor) since one leaf may feed several targets.
     *
     * @param functionName the aggregate
     * @param executionId  the execution scope from {@code bind}
     * @param mergeBatch   IPC batch with {@code source_group_id}/{@code target_group_id} columns
     */
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
            // Window-segment-tree combines may reference the same source
            // multiple times (a leaf state combined into several output rows),
            // so we cannot delete the source after combining. The destructor
            // RPC is what eventually frees the leaf state. Mirrors the Python
            // worker's aggregate_combine semantics.
            LinkedHashSet<Long> touchedTargets = new LinkedHashSet<>();
            for (int i = 0; i < n; i++) {
                long src = srcs[i];
                long tgt = tgts[i];
                Object srcState = states.get(src);
                Object tgtState = states.get(tgt);
                if (srcState == null && tgtState == null) continue;
                if (srcState == null) {
                    touchedTargets.add(tgt);
                    continue;
                }
                if (tgtState == null) {
                    tgtState = fn.newState();
                    states.put(tgt, tgtState);
                }
                fn.combine(tgtState, srcState);
                touchedTargets.add(tgt);
            }
            // Persist only the touched targets — sources retain whatever they
            // had on disk so subsequent combines / finalizes can reread them.
            Map<Long, byte[]> dirty = new LinkedHashMap<>();
            for (Long t : touchedTargets) {
                Object s = states.get(t);
                if (s != null) dirty.put(t, fn.serializeState(s));
            }
            store.saveStates(executionId, functionName, dirty);
            return null;
        });
    }

    /**
     * Handle {@code aggregate_finalize}: produce one output row per requested
     * group id, calling {@code finalizeEmpty} for groups with no accumulated state.
     *
     * @param functionName   the aggregate
     * @param executionId    the execution scope from {@code bind}
     * @param groupIdsBatch  IPC batch with a {@code group_id} column listing groups to finalize
     * @param outputSchemaIpc IPC-encoded output schema (single result column)
     * @return the finalize response carrying the result batch
     */
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
            // Aggregates produce exactly one output column — resolve it once
            // here so per-row finalize calls don't repeat the lookup/cast.
            org.apache.arrow.vector.FieldVector result = output.getFieldVectors().get(0);
            Map<Long, byte[]> raw = store.loadStates(executionId, functionName, gids);
            for (int i = 0; i < gids.length; i++) {
                byte[] bytes = raw.get(gids[i]);
                if (bytes == null) {
                    fn.finalizeEmpty(result, i);
                } else {
                    Object state = fn.deserializeState(bytes);
                    fn.finalize(result, i, state, bindArgs);
                }
            }
            output.setRowCount(gids.length);
            return new AggregateFinalizeResponse(BatchUtil.writeSingleBatch(output));
        }
    }

    /**
     * Handle {@code aggregate_destructor}: free the persisted state for the
     * listed group ids.
     *
     * @param functionName  the aggregate
     * @param executionId   the execution scope from {@code bind}
     * @param groupIdsBatch IPC batch with a {@code group_id} column
     */
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
        return HexId.newExecutionId();
    }
}
