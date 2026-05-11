// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.example.table;

import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.pushdown.FilterApplier;
import farm.query.vgi.pushdown.PushdownFilters;
import farm.query.vgi.pushdown.PushdownFiltersDecoder;
import farm.query.vgi.table.TableBindParams;
import farm.query.vgi.table.TableFunction;
import farm.query.vgi.table.TableInitParams;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.types.Schemas;
import farm.query.vgi.types.CachedSchema;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * {@code filter_echo_partitioned(count BIGINT [const])} — combines the
 * shared work-queue distribution of {@link PartitionedSequenceFunction}
 * with the filter-pushdown echo of {@link FilterEchoFunction}. Each
 * worker emits {@code (n, s, pushed_filters, worker_pid)} tuples, where
 * {@code worker_pid} is the JVM thread id (distinct per scan thread under
 * launcher-mode AF_UNIX transport).
 */
public final class FilterEchoPartitionedFunction implements TableFunction {

    private static final Schema OUTPUT_SCHEMA = new Schema(List.of(
            new Field("n", new FieldType(true, Schemas.INT64, null), null),
            new Field("s", new FieldType(true, Schemas.UTF8, null), null),
            new Field("pushed_filters", new FieldType(true, Schemas.UTF8, null), null),
            new Field("worker_pid", new FieldType(true, Schemas.INT32, null), null)));
    private static final byte[] OUTPUT_SCHEMA_IPC =
            farm.query.vgi.internal.SchemaUtil.serializeSchema(OUTPUT_SCHEMA);
    private static final long CHUNK = 1000L;

    private static final ConcurrentHashMap<String, ConcurrentLinkedQueue<long[]>> QUEUES =
            new ConcurrentHashMap<>();

    private static String key(byte[] executionId) { return farm.query.vgi.internal.HexId.encode(executionId); }

    @Override public String name() { return "filter_echo_partitioned"; }

    @Override public FunctionMetadata metadata() {
        return FunctionMetadata.describe(
                "Multi-worker partitioned sequence that echoes pushed-down filters")
                .withPushdown(/*projection=*/true, /*filter=*/true, /*autoApply=*/true);
    }

    @Override public List<ArgSpec> argumentSpecs() {
        return List.of(new ArgSpec("count", 0, Schemas.INT64, /*isConst=*/true));
    }

    @Override public BindResponse onBind(TableBindParams params) { return BindResponse.forSchema(OUTPUT_SCHEMA_IPC); }

    @Override public long cardinality(TableBindParams p) {
        Object c = p.arguments().positionalAt(0);
        return c instanceof Number n ? n.longValue() : -1L;
    }

    @Override public long maxWorkers() { return 8L; }

    @Override public TableProducerState createProducer(TableInitParams params) {
        long count = ((Number) params.arguments().positionalAt(0)).longValue();
        String execKey = key(params.executionId());
        ConcurrentLinkedQueue<long[]> queue = QUEUES.computeIfAbsent(execKey, k -> {
            ConcurrentLinkedQueue<long[]> q = new ConcurrentLinkedQueue<>();
            for (long start = 0; start < count; start += CHUNK) {
                long end = Math.min(start + CHUNK, count);
                q.add(new long[] {start, end});
            }
            return q;
        });
        byte[] pfBytes = params.pushdownFilters();
        PushdownFilters pf = pfBytes == null
                ? PushdownFilters.empty()
                : PushdownFiltersDecoder.decode(pfBytes, params.joinKeys());
        return new State(queue, execKey, pf.formatInline(), pfBytes,
                new CachedSchema(params.outputSchema()),
                params.joinKeys());
    }

    public static final class State extends TableProducerState implements Serializable {
        private static final long serialVersionUID = 1L;
        public String execKey;
        public String filterStr;
        public byte[] filterBytes;
        public CachedSchema outputSchema;
        public List<byte[]> joinKeysIpc;
        public transient ConcurrentLinkedQueue<long[]> queueRef;

        public long currentStart = -1;
        public long currentEnd = -1;
        public long currentIdx = 0;

        public State() {}

        State(ConcurrentLinkedQueue<long[]> queue, String execKey, String filterStr,
                byte[] filterBytes, CachedSchema outputSchema, List<byte[]> joinKeysIpc) {
            this.queueRef = queue;
            this.execKey = execKey;
            this.filterStr = filterStr;
            this.filterBytes = filterBytes;
            this.outputSchema = outputSchema;
            this.joinKeysIpc = joinKeysIpc;
        }

        private ConcurrentLinkedQueue<long[]> queue() {
            if (queueRef == null) queueRef = QUEUES.get(execKey);
            return queueRef;
        }

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            if (currentIdx >= currentEnd) {
                long[] next = queue() == null ? null : queue().poll();
                if (next == null) { out.finish(); return; }
                currentStart = next[0];
                currentEnd = next[1];
                currentIdx = currentStart;
            }
            int n = (int) Math.min(2048L, currentEnd - currentIdx);
            VectorSchemaRoot work = VectorSchemaRoot.create(OUTPUT_SCHEMA, Allocators.root());
            work.allocateNew();
            BigIntVector nv = (BigIntVector) work.getVector("n");
            VarCharVector sv = (VarCharVector) work.getVector("s");
            VarCharVector pv = (VarCharVector) work.getVector("pushed_filters");
            IntVector wv = (IntVector) work.getVector("worker_pid");
            Text filterText = new Text(filterStr);
            int tid = (int) (Thread.currentThread().threadId() & 0x7FFFFFFF);
            for (int i = 0; i < n; i++) {
                long row = currentIdx + i;
                nv.setSafe(i, row);
                sv.setSafe(i, new Text("row_" + row));
                pv.setSafe(i, filterText);
                wv.setSafe(i, tid);
            }
            work.setRowCount(n);
            currentIdx += n;
            if (filterBytes != null) {
                work = FilterApplier.from(filterBytes, joinKeysIpc).apply(work);
            }
            VectorSchemaRoot emit = projectTo(work, outputSchema.get());
            out.emit(emit);
        }

        private static VectorSchemaRoot projectTo(VectorSchemaRoot src, Schema target) {
            if (src.getSchema().equals(target)) return src;
            VectorSchemaRoot dst = VectorSchemaRoot.create(target, Allocators.root());
            dst.allocateNew();
            int rows = src.getRowCount();
            for (Field f : target.getFields()) {
                FieldVector dv = dst.getVector(f.getName());
                FieldVector sv = src.getVector(f.getName());
                if (sv == null) continue;
                for (int i = 0; i < rows; i++) dv.copyFromSafe(i, i, sv);
            }
            dst.setRowCount(rows);
            src.close();
            return dst;
        }
    }
}
