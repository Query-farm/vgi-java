// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.example.table;

import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.internal.HexId;
import farm.query.vgi.internal.SchemaUtil;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.FunctionMetadata.OrderPreservation;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.table.TableBindParams;
import farm.query.vgi.table.TableFunction;
import farm.query.vgi.table.TableInitParams;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Three sister fixtures that emit identical {@code n: int64} sequences but
 * advertise different {@code order_preservation_type} metadata. DuckDB's
 * planner uses that to pick its parallelisation strategy:
 * {@code FIXED_ORDER} forces a single connection,
 * {@code INSERTION_ORDER} / {@code NO_ORDER_PRESERVED} let the planner
 * fan out. Used by table/order_preservation_modes.test to verify the
 * metadata flows end-to-end.
 */
public final class PartitionedOrderModeFunctions {

    private PartitionedOrderModeFunctions() {}

    private static final Schema OUTPUT_SCHEMA = new Schema(List.of(
            Schemas.nullable("n", Schemas.INT64)));
    private static final byte[] OUTPUT_SCHEMA_IPC =
            SchemaUtil.serializeSchema(OUTPUT_SCHEMA);
    private static final long CHUNK = 10_000L;

    private static final ConcurrentHashMap<String, ConcurrentLinkedQueue<long[]>> QUEUES =
            new ConcurrentHashMap<>();

    private static String key(byte[] executionId) { return HexId.encode(executionId); }

    private static ConcurrentLinkedQueue<long[]> queueFor(String execKey, long count) {
        return QUEUES.computeIfAbsent(execKey, k -> {
            ConcurrentLinkedQueue<long[]> q = new ConcurrentLinkedQueue<>();
            for (long start = 0; start < count; start += CHUNK) {
                long end = Math.min(start + CHUNK, count);
                q.add(new long[] {start, end});
            }
            return q;
        });
    }

    private abstract static class Base implements TableFunction {
        private final String name;
        private final String description;
        private final OrderPreservation order;
        private final long maxWorkers;

        Base(String name, String description, OrderPreservation order, long maxWorkers) {
            this.name = name;
            this.description = description;
            this.order = order;
            this.maxWorkers = maxWorkers;
        }

        @Override public String name() { return name; }
        @Override public FunctionMetadata metadata() {
            return FunctionMetadata.describe(description)
                    .withOrderPreservation(order);
        }
        @Override public List<ArgSpec> argumentSpecs() {
            return List.of(ArgSpec.positional("count", 0, Schemas.INT64));
        }
        @Override public BindResponse onBind(TableBindParams p) { return BindResponse.forSchema(OUTPUT_SCHEMA_IPC); }
        @Override public long cardinality(TableBindParams p) {
            Object c = p.arguments().positionalAt(0);
            return c instanceof Number n ? n.longValue() : -1L;
        }
        @Override public long maxWorkers() { return maxWorkers; }

        @Override public TableProducerState createProducer(TableInitParams p) {
            long count = ((Number) p.arguments().positionalAt(0)).longValue();
            String execKey = key(p.executionId());
            return new State(queueFor(execKey, count), execKey);
        }
    }

    public static final class FixedOrder extends Base {
        public FixedOrder() { super(
                "partitioned_fixed_order",
                "Multi-worker partitioned sequence; preserves_order=FIXED_ORDER (DuckDB serializes the pipeline so a single worker produces all rows).",
                OrderPreservation.FIXED_ORDER, 1L); }
    }

    public static final class PreservesOrder extends Base {
        public PreservesOrder() { super(
                "partitioned_preserves_order",
                "Multi-worker partitioned sequence; preserves_order=PRESERVES_ORDER (maps to DuckDB INSERTION_ORDER).",
                OrderPreservation.INSERTION_ORDER, 8L); }
    }

    public static final class NoOrderGuarantee extends Base {
        public NoOrderGuarantee() { super(
                "partitioned_no_order_guarantee",
                "Multi-worker partitioned sequence; preserves_order=NO_ORDER_GUARANTEE (maps to DuckDB NO_ORDER).",
                OrderPreservation.NO_ORDER_PRESERVED, 8L); }
    }

    public static final class State extends TableProducerState implements Serializable {
        private static final long serialVersionUID = 1L;
        public String execKey;
        public transient ConcurrentLinkedQueue<long[]> queueRef;
        public long currentStart = -1;
        public long currentEnd = -1;
        public long currentIdx = 0;

        public State() {}
        State(ConcurrentLinkedQueue<long[]> q, String execKey) { this.queueRef = q; this.execKey = execKey; }

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
            VectorSchemaRoot root = VectorSchemaRoot.create(OUTPUT_SCHEMA, Allocators.root());
            root.allocateNew();
            BigIntVector v = (BigIntVector) root.getVector("n");
            for (int i = 0; i < n; i++) v.setSafe(i, currentIdx + i);
            root.setRowCount(n);
            out.emit(root);
            currentIdx += n;
        }
    }
}
