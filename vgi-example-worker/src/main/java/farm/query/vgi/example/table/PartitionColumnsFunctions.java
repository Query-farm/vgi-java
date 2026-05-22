// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.example.table;

import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.FunctionMetadata.PartitionKind;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.internal.EmitMetadata;
import farm.query.vgi.internal.HexId;
import farm.query.vgi.internal.SchemaUtil;
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
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * v2 PartitionColumns (Hive-style) reference fixtures. Each declares a
 * {@code Meta.partition_kind} and annotates output-schema fields via
 * {@link EmitMetadata#partitionField}; the worker emits one Arrow batch per
 * partition tagged with {@code vgi_partition_values#b64} so DuckDB's planner
 * can pick {@code PhysicalPartitionedAggregate}. Mirrors vgi-python's
 * {@code partition_columns.py}.
 */
public final class PartitionColumnsFunctions {

    private PartitionColumnsFunctions() {}

    /** Per-execution queue of partition indices, drained by every scan worker. */
    private static final ConcurrentHashMap<String, ConcurrentLinkedQueue<Integer>> QUEUES =
            new ConcurrentHashMap<>();

    private static ConcurrentLinkedQueue<Integer> buildQueue(String key, int partitions) {
        return QUEUES.computeIfAbsent(key, k -> {
            ConcurrentLinkedQueue<Integer> q = new ConcurrentLinkedQueue<>();
            for (int i = 0; i < partitions; i++) q.add(i);
            return q;
        });
    }

    private static void setUtf8(VarCharVector v, int row, String s) {
        v.setSafe(row, s.getBytes(StandardCharsets.UTF_8));
    }

    // =====================================================================
    // country_partitioned_sales(rows_per_country) -> (country, sales)
    // =====================================================================

    public static final class CountryPartitionedSales implements TableFunction {

        private static final String[] COUNTRIES = {"AU", "BR", "CA", "FR", "US"};
        static final Schema OUTPUT = Schemas.of(
                EmitMetadata.partitionField("country", Schemas.UTF8),
                Schemas.nullable("sales", Schemas.INT64));
        private static final byte[] OUTPUT_IPC = SchemaUtil.serializeSchema(OUTPUT);

        private static final FunctionSpec SPEC = FunctionSpec.builder("country_partitioned_sales")
                .metadata(FunctionMetadata.describe(
                        "Per-country sales rows, one Arrow batch per country. Declares country as a SINGLE_VALUE partition column.")
                        .withCategories("generator", "partitioning")
                        .withPartitionKind(PartitionKind.SINGLE_VALUE_PARTITIONS))
                .constArg("rows_per_country", Schemas.INT64)
                .build();

        @Override public FunctionSpec spec() { return SPEC; }

        @Override public BindResponse onBind(TableBindParams p) {
            return BindResponse.forSchema(OUTPUT_IPC);
        }

        @Override public long maxWorkers() { return 8L; }

        @Override public TableProducerState createProducer(TableInitParams p) {
            int rpc = (int) ((Number) p.arguments().positionalAt(0)).longValue();
            String key = HexId.encode(p.executionId());
            return new State(buildQueue(key, COUNTRIES.length), key, rpc);
        }

        public static final class State extends TableProducerState {
            public String execKey;
            public int rpc;
            public transient ConcurrentLinkedQueue<Integer> queueRef;

            public State() {}

            State(ConcurrentLinkedQueue<Integer> q, String execKey, int rpc) {
                this.queueRef = q;
                this.execKey = execKey;
                this.rpc = rpc;
            }

            private ConcurrentLinkedQueue<Integer> queue() {
                if (queueRef == null) queueRef = QUEUES.get(execKey);
                return queueRef;
            }

            @Override public void produceTick(OutputCollector out, CallContext ctx) {
                Integer idx = queue() == null ? null : queue().poll();
                if (idx == null) { out.finish(); return; }
                String country = COUNTRIES[idx];
                long base = (long) idx * 1_000_000L;
                VectorSchemaRoot root = VectorSchemaRoot.create(OUTPUT, Allocators.root());
                root.allocateNew();
                VarCharVector cv = (VarCharVector) root.getVector("country");
                BigIntVector sv = (BigIntVector) root.getVector("sales");
                for (int i = 0; i < rpc; i++) {
                    setUtf8(cv, i, country);
                    sv.setSafe(i, base + i);
                }
                root.setRowCount(rpc);
                out.emit(root, EmitMetadata.partitionValues(OUTPUT, root, null));
            }
        }
    }

    // =====================================================================
    // region_year_partitioned(rows_per_partition) -> (region, year, value)
    // =====================================================================

    public static final class RegionYearPartitioned implements TableFunction {

        private static final String[] REGIONS = {"AMER", "AMER", "EMEA", "EMEA", "APAC", "APAC"};
        private static final long[] YEARS = {2023, 2024, 2023, 2024, 2023, 2024};
        static final Schema OUTPUT = Schemas.of(
                EmitMetadata.partitionField("region", Schemas.UTF8),
                EmitMetadata.partitionField("year", Schemas.INT64),
                Schemas.nullable("value", Schemas.FLOAT64));
        private static final byte[] OUTPUT_IPC = SchemaUtil.serializeSchema(OUTPUT);

        private static final FunctionSpec SPEC = FunctionSpec.builder("region_year_partitioned")
                .metadata(FunctionMetadata.describe(
                        "Per-(region, year) value rows. Declares both region and year as SINGLE_VALUE partition columns; GROUP BY region, year plans as PARTITIONED_AGGREGATE.")
                        .withCategories("generator", "partitioning")
                        .withPartitionKind(PartitionKind.SINGLE_VALUE_PARTITIONS))
                .constArg("rows_per_partition", Schemas.INT64)
                .build();

        @Override public FunctionSpec spec() { return SPEC; }

        @Override public BindResponse onBind(TableBindParams p) {
            return BindResponse.forSchema(OUTPUT_IPC);
        }

        @Override public long maxWorkers() { return 8L; }

        @Override public TableProducerState createProducer(TableInitParams p) {
            int rpp = (int) ((Number) p.arguments().positionalAt(0)).longValue();
            String key = HexId.encode(p.executionId());
            return new State(buildQueue(key, REGIONS.length), key, rpp);
        }

        public static final class State extends TableProducerState {
            public String execKey;
            public int rpp;
            public transient ConcurrentLinkedQueue<Integer> queueRef;

            public State() {}

            State(ConcurrentLinkedQueue<Integer> q, String execKey, int rpp) {
                this.queueRef = q;
                this.execKey = execKey;
                this.rpp = rpp;
            }

            private ConcurrentLinkedQueue<Integer> queue() {
                if (queueRef == null) queueRef = QUEUES.get(execKey);
                return queueRef;
            }

            @Override public void produceTick(OutputCollector out, CallContext ctx) {
                Integer idx = queue() == null ? null : queue().poll();
                if (idx == null) { out.finish(); return; }
                double base = idx * 1000.0;
                VectorSchemaRoot root = VectorSchemaRoot.create(OUTPUT, Allocators.root());
                root.allocateNew();
                VarCharVector rv = (VarCharVector) root.getVector("region");
                BigIntVector yv = (BigIntVector) root.getVector("year");
                Float8Vector vv = (Float8Vector) root.getVector("value");
                for (int i = 0; i < rpp; i++) {
                    setUtf8(rv, i, REGIONS[idx]);
                    yv.setSafe(i, YEARS[idx]);
                    vv.setSafe(i, base + i);
                }
                root.setRowCount(rpp);
                out.emit(root, EmitMetadata.partitionValues(OUTPUT, root, null));
            }
        }
    }

    // =====================================================================
    // partitioned_with_explicit_override(rows_per_category) -> (category, revenue)
    // =====================================================================

    public static final class PartitionedWithExplicitOverride implements TableFunction {

        private static final String[] CATEGORIES = {"books", "music", "video"};
        static final Schema OUTPUT = Schemas.of(
                EmitMetadata.partitionField("category", Schemas.UTF8),
                Schemas.nullable("revenue", Schemas.INT64));
        private static final byte[] OUTPUT_IPC = SchemaUtil.serializeSchema(OUTPUT);

        private static final FunctionSpec SPEC = FunctionSpec.builder("partitioned_with_explicit_override")
                .metadata(FunctionMetadata.describe(
                        "Partition column ``category`` is in the bind schema and the emitted batches; worker uses the explicit ``partition_values=`` override on ``out.emit`` to exercise the override code path.")
                        .withCategories("generator", "partitioning", "testing")
                        .withPartitionKind(PartitionKind.SINGLE_VALUE_PARTITIONS))
                .constArg("rows_per_category", Schemas.INT64)
                .build();

        @Override public FunctionSpec spec() { return SPEC; }

        @Override public BindResponse onBind(TableBindParams p) {
            return BindResponse.forSchema(OUTPUT_IPC);
        }

        @Override public long maxWorkers() { return 8L; }

        @Override public TableProducerState createProducer(TableInitParams p) {
            int rpc = (int) ((Number) p.arguments().positionalAt(0)).longValue();
            String key = HexId.encode(p.executionId());
            return new State(buildQueue(key, CATEGORIES.length), key, rpc);
        }

        public static final class State extends TableProducerState {
            public String execKey;
            public int rpc;
            public transient ConcurrentLinkedQueue<Integer> queueRef;

            public State() {}

            State(ConcurrentLinkedQueue<Integer> q, String execKey, int rpc) {
                this.queueRef = q;
                this.execKey = execKey;
                this.rpc = rpc;
            }

            private ConcurrentLinkedQueue<Integer> queue() {
                if (queueRef == null) queueRef = QUEUES.get(execKey);
                return queueRef;
            }

            @Override public void produceTick(OutputCollector out, CallContext ctx) {
                Integer idx = queue() == null ? null : queue().poll();
                if (idx == null) { out.finish(); return; }
                String category = CATEGORIES[idx];
                long base = (long) (idx + 1) * 100L;
                VectorSchemaRoot root = VectorSchemaRoot.create(OUTPUT, Allocators.root());
                root.allocateNew();
                VarCharVector cv = (VarCharVector) root.getVector("category");
                BigIntVector rv = (BigIntVector) root.getVector("revenue");
                for (int i = 0; i < rpc; i++) {
                    setUtf8(cv, i, category);
                    rv.setSafe(i, base + i);
                }
                root.setRowCount(rpc);
                // Explicit override even though the column is present in the batch.
                out.emit(root, EmitMetadata.partitionValues(OUTPUT, root,
                        Map.of("category", new EmitMetadata.Range(category, category))));
            }
        }
    }

    // =====================================================================
    // disjoint_range_partitioned(partitions, rows_per_partition := 10)
    //   -> (key, value)
    // =====================================================================

    public static final class DisjointRangePartitioned implements TableFunction {

        static final Schema OUTPUT = Schemas.of(
                EmitMetadata.partitionField("key", Schemas.INT64),
                Schemas.nullable("value", Schemas.INT64));
        private static final byte[] OUTPUT_IPC = SchemaUtil.serializeSchema(OUTPUT);

        private static final FunctionSpec SPEC = FunctionSpec.builder("disjoint_range_partitioned")
                .metadata(FunctionMetadata.describe(
                        "Disjoint per-chunk integer ranges on ``key``. Declares DISJOINT_PARTITIONS (wire-level only; DuckDB falls back to HASH_GROUP_BY for now).")
                        .withCategories("generator", "partitioning", "testing")
                        .withPartitionKind(PartitionKind.DISJOINT_PARTITIONS))
                .constArg("partitions", Schemas.INT64)
                .named("rows_per_partition", Schemas.INT64, "10")
                .build();

        @Override public FunctionSpec spec() { return SPEC; }

        @Override public BindResponse onBind(TableBindParams p) {
            return BindResponse.forSchema(OUTPUT_IPC);
        }

        @Override public long maxWorkers() { return 8L; }

        @Override public TableProducerState createProducer(TableInitParams p) {
            int partitions = (int) ((Number) p.arguments().positionalAt(0)).longValue();
            int rpp = (int) p.arguments().namedLong("rows_per_partition", 10L);
            String key = HexId.encode(p.executionId());
            return new State(buildQueue(key, partitions), key, rpp);
        }

        public static final class State extends TableProducerState {
            public String execKey;
            public int rpp;
            public transient ConcurrentLinkedQueue<Integer> queueRef;

            public State() {}

            State(ConcurrentLinkedQueue<Integer> q, String execKey, int rpp) {
                this.queueRef = q;
                this.execKey = execKey;
                this.rpp = rpp;
            }

            private ConcurrentLinkedQueue<Integer> queue() {
                if (queueRef == null) queueRef = QUEUES.get(execKey);
                return queueRef;
            }

            @Override public void produceTick(OutputCollector out, CallContext ctx) {
                Integer idx = queue() == null ? null : queue().poll();
                if (idx == null) { out.finish(); return; }
                long base = (long) idx * 1000L;
                VectorSchemaRoot root = VectorSchemaRoot.create(OUTPUT, Allocators.root());
                root.allocateNew();
                BigIntVector kv = (BigIntVector) root.getVector("key");
                BigIntVector vv = (BigIntVector) root.getVector("value");
                for (int i = 0; i < rpp; i++) {
                    kv.setSafe(i, base + i);
                    vv.setSafe(i, (long) idx * 10L + i);
                }
                root.setRowCount(rpp);
                out.emit(root, EmitMetadata.partitionValues(OUTPUT, root, null));
            }
        }
    }
}
