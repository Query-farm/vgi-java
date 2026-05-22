// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.example.table;

import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.FunctionMetadata.PartitionKind;
import farm.query.vgi.internal.EmitMetadata;
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
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * Deliberately-broken v2 PartitionColumns fixtures. Two violations are caught
 * worker-side by {@link EmitMetadata#partitionValues} (before the wire); two
 * reach the C++ extension's {@code InstallBatch} defense-in-depth check.
 * Mirrors vgi-python's {@code partition_columns_broken.py}.
 */
public final class BrokenPartitionColumnsFunctions {

    private BrokenPartitionColumnsFunctions() {}

    private static void setUtf8(VarCharVector v, int row, String s) {
        v.setSafe(row, s.getBytes(StandardCharsets.UTF_8));
    }

    private abstract static class BrokenBase implements TableFunction {
        @Override public List<ArgSpec> argumentSpecs() {
            return List.of(ArgSpec.positional("count", 0, Schemas.INT64));
        }
        @Override public long maxWorkers() { return 1L; }
    }

    /** country/sales batch emitted with NO vgi_partition_values metadata — C++ raises. */
    public static final class BrokenMissingPartitionValues extends BrokenBase {
        static final Schema OUTPUT = Schemas.of(
                EmitMetadata.partitionField("country", Schemas.UTF8),
                Schemas.nullable("sales", Schemas.INT64));
        private static final byte[] OUTPUT_IPC = SchemaUtil.serializeSchema(OUTPUT);

        @Override public String name() { return "broken_missing_partition_values"; }
        @Override public FunctionMetadata metadata() {
            return FunctionMetadata.describe(
                    "DELIBERATELY BROKEN: declares partition_kind + partition-annotated field but emits a data batch without vgi_partition_values#b64 metadata. C++ extension's contract check raises.")
                    .withCategories("testing", "broken")
                    .withPartitionKind(PartitionKind.SINGLE_VALUE_PARTITIONS);
        }
        @Override public BindResponse onBind(TableBindParams p) { return BindResponse.forSchema(OUTPUT_IPC); }
        @Override public TableProducerState createProducer(TableInitParams p) {
            return new St((int) ((Number) p.arguments().positionalAt(0)).longValue());
        }
        public static final class St extends TableProducerState {
            public int count;
            public boolean emitted = false;
            public St() {}
            St(int count) { this.count = count; }
            @Override public void produceTick(OutputCollector out, CallContext ctx) {
                if (emitted) { out.finish(); return; }
                VectorSchemaRoot root = VectorSchemaRoot.create(OUTPUT, Allocators.root());
                root.allocateNew();
                VarCharVector cv = (VarCharVector) root.getVector("country");
                BigIntVector sv = (BigIntVector) root.getVector("sales");
                for (int i = 0; i < count; i++) { setUtf8(cv, i, "US"); sv.setSafe(i, i); }
                root.setRowCount(count);
                out.emit(root);   // no vgi_partition_values#b64 metadata — C++ raises
                emitted = true;
            }
        }
    }

    /** SINGLE_VALUE_PARTITIONS but explicit override supplies min != max — C++ raises. */
    public static final class BrokenPartitionMinNeqMax extends BrokenBase {
        static final Schema OUTPUT = Schemas.of(
                EmitMetadata.partitionField("country", Schemas.UTF8),
                Schemas.nullable("sales", Schemas.INT64));
        private static final byte[] OUTPUT_IPC = SchemaUtil.serializeSchema(OUTPUT);

        @Override public String name() { return "broken_partition_min_neq_max"; }
        @Override public FunctionMetadata metadata() {
            return FunctionMetadata.describe(
                    "DELIBERATELY BROKEN: declares SINGLE_VALUE_PARTITIONS but supplies an explicit partition_values override with min != max. The framework's wrapper validation doesn't compare min vs max for SINGLE_VALUE; the C++ extension's defense-in-depth check in InstallBatch raises.")
                    .withCategories("testing", "broken")
                    .withPartitionKind(PartitionKind.SINGLE_VALUE_PARTITIONS);
        }
        @Override public BindResponse onBind(TableBindParams p) { return BindResponse.forSchema(OUTPUT_IPC); }
        @Override public TableProducerState createProducer(TableInitParams p) {
            return new St((int) ((Number) p.arguments().positionalAt(0)).longValue());
        }
        public static final class St extends TableProducerState {
            public int count;
            public boolean emitted = false;
            public St() {}
            St(int count) { this.count = count; }
            @Override public void produceTick(OutputCollector out, CallContext ctx) {
                if (emitted) { out.finish(); return; }
                VectorSchemaRoot root = VectorSchemaRoot.create(OUTPUT, Allocators.root());
                root.allocateNew();
                VarCharVector cv = (VarCharVector) root.getVector("country");
                BigIntVector sv = (BigIntVector) root.getVector("sales");
                for (int i = 0; i < count; i++) { setUtf8(cv, i, "US"); sv.setSafe(i, i); }
                root.setRowCount(count);
                // min != max defeats the worker check; C++ defense-in-depth raises.
                out.emit(root, EmitMetadata.partitionValues(OUTPUT, root,
                        Map.of("country", new EmitMetadata.Range("US", "BR"))));
                emitted = true;
            }
        }
    }

    /** No partition-annotated field, but the worker passes partition_values — worker-side raise. */
    public static final class BrokenPartitionValuesNoAnnotation extends BrokenBase {
        // No partition_field() — plain schema.
        static final Schema OUTPUT = Schemas.of(
                Schemas.nullable("country", Schemas.UTF8),
                Schemas.nullable("sales", Schemas.INT64));
        private static final byte[] OUTPUT_IPC = SchemaUtil.serializeSchema(OUTPUT);

        @Override public String name() { return "broken_partition_values_no_annotation"; }
        @Override public FunctionMetadata metadata() {
            return FunctionMetadata.describe(
                    "DELIBERATELY BROKEN: no field carries vgi.partition_column metadata (and partition_kind defaults to NOT_PARTITIONED), but the worker passes partition_values= on out.emit. The framework rejects with RuntimeError before the wire.")
                    .withCategories("testing", "broken");
        }
        @Override public BindResponse onBind(TableBindParams p) { return BindResponse.forSchema(OUTPUT_IPC); }
        @Override public TableProducerState createProducer(TableInitParams p) {
            return new St((int) ((Number) p.arguments().positionalAt(0)).longValue());
        }
        public static final class St extends TableProducerState {
            public int count;
            public boolean emitted = false;
            public St() {}
            St(int count) { this.count = count; }
            @Override public void produceTick(OutputCollector out, CallContext ctx) {
                if (emitted) { out.finish(); return; }
                VectorSchemaRoot root = VectorSchemaRoot.create(OUTPUT, Allocators.root());
                root.allocateNew();
                VarCharVector cv = (VarCharVector) root.getVector("country");
                BigIntVector sv = (BigIntVector) root.getVector("sales");
                for (int i = 0; i < count; i++) { setUtf8(cv, i, "US"); sv.setSafe(i, i); }
                root.setRowCount(count);
                // OUTPUT has no partition-annotated field — helper raises before the wire.
                EmitMetadata.partitionValues(OUTPUT, root,
                        Map.of("country", new EmitMetadata.Range("US", "US")));
                out.emit(root);   // unreached
                emitted = true;
            }
        }
    }

    /** Annotated partition column absent from the emitted batch, no override — worker-side raise. */
    public static final class BrokenPartitionColumnAbsentFromBatch extends BrokenBase {
        static final Schema OUTPUT = Schemas.of(
                EmitMetadata.partitionField("category", Schemas.UTF8),
                Schemas.nullable("revenue", Schemas.INT64));
        private static final byte[] OUTPUT_IPC = SchemaUtil.serializeSchema(OUTPUT);
        // The batch the fixture actually emits — missing the 'category' column.
        private static final Schema BATCH_ONLY = Schemas.of(Schemas.nullable("revenue", Schemas.INT64));

        @Override public String name() { return "broken_partition_column_absent_from_batch"; }
        @Override public FunctionMetadata metadata() {
            return FunctionMetadata.describe(
                    "DELIBERATELY BROKEN: declares partition_kind on 'category' but emits a batch without 'category' AND doesn't supply an explicit partition_values override. The framework's auto-extract fails with RuntimeError before the wire.")
                    .withCategories("testing", "broken")
                    .withPartitionKind(PartitionKind.SINGLE_VALUE_PARTITIONS);
        }
        @Override public BindResponse onBind(TableBindParams p) { return BindResponse.forSchema(OUTPUT_IPC); }
        @Override public TableProducerState createProducer(TableInitParams p) {
            return new St((int) ((Number) p.arguments().positionalAt(0)).longValue());
        }
        public static final class St extends TableProducerState {
            public int count;
            public boolean emitted = false;
            public St() {}
            St(int count) { this.count = count; }
            @Override public void produceTick(OutputCollector out, CallContext ctx) {
                if (emitted) { out.finish(); return; }
                VectorSchemaRoot root = VectorSchemaRoot.create(BATCH_ONLY, Allocators.root());
                root.allocateNew();
                BigIntVector rv = (BigIntVector) root.getVector("revenue");
                for (int i = 0; i < count; i++) rv.setSafe(i, i);
                root.setRowCount(count);
                // 'category' is partition-annotated in OUTPUT but absent from the
                // emitted batch and no override is supplied — helper raises.
                EmitMetadata.partitionValues(OUTPUT, root, null);
                out.emit(root);   // unreached
                emitted = true;
            }
        }
    }
}
