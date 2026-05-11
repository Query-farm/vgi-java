// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.example.table;

import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.pushdown.FilterApplier;
import farm.query.vgi.table.BatchState;
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
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

/** {@code sequence(count BIGINT, batch_size := 1000, increment := 1)}. */
public final class SequenceFunction implements TableFunction {

    private static final Schema OUTPUT_SCHEMA = new Schema(List.of(
            new Field("n", new FieldType(true, Schemas.INT64, null), null)));
    private static final byte[] OUTPUT_SCHEMA_IPC =
            farm.query.vgi.internal.SchemaUtil.serializeSchema(OUTPUT_SCHEMA);

    @Override public String name() { return "sequence"; }

    @Override public FunctionMetadata metadata() {
        return FunctionMetadata.describe("Generates a sequence of integers from 0 to n-1")
                .withPushdown(false, true, false).withCategories("generator", "utility");
    }

    @Override public List<ArgSpec> argumentSpecs() {
        return List.of(
                new ArgSpec("count", 0, Schemas.INT64, /*isConst=*/true),
                new ArgSpec("batch_size", -1, Schemas.INT64, "", true, true, "1000", List.of(), false, false),
                new ArgSpec("increment", -1, Schemas.INT64, "", true, true, "1", List.of(), false, false));
    }

    @Override public BindResponse onBind(TableBindParams params) {
        validate(params.arguments().named());
        return BindResponse.forSchema(OUTPUT_SCHEMA_IPC);
    }

    private static void validate(java.util.Map<String, Object> named) {
        // Required positional must be non-NULL.
        if (named.containsKey("positional_0") && named.get("positional_0") == null) {
            throw new IllegalArgumentException("count cannot be NULL");
        }
        if (named.get("count") == null && named.containsKey("count")) {
            throw new IllegalArgumentException("count cannot be NULL");
        }
        // Optional named args: NULL is a hard error (different from "absent").
        if (named.containsKey("batch_size") && named.get("batch_size") == null) {
            throw new IllegalArgumentException("batch_size cannot be NULL");
        }
        if (named.containsKey("increment") && named.get("increment") == null) {
            throw new IllegalArgumentException("increment cannot be NULL");
        }
        Object bs = named.get("batch_size");
        if (bs instanceof Number n && n.longValue() < 1L) {
            throw new IllegalArgumentException("batch_size must be >= 1, got " + n);
        }
        Object inc = named.get("increment");
        if (inc instanceof Number n && n.longValue() < 1L) {
            throw new IllegalArgumentException("increment must be >= 1, got " + n);
        }
    }

    @Override public TableProducerState createProducer(TableInitParams params) {
        validate(params.arguments().named());
        long count = ((Number) params.arguments().positionalAt(0)).longValue();
        long batchSize = params.arguments().namedLong("batch_size", 1000L);
        long increment = params.arguments().namedLong("increment", 1L);
        return new SequenceState(new BatchState(count, batchSize), increment,
                FilterApplier.from(params.pushdownFilters(), params.joinKeys()));
    }

    public static final class SequenceState extends TableProducerState {
        public BatchState batch;
        public long increment;
        public FilterApplier filters;

        public SequenceState() {}

        SequenceState(BatchState batch, long increment, FilterApplier filters) {
            this.batch = batch;
            this.increment = increment;
            this.filters = filters;
        }

        @Override
        public void produceTick(OutputCollector out, CallContext ctx) {
            if (batch.done()) {
                out.finish();
                return;
            }
            int n = batch.nextBatchSize();
            long start = batch.index();
            VectorSchemaRoot root = VectorSchemaRoot.create(OUTPUT_SCHEMA, Allocators.root());
            root.allocateNew();
            BigIntVector v = (BigIntVector) root.getVector("n");
            for (int i = 0; i < n; i++) {
                v.setSafe(i, (start + i) * increment);
            }
            root.setRowCount(n);
            if (filters != null) root = filters.apply(root);
            out.emit(root);
            batch.advance(n);
        }
    }
}
