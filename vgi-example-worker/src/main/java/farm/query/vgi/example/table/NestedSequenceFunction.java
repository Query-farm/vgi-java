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
import farm.query.vgi.types.CachedSchema;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.util.List;

/**
 * {@code nested_sequence(count [, batch_size := 1000, history_size := 20])}
 * — emits {@code n}, {@code metadata: STRUCT(index, label)}, and
 * {@code history: LIST<BIGINT>}. Opts into projection + filter pushdown so
 * DuckDB can prune unwanted columns and push WHERE clauses.
 */
public final class NestedSequenceFunction implements TableFunction {

    private static final FieldType F_NULLABLE_INT64 =
            new FieldType(true, Schemas.INT64, null);
    private static final FieldType F_NULLABLE_UTF8 =
            new FieldType(true, Schemas.UTF8, null);

    private static final Schema FULL_SCHEMA = new Schema(List.of(
            new Field("n", F_NULLABLE_INT64, null),
            new Field("metadata",
                    new FieldType(true, ArrowType.Struct.INSTANCE, null),
                    List.of(
                            new Field("index", F_NULLABLE_INT64, null),
                            new Field("label", F_NULLABLE_UTF8, null))),
            new Field("history",
                    new FieldType(true, new ArrowType.List(), null),
                    List.of(new Field("item", F_NULLABLE_INT64, null)))));
    private static final byte[] FULL_SCHEMA_IPC =
            farm.query.vgi.internal.SchemaUtil.serializeSchema(FULL_SCHEMA);

    @Override public String name() { return "nested_sequence"; }

    @Override public FunctionMetadata metadata() {
        return FunctionMetadata.describe("Generates a sequence with nested struct and list columns")
                .withPushdown(/*projection=*/true, /*filter=*/true, /*autoApply=*/true);
    }

    @Override public List<ArgSpec> argumentSpecs() {
        return List.of(
                new ArgSpec("count", 0, Schemas.INT64, /*isConst=*/true),
                ArgSpec.named("batch_size", Schemas.INT64, "1000"),
                ArgSpec.named("history_size", Schemas.INT64, "20"));
    }

    @Override public BindResponse onBind(TableBindParams params) {
        return BindResponse.forSchema(FULL_SCHEMA_IPC);
    }

    @Override public TableProducerState createProducer(TableInitParams params) {
        long count = ((Number) params.arguments().positionalAt(0)).longValue();
        long batchSize = params.arguments().namedLong("batch_size", 1000L);
        long historySize = params.arguments().namedLong("history_size", 20L);
        return new State(new BatchState(count, batchSize), historySize,
                new CachedSchema(params.outputSchema()),
                FilterApplier.from(params.pushdownFilters(), params.joinKeys()));
    }

    public static final class State extends TableProducerState {
        public BatchState batch;
        public long historySize;
        public CachedSchema outputSchema;
        public FilterApplier filters;

        public State() {}

        State(BatchState batch, long historySize, CachedSchema outputSchema, FilterApplier filters) {
            this.batch = batch;
            this.historySize = historySize;
            this.outputSchema = outputSchema;
            this.filters = filters;
        }

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            Schema s = outputSchema.get();
            farm.query.vgi.internal.BatchUtil.produceBatch(batch, s, filters, out, (root, n, start) -> {
                for (Field f : s.getFields()) {
                    FieldVector v = root.getVector(f.getName());
                    switch (f.getName()) {
                        case "n" -> {
                            BigIntVector b = (BigIntVector) v;
                            for (int i = 0; i < n; i++) b.setSafe(i, start + i);
                        }
                        case "metadata" -> fillMetadata((StructVector) v, n, start);
                        case "history" -> fillHistory((ListVector) v, n, start, historySize);
                        default -> {}
                    }
                }
            });
        }

        private static void fillMetadata(StructVector struct, int n, long start) {
            BigIntVector idx = (BigIntVector) struct.getChildByOrdinal(0);
            VarCharVector label = (VarCharVector) struct.getChildByOrdinal(1);
            for (int i = 0; i < n; i++) {
                long row = start + i;
                idx.setSafe(i, row);
                label.setSafe(i, new Text("row_" + row));
                struct.setIndexDefined(i);
            }
            struct.setValueCount(n);
        }

        private static void fillHistory(ListVector list, int n, long start, long historySize) {
            BigIntVector inner = (BigIntVector) list.getDataVector();
            org.apache.arrow.vector.complex.impl.UnionListWriter writer = list.getWriter();
            for (int i = 0; i < n; i++) {
                long row = start + i;
                long histStart = Math.max(0, row - historySize + 1);
                writer.setPosition(i);
                writer.startList();
                for (long j = histStart; j <= row; j++) {
                    writer.bigInt().writeBigInt(j);
                }
                writer.endList();
            }
            // Caller will set value count via setRowCount on the parent VSR;
            // explicitly bump the list vector too so its offsets buffer is sized.
            list.setValueCount(n);
        }
    }
}
