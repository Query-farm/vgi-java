// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.table;

import farm.query.vgi.function.ParameterExtractor;
import farm.query.vgi.internal.BatchUtil;
import farm.query.vgi.internal.SchemaUtil;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.table.BatchState;
import farm.query.vgi.table.TableBindParams;
import farm.query.vgi.table.TableFunction;
import farm.query.vgi.table.TableInitParams;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.types.CachedSchema;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.util.List;

/**
 * {@code projected_data(count)} — emits 4 columns ({@code id}, {@code name},
 * {@code value}, {@code extra}). Opts into projection pushdown so DuckDB only
 * receives the columns the SELECT actually references.
 */
public final class ProjectedDataFunction implements TableFunction {

    private static final Schema FULL_SCHEMA = new Schema(List.of(
            Schemas.nullable("id", Schemas.INT64),
            Schemas.nullable("name", Schemas.UTF8),
            Schemas.nullable("value", Schemas.FLOAT64),
            Schemas.nullable("extra", Schemas.INT64)));
    private static final byte[] FULL_SCHEMA_IPC =
            SchemaUtil.serializeSchema(FULL_SCHEMA);

    private static final FunctionSpec SPEC = FunctionSpec.builder("projected_data")
            .metadata(FunctionMetadata.describe("Generates data with 4 columns, supporting projection pushdown")
                    .withPushdown(/*projection=*/true, /*filter=*/false, /*autoApply=*/false))
            .constArg("count", Schemas.INT64)
            .build();

    @Override public FunctionSpec spec() { return SPEC; }

    @Override public BindResponse onBind(TableBindParams params) {
        return BindResponse.forSchema(FULL_SCHEMA_IPC);
    }

    @Override public TableProducerState createProducer(TableInitParams params) {
        long count = ParameterExtractor.of(params.arguments())
                .positional(0, "count").asLong().required();
        return new State(new BatchState(count, 1000), new CachedSchema(params.outputSchema()));
    }

    public static final class State extends TableProducerState {
        public BatchState batch;
        public CachedSchema outputSchema;

        public State() {}

        State(BatchState batch, CachedSchema outputSchema) {
            this.batch = batch;
            this.outputSchema = outputSchema;
        }

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            Schema s = outputSchema.get();
            BatchUtil.produceBatch(batch, s, null, out, (root, n, start) -> {
                for (Field f : s.getFields()) {
                    FieldVector v = root.getVector(f.getName());
                    switch (f.getName()) {
                        case "id" -> {
                            BigIntVector b = (BigIntVector) v;
                            for (int i = 0; i < n; i++) b.setSafe(i, start + i);
                        }
                        case "name" -> {
                            VarCharVector vc = (VarCharVector) v;
                            for (int i = 0; i < n; i++) vc.setSafe(i, new Text("item_" + (start + i)));
                        }
                        case "value" -> {
                            Float8Vector f8 = (Float8Vector) v;
                            for (int i = 0; i < n; i++) f8.setSafe(i, (start + i) * 1.5);
                        }
                        case "extra" -> {
                            BigIntVector b = (BigIntVector) v;
                            for (int i = 0; i < n; i++) {
                                long x = start + i;
                                b.setSafe(i, x * x);
                            }
                        }
                        default -> {}
                    }
                }
            });
        }
    }
}
