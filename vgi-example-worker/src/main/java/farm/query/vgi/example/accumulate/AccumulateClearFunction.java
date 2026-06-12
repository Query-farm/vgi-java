// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.accumulate;

import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.internal.SchemaUtil;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.storage.BoundStorage;
import farm.query.vgi.table.TableBindParams;
import farm.query.vgi.table.TableFunction;
import farm.query.vgi.table.TableInitParams;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import java.nio.charset.StandardCharsets;

/**
 * {@code accumulate_clear(name)} — remove an accumulated collection (rows +
 * pinned schema + counter), freeing the name for re-accumulation with any
 * schema. Emits a single {@code (name, rows_cleared)} row. Mirrors
 * vgi-python's {@code AccumulateClearFunction}.
 */
public final class AccumulateClearFunction implements TableFunction {

    private static final Schema OUTPUT = Schemas.of(
            Schemas.nullable("name", Schemas.UTF8),
            Schemas.nullable("rows_cleared", Schemas.INT64));
    private static final byte[] OUTPUT_IPC = SchemaUtil.serializeSchema(OUTPUT);

    private static final FunctionSpec SPEC = FunctionSpec.builder("accumulate_clear")
            .metadata(FunctionMetadata.describe(
                    "Remove an accumulated collection by name; returns rows cleared")
                    .withCategories("stateful", "utility"))
            .constArg("name", Schemas.UTF8)
            .build();

    @Override public FunctionSpec spec() { return SPEC; }

    @Override
    public BindResponse onBind(TableBindParams params) {
        if (params.attachStorage() != null) {
            String name = params.arguments().positionalAt(0) instanceof String s ? s : "";
            AccumulateStore.validateName(name);
        }
        return BindResponse.forSchema(OUTPUT_IPC);
    }

    @Override public long cardinality(TableBindParams params) { return 1L; }

    @Override public long maxWorkers() { return 1L; }

    @Override
    public TableProducerState createProducer(TableInitParams params) {
        BoundStorage ps = params.storage().rescope(params.attachId());
        String name = params.arguments().positionalAt(0) instanceof String s ? s : "";
        return new TableProducerState() {
            private boolean done = false;

            @Override public void produceTick(OutputCollector out, CallContext ctx) {
                if (done) {
                    out.finish();
                    return;
                }
                long cleared = AccumulateStore.clearCollection(
                        ps, name.getBytes(StandardCharsets.UTF_8));
                VectorSchemaRoot root = VectorSchemaRoot.create(OUTPUT, params.allocator());
                VarCharVector nameVec = (VarCharVector) root.getVector("name");
                BigIntVector clearedVec = (BigIntVector) root.getVector("rows_cleared");
                nameVec.allocateNew(1);
                nameVec.setSafe(0, name.getBytes(StandardCharsets.UTF_8));
                nameVec.setValueCount(1);
                clearedVec.allocateNew(1);
                clearedVec.set(0, cleared);
                clearedVec.setValueCount(1);
                root.setRowCount(1);
                out.emit(root);
                done = true;
            }
        };
    }
}
