// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.buffering;

import farm.query.vgi.buffering.BufferingFinalizeProducer;
import farm.query.vgi.buffering.TableBufferingCombineParams;
import farm.query.vgi.buffering.TableBufferingFinalizeParams;
import farm.query.vgi.buffering.TableBufferingFunction;
import farm.query.vgi.buffering.TableBufferingProcessParams;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.function.ParameterExtractor;
import farm.query.vgi.internal.SchemaUtil;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.tableinout.TableInOutBindParams;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

/**
 * {@code buffer_emit_wide(rows, data TABLE)} — a buffering function whose Source
 * phase ({@code finalize}) emits ONE batch of {@code rows} rows. Unlike
 * {@code buffer_input} (which echoes input batches, each already capped at
 * DuckDB's standard vector size) this emits a single, arbitrarily large output
 * batch, exercising whether the buffering Source path supports output batches
 * larger than the standard vector size (2048 rows) the same way a regular
 * {@code TableFunctionGenerator} does.
 *
 * <p>Backs {@code test/sql/integration/table_in_out/table_buffering_large_batch.test}.
 */
public final class BufferEmitWideFunction implements TableBufferingFunction {

    private static final Schema OUTPUT = Schemas.of(Schemas.nullable("n", Schemas.INT64));

    private static final FunctionSpec SPEC = FunctionSpec.builder("buffer_emit_wide")
            .metadata(FunctionMetadata.describe("Emit a single finalize batch of N rows (vector-size repro)")
                    .withCategories("test", "buffer"))
            .constArg("rows", Schemas.INT64)
            .table("data")
            .build();

    @Override public FunctionSpec spec() { return SPEC; }

    @Override public BindResponse onBind(TableInOutBindParams params) {
        return BindResponse.forSchema(SchemaUtil.serializeSchema(OUTPUT));
    }

    @Override public byte[] process(VectorSchemaRoot batch, TableBufferingProcessParams params) {
        return params.executionId();  // sink absorbs; input content ignored
    }

    @Override public List<byte[]> combine(List<byte[]> stateIds, TableBufferingCombineParams params) {
        return List.of(params.executionId());
    }

    @Override public TableProducerState createFinalizeProducer(TableBufferingFinalizeParams params) {
        long rows = ParameterExtractor.of(params.initParams().arguments())
                .positional(0, "rows").asLong().required();
        return new WideProducer(params, rows);
    }

    private static final class WideProducer extends BufferingFinalizeProducer {
        private long rows;
        private boolean emitted = false;

        private WideProducer() {}

        WideProducer(TableBufferingFinalizeParams params, long rows) {
            super(params);
            this.rows = rows;
        }

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            if (emitted) { out.finish(); return; }
            emitted = true;
            Schema schema = outputSchema != null ? outputSchema : OUTPUT;
            VectorSchemaRoot root = VectorSchemaRoot.create(schema, Allocators.root());
            root.allocateNew();
            BigIntVector v = (BigIntVector) root.getVector("n");
            int n = (int) rows;
            for (int i = 0; i < n; i++) v.setSafe(i, i);
            root.setRowCount(n);
            out.emit(root);
        }
    }
}
