// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.example.buffering;

import farm.query.vgi.buffering.BufferingFinalizeProducer;
import farm.query.vgi.storage.FunctionStorage;
import farm.query.vgi.buffering.TableBufferingFinalizeParams;
import farm.query.vgi.buffering.TableBufferingProcessParams;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * {@code large_state} — appends ~1 MB per input batch to per-execution state,
 * then emits a single row carrying the total payload size during finalize.
 * Exercises IPC chunking on the response path. Mirrors vgi-python
 * {@code LargeStateFunction}.
 */
public final class LargeStateFunction extends AbstractBufferAndDrain {

    private static final byte[] NS_LARGE = "large".getBytes(StandardCharsets.UTF_8);
    private static final int CHUNK = 1024 * 1024;

    private static final FunctionSpec SPEC = FunctionSpec.builder("large_state")
            .metadata(FunctionMetadata.describe("Buffers ~1 MB per input batch into state (IPC test)")
                    .withCategories("test", "memory"))
            .table("data")
            .build();

    @Override public FunctionSpec spec() { return SPEC; }

    @Override public byte[] process(VectorSchemaRoot batch, TableBufferingProcessParams params) {
        params.storage().stateAppend(NS_LARGE, KEY, new byte[CHUNK]);
        return params.executionId();
    }

    // combine() inherited: returns [execution_id].

    @Override public TableProducerState createFinalizeProducer(TableBufferingFinalizeParams params) {
        return new TotalSizeProducer(params);
    }

    /** Emits one row whose every column is the total buffered payload size. */
    private static final class TotalSizeProducer extends BufferingFinalizeProducer {
        private boolean emitted = false;

        TotalSizeProducer(TableBufferingFinalizeParams params) { super(params); }

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            if (emitted) { out.finish(); return; }
            long total = 0;
            for (FunctionStorage.LogEntry e : storage.stateLogScan(NS_LARGE, KEY, -1, Integer.MAX_VALUE)) {
                total += e.value().length;
            }
            List<FieldVector> vectors = new ArrayList<>();
            for (Field f : outputSchema.getFields()) {
                BigIntVector v = (BigIntVector) f.createVector(Allocators.root());
                v.allocateNew();
                v.setSafe(0, total);
                v.setValueCount(1);
                vectors.add(v);
            }
            VectorSchemaRoot root = new VectorSchemaRoot(vectors);
            root.setRowCount(1);
            out.emit(root);
            emitted = true;
        }
    }
}
