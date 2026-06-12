// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.buffering;

import farm.query.vgi.storage.BoundStorage;
import farm.query.vgi.table.TableInitParams;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.TransferPair;

import java.util.ArrayList;
import java.util.List;

/**
 * Base producer for the Source phase of a buffering function. Holds the
 * {@code finalize_state_id} + storage view and reuses {@link TableProducerState}
 * for the (already projection-narrowed) {@link #outputSchema} and the pushdown
 * {@link #filters}. Subclasses implement {@link #produceTick} and call
 * {@link #emitProjected} to ship a buffered batch — the base narrows it to the
 * projected columns (by name) and applies pushdown filters, exactly as the
 * C++ {@code _FilteringOutputCollector} does for the canonical worker.
 */
public abstract class BufferingFinalizeProducer extends TableProducerState {

    /** Read view over the batches buffered during the Sink phase. */
    protected final BoundStorage storage;
    /** State id selecting which combined partition this producer drains. */
    protected final byte[] finalizeStateId;

    /**
     * Captures the storage view + {@code finalize_state_id} and forwards the
     * init params (output schema, pushdown filters) to {@link TableProducerState}.
     *
     * @param params the finalize-phase parameters for this output stream.
     */
    protected BufferingFinalizeProducer(TableBufferingFinalizeParams params) {
        super(params.initParams());
        this.storage = params.storage();
        this.finalizeStateId = params.finalizeStateId();
    }

    /**
     * Narrows {@code full} to {@link #outputSchema} (by field name), applies
     * pushdown filters, and emits the result. The emitted root's vectors are
     * transferred out of {@code full}, so the caller still owns and must close
     * {@code full}.
     *
     * @param full the buffered batch holding (at least) the projected columns.
     * @param out the collector to emit the narrowed-and-filtered batch into.
     */
    protected void emitProjected(VectorSchemaRoot full, OutputCollector out) {
        VectorSchemaRoot narrowed = (outputSchema == null)
                ? transferAll(full)
                : narrowByName(full);
        if (filters != null) narrowed = filters.apply(narrowed);
        out.emit(narrowed);
    }

    private VectorSchemaRoot transferAll(VectorSchemaRoot full) {
        int rows = full.getRowCount();
        List<FieldVector> out = new ArrayList<>();
        for (FieldVector v : full.getFieldVectors()) {
            TransferPair tp = v.getTransferPair(Allocators.root());
            tp.transfer();
            out.add((FieldVector) tp.getTo());
        }
        VectorSchemaRoot r = new VectorSchemaRoot(out);
        r.setRowCount(rows);
        return r;
    }

    private VectorSchemaRoot narrowByName(VectorSchemaRoot full) {
        int rows = full.getRowCount();
        List<FieldVector> out = new ArrayList<>();
        for (Field f : outputSchema.getFields()) {
            FieldVector src = (FieldVector) full.getVector(f.getName());
            if (src == null) {
                throw new IllegalStateException(
                        "buffering finalize: projected column '" + f.getName()
                        + "' missing from buffered batch " + full.getSchema());
            }
            TransferPair tp = src.getTransferPair(Allocators.root());
            tp.transfer();
            out.add((FieldVector) tp.getTo());
        }
        VectorSchemaRoot r = new VectorSchemaRoot(out);
        r.setRowCount(rows);
        return r;
    }
}
