// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.buffering;

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

    protected final BufferingStorage storage;
    protected final byte[] finalizeStateId;

    protected BufferingFinalizeProducer(TableBufferingFinalizeParams params) {
        super(params.initParams());
        this.storage = params.storage();
        this.finalizeStateId = params.finalizeStateId();
    }

    /** Narrow {@code full} to {@link #outputSchema} (by field name), apply
     *  pushdown filters, and emit. {@code full} is closed by the framework
     *  after emit (ownership transfer); its borrowed vectors are transferred. */
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
