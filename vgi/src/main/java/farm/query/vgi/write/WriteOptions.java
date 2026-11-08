// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.write;

import farm.query.vgi.function.Arguments;
import farm.query.vgi.internal.BatchUtil;
import farm.query.vgi.internal.VectorScalarCodec;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.ArrayList;
import java.util.List;

/**
 * What DuckDB asked a write function for: the {@code write_options} named argument of its bind
 * (a serialized one-row batch), decoded.
 *
 * @param resultMode        the result shape to produce (and bind as the output schema,
 *                          {@link WriteResults#resultSchema})
 * @param onConflict        {@code throw} (the default), or the {@code INSERT ... ON CONFLICT}
 *                          action requested
 * @param onConflictColumns the conflict target columns, empty when none
 */
public record WriteOptions(WriteResultMode resultMode, String onConflict, List<String> onConflictColumns) {

    /** Name of the named argument DuckDB passes to every write function. */
    public static final String ARGUMENT_NAME = "write_options";

    /**
     * A plain {@code count} write with no conflict handling: what a missing argument means.
     *
     * @return the default options
     */
    public static WriteOptions defaults() {
        return new WriteOptions(WriteResultMode.COUNT, "throw", List.of());
    }

    /**
     * Decode the write options of a write function's call.
     *
     * @param arguments the bind's (or init's) arguments
     * @return the options; {@link #defaults()} when the call carries none
     * @throws IllegalArgumentException when the argument does not decode or names an unknown mode
     */
    public static WriteOptions from(Arguments arguments) {
        Object raw = arguments == null ? null : arguments.named().get(ARGUMENT_NAME);
        if (!(raw instanceof byte[] bytes) || bytes.length == 0) return defaults();
        try (VectorSchemaRoot root = BatchUtil.readSingleBatch(bytes, Allocators.root())) {
            if (root == null || root.getRowCount() == 0) return defaults();
            WriteResultMode mode = WriteResultMode.COUNT;
            String onConflict = "throw";
            List<String> columns = List.of();
            FieldVector m = root.getVector("result_mode");
            if (m != null && !m.isNull(0)) mode = WriteResultMode.fromWire(String.valueOf(VectorScalarCodec.read(m, 0)));
            FieldVector c = root.getVector("on_conflict");
            if (c != null && !c.isNull(0)) onConflict = String.valueOf(VectorScalarCodec.read(c, 0));
            FieldVector cc = root.getVector("on_conflict_columns");
            if (cc != null && !cc.isNull(0) && VectorScalarCodec.read(cc, 0) instanceof List<?> list) {
                List<String> out = new ArrayList<>(list.size());
                for (Object o : list) out.add(String.valueOf(o));
                columns = List.copyOf(out);
            }
            return new WriteOptions(mode, onConflict, columns);
        } catch (RuntimeException e) {
            if (e instanceof IllegalArgumentException iae) throw iae;
            throw new IllegalArgumentException("cannot decode write_options: " + e.getMessage(), e);
        }
    }
}
