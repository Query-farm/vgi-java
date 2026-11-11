// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.write;

import farm.query.vgi.function.Arguments;
import farm.query.vgi.internal.BatchUtil;
import farm.query.vgi.internal.VectorScalarCodec;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.List;

/**
 * What DuckDB asked a write function for: the {@code write_options} named argument of its bind
 * (a serialized one-row batch), decoded.
 *
 * <p>Protocol 3.3 carries only {@code result_mode: utf8 not null} in that batch. The
 * {@code on_conflict} / {@code on_conflict_columns} fields of earlier revisions are gone: DuckDB's
 * binder rewrites every {@code INSERT ... ON CONFLICT} and {@code INSERT OR REPLACE} into a
 * {@code MERGE} before a catalog sees it, so they were always {@code "throw"} / empty. The
 * extension emulates those statements with one ordinary write call per MERGE action. The
 * {@link #onConflict()} / {@link #onConflictColumns()} accessors remain, deprecated, for source
 * compatibility.
 *
 * @param resultMode the result shape to produce (and bind as the output schema,
 *                   {@link WriteResults#resultSchema})
 */
public record WriteOptions(WriteResultMode resultMode) {

    /** Name of the named argument DuckDB passes to every write function. */
    public static final String ARGUMENT_NAME = "write_options";

    /** A null mode reads as {@link WriteResultMode#COUNT}. */
    public WriteOptions {
        resultMode = resultMode == null ? WriteResultMode.COUNT : resultMode;
    }

    /**
     * Compatibility constructor of protocol 3.2 and earlier. The conflict arguments are ignored:
     * the write options no longer carry them.
     *
     * @param resultMode        the result mode
     * @param onConflict        ignored
     * @param onConflictColumns ignored
     * @deprecated protocol 3.3 removed {@code on_conflict}; use {@link #WriteOptions(WriteResultMode)}
     */
    @Deprecated
    public WriteOptions(WriteResultMode resultMode, String onConflict, List<String> onConflictColumns) {
        this(resultMode);
    }

    /**
     * Always {@code "throw"}: the conflict action is no longer part of the write options.
     *
     * @return {@code "throw"}
     * @deprecated protocol 3.3 removed {@code on_conflict}; {@code INSERT ... ON CONFLICT} reaches
     *     the worker as the ordinary insert / update calls of a MERGE
     */
    @Deprecated
    public String onConflict() {
        return "throw";
    }

    /**
     * Always empty: the conflict target is no longer part of the write options.
     *
     * @return an empty list
     * @deprecated protocol 3.3 removed {@code on_conflict_columns}
     */
    @Deprecated
    public List<String> onConflictColumns() {
        return List.of();
    }

    /**
     * A plain {@code count} write: what a missing argument means.
     *
     * @return the default options
     */
    public static WriteOptions defaults() {
        return new WriteOptions(WriteResultMode.COUNT);
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
            FieldVector m = root.getVector("result_mode");
            if (m == null || m.isNull(0)) return defaults();
            return new WriteOptions(WriteResultMode.fromWire(String.valueOf(VectorScalarCodec.read(m, 0))));
        } catch (RuntimeException e) {
            if (e instanceof IllegalArgumentException iae) throw iae;
            throw new IllegalArgumentException("cannot decode write_options: " + e.getMessage(), e);
        }
    }

}
