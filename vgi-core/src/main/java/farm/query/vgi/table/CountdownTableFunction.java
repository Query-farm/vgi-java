// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.table;

import farm.query.vgi.catalog.ColumnStatistics;
import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.ParameterExtractor;
import farm.query.vgi.types.Schemas;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.List;

/**
 * Base for sequence-like table functions that emit a known number of rows in
 * fixed-size batches. Mirrors vgi-python's
 * {@code TableFunctionGenerator + @bind_fixed_schema + @cardinality_from_count}
 * pattern.
 *
 * <p>Subclasses declare:
 * <ul>
 *   <li>{@link #outputSchema()} — the fixed output schema, used by both
 *       {@link #onBind} and (typically) by the producer state.</li>
 *   <li>{@link #createProducer(TableInitParams)} — construct the per-execution
 *       state. The standard countdown args are accessible via the
 *       {@link TableInitParams#arguments()} as {@code positional[0] = count},
 *       {@code named["batch_size"]}, plus any {@link #extraArgs() extras}.</li>
 *   <li>Override {@link #extraArgs()} to add named-only args beyond the
 *       built-in {@code count + batch_size} pair.</li>
 * </ul>
 *
 * <p>The base class provides {@code argumentSpecs}, {@code onBind}, and
 * {@code cardinality} so subclasses don't repeat that scaffolding.</p>
 */
public abstract class CountdownTableFunction extends SimpleTableFunction {

    /** Default {@code batch_size} when the caller doesn't pass one. */
    protected long defaultBatchSize() { return 1000L; }

    /** Named-only arguments declared in addition to {@code count} + {@code batch_size}. */
    protected List<ArgSpec> extraArgs() { return List.of(); }

    @Override
    public List<ArgSpec> argumentSpecs() {
        List<ArgSpec> all = new ArrayList<>();
        all.add(new ArgSpec("count", 0, Schemas.INT64, /*isConst=*/true));
        all.add(ArgSpec.named("batch_size", Schemas.INT64, String.valueOf(defaultBatchSize())));
        all.addAll(extraArgs());
        return all;
    }

    @Override
    public long cardinality(TableBindParams p) {
        Object c = p.arguments().positionalAt(0);
        return c instanceof Number n ? n.longValue() : -1L;
    }

    /**
     * Default statistics for the canonical countdown pattern: a single-column
     * output schema where the column is BIGINT or DOUBLE and the values are
     * {@code 0, increment, 2*increment, ...}. Mirrors vgi-python's
     * {@code @_cardinality_from_count} decorator.
     *
     * <p>Returns {@code null} (no stats) when:
     * <ul>
     *   <li>{@code positional[0]} is absent or not a {@link Number},
     *   <li>{@code count <= 0},
     *   <li>the output schema isn't single-column INT64 or FLOAT64.
     * </ul>
     * Subclasses with multi-column or non-arithmetic schemas should override
     * {@code statistics} themselves.
     */
    @Override
    public List<ColumnStatistics> statistics(TableBindParams params) {
        Object countObj = params.arguments().positional().isEmpty()
                ? null : params.arguments().positionalAt(0);
        if (!(countObj instanceof Number cn)) return null;
        long count = cn.longValue();
        if (count <= 0) return null;
        Schema s = outputSchema();
        if (s.getFields().size() != 1) return null;
        Field f = s.getFields().get(0);
        ArrowType t = f.getType();
        ParameterExtractor p = ParameterExtractor.of(params.arguments());
        if (t instanceof ArrowType.Int i && i.getBitWidth() == 64 && i.getIsSigned()) {
            long increment = p.named("increment").asLong().orElse(1L);
            long max = (count - 1) * increment;
            return List.of(ColumnStatistics.ofInt64(f.getName(), 0L, max, false, count));
        }
        if (t instanceof ArrowType.FloatingPoint fp
                && fp.getPrecision() == FloatingPointPrecision.DOUBLE) {
            double increment = p.named("increment").asDouble().orElse(1.0);
            double max = (count - 1) * increment;
            return List.of(ColumnStatistics.ofFloat64(f.getName(), 0.0, max, false, count));
        }
        return null;
    }
}
