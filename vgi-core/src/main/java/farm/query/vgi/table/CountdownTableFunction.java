// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.table;

import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.internal.SchemaUtil;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.types.Schemas;
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
public abstract class CountdownTableFunction implements TableFunction {

    /** Returns the fixed output schema. Called once at bind time. */
    protected abstract Schema outputSchema();

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
    public BindResponse onBind(TableBindParams params) {
        return BindResponse.forSchema(SchemaUtil.serializeSchema(outputSchema()));
    }

    @Override
    public long cardinality(TableBindParams p) {
        Object c = p.arguments().positionalAt(0);
        return c instanceof Number n ? n.longValue() : -1L;
    }
}
