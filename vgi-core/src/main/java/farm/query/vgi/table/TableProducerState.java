// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.table;

import farm.query.vgi.pushdown.FilterApplier;
import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.ProducerState;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Base class for table-function producer states. Subclasses implement
 * {@link #produceTick(OutputCollector, CallContext)} which is called once per
 * tick; each call must either emit one data batch via {@code out.emit(...)}
 * or call {@code out.finish()} to signal end-of-stream.
 *
 * <p>The framework also delivers per-tick {@code custom_metadata} (dynamic
 * filter updates, cancel signals) via the {@link AnnotatedBatch} input.
 * Producers that need this information override {@link
 * #produceTick(AnnotatedBatch, OutputCollector, CallContext)} instead;
 * the no-input variant is the default for fixtures that don't care.
 *
 * <p>Subclasses that need filter pushdown or the projected output schema
 * should call the {@link #TableProducerState(TableInitParams)} constructor;
 * {@link #filters} and {@link #outputSchema} are then populated once and
 * reused across every tick.
 *
 * <p><b>State serialisation note.</b> Producer state is process-local for the
 * stdio and AF_UNIX transports — the framework never serialises it. The HTTP
 * transport <em>does</em> persist state across requests via state tokens, but
 * does so through {@code StateSerializer} (Jackson JSON) by default, or
 * {@link farm.query.vgirpc.PortableStreamState} when the subclass takes over
 * encoding. In neither case does the framework use Java's
 * {@link java.io.Serializable} machinery, so declaring
 * {@code implements Serializable} or carrying a {@code serialVersionUID} on a
 * subclass has no effect and should be removed from older fixtures. Hold
 * decoded forms ({@link org.apache.arrow.vector.types.pojo.Schema}, the
 * pre-built {@link farm.query.vgi.pushdown.FilterApplier}) on plain fields
 * rather than the raw IPC bytes that the wire delivered.
 */
public abstract class TableProducerState extends ProducerState {

    /** Pre-decoded pushdown filter, ready to call {@code apply(root)} on each
     *  emitted batch. {@code null} when the state was created via the no-arg
     *  constructor. */
    protected final FilterApplier filters;

    /** Output schema after projection pushdown is applied (or the full
     *  declared schema when no projection was pushed). {@code null} when the
     *  state was created via the no-arg constructor. */
    protected final Schema outputSchema;

    protected TableProducerState() {
        this.filters = null;
        this.outputSchema = null;
    }

    protected TableProducerState(TableInitParams params) {
        this.filters = params.filters();
        this.outputSchema = params.projectedOutputSchema();
    }

    @Override
    public final void produce(OutputCollector out, CallContext ctx) {
        produceTick(out, ctx);
    }

    @Override
    public final void produce(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
        produceTick(input, out, ctx);
    }

    /** User-supplied per-tick generator. Emit one batch or call {@code out.finish()}. */
    public abstract void produceTick(OutputCollector out, CallContext ctx);

    /**
     * Per-tick generator with access to the framework's tick {@code
     * custom_metadata}. Default delegates to the no-input overload so
     * existing fixtures keep working unchanged.
     */
    public void produceTick(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
        produceTick(out, ctx);
    }
}
