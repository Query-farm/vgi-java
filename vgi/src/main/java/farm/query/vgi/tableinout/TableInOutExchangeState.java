// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.tableinout;

import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.ExchangeState;
import farm.query.vgirpc.OutputCollector;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Base for table-in-out exchange states: each input batch produces zero or
 * one output batch via {@link #process}. Subclasses override
 * {@link #onInputBatch} which receives the {@link AnnotatedBatch} input root
 * and the {@link OutputCollector} to emit on.
 *
 * <p><strong>Continued output.</strong> One input batch may be answered with
 * several output batches. {@link #onInputBatch} emits the first with
 * {@link #withMoreOutput withMoreOutput(true, metadata)}, which puts
 * {@code vgi.status: HAVE_MORE_OUTPUT} on the reply; the client then asks for
 * each next batch with a continuation tick (a zero-row input batch carrying
 * {@code vgi.status: CONTINUE}), which this class answers by calling
 * {@link #resume}. The last batch is emitted without "more". Every batch's
 * {@code parent_rows} index the input batch {@code onInputBatch} received.
 *
 * <p>{@code resume} gets no input, so this object's fields must hold where to
 * resume. Over HTTP they reach the next turn only inside the stream's state
 * token, so keep them small: positions to resume from, never buffered output.
 * A function whose exchange continues its output declares that exchange class
 * through {@link TableInOutFunction#continuationState()}; the worker checks the
 * declaration when it starts.
 *
 * <p>The protocol is strict: while output is pending a tick is the only valid
 * input, a tick is valid only then, and cache control ({@code vgi.cache.*}) may
 * only ride an input's first batch, never one answering a tick. Mirrors
 * vgi-python's {@code emit(more=True)} / {@code resume()}.
 */
public abstract class TableInOutExchangeState extends ExchangeState {

    /** Batch metadata key carrying the continued-output status. */
    public static final String STATUS_KEY = "vgi.status";
    /** {@link #STATUS_KEY} value on a reply that has more output for the same input. */
    public static final String HAVE_MORE_OUTPUT = "HAVE_MORE_OUTPUT";
    /** {@link #STATUS_KEY} value on a continuation tick, asking for the next reply. */
    public static final String CONTINUE = "CONTINUE";

    // Whether the last reply said HAVE_MORE_OUTPUT, so the next input must be a
    // tick. Not transient: over HTTP the next turn learns it from the state token.
    private boolean morePending;

    /** Sole constructor; per-execution state lives in the subclass. */
    protected TableInOutExchangeState() {}

    /**
     * Final dispatch hook from the RPC layer: routes a real input batch to
     * {@link #onInputBatch} and a continuation tick to {@link #resume},
     * enforcing the continued-output rules around both.
     *
     * @param input the annotated input batch (root plus per-batch metadata).
     * @param out the collector to emit output batches on.
     * @param ctx the call context (client logging, cancellation, etc.).
     */
    @Override
    public final void exchange(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
        boolean tick = CONTINUE.equals(input.customMetadata().get(STATUS_KEY));
        String name = getClass().getSimpleName();
        if (tick && !morePending) {
            throw new IllegalStateException(name + ": continuation tick received, but no output is pending");
        }
        if (tick && input.root().getRowCount() != 0) {
            throw new IllegalStateException(name + ": a continuation tick must have no rows, got "
                    + input.root().getRowCount());
        }
        if (!tick && morePending) {
            throw new IllegalStateException(
                    name + ": input received while output is pending; expected a continuation tick");
        }
        if (tick) resume(out, ctx);
        else onInputBatch(input, out, ctx);

        Map<String, String> md = emittedMetadata(out);
        boolean more = saysMore(md);
        if (more && !overridesResume(getClass())) {
            throw refuse(out, name + ": emitted more output but does not implement resume()");
        }
        if (tick && md != null && md.keySet().stream().anyMatch(k -> k.startsWith("vgi.cache."))) {
            throw refuse(out, name + ": cache control is only valid on an input's first reply, "
                    + "not on one answering a continuation tick");
        }
        morePending = more;
    }

    /**
     * Release a turn's emitted batches, which the RPC layer drops unsent when the
     * turn fails, and build the failure for a reply the protocol forbids.
     *
     * @param out the failing turn's collector.
     * @param message the failure message.
     * @return the exception to throw.
     */
    public static IllegalStateException refuse(OutputCollector out, String message) {
        for (OutputCollector.Entry e : out.entries()) e.root().close();
        return new IllegalStateException(message);
    }

    /**
     * Processes a single input batch, emitting zero or one output batch.
     *
     * @param input the annotated input batch (root plus per-batch metadata).
     * @param out the collector to emit output batches on; {@code emit} takes
     *     ownership of the emitted root.
     * @param ctx the call context (client logging, cancellation, etc.).
     */
    public abstract void onInputBatch(AnnotatedBatch input, OutputCollector out, CallContext ctx);

    /**
     * Emits the next batch of output for the last input batch, answering a
     * continuation tick. Called only after the previous batch (from
     * {@link #onInputBatch} or an earlier {@code resume}) was emitted with
     * {@link #withMoreOutput withMoreOutput(true, ...)}. Like
     * {@code onInputBatch} it must emit exactly one batch, with "more" unless it
     * is the last, and without cache control.
     *
     * @param out the collector to emit the next batch on.
     * @param ctx the call context (client logging, cancellation, etc.).
     */
    public void resume(OutputCollector out, CallContext ctx) {
        throw new IllegalStateException(getClass().getSimpleName()
                + " emitted more output but does not implement resume()");
    }

    /**
     * Mark an emit's metadata as one of several replies for the same input
     * batch when {@code more} is true; {@code false} returns {@code metadata}
     * unchanged, so the last reply can pass its own condition:
     *
     * <pre>
     *   out.emit(root, withMoreOutput(row &lt; counts.length,
     *           RowTransformFunction.parentRows(parents, n, null)));
     * </pre>
     *
     * Valid only on the batch a table-in-out exchange emits from
     * {@link #onInputBatch} or {@link #resume}; a table function's producer
     * that says more is refused.
     *
     * @param more whether more output follows for the same input batch.
     * @param metadata metadata to merge into, or {@code null}.
     * @return the merged metadata map ({@code metadata} itself when {@code more} is false).
     */
    public static Map<String, String> withMoreOutput(boolean more, Map<String, String> metadata) {
        if (!more) return metadata;
        Map<String, String> merged = metadata == null ? new LinkedHashMap<>() : new LinkedHashMap<>(metadata);
        merged.put(STATUS_KEY, HAVE_MORE_OUTPUT);
        return merged;
    }

    /**
     * Whether emit metadata says {@code HAVE_MORE_OUTPUT}.
     *
     * @param metadata a batch's custom metadata, or {@code null}.
     * @return {@code true} when the batch has more output for the same input.
     */
    public static boolean saysMore(Map<String, String> metadata) {
        return metadata != null && HAVE_MORE_OUTPUT.equals(metadata.get(STATUS_KEY));
    }

    /**
     * The custom metadata of the data batch emitted on {@code out} this turn.
     *
     * @param out a stream turn's collector.
     * @return the data batch's metadata, or {@code null} when none was emitted or it has none.
     */
    public static Map<String, String> emittedMetadata(OutputCollector out) {
        for (OutputCollector.Entry e : out.entries()) {
            if (e.isData()) return e.customMetadata();
        }
        return null;
    }

    /**
     * Whether an exchange class implements {@link #resume}.
     *
     * @param cls the exchange class.
     * @return {@code true} when {@code cls} or a superclass below this one overrides {@code resume}.
     */
    public static boolean overridesResume(Class<? extends TableInOutExchangeState> cls) {
        try {
            return cls.getMethod("resume", OutputCollector.class, CallContext.class).getDeclaringClass()
                    != TableInOutExchangeState.class;
        } catch (NoSuchMethodException e) {
            throw new AssertionError(e);
        }
    }
}
