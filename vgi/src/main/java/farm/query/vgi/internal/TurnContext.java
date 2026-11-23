// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import farm.query.vgirpc.CallContext;

/**
 * The call context of the stream turn this thread is running, for framework values that are
 * built or read where no context is passed: the {@link farm.query.vgi.pushdown.FilterApplier}
 * snapshot, carried as a {@code StateRef} (protocol 3.4 {@code init: StateRef}), is created in
 * the init handler and resolved on later turns from inside a producer's {@code produceTick}.
 *
 * <p>Framework-internal; set by {@code VgiServiceImpl.init} around the producer factories and by
 * {@link farm.query.vgi.table.TableProducerState} around each tick.
 */
public final class TurnContext {

    private static final ThreadLocal<CallContext> CURRENT = new ThreadLocal<>();

    private TurnContext() {}

    /** @return the current turn's context, or {@code null} outside one */
    public static CallContext current() {
        return CURRENT.get();
    }

    /**
     * Make {@code ctx} the current turn's context until the returned scope closes.
     *
     * @param ctx the context ({@code null} clears it)
     * @return the scope restoring the previous context
     */
    public static Scope enter(CallContext ctx) {
        CallContext previous = CURRENT.get();
        if (ctx == null) CURRENT.remove();
        else CURRENT.set(ctx);
        return () -> {
            if (previous == null) CURRENT.remove();
            else CURRENT.set(previous);
        };
    }

    /** A scope that restores the previous context; never throws. */
    public interface Scope extends AutoCloseable {
        @Override
        void close();
    }
}
