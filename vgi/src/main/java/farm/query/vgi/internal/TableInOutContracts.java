// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import farm.query.vgi.tableinout.RowTransformFunction;
import farm.query.vgi.tableinout.TableInOutExchangeState;
import farm.query.vgi.tableinout.TableInOutFunction;
import farm.query.vgirpc.PortableStreamState;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

/**
 * Refuses a table-in-out registration the client cannot drive: continued output
 * ({@link TableInOutFunction#continuationState()}) without a state to resume
 * from or on a {@code max_workers = 1} function, and an
 * {@link RowTransformFunction#inputRowsPerCall()} that is not positive. Run
 * when the worker builds its service, so a bad registration fails at startup
 * rather than on the first query that calls it.
 */
public final class TableInOutContracts {

    private TableInOutContracts() {}

    /**
     * Check one registration.
     *
     * @param fn the table-in-out function.
     * @throws IllegalArgumentException when the client could not drive it.
     */
    public static void check(TableInOutFunction fn) {
        String name = fn.name();
        if (fn instanceof RowTransformFunction rt && rt.inputRowsPerCall() != null && rt.inputRowsPerCall() < 1) {
            throw new IllegalArgumentException(name + ": inputRowsPerCall must be a positive row count, got "
                    + rt.inputRowsPerCall());
        }
        Class<? extends TableInOutExchangeState> cls = fn.continuationState();
        if (cls == null) return;
        if (fn.maxWorkers() == 1) {
            throw new IllegalArgumentException(name + ": continued output is not supported with maxWorkers() = 1; "
                    + "the client runs such a function on one shared connection");
        }
        if (!TableInOutExchangeState.overridesResume(cls)) {
            throw new IllegalArgumentException(name + ": continuationState() " + cls.getName()
                    + " does not implement resume()");
        }
        if (PortableStreamState.class.isAssignableFrom(cls)) {
            throw new IllegalArgumentException(name + ": continuationState() " + cls.getName()
                    + " encodes its own state, so the pending-output flag cannot ride the HTTP state token");
        }
        try {
            cls.getDeclaredConstructor();
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException(name + ": continuationState() " + cls.getName()
                    + " needs a no-arg constructor to round-trip the HTTP state token");
        }
        if (!hasOwnState(cls)) {
            throw new IllegalArgumentException(name + ": resume() continues from its state alone, so "
                    + cls.getName() + " must hold the resume position in non-static, non-transient fields");
        }
    }

    /**
     * Check the exchange a function created against its declaration: a state
     * that can continue must be the declared {@code continuationState()}, or the
     * startup checks above never saw it.
     *
     * @param fn the function.
     * @param state the exchange {@code fn.createExchange} returned.
     * @throws IllegalStateException when the exchange implements {@code resume()} undeclared.
     */
    public static void checkExchange(TableInOutFunction fn, TableInOutExchangeState state) {
        if (!TableInOutExchangeState.overridesResume(state.getClass())) return;
        Class<? extends TableInOutExchangeState> declared = fn.continuationState();
        if (declared == null || !declared.isInstance(state)) {
            throw new IllegalStateException(fn.name() + ": exchange " + state.getClass().getName()
                    + " implements resume() but is not the function's continuationState()");
        }
    }

    private static boolean hasOwnState(Class<?> cls) {
        for (Class<?> c = cls; c != null && c != TableInOutExchangeState.class; c = c.getSuperclass()) {
            for (Field f : c.getDeclaredFields()) {
                int m = f.getModifiers();
                if (!Modifier.isStatic(m) && !Modifier.isTransient(m) && !f.isSynthetic()) return true;
            }
        }
        return false;
    }
}
