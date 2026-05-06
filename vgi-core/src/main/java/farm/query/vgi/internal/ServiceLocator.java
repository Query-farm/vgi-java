// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.internal;

import farm.query.vgi.aggregate.AggregateFunction;
import farm.query.vgi.scalar.ScalarFunction;
import farm.query.vgi.table.TableFunction;
import farm.query.vgi.tableinout.TableInOutFunction;

import java.util.List;
import java.util.Map;

/**
 * Process-local registry of registered functions. Populated once by
 * {@link VgiServiceImpl} during construction, queried by {@link
 * farm.query.vgirpc.StreamState} subclasses at process-time so they can hold
 * just the function {@code name} (a String — easily serialised) instead of
 * the function reference (which is not serialisable).
 *
 * <p>Each name maps to a {@code List} of overload variants. Stateless lookup
 * by (name, arity) — at exchange time we re-pick the variant matching the
 * recorded arg count.</p>
 */
public final class ServiceLocator {

    private static volatile ServiceLocator current;

    private final Map<String, List<ScalarFunction>> scalars;
    private final Map<String, List<TableFunction>> tables;
    private final Map<String, List<TableInOutFunction>> tableInOuts;
    private final Map<String, List<AggregateFunction<?>>> aggregates;

    public ServiceLocator(Map<String, List<ScalarFunction>> scalars,
                            Map<String, List<TableFunction>> tables,
                            Map<String, List<TableInOutFunction>> tableInOuts,
                            Map<String, List<AggregateFunction<?>>> aggregates) {
        this.scalars = scalars;
        this.tables = tables;
        this.tableInOuts = tableInOuts;
        this.aggregates = aggregates;
    }

    public static void setCurrent(ServiceLocator s) { current = s; }

    public static ServiceLocator current() {
        ServiceLocator s = current;
        if (s == null) {
            throw new IllegalStateException(
                    "ServiceLocator not initialised — VgiServiceImpl must be constructed first");
        }
        return s;
    }

    public ScalarFunction scalar(String name, int argCount) {
        return pick(scalars.get(name), argCount, "scalar", name);
    }

    public TableFunction table(String name, int argCount) {
        return pick(tables.get(name), argCount, "table", name);
    }

    public TableInOutFunction tableInOut(String name, int argCount) {
        return pick(tableInOuts.get(name), argCount, "table-in-out", name);
    }

    public AggregateFunction<?> aggregate(String name, int argCount) {
        return pick(aggregates.get(name), argCount, "aggregate", name);
    }

    /**
     * Variant lookup by stable index. Used by stream states that captured the
     * picked variant at bind time and need to re-resolve to the same one
     * across exchange ticks even when overload pickers cannot disambiguate by
     * arity alone.
     */
    public ScalarFunction scalarAt(String name, int idx) { return atIdx(scalars.get(name), idx, "scalar", name); }
    public TableFunction tableAt(String name, int idx) { return atIdx(tables.get(name), idx, "table", name); }
    public TableInOutFunction tableInOutAt(String name, int idx) { return atIdx(tableInOuts.get(name), idx, "table-in-out", name); }

    public int scalarIndexOf(String name, ScalarFunction fn) { return idxOf(scalars.get(name), fn); }
    public int tableIndexOf(String name, TableFunction fn) { return idxOf(tables.get(name), fn); }
    public int tableInOutIndexOf(String name, TableInOutFunction fn) { return idxOf(tableInOuts.get(name), fn); }

    private static <T> T atIdx(List<T> variants, int idx, String kind, String name) {
        if (variants == null || variants.isEmpty()) {
            throw new IllegalArgumentException("Unknown " + kind + " function: " + name);
        }
        if (idx < 0 || idx >= variants.size()) return variants.get(0);
        return variants.get(idx);
    }

    private static <T> int idxOf(List<T> variants, T fn) {
        if (variants == null) return 0;
        for (int i = 0; i < variants.size(); i++) if (variants.get(i) == fn) return i;
        return 0;
    }

    /** Convenience for callers that don't track arity (single-variant fns). */
    public ScalarFunction scalar(String name) { return pick(scalars.get(name), -1, "scalar", name); }
    public TableFunction table(String name) { return pick(tables.get(name), -1, "table", name); }
    public TableInOutFunction tableInOut(String name) { return pick(tableInOuts.get(name), -1, "table-in-out", name); }
    public AggregateFunction<?> aggregate(String name) { return pick(aggregates.get(name), -1, "aggregate", name); }

    private static <T> T pick(List<T> variants, int argCount, String kind, String name) {
        if (variants == null || variants.isEmpty()) {
            throw new IllegalArgumentException("Unknown " + kind + " function: " + name);
        }
        if (variants.size() == 1 || argCount < 0) return variants.get(0);
        for (T v : variants) {
            int n = countArgs(v);
            if (n == argCount) return v;
        }
        // Fall back to first variant if no exact match — DuckDB has already
        // resolved overloading at the catalog layer, so a mismatch here is
        // exotic; first variant is usually the "main" implementation.
        return variants.get(0);
    }

    private static int countArgs(Object fn) {
        if (fn instanceof ScalarFunction f) return f.argumentSpecs().size();
        if (fn instanceof TableFunction f) return f.argumentSpecs().size();
        if (fn instanceof TableInOutFunction f) return f.argumentSpecs().size();
        if (fn instanceof AggregateFunction<?> f) return f.argumentSpecs().size();
        return -1;
    }
}
