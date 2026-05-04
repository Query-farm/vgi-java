// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.internal;

import farm.query.vgi.aggregate.AggregateFunction;
import farm.query.vgi.scalar.ScalarFunction;
import farm.query.vgi.table.TableFunction;
import farm.query.vgi.tableinout.TableInOutFunction;

import java.util.Map;

/**
 * Process-local registry of registered functions. Populated once by
 * {@link VgiServiceImpl} during construction, queried by {@link
 * farm.query.vgirpc.StreamState} subclasses at process-time so they can hold
 * just the function {@code name} (a String — easily serialised) instead of
 * the function reference (which is not serialisable).
 *
 * <p>Stateless code-path lookup, intentionally — under HTTP transport the
 * request that performs the {@code init} may run on a different worker than
 * the one that resumes via {@code /exchange}, but every worker is started
 * from the same registration block so the lookup is identical on each.</p>
 */
public final class ServiceLocator {

    private static volatile ServiceLocator current;

    private final Map<String, ScalarFunction> scalars;
    private final Map<String, TableFunction> tables;
    private final Map<String, TableInOutFunction> tableInOuts;
    private final Map<String, AggregateFunction<?>> aggregates;

    public ServiceLocator(Map<String, ScalarFunction> scalars,
                            Map<String, TableFunction> tables,
                            Map<String, TableInOutFunction> tableInOuts,
                            Map<String, AggregateFunction<?>> aggregates) {
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

    public ScalarFunction scalar(String name) {
        ScalarFunction f = scalars.get(name);
        if (f == null) throw new IllegalArgumentException("Unknown scalar function: " + name);
        return f;
    }

    public TableFunction table(String name) {
        TableFunction f = tables.get(name);
        if (f == null) throw new IllegalArgumentException("Unknown table function: " + name);
        return f;
    }

    public TableInOutFunction tableInOut(String name) {
        TableInOutFunction f = tableInOuts.get(name);
        if (f == null) throw new IllegalArgumentException("Unknown table-in-out function: " + name);
        return f;
    }

    public AggregateFunction<?> aggregate(String name) {
        AggregateFunction<?> f = aggregates.get(name);
        if (f == null) throw new IllegalArgumentException("Unknown aggregate function: " + name);
        return f;
    }
}
