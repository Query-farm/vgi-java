// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.internal;

import farm.query.vgi.scalar.ScalarFunction;

import java.util.List;
import java.util.Map;

/**
 * Process-local registry of registered functions. Populated once by
 * {@link VgiServiceImpl} during construction, queried by {@link
 * farm.query.vgirpc.StreamState} subclasses at process-time so they can hold
 * just the function {@code name} (a String — easily serialised) instead of
 * the function reference (which is not serialisable).
 */
public final class ServiceLocator {

    private static volatile ServiceLocator current;

    private final Map<String, List<ScalarFunction>> scalars;

    public ServiceLocator(Map<String, List<ScalarFunction>> scalars) {
        this.scalars = scalars;
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

    public ScalarFunction scalarAt(String name, int idx) {
        List<ScalarFunction> variants = scalars.get(name);
        if (variants == null || variants.isEmpty()) {
            throw new IllegalArgumentException("Unknown scalar function: " + name);
        }
        if (idx < 0 || idx >= variants.size()) return variants.get(0);
        return variants.get(idx);
    }

    public int scalarIndexOf(String name, ScalarFunction fn) {
        List<ScalarFunction> variants = scalars.get(name);
        if (variants == null) return 0;
        for (int i = 0; i < variants.size(); i++) if (variants.get(i) == fn) return i;
        return 0;
    }
}
