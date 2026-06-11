// Copyright 2026 Query Farm LLC - https://query.farm

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

    /**
     * Creates a locator over a fixed registration snapshot.
     *
     * @param scalars registered scalar functions, keyed by name, each value an overload list
     */
    public ServiceLocator(Map<String, List<ScalarFunction>> scalars) {
        this.scalars = scalars;
    }

    /**
     * Install the process-wide locator. Called once during {@link VgiServiceImpl} construction.
     *
     * @param s the locator to make current
     */
    public static void setCurrent(ServiceLocator s) { current = s; }

    /**
     * The process-wide locator installed by {@link #setCurrent}.
     *
     * @return the installed process-wide locator
     * @throws IllegalStateException if no locator has been installed yet
     */
    public static ServiceLocator current() {
        ServiceLocator s = current;
        if (s == null) {
            throw new IllegalStateException(
                    "ServiceLocator not initialised — VgiServiceImpl must be constructed first");
        }
        return s;
    }

    /**
     * Look up a scalar overload by name and variant index.
     *
     * @param name the registered scalar name
     * @param idx the overload index; out-of-range falls back to variant 0
     * @return the resolved function
     * @throws IllegalArgumentException if {@code name} is not registered
     */
    public ScalarFunction scalarAt(String name, int idx) {
        List<ScalarFunction> variants = scalars.get(name);
        if (variants == null || variants.isEmpty()) {
            throw new IllegalArgumentException("Unknown scalar function: " + name);
        }
        if (idx < 0 || idx >= variants.size()) return variants.get(0);
        return variants.get(idx);
    }

    /**
     * Reverse of {@link #scalarAt}: find the variant index of a specific function instance.
     *
     * @param name the registered scalar name
     * @param fn the function instance to locate (identity comparison)
     * @return the variant index, or {@code 0} when not found
     */
    public int scalarIndexOf(String name, ScalarFunction fn) {
        List<ScalarFunction> variants = scalars.get(name);
        if (variants == null) return 0;
        for (int i = 0; i < variants.size(); i++) if (variants.get(i) == fn) return i;
        return 0;
    }
}
