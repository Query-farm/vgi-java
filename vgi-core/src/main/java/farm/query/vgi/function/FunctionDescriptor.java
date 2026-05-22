// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.function;

import java.util.List;

/**
 * Common surface for scalar, table, table-in-out, and aggregate function
 * implementations: a name, metadata, and an argument-spec list. Pulled out
 * so catalog-functions plumbing (FunctionInfo construction) can treat all
 * four kinds uniformly.
 *
 * <p>A function declares this constant data once via {@link #spec()}; the
 * {@code name()}/{@code metadata()}/{@code argumentSpecs()} accessors default to
 * reading it. Implement {@code spec()} (returning a {@code static final}
 * {@link FunctionSpec} for the common case) and you get all three for free.
 * Functions whose metadata is genuinely computed may instead override the three
 * accessors directly — the defaults below only apply when they are not
 * overridden.</p>
 */
public interface FunctionDescriptor {

    /**
     * The function's constant descriptor data. Default throws — a function must
     * either provide a {@code spec()} or override {@link #name()},
     * {@link #metadata()}, and {@link #argumentSpecs()} directly.
     */
    default FunctionSpec spec() {
        throw new UnsupportedOperationException(
                getClass().getName() + " must override spec() or the "
                        + "name()/metadata()/argumentSpecs() trio");
    }

    default String name() { return spec().name(); }

    default FunctionMetadata metadata() { return spec().metadata(); }

    default List<ArgSpec> argumentSpecs() { return spec().argumentSpecs(); }
}
