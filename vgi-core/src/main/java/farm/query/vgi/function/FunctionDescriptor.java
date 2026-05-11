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
 * <p>The four function interfaces extend this without adding redeclarations
 * of the inherited methods.</p>
 */
public interface FunctionDescriptor {
    String name();
    FunctionMetadata metadata();
    List<ArgSpec> argumentSpecs();
}
