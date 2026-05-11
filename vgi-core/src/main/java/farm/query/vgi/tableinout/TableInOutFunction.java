// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.tableinout;

import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.FunctionDescriptor;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.protocol.BindResponse;

import java.util.List;

/**
 * A VGI table-in-out function: receives input batches and emits output batches.
 * One output batch per input batch (echo, filter, transform); state can
 * accumulate across calls. An optional finalize phase emits buffered data
 * after the last input.
 *
 * <p>Mirrors {@code vgi.TableInOutFunction} in vgi-go.
 */
public interface TableInOutFunction extends FunctionDescriptor {

    String name();

    FunctionMetadata metadata();

    List<ArgSpec> argumentSpecs();

    BindResponse onBind(TableInOutBindParams params);

    TableInOutExchangeState createExchange(TableInOutInitParams params);

    /**
     * Optional: emit buffered batches at the end of input. Called once after
     * all input batches have been delivered; the {@code state} is the same
     * exchange-state instance returned by {@link #createExchange} (the
     * framework keeps it alive across the INPUT→FINALIZE phase boundary by
     * keying on {@code execution_id}). Default: no buffered output.
     */
    default java.util.List<org.apache.arrow.vector.VectorSchemaRoot> finalizeBatches(
            TableInOutExchangeState state, TableInOutInitParams params) {
        return java.util.List.of();
    }

    /**
     * Whether this function actually emits buffered output during FINALIZE.
     * Mirrors vgi-go's {@code HasFinalize} metadata flag — DuckDB skips the
     * FINALIZE-phase RPC entirely when {@code false}.
     */
    default boolean hasFinalize() { return false; }
}
