// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.scalar;

import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.FunctionDescriptor;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.protocol.BindResponse;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.List;

/**
 * A scalar VGI function: 1:1 row mapping. Output row count must equal input row count.
 *
 * <p>Mirrors {@code vgi.ScalarFunction} in vgi-go.
 */
public interface ScalarFunction extends FunctionDescriptor {

    String name();

    FunctionMetadata metadata();

    List<ArgSpec> argumentSpecs();

    /** Resolve output schema from bound argument types. */
    BindResponse onBind(ScalarBindParams params);

    /**
     * Transform an input batch into an output batch with the same row count and the
     * schema returned by {@link #onBind}. The caller owns the input root; the function
     * returns a freshly allocated output root that the caller will close.
     */
    VectorSchemaRoot process(ScalarProcessParams params, VectorSchemaRoot input, BufferAllocator alloc);
}
