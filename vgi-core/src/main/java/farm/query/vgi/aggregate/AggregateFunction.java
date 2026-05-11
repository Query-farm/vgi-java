// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.aggregate;

import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.FunctionDescriptor;
import farm.query.vgi.function.FunctionMetadata;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

/**
 * A VGI aggregate function. State is per-group and accumulates across
 * {@code update} calls; {@code combine} merges partial states from parallel
 * workers; {@code finalize} emits the result row per group.
 *
 * <p>{@code <S>} is the per-group state type. Two {@link #combine combine} calls
 * produce a third {@code S}; the framework persists the bytes between calls.
 *
 * <p>Mirrors {@code vgi.AggregateFunction} in vgi-go.
 */
public interface AggregateFunction<S> extends FunctionDescriptor {

    String name();

    FunctionMetadata metadata();

    List<ArgSpec> argumentSpecs();

    /** Output type for the finalized result (catalog enumeration default). */
    Schema outputSchema();

    /**
     * Resolve the output schema for a specific {@code aggregate_bind} call.
     * Default: ignore input shape, return {@link #outputSchema()}. ANY-typed
     * aggregates override to derive output type from {@code inputSchema}.
     */
    default Schema bindOutputSchema(Schema inputSchema) {
        return outputSchema();
    }

    /**
     * Variant that also receives the bind-time {@link
     * farm.query.vgi.function.Arguments}; lets aggregates validate const
     * params (e.g. percentile {@code p} in {@code [0, 1]}) and reject NULL /
     * NaN / Inf inputs with structured errors. Default delegates.
     */
    default Schema bindOutputSchema(Schema inputSchema, farm.query.vgi.function.Arguments args) {
        return bindOutputSchema(inputSchema);
    }

    /** Build a fresh per-group state. */
    S newState();

    /**
     * Update each group's state with a slice of {@code input}. {@code groupIds}
     * is a parallel int64 column; row {@code i} of {@code input} belongs to
     * group {@code groupIds[i]}.
     */
    void update(java.util.Map<Long, S> states, long[] groupIds, VectorSchemaRoot input);

    /**
     * Variant that also receives the bind-time {@link farm.query.vgi.function.Arguments}
     * (for aggregates with const params like {@code vgi_percentile(value, p)}).
     * Default delegates to the no-args overload.
     */
    default void update(java.util.Map<Long, S> states, long[] groupIds, VectorSchemaRoot input,
                          farm.query.vgi.function.Arguments args) {
        update(states, groupIds, input);
    }

    /** Variant that receives bind-time arguments. Default ignores them. */
    default void finalize(VectorSchemaRoot output, int rowIndex, S state,
                            farm.query.vgi.function.Arguments args) {
        finalize(output, rowIndex, state);
    }

    /**
     * Merge {@code source} into {@code target} (in-place). Used when partial
     * results from multiple workers must be combined.
     */
    void combine(S target, S source);

    /** Emit the finalized result for {@code state} into row {@code i} of {@code output}. */
    void finalize(VectorSchemaRoot output, int rowIndex, S state);

    /**
     * Emit a row when no state was accumulated for the group (empty input).
     * Default writes NULL — SUM/AVG/etc. behaviour. {@code count}-style
     * aggregates override to emit 0 instead.
     */
    default void finalizeEmpty(VectorSchemaRoot output, int rowIndex) {
        for (org.apache.arrow.vector.FieldVector v : output.getFieldVectors()) {
            v.setNull(rowIndex);
        }
    }

    /** Encode state to bytes for the wire. Default: Java serialization. */
    default byte[] serializeState(S state) {
        try (java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
             java.io.ObjectOutputStream oos = new java.io.ObjectOutputStream(baos)) {
            oos.writeObject(state);
            return baos.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException("aggregate state serialize failed", e);
        }
    }

    /** Decode state from bytes. Default: Java deserialization. */
    @SuppressWarnings("unchecked")
    default S deserializeState(byte[] bytes) {
        try (java.io.ByteArrayInputStream bais = new java.io.ByteArrayInputStream(bytes);
             java.io.ObjectInputStream ois = new java.io.ObjectInputStream(bais)) {
            return (S) ois.readObject();
        } catch (Exception e) {
            throw new RuntimeException("aggregate state deserialize failed", e);
        }
    }
}
