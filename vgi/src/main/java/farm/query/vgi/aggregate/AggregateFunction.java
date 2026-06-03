// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.aggregate;

import farm.query.vgi.function.FunctionDescriptor;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;


/**
 * A VGI aggregate function. State is per-group and accumulates across
 * {@code update} calls; {@code combine} merges partial states from parallel
 * workers; {@code finalize} emits the result row per group.
 *
 * <p>{@code <S>} is the per-group state type. Two {@link #combine combine} calls
 * produce a third {@code S}; the framework persists the bytes between calls.
 *
 * <p>Aggregates produce exactly one output column. {@link #outputSchema} is
 * expected to be a single-field schema; the framework resolves that single
 * {@link FieldVector} once per finalize batch and passes it into
 * {@link #finalize} and {@link #finalizeEmpty} — implementations cast it to the
 * concrete vector type they wrote in {@code outputSchema}.
 *
 * <p>Mirrors {@code vgi.AggregateFunction} in vgi-go.
 *
 * @param <S> the per-group accumulator state type
 */
public interface AggregateFunction<S> extends FunctionDescriptor {

    /**
     * Output type for the finalized result (catalog enumeration default).
     *
     * @return a single-field schema describing the one output column
     */
    Schema outputSchema();

    /**
     * Resolve the output schema for a specific {@code aggregate_bind} call.
     * Default: ignore input shape, return {@link #outputSchema()}. ANY-typed
     * aggregates override to derive output type from {@code inputSchema}.
     *
     * @param inputSchema the bound input schema for this call
     * @return the resolved single-field output schema
     */
    default Schema bindOutputSchema(Schema inputSchema) {
        return outputSchema();
    }

    /**
     * Variant that also receives the bind-time {@link
     * farm.query.vgi.function.Arguments}; lets aggregates validate const
     * params (e.g. percentile {@code p} in {@code [0, 1]}) and reject NULL /
     * NaN / Inf inputs with structured errors. Default delegates.
     *
     * @param inputSchema the bound input schema for this call
     * @param args the bind-time const arguments
     * @return the resolved single-field output schema
     */
    default Schema bindOutputSchema(Schema inputSchema, farm.query.vgi.function.Arguments args) {
        return bindOutputSchema(inputSchema);
    }

    /**
     * Build a fresh per-group state.
     *
     * @return a new, empty accumulator for one group
     */
    S newState();

    /**
     * Update each group's state with a slice of {@code input}. {@code groupIds}
     * is a parallel int64 column; row {@code i} of {@code input} belongs to
     * group {@code groupIds[i]}.
     *
     * @param states per-group state map, keyed by group id (mutated in place)
     * @param groupIds group id for each input row, parallel to {@code input}
     * @param input the batch of input rows to fold into the states
     */
    void update(java.util.Map<Long, S> states, long[] groupIds, VectorSchemaRoot input);

    /**
     * Variant that also receives the bind-time {@link farm.query.vgi.function.Arguments}
     * (for aggregates with const params like {@code vgi_percentile(value, p)}).
     * Default delegates to the no-args overload.
     *
     * @param states per-group state map, keyed by group id (mutated in place)
     * @param groupIds group id for each input row, parallel to {@code input}
     * @param input the batch of input rows to fold into the states
     * @param args the bind-time const arguments
     */
    default void update(java.util.Map<Long, S> states, long[] groupIds, VectorSchemaRoot input,
                          farm.query.vgi.function.Arguments args) {
        update(states, groupIds, input);
    }

    /**
     * Variant of {@link #finalize(FieldVector, int, Object)} that receives
     * bind-time arguments. Default ignores them and delegates.
     *
     * @param result the resolved output vector to write into
     * @param rowIndex the output row to populate for this group
     * @param state the accumulated state for the group
     * @param args the bind-time const arguments
     */
    default void finalize(FieldVector result, int rowIndex, S state,
                            farm.query.vgi.function.Arguments args) {
        finalize(result, rowIndex, state);
    }

    /**
     * Merge {@code source} into {@code target} (in-place). Used when partial
     * results from multiple workers must be combined.
     *
     * @param target the accumulator to merge into (mutated in place)
     * @param source the partial state to fold into {@code target}
     */
    void combine(S target, S source);

    /**
     * Emit the finalized result for {@code state} into row {@code rowIndex} of
     * {@code result}.
     *
     * @param result the resolved output vector to write into
     * @param rowIndex the output row to populate for this group
     * @param state the accumulated state for the group
     */
    void finalize(FieldVector result, int rowIndex, S state);

    /**
     * Emit a row when no state was accumulated for the group (empty input).
     * Default writes NULL — SUM/AVG/etc. behaviour. {@code count}-style
     * aggregates override to emit 0 instead.
     *
     * @param result the resolved output vector to write into
     * @param rowIndex the output row to populate for the empty group
     */
    default void finalizeEmpty(FieldVector result, int rowIndex) {
        result.setNull(rowIndex);
    }

    /**
     * Encode state to bytes for the wire. Default: Java serialization.
     *
     * @param state the per-group state to encode
     * @return the serialized state bytes
     */
    default byte[] serializeState(S state) {
        try (java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
             java.io.ObjectOutputStream oos = new java.io.ObjectOutputStream(baos)) {
            oos.writeObject(state);
            return baos.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException("aggregate state serialize failed", e);
        }
    }

    /**
     * Decode state from bytes. Default: Java deserialization.
     *
     * @param bytes the serialized state bytes
     * @return the reconstructed per-group state
     */
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
