// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.aggregate;

import farm.query.vgi.Secrets;
import farm.query.vgi.aggregate.AggregateFunction;
import farm.query.vgi.function.Arguments;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.protocol.FunctionRequiredSecret;
import farm.query.vgi.types.ScalarHelpers;
import farm.query.vgi.types.Schemas;
import farm.query.vgi.types.TypeRules;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * {@code secret_typed_sum(value) -> ANY} — sums an integer column, choosing the
 * result type from a statically-resolved secret.
 *
 * <p>Declares a {@code vgi_example} required secret; the C++ extension
 * pre-resolves it and delivers it on {@code AggregateBindRequest.secrets}.
 * {@code bindOutputSchema} reads the secret's {@code use_ssl} field: DOUBLE when
 * true, BIGINT otherwise. The sum itself is computed normally in
 * update/combine; the secret only shapes the output type. Mirrors vgi-python's
 * {@code SecretTypedSumFunction}.
 */
public final class SecretTypedSumFunction implements AggregateFunction<SecretTypedSumFunction.State> {

    public static final class State implements Serializable {
        private static final long serialVersionUID = 1L;
        long total;
    }

    // Catalog-enumeration placeholder: Null-typed "result" tagged as ANY so
    // duckdb_functions() reports return_type ANY; the per-query type is decided
    // at bind from the secret.
    private static final Schema OUTPUT_SCHEMA = new Schema(List.of(
            new Field("result", new FieldType(true, new ArrowType.Null(),
                    null, Map.of("vgi_type", "any")), null)));

    private static final FunctionSpec SPEC = FunctionSpec.builder("secret_typed_sum")
            .description("Sum an integer column; the result type is chosen from a secret")
            .arg("value", Schemas.INT64)
            .build();

    @Override public FunctionSpec spec() { return SPEC; }
    @Override public Schema outputSchema() { return OUTPUT_SCHEMA; }

    @Override
    public List<FunctionRequiredSecret> requiredSecrets() {
        return List.of(new FunctionRequiredSecret("vgi_example", null, null));
    }

    @Override
    public Schema bindOutputSchema(Schema inputSchema, Arguments args, Secrets secrets) {
        boolean useSsl = secrets.ofType("vgi_example").stream()
                .findFirst()
                .map(m -> "true".equalsIgnoreCase(m.get("use_ssl")))
                .orElse(false);
        ArrowType resultType = useSsl ? Schemas.FLOAT64 : Schemas.INT64;
        return new Schema(List.of(Schemas.nullable("result", resultType)));
    }

    @Override public State newState() { return new State(); }

    @Override
    public void update(Map<Long, State> states, long[] groupIds, VectorSchemaRoot input) {
        FieldVector v = input.getFieldVectors().get(0);
        int rows = input.getRowCount();
        for (int i = 0; i < rows; i++) {
            if (v.isNull(i)) continue;
            State s = states.computeIfAbsent(groupIds[i], k -> new State());
            s.total += ScalarHelpers.toLong(v, i);
        }
    }

    @Override
    public void combine(State target, State source) { target.total += source.total; }

    @Override
    public void finalize(FieldVector result, int rowIndex, State state) {
        ArrowType t = result.getField().getType();
        if (TypeRules.isFloating(t)) {
            ((Float8Vector) result).setSafe(rowIndex, (double) state.total);
        } else {
            ((BigIntVector) result).setSafe(rowIndex, state.total);
        }
    }
}
