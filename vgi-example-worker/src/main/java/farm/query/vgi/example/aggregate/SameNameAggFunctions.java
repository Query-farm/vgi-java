// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.aggregate;

import farm.query.vgi.aggregate.AggregateFunction;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.protocol.FunctionExample;
import farm.query.vgi.types.Schemas;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * {@code test_same_name_agg(value BIGINT) -> VARCHAR} — the SAME aggregate name
 * registered in two different schemas of the {@code example} catalog.
 *
 * <p>Aggregates are the widest surface of the schema-disambiguation family.
 * Every aggregate RPC — {@code aggregate_update} / {@code _combine} /
 * {@code _finalize} / {@code _destructor} — is unary and stateless: there is no
 * bound connection carrying the binding, so each one re-resolves the function by
 * name, and before protocol 1.2.0 none of those requests carried a schema (not
 * even {@code aggregate_bind}). A name declared in two schemas therefore
 * resolved to whichever the by-name lookup found first.
 *
 * <p>The tag is stamped at finalize while accumulation happens in update, so a
 * partial mis-route — bind to one implementation, update or finalize to the
 * other — is visible too. Mirrors vgi-python's {@code SameNameMainAgg} /
 * {@code SameNameDataAgg}; driven by
 * {@code test/sql/integration/aggregate/same_name_schemas.test}.
 */
public final class SameNameAggFunctions {

    private SameNameAggFunctions() {}

    /** The colliding registered name — deliberately identical in both schemas. */
    public static final String NAME = "test_same_name_agg";

    /** Running total for one group. */
    public static final class State implements Serializable {
        private static final long serialVersionUID = 1L;
        long total;
    }

    private static final Schema OUTPUT_SCHEMA = Schemas.of(Schemas.nullable("result", Schemas.UTF8));

    /** Shared body; each subclass supplies the schema it is declared in. */
    abstract static class Summing implements AggregateFunction<State> {

        /** Schema this implementation is registered into — the tag it stamps. */
        abstract String owningSchema();

        @Override public Schema outputSchema() { return OUTPUT_SCHEMA; }

        @Override public State newState() { return new State(); }

        @Override
        public void update(Map<Long, State> states, long[] groupIds, VectorSchemaRoot input) {
            FieldVector v = input.getFieldVectors().get(0);
            if (!(v instanceof BigIntVector b)) return;
            int rows = input.getRowCount();
            for (int i = 0; i < rows; i++) {
                if (b.isNull(i)) continue;
                states.computeIfAbsent(groupIds[i], k -> new State()).total += b.get(i);
            }
        }

        @Override
        public void combine(State target, State source) {
            target.total += source.total;
        }

        /** Tag the group's total with the owning schema. */
        @Override
        public void finalize(FieldVector result, int rowIndex, State state) {
            ((VarCharVector) result).setSafe(rowIndex, new Text(owningSchema() + ":" + state.total));
        }

        /** A group with no accumulated rows still reports its owning schema. */
        @Override
        public void finalizeEmpty(FieldVector result, int rowIndex) {
            ((VarCharVector) result).setSafe(rowIndex, new Text(owningSchema() + ":0"));
        }
    }

    /** {@code test_same_name_agg} as declared in the {@code main} schema. */
    public static final class MainSchema extends Summing {

        private static final FunctionSpec SPEC = FunctionSpec.builder(NAME)
                .metadata(FunctionMetadata.describe("Schema-disambiguation probe; the main-schema aggregate")
                        .withCategories("test")
                        .withExamples(List.of(new FunctionExample(
                                "SELECT example.main.test_same_name_agg(n) FROM range(3) t(n)",
                                "Returns 'main:3'", null))))
                .arg("value", Schemas.INT64)
                .build();

        @Override public FunctionSpec spec() { return SPEC; }

        @Override String owningSchema() { return "main"; }
    }

    /** {@code test_same_name_agg} as declared in the {@code data} schema. */
    public static final class DataSchema extends Summing {

        private static final FunctionSpec SPEC = FunctionSpec.builder(NAME)
                .metadata(FunctionMetadata.describe("Schema-disambiguation probe; the data-schema aggregate")
                        .withCategories("test")
                        .withExamples(List.of(new FunctionExample(
                                "SELECT example.data.test_same_name_agg(n) FROM range(3) t(n)",
                                "Returns 'data:3'", null))))
                .arg("value", Schemas.INT64)
                .build();

        @Override public FunctionSpec spec() { return SPEC; }

        @Override String owningSchema() { return "data"; }
    }
}
