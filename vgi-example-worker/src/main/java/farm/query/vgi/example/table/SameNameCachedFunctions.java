// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.table;

import farm.query.vgi.cache.CacheControl;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.internal.BatchUtil;
import farm.query.vgi.protocol.FunctionExample;
import farm.query.vgi.table.SimpleTableFunction;
import farm.query.vgi.table.TableInitParams;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.util.List;

/**
 * {@code test_same_name_cached() -> tag VARCHAR} — the RESULT-CACHE member of the
 * schema-disambiguation family (see {@code scalar/SameNameFunctions},
 * {@code tableinout/SameNameTransformFunctions},
 * {@code aggregate/SameNameAggFunctions}). Those probe <em>dispatch</em>; this one
 * probes the <em>result cache</em>, a distinct layer.
 *
 * <p>A one-row producer table function that advertises {@code vgi.cache.ttl} and
 * is registered in BOTH the {@code main} and {@code data} schemas of the
 * {@code example} catalog. Each schema's implementation emits a single row tagged
 * with its own schema name.
 *
 * <p>The result cache keyed on catalog + auth + function name with no schema
 * dimension, so the two implementations produced byte-identical cache keys and one
 * schema's memoized rows cross-served the other — the caching-layer twin of the
 * {@code (schema, name)} dispatch bug. The tag makes a cross-serve visible:
 * {@code example.data.test_same_name_cached()} would return a {@code main} row.
 * With the schema in the key, each schema gets its own entry (so
 * {@code vgi_result_cache()} holds two rows for the one function name) and returns
 * its own tag. Mirrors vgi-python's {@code SameNameMainCached} /
 * {@code SameNameDataCached}; driven by {@code cache/same_name_schemas.test}.
 */
public final class SameNameCachedFunctions {

    private SameNameCachedFunctions() {}

    /** The colliding registered name — deliberately identical in both schemas. */
    public static final String NAME = "test_same_name_cached";

    /** Single VARCHAR column {@code tag}. */
    private static final Schema OUTPUT = Schemas.of(Schemas.nullable("tag", Schemas.UTF8));

    /** Long enough that the TTL never lapses mid-test. */
    static final int TTL_SECONDS = 300;

    /** Shared body; each subclass supplies the schema it is declared in. */
    abstract static class Tagging extends SimpleTableFunction {

        /** Schema this implementation is registered into — the tag it stamps. */
        abstract String owningSchema();

        @Override protected Schema outputSchema() { return OUTPUT; }

        @Override public TableProducerState createProducer(TableInitParams p) {
            return new State(owningSchema());
        }

        /** One-shot emit latch for the single schema-tagged row. */
        public static final class State extends TableProducerState {
            /** The owning schema name, stamped on the emitted row. */
            public String tag;
            /** Whether the single row has been emitted. */
            public boolean done;

            /** Required no-arg constructor for state deserialization. */
            public State() {}

            State(String tag) { this.tag = tag; }

            @Override public void produceTick(OutputCollector out, CallContext ctx) {
                if (done) { out.finish(); return; }
                done = true;
                String value = tag;
                BatchUtil.emit(OUTPUT, 1, out, CacheControl.ttl(TTL_SECONDS).toMetadata(),
                        (root, rows, ignored) ->
                                ((VarCharVector) root.getVector("tag")).setSafe(0, new Text(value)));
            }
        }
    }

    /** {@code test_same_name_cached} as declared in the {@code main} schema. */
    public static final class MainSchema extends Tagging {

        private static final FunctionSpec SPEC = FunctionSpec.builder(NAME)
                .metadata(FunctionMetadata.describe(
                                "Schema-disambiguation probe; the main-schema cacheable producer")
                        .withCategories("generator", "cache", "testing")
                        .withExamples(List.of(new FunctionExample(
                                "SELECT * FROM example.main.test_same_name_cached()",
                                "One cacheable row tagged 'main'", null))))
                .build();

        @Override public FunctionSpec spec() { return SPEC; }

        @Override String owningSchema() { return "main"; }
    }

    /** {@code test_same_name_cached} as declared in the {@code data} schema. */
    public static final class DataSchema extends Tagging {

        private static final FunctionSpec SPEC = FunctionSpec.builder(NAME)
                .metadata(FunctionMetadata.describe(
                                "Schema-disambiguation probe; the data-schema cacheable producer")
                        .withCategories("generator", "cache", "testing")
                        .withExamples(List.of(new FunctionExample(
                                "SELECT * FROM example.data.test_same_name_cached()",
                                "One cacheable row tagged 'data'", null))))
                .build();

        @Override public FunctionSpec spec() { return SPEC; }

        @Override String owningSchema() { return "data"; }
    }
}
