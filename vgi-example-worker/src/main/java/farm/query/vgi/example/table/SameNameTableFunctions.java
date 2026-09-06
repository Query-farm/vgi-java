// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.table;

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

/** Same-named scan functions backing same-named declarative tables in two schemas. */
public final class SameNameTableFunctions {

    private SameNameTableFunctions() {}

    /** Deliberately colliding scan-function name. */
    public static final String FUNCTION_NAME = "test_same_name_table_scan";
    /** Deliberately colliding declarative table name. */
    public static final String TABLE_NAME = "test_same_name_table";
    /** Shared one-column output schema. */
    public static final Schema OUTPUT = Schemas.of(Schemas.nullable("tag", Schemas.UTF8));

    abstract static class Tagging extends SimpleTableFunction {
        abstract String owningSchema();

        @Override protected Schema outputSchema() { return OUTPUT; }

        @Override public TableProducerState createProducer(TableInitParams params) {
            return new State(owningSchema());
        }
    }

    /** One-shot schema-tagged producer. */
    public static final class State extends TableProducerState {
        /** Value identifying the function's owning schema. */
        public String tag;
        /** Whether the row has already been emitted. */
        public boolean done;

        /** Required for continuation-state deserialization. */
        public State() {}

        State(String tag) { this.tag = tag; }

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            if (done) { out.finish(); return; }
            done = true;
            BatchUtil.emit(OUTPUT, 1, out, (root, rows, ignored) ->
                    ((VarCharVector) root.getVector("tag")).setSafe(0, new Text(tag)));
        }
    }

    /** Main-schema implementation. */
    public static final class MainSchema extends Tagging {
        private static final FunctionSpec SPEC = FunctionSpec.builder(FUNCTION_NAME)
                .metadata(FunctionMetadata.describe(
                                "Schema-disambiguation probe; the main-schema table producer")
                        .withCategories("generator", "testing")
                        .withExamples(List.of(new FunctionExample(
                                "SELECT * FROM example.main.test_same_name_table",
                                "One row tagged 'main'", null))))
                .build();

        @Override public FunctionSpec spec() { return SPEC; }
        @Override String owningSchema() { return "main"; }
    }

    /** Data-schema implementation. */
    public static final class DataSchema extends Tagging {
        private static final FunctionSpec SPEC = FunctionSpec.builder(FUNCTION_NAME)
                .metadata(FunctionMetadata.describe(
                                "Schema-disambiguation probe; the data-schema table producer")
                        .withCategories("generator", "testing")
                        .withExamples(List.of(new FunctionExample(
                                "SELECT * FROM example.data.test_same_name_table",
                                "One row tagged 'data'", null))))
                .build();

        @Override public FunctionSpec spec() { return SPEC; }
        @Override String owningSchema() { return "data"; }
    }
}
