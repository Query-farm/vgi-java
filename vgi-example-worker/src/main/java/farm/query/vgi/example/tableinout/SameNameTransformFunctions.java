// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.tableinout;

import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.internal.SchemaUtil;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.protocol.FunctionExample;
import farm.query.vgi.tableinout.TableInOutBindParams;
import farm.query.vgi.tableinout.TableInOutExchangeState;
import farm.query.vgi.tableinout.TableInOutFunction;
import farm.query.vgi.tableinout.TableInOutInitParams;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.util.List;

/**
 * {@code test_same_name_transform(data TABLE) -> tag} — the SAME table-in-out
 * name registered in two different schemas of the {@code example} catalog.
 *
 * <p>The exchange-mode member of the schema-disambiguation family (the scalar is
 * {@code farm.query.vgi.example.scalar.SameNameFunctions}). It matters
 * separately because exchange-mode functions reach the worker through a
 * different bind call site: the extension's {@code VgiTableInOutBind} builds its
 * bind-time connection directly instead of going through
 * {@code AcquireAndBindConnection}, and originally never named the schema on
 * that request — so every exchange-mode bind arrived with no
 * {@code BindRequest.schema_name} and was unresolvable across two schemas. A
 * scalar fixture cannot catch that.
 *
 * <p>Each implementation tags its rows with its own schema, so a mis-routed bind
 * reads as the wrong tag rather than a plausible answer. Mirrors vgi-python's
 * {@code SameNameMainTransform} / {@code SameNameDataTransform}; driven by
 * {@code test/sql/integration/table_in_out/same_name_schemas.test}.
 */
public final class SameNameTransformFunctions {

    private SameNameTransformFunctions() {}

    /** The colliding registered name — deliberately identical in both schemas. */
    public static final String NAME = "test_same_name_transform";

    /** The single column every implementation in this family emits. */
    public static final Schema OUTPUT_TAG_SCHEMA = Schemas.of(Schemas.nullable("tag", Schemas.UTF8));

    /**
     * Render {@code <schema>:<value>} for every row of the first input column,
     * preserving nulls. Shared with the buffered sibling in
     * {@code farm.query.vgi.example.buffering}.
     *
     * @param schema the owning schema name to stamp
     * @param in the input batch whose first column supplies the values
     * @return a fresh single-column batch of tags; the caller owns it
     */
    public static VectorSchemaRoot tagRows(String schema, VectorSchemaRoot in) {
        FieldVector src = in.getFieldVectors().get(0);
        int rows = in.getRowCount();
        VectorSchemaRoot out = VectorSchemaRoot.create(OUTPUT_TAG_SCHEMA, Allocators.root());
        out.allocateNew();
        VarCharVector tags = (VarCharVector) out.getVector("tag");
        for (int i = 0; i < rows; i++) {
            if (src.isNull(i)) { tags.setNull(i); continue; }
            tags.setSafe(i, new Text(schema + ":" + src.getObject(i)));
        }
        out.setRowCount(rows);
        return out;
    }

    /** Shared body; each subclass supplies the schema it is declared in. */
    abstract static class Tagging implements TableInOutFunction {

        /** Schema this implementation is registered into — the tag it stamps. */
        abstract String owningSchema();

        @Override public String name() { return NAME; }

        @Override public List<ArgSpec> argumentSpecs() {
            return List.of(ArgSpec.table("data", 0));
        }

        /** One VARCHAR column regardless of the input schema. */
        @Override public BindResponse onBind(TableInOutBindParams params) {
            return BindResponse.forSchema(SchemaUtil.serializeSchema(OUTPUT_TAG_SCHEMA));
        }
    }

    /** {@code test_same_name_transform} as declared in the {@code main} schema. */
    public static final class MainSchema extends Tagging {

        @Override String owningSchema() { return "main"; }

        @Override public FunctionMetadata metadata() {
            return FunctionMetadata.describe("Schema-disambiguation probe; the main-schema table-in-out")
                    .withCategories("test")
                    .withExamples(List.of(new FunctionExample(
                            "SELECT * FROM example.main.test_same_name_transform((SELECT 1 AS n))",
                            "Returns 'main:1'", null)));
        }

        @Override public TableInOutExchangeState createExchange(TableInOutInitParams params) {
            return new MainState();
        }

        /** Named + no-arg for the HTTP state-token round-trip. */
        public static final class MainState extends TableInOutExchangeState {
            /** No-arg constructor for HTTP state-token deserialization. */
            public MainState() {}

            @Override public void onInputBatch(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
                out.emit(tagRows("main", input.root()));
            }
        }
    }

    /** {@code test_same_name_transform} as declared in the {@code data} schema. */
    public static final class DataSchema extends Tagging {

        @Override String owningSchema() { return "data"; }

        @Override public FunctionMetadata metadata() {
            return FunctionMetadata.describe("Schema-disambiguation probe; the data-schema table-in-out")
                    .withCategories("test")
                    .withExamples(List.of(new FunctionExample(
                            "SELECT * FROM example.data.test_same_name_transform((SELECT 1 AS n))",
                            "Returns 'data:1'", null)));
        }

        @Override public TableInOutExchangeState createExchange(TableInOutInitParams params) {
            return new DataState();
        }

        /** Named + no-arg for the HTTP state-token round-trip. */
        public static final class DataState extends TableInOutExchangeState {
            /** No-arg constructor for HTTP state-token deserialization. */
            public DataState() {}

            @Override public void onInputBatch(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
                out.emit(tagRows("data", input.root()));
            }
        }
    }
}
