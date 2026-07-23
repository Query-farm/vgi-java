// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.buffering;

import farm.query.vgi.buffering.TableBufferingProcessParams;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.internal.BatchUtil;
import farm.query.vgi.internal.SchemaUtil;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.protocol.FunctionExample;
import farm.query.vgi.tableinout.TableInOutBindParams;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.List;

import static farm.query.vgi.example.tableinout.SameNameTransformFunctions.OUTPUT_TAG_SCHEMA;
import static farm.query.vgi.example.tableinout.SameNameTransformFunctions.tagRows;

/**
 * {@code test_same_name_buffered(data TABLE) -> tag} — the SAME table-buffering
 * name registered in two different schemas of the {@code example} catalog.
 *
 * <p>The Sink+Source member of the schema-disambiguation family. It shares the
 * bind call site with {@code test_same_name_transform} but acquires its runtime
 * connections through the buffering operator's own {@code BuildAcquireParams},
 * and its {@code table_buffering_process} / {@code _combine} RPCs re-resolve the
 * function by name on every call — so it is independent coverage of protocol
 * 1.2.0's {@code schema_name} on those unary requests.
 *
 * <p>Tagging happens in the SINK phase rather than at finalize, deliberately:
 * that proves the sink-side worker resolved the right implementation, which is
 * a different connection from the one the Source phase acquires. Mirrors
 * vgi-python's {@code SameNameMainBuffered} / {@code SameNameDataBuffered};
 * driven by {@code test/sql/integration/table_in_out/same_name_schemas.test}.
 */
public final class SameNameBufferedFunctions {

    private SameNameBufferedFunctions() {}

    /** The colliding registered name — deliberately identical in both schemas. */
    public static final String NAME = "test_same_name_buffered";

    /** Shared body: tag in Sink, buffer, drain one batch per finalize tick. */
    abstract static class Tagging extends AbstractBufferAndDrain {

        /** Schema this implementation is registered into — the tag it stamps. */
        abstract String owningSchema();

        /** One VARCHAR column regardless of the input schema. */
        @Override public BindResponse onBind(TableInOutBindParams params) {
            return BindResponse.forSchema(SchemaUtil.serializeSchema(OUTPUT_TAG_SCHEMA));
        }

        @Override public byte[] process(VectorSchemaRoot batch, TableBufferingProcessParams params) {
            try (VectorSchemaRoot tagged = tagRows(owningSchema(), batch)) {
                params.storage().stateAppend(NS_BUF, KEY, BatchUtil.writeSingleBatch(tagged));
            }
            return params.executionId();
        }
    }

    /** {@code test_same_name_buffered} as declared in the {@code main} schema. */
    public static final class MainSchema extends Tagging {

        private static final FunctionSpec SPEC = FunctionSpec.builder(NAME)
                .metadata(FunctionMetadata.describe(
                                "Schema-disambiguation probe; the main-schema buffered function")
                        .withCategories("test")
                        .withExamples(List.of(new FunctionExample(
                                "SELECT * FROM example.main.test_same_name_buffered((SELECT 1 AS n))",
                                "Returns 'main:1'", null))))
                .table("data")
                .build();

        @Override public FunctionSpec spec() { return SPEC; }

        @Override String owningSchema() { return "main"; }
    }

    /** {@code test_same_name_buffered} as declared in the {@code data} schema. */
    public static final class DataSchema extends Tagging {

        private static final FunctionSpec SPEC = FunctionSpec.builder(NAME)
                .metadata(FunctionMetadata.describe(
                                "Schema-disambiguation probe; the data-schema buffered function")
                        .withCategories("test")
                        .withExamples(List.of(new FunctionExample(
                                "SELECT * FROM example.data.test_same_name_buffered((SELECT 1 AS n))",
                                "Returns 'data:1'", null))))
                .table("data")
                .build();

        @Override public FunctionSpec spec() { return SPEC; }

        @Override String owningSchema() { return "data"; }
    }
}
