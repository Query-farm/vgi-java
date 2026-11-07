// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import farm.query.vgi.Worker;
import farm.query.vgi.client.ArgumentsEncoder;
import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.Arguments;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.protocol.BindRequest;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.protocol.CatalogAttachRequest;
import farm.query.vgi.protocol.FunctionInfo;
import farm.query.vgi.protocol.ItemsResponse;
import farm.query.vgi.scalar.ScalarBindParams;
import farm.query.vgi.scalar.ScalarFunction;
import farm.query.vgi.scalar.ScalarProcessParams;
import farm.query.vgi.table.TableBindParams;
import farm.query.vgi.table.TableFunction;
import farm.query.vgi.table.TableInitParams;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.marshal.RecordCodec;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Catalog listing binds every function with empty arguments to learn its output
 * schema. A function that validates its arguments in {@code onBind} used to throw
 * out of that probe and fail ATTACH for the whole catalog. The listing now
 * advertises an empty output schema for it; the real bind still validates.
 */
class CatalogListingBindFailureTest {

    private static final Schema OUTPUT = Schemas.of(Schemas.nullable("n", Schemas.INT64));

    /** Requires {@code count} and rejects anything below 1 in onBind. */
    static final class Strict implements TableFunction {
        @Override public String name() { return "strict"; }
        @Override public FunctionMetadata metadata() { return FunctionMetadata.describe("strict"); }
        @Override public List<ArgSpec> argumentSpecs() {
            return List.of(ArgSpec.positional("count", 0, Schemas.INT64)
                    .withConstraints(ArgSpec.Constraints.range(1, null, null, null)));
        }
        @Override public BindResponse onBind(TableBindParams params) {
            Object count = params.arguments().positionalAt(0);
            if (!(count instanceof Long n) || n < 1) {
                throw new IllegalArgumentException("strict: count is required and must be >= 1");
            }
            return BindResponse.forSchema(SchemaUtil.serializeSchema(OUTPUT));
        }
        @Override public TableProducerState createProducer(TableInitParams params) {
            throw new UnsupportedOperationException();
        }
    }

    /** A scalar whose onBind always throws. */
    static final class StrictScalar implements ScalarFunction {
        @Override public String name() { return "strict_scalar"; }
        @Override public FunctionMetadata metadata() { return FunctionMetadata.describe("strict"); }
        @Override public List<ArgSpec> argumentSpecs() { return List.of(); }
        @Override public BindResponse onBind(ScalarBindParams params) {
            throw new IllegalStateException("needs arguments");
        }
        @Override public VectorSchemaRoot process(ScalarProcessParams params, VectorSchemaRoot input,
                                                  org.apache.arrow.memory.BufferAllocator alloc) {
            throw new UnsupportedOperationException();
        }
    }

    private static VgiServiceImpl service(Worker w) {
        return new VgiServiceImpl(w, w.scalars(), w.tables(), w.tableInOuts(), w.aggregates());
    }

    @Test
    void throwingOnBindAdvertisesEmptyOutputSchema() {
        FunctionInfo info = VgiServiceImpl.tableFunctionInfo(new Strict(), "main");
        assertEquals(0, SchemaUtil.deserializeSchema(info.output_schema()).getFields().size());
        FunctionInfo scalar = VgiServiceImpl.scalarFunctionInfo(new StrictScalar(), "main");
        assertEquals(0, SchemaUtil.deserializeSchema(scalar.output_schema()).getFields().size());
    }

    @Test
    void catalogStillAttachesAndListsAndTheFunctionStillBinds() {
        VgiServiceImpl svc = service(Worker.builder()
                .catalogName("strict_catalog")
                .registerTable("main", new Strict()));
        byte[] attach = svc.catalog_attach(
                CatalogAttachRequest.of("strict_catalog", null, null, null), null).attach_opaque_data();
        ItemsResponse items = svc.catalog_schema_contents_functions(attach, List.of("main"), null, null, null);
        List<String> names = items.items().stream()
                .map(b -> RecordCodec.deserializeFromBytes(b, FunctionInfo.class).name())
                .toList();
        assertEquals(List.of("strict"), names);

        BindResponse ok = svc.bind(bind(attach, ArgumentsEncoder.positionalArgs(5L)), null);
        assertEquals(OUTPUT, SchemaUtil.deserializeSchema(ok.output_schema()));

        IllegalArgumentException bad = assertThrows(IllegalArgumentException.class,
                () -> svc.bind(bind(attach, ArgumentsEncoder.positionalArgs(0L)), null));
        assertTrue(bad.getMessage().contains("count"), bad.getMessage());
    }

    @Test
    void realBindStillThrowsFromOnBind() {
        assertThrows(IllegalArgumentException.class, () -> new Strict().onBind(
                new TableBindParams("strict", Arguments.empty(), null, java.util.Map.of())));
    }

    private static BindRequest bind(byte[] attach, byte[] args) {
        return new BindRequest("strict", args, "TABLE", null, null, null, attach, null,
                false, null, null, null, null, List.of("main"));
    }
}
