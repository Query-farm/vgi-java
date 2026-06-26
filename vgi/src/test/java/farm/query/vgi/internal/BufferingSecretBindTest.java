// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import farm.query.vgi.Worker;
import farm.query.vgi.buffering.TableBufferingCombineParams;
import farm.query.vgi.buffering.TableBufferingFinalizeParams;
import farm.query.vgi.buffering.TableBufferingFunction;
import farm.query.vgi.buffering.TableBufferingProcessParams;
import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.protocol.BindRequest;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.tableinout.TableInOutBindParams;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A table-buffering function can request DuckDB secrets through the two-phase
 * secret bind, exactly like scalar and table functions.
 *
 * <p>The buffering bind path used to hand {@code onBind} a {@link
 * TableInOutBindParams} with no secrets and no {@code resolvedSecretsProvided}
 * flag, and {@code BoundBuffering} dropped the resolved secrets so they never
 * reached the finalize phase. These tests pin that the first bind pass surfaces
 * the secret-lookup request and the re-bind (resolved) flows the flag through to
 * {@code onBind}.</p>
 */
class BufferingSecretBindTest {

    private static final Schema OUT =
            new Schema(List.of(new Field("x", FieldType.nullable(new ArrowType.Int(64, true)), null)));

    /** A buffering sink that needs an {@code s3} secret on the first bind pass. */
    static final class SecretSink implements TableBufferingFunction {
        @Override public String name() { return "secret_sink"; }
        @Override public FunctionMetadata metadata() { return FunctionMetadata.describe("doc"); }
        @Override public List<ArgSpec> argumentSpecs() { return List.of(); }

        @Override public BindResponse onBind(TableInOutBindParams params) {
            if (!params.resolvedSecretsProvided()) {
                return new BindResponse(SchemaUtil.serializeSchema(OUT), new byte[0],
                        List.of("s3"), List.of("s3://bucket/out.dat"), List.of(""));
            }
            return BindResponse.forSchema(SchemaUtil.serializeSchema(OUT));
        }

        @Override public byte[] process(VectorSchemaRoot batch, TableBufferingProcessParams params) {
            throw new UnsupportedOperationException();
        }
        @Override public List<byte[]> combine(List<byte[]> stateIds, TableBufferingCombineParams params) {
            throw new UnsupportedOperationException();
        }
        @Override public TableProducerState createFinalizeProducer(TableBufferingFinalizeParams params) {
            throw new UnsupportedOperationException();
        }
    }

    private static VgiServiceImpl service() {
        Worker w = Worker.builder().registerTableBuffering(new SecretSink());
        return new VgiServiceImpl(w, List.of(), List.of(), List.of(), List.of());
    }

    private static BindRequest bindRequest(boolean resolved) {
        return new BindRequest("secret_sink", null, "table_buffering",
                null, null, null, null, null, resolved, null, null);
    }

    @Test
    void firstPassRequestsSecrets() {
        BindResponse resp = service().bind(bindRequest(false), null);
        assertEquals(List.of("s3"), resp.lookup_secret_types());
        assertEquals(List.of("s3://bucket/out.dat"), resp.lookup_scopes());
    }

    @Test
    void secondPassResolvesNormally() {
        BindResponse resp = service().bind(bindRequest(true), null);
        assertTrue(resp.lookup_secret_types().isEmpty(),
                "resolved pass should not request more secrets");
        assertFalse(resp.output_schema().length == 0,
                "on_bind should have produced the output schema");
    }
}
