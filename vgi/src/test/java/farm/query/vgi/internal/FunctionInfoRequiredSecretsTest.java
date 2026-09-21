// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.protocol.FunctionInfo;
import farm.query.vgi.protocol.FunctionRequiredSecret;
import farm.query.vgi.table.SimpleTableFunction;
import farm.query.vgi.table.TableInitParams;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A table function's {@link farm.query.vgi.table.TableFunction#requiredSecrets()}
 * reaches the wire {@link FunctionInfo#required_secrets()}. Until it did, only
 * aggregates could declare a secret; a table function had to request one in a
 * two-phase bind even when nothing about the call chose which secret.
 */
class FunctionInfoRequiredSecretsTest {

    private static final Schema OUTPUT = Schemas.of(Schemas.nullable("v", Schemas.INT64));

    /** Declares one {@code vgi_example} secret. */
    static final class Declares extends SimpleTableFunction {
        @Override public FunctionSpec spec() { return FunctionSpec.builder("declares").description("doc").build(); }
        @Override protected Schema outputSchema() { return OUTPUT; }
        @Override public List<FunctionRequiredSecret> requiredSecrets() {
            return List.of(new FunctionRequiredSecret("vgi_example", null, null));
        }
        @Override public TableProducerState createProducer(TableInitParams p) {
            throw new UnsupportedOperationException();
        }
    }

    /** Declares nothing (control). */
    static final class DeclaresNothing extends SimpleTableFunction {
        @Override public FunctionSpec spec() { return FunctionSpec.builder("declares_nothing").description("doc").build(); }
        @Override protected Schema outputSchema() { return OUTPUT; }
        @Override public TableProducerState createProducer(TableInitParams p) {
            throw new UnsupportedOperationException();
        }
    }

    @Test
    void declaredSecretSurfacesOnFunctionInfo() {
        FunctionInfo info = VgiServiceImpl.tableFunctionInfo(new Declares(), "main");
        assertEquals(List.of(new FunctionRequiredSecret("vgi_example", null, null)),
                info.required_secrets());
    }

    @Test
    void declaredSecretSurvivesSerialization() {
        FunctionInfo info = VgiServiceImpl.tableFunctionInfo(new Declares(), "main");
        try (VectorSchemaRoot encoded = BatchUtil.readSingleBatch(
                FunctionInfoSerializer.serialize(info), Allocators.root())) {
            List<?> items = ((ListVector) encoded.getVector("required_secrets")).getObject(0);
            assertEquals(1, items.size());
            assertEquals("vgi_example", String.valueOf(((Map<?, ?>) items.get(0)).get("secret_type")));
        }
    }

    @Test
    void noDeclarationYieldsEmptyList() {
        FunctionInfo info = VgiServiceImpl.tableFunctionInfo(new DeclaresNothing(), "main");
        assertTrue(info.required_secrets().isEmpty());
    }
}
