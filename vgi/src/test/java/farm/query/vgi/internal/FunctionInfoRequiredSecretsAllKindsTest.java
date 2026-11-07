// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import farm.query.vgi.buffering.TableBufferingCombineParams;
import farm.query.vgi.buffering.TableBufferingFinalizeParams;
import farm.query.vgi.buffering.TableBufferingFunction;
import farm.query.vgi.buffering.TableBufferingProcessParams;
import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.protocol.FunctionInfo;
import farm.query.vgi.protocol.FunctionRequiredSecret;
import farm.query.vgi.scalar.ScalarBindParams;
import farm.query.vgi.scalar.ScalarFunction;
import farm.query.vgi.scalar.ScalarProcessParams;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.tableinout.TableInOutBindParams;
import farm.query.vgi.tableinout.TableInOutExchangeState;
import farm.query.vgi.tableinout.TableInOutFunction;
import farm.query.vgi.tableinout.TableInOutInitParams;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Every function kind, not only table and aggregate, can declare required
 * secrets, and the declaration reaches {@link FunctionInfo#required_secrets()}
 * (the extension resolves them before bind for every kind). Before, scalar,
 * table-in-out and buffering functions had no hook and always advertised an
 * empty list.
 */
class FunctionInfoRequiredSecretsAllKindsTest {

    private static final List<FunctionRequiredSecret> DECLARED = List.of(
            FunctionRequiredSecret.ofType("vgi_example"),
            FunctionRequiredSecret.scoped("s3", "s3://bucket/"),
            FunctionRequiredSecret.named("vgi_example", "mine"));

    private static List<FunctionRequiredSecret> withNull() {
        List<FunctionRequiredSecret> l = new ArrayList<>(DECLARED);
        l.add(null);
        return l;
    }

    static class Scalar implements ScalarFunction {
        final List<FunctionRequiredSecret> req;
        Scalar(List<FunctionRequiredSecret> req) { this.req = req; }
        @Override public String name() { return "scalar_secret"; }
        @Override public FunctionMetadata metadata() { return FunctionMetadata.describe("doc"); }
        @Override public List<ArgSpec> argumentSpecs() { return List.of(); }
        @Override public List<FunctionRequiredSecret> requiredSecrets() { return req; }
        @Override public BindResponse onBind(ScalarBindParams p) { throw new UnsupportedOperationException(); }
        @Override public VectorSchemaRoot process(ScalarProcessParams p, VectorSchemaRoot in, BufferAllocator a) {
            throw new UnsupportedOperationException();
        }
    }

    static class InOut implements TableInOutFunction {
        final List<FunctionRequiredSecret> req;
        InOut(List<FunctionRequiredSecret> req) { this.req = req; }
        @Override public String name() { return "tio_secret"; }
        @Override public FunctionMetadata metadata() { return FunctionMetadata.describe("doc"); }
        @Override public List<ArgSpec> argumentSpecs() { return List.of(); }
        @Override public List<FunctionRequiredSecret> requiredSecrets() { return req; }
        @Override public BindResponse onBind(TableInOutBindParams p) { throw new UnsupportedOperationException(); }
        @Override public TableInOutExchangeState createExchange(TableInOutInitParams p) {
            throw new UnsupportedOperationException();
        }
    }

    static class Buffering implements TableBufferingFunction {
        final List<FunctionRequiredSecret> req;
        Buffering(List<FunctionRequiredSecret> req) { this.req = req; }
        @Override public String name() { return "buffering_secret"; }
        @Override public FunctionMetadata metadata() { return FunctionMetadata.describe("doc"); }
        @Override public List<ArgSpec> argumentSpecs() { return List.of(); }
        @Override public List<FunctionRequiredSecret> requiredSecrets() { return req; }
        @Override public BindResponse onBind(TableInOutBindParams p) { throw new UnsupportedOperationException(); }
        @Override public byte[] process(VectorSchemaRoot b, TableBufferingProcessParams p) {
            throw new UnsupportedOperationException();
        }
        @Override public List<byte[]> combine(List<byte[]> ids, TableBufferingCombineParams p) {
            throw new UnsupportedOperationException();
        }
        @Override public TableProducerState createFinalizeProducer(TableBufferingFinalizeParams p) {
            throw new UnsupportedOperationException();
        }
    }

    @Test
    void scalarDeclarationReachesFunctionInfo() {
        assertEquals(DECLARED, VgiServiceImpl.scalarFunctionInfo(new Scalar(DECLARED), "main").required_secrets());
        assertTrue(VgiServiceImpl.scalarFunctionInfo(new Scalar(List.of()), "main").required_secrets().isEmpty());
    }

    @Test
    void tableInOutDeclarationReachesFunctionInfo() {
        assertEquals(DECLARED, VgiServiceImpl.toTableInOutFunctionInfo(new InOut(DECLARED), "main").required_secrets());
        assertTrue(VgiServiceImpl.toTableInOutFunctionInfo(new InOut(List.of()), "main").required_secrets().isEmpty());
    }

    @Test
    void bufferingDeclarationReachesFunctionInfo() {
        assertEquals(DECLARED, VgiServiceImpl.toBufferingFunctionInfo(new Buffering(DECLARED), "main").required_secrets());
        assertTrue(VgiServiceImpl.toBufferingFunctionInfo(new Buffering(List.of()), "main").required_secrets().isEmpty());
    }

    @Test
    void nullEntryIsAnErrorForEveryKind() {
        assertThrows(IllegalArgumentException.class,
                () -> VgiServiceImpl.scalarFunctionInfo(new Scalar(withNull()), "main"));
        assertThrows(IllegalArgumentException.class,
                () -> VgiServiceImpl.toTableInOutFunctionInfo(new InOut(withNull()), "main"));
        assertThrows(IllegalArgumentException.class,
                () -> VgiServiceImpl.toBufferingFunctionInfo(new Buffering(withNull()), "main"));
    }

    @Test
    void declarationSurvivesSerializationForEveryKind() {
        for (FunctionInfo info : List.of(
                VgiServiceImpl.scalarFunctionInfo(new Scalar(DECLARED), "main"),
                VgiServiceImpl.toTableInOutFunctionInfo(new InOut(DECLARED), "main"),
                VgiServiceImpl.toBufferingFunctionInfo(new Buffering(DECLARED), "main"))) {
            try (VectorSchemaRoot encoded = BatchUtil.readSingleBatch(
                    FunctionInfoSerializer.serialize(info), farm.query.vgirpc.wire.Allocators.root())) {
                List<?> items = ((org.apache.arrow.vector.complex.ListVector)
                        encoded.getVector("required_secrets")).getObject(0);
                assertEquals(3, items.size(), info.name());
            }
        }
    }
}
