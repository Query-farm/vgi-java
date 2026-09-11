// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.ArgumentMonotonicity;
import farm.query.vgi.protocol.FunctionInfo;
import farm.query.vgi.scalar.ScalarFn;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.complex.ListVector;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FunctionInfoArgumentMonotonicityTest {

    static class Identity extends ScalarFn {
        private final List<ArgumentMonotonicity> monotonicity;

        Identity() {
            this(List.of(
                    ArgumentMonotonicity.STRICTLY_INCREASING,
                    ArgumentMonotonicity.CONSTANT));
        }

        Identity(List<ArgumentMonotonicity> monotonicity) {
            this.monotonicity = monotonicity;
        }

        @Override public String name() { return "identity"; }
        @Override public String description() { return "identity"; }
        @Override public List<ArgSpec> argumentSpecs() {
            return List.of(
                    ArgSpec.positional("value", 0, Schemas.INT64),
                    ArgSpec.positional("offset", 1, Schemas.INT64));
        }
        @Override public List<ArgumentMonotonicity> argumentMonotonicity() {
            return monotonicity;
        }
        public void compute(
                @farm.query.vgi.scalar.Vector BigIntVector value,
                @farm.query.vgi.scalar.Vector BigIntVector offset,
                BigIntVector result) {}
    }

    @Test
    void serializesClaimsInDeclarationOrder() {
        FunctionInfo info = VgiServiceImpl.scalarFunctionInfo(new Identity(), "main");
        assertEquals(List.of("STRICTLY_INCREASING", "CONSTANT"), info.argument_monotonicity());
        try (var encoded = BatchUtil.readSingleBatch(
                FunctionInfoSerializer.serialize(info), Allocators.root())) {
            var values = (ListVector) encoded.getVector("argument_monotonicity");
            assertEquals("STRICTLY_INCREASING", values.getObject(0).get(0).toString());
            assertEquals("CONSTANT", values.getObject(0).get(1).toString());
        }
    }

    @Test
    void rejectsWrongDeclarationCount() {
        var error = assertThrows(IllegalArgumentException.class,
                () -> VgiServiceImpl.scalarFunctionInfo(new Identity(List.of()), "main"));
        assertTrue(error.getMessage().contains("expected 2 declaration slots"));
    }
}
