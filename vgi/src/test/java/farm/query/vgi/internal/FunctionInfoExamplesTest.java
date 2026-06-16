// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.protocol.FunctionExample;
import farm.query.vgi.protocol.FunctionInfo;
import farm.query.vgi.scalar.ScalarFn;
import org.apache.arrow.vector.BigIntVector;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that per-function {@code examples} declared on a scalar's
 * {@link FunctionMetadata} flow through the catalog-enumeration mapping
 * ({@link VgiServiceImpl#scalarFunctionInfo}) onto the wire
 * {@link FunctionInfo#examples()}.
 */
class FunctionInfoExamplesTest {

    /** A scalar that advertises two usage examples via {@code metadata()}. */
    static final class WithExamples extends ScalarFn {
        @Override public String name() { return "with_examples"; }
        @Override public String description() { return "doc"; }

        @Override public FunctionMetadata metadata() {
            return FunctionMetadata.describe(description()).withExamples(List.of(
                    new FunctionExample("SELECT with_examples(1)", "first", "1"),
                    new FunctionExample("SELECT with_examples(2)", "second", null)));
        }

        public void compute(@farm.query.vgi.scalar.Vector BigIntVector v, BigIntVector result) {
            for (int i = 0; i < v.getValueCount(); i++) result.setSafe(i, v.get(i));
        }
    }

    /** A scalar that declares no examples (control). */
    static final class NoExamples extends ScalarFn {
        @Override public String name() { return "no_examples"; }
        @Override public String description() { return "doc"; }

        public void compute(@farm.query.vgi.scalar.Vector BigIntVector v, BigIntVector result) {
            for (int i = 0; i < v.getValueCount(); i++) result.setSafe(i, v.get(i));
        }
    }

    @Test
    void scalarMetadataExamplesSurfaceOnFunctionInfo() {
        FunctionInfo info = VgiServiceImpl.scalarFunctionInfo(new WithExamples(), "main");

        List<FunctionExample> examples = info.examples();
        assertEquals(2, examples.size());

        assertEquals("SELECT with_examples(1)", examples.get(0).sql());
        assertEquals("first", examples.get(0).description());
        assertEquals("1", examples.get(0).expected_output());

        assertEquals("SELECT with_examples(2)", examples.get(1).sql());
        assertEquals("second", examples.get(1).description());
        assertNull(examples.get(1).expected_output());
    }

    @Test
    void scalarWithoutExamplesYieldsEmptyList() {
        FunctionInfo info = VgiServiceImpl.scalarFunctionInfo(new NoExamples(), "main");
        assertTrue(info.examples().isEmpty());
    }
}
