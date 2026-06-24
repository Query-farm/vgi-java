// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.protocol.FunctionExample;
import farm.query.vgi.protocol.FunctionInfo;
import farm.query.vgi.scalar.ScalarFn;
import org.apache.arrow.vector.BigIntVector;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that worker-provided per-function tags declared on a function's
 * {@link FunctionMetadata} via {@link FunctionMetadata#withTag(String, String)}
 * / {@link FunctionMetadata#withTags(Map)} flow through the catalog-enumeration
 * mapping ({@link VgiServiceImpl#scalarFunctionInfo}) onto the wire
 * {@link FunctionInfo#tags()} (surfaced by DuckDB as the function's {@code tags}
 * map and read by the vgi-lint metadata linter, e.g. rule VGI307's
 * {@code vgi.columns_md}). It also confirms tags merge with structured
 * {@code examples} rather than clobbering them, and that the no-tags default is
 * an empty map.
 */
class FunctionInfoTagsTest {

    /** A scalar advertising two tags plus a structured example. */
    static final class Tagged extends ScalarFn {
        @Override public String name() { return "tagged"; }
        @Override public String description() { return "doc"; }

        @Override public FunctionMetadata metadata() {
            return FunctionMetadata.describe(description())
                    .withExamples(List.of(new FunctionExample("SELECT tagged(1)", "ex", "1")))
                    .withTag("vgi.columns_md", "# Columns\nid")
                    .withTags(Map.of("vgi.description_md", "# Tagged"));
        }

        public void compute(@farm.query.vgi.scalar.Vector BigIntVector v, BigIntVector result) {
            for (int i = 0; i < v.getValueCount(); i++) result.setSafe(i, v.get(i));
        }
    }

    /** A scalar declaring no tags (control). */
    static final class Untagged extends ScalarFn {
        @Override public String name() { return "untagged"; }
        @Override public String description() { return "doc"; }

        public void compute(@farm.query.vgi.scalar.Vector BigIntVector v, BigIntVector result) {
            for (int i = 0; i < v.getValueCount(); i++) result.setSafe(i, v.get(i));
        }
    }

    @Test
    void functionTagsSurfaceOnFunctionInfo() {
        FunctionInfo info = VgiServiceImpl.scalarFunctionInfo(new Tagged(), "main");

        assertEquals("# Columns\nid", info.tags().get("vgi.columns_md"));
        assertEquals("# Tagged", info.tags().get("vgi.description_md"));
        assertEquals(2, info.tags().size());

        // withTags must merge with (not clobber) the structured examples carried
        // alongside on the same metadata.
        assertEquals(1, info.examples().size());
        assertEquals("SELECT tagged(1)", info.examples().get(0).sql());
    }

    @Test
    void withTagsMergesAcrossCalls() {
        FunctionMetadata md = FunctionMetadata.describe("doc")
                .withTags(Map.of("a", "1"))
                .withTags(Map.of("b", "2"))
                .withTag("a", "overwritten");

        assertEquals("overwritten", md.tags().get("a"));
        assertEquals("2", md.tags().get("b"));
        assertEquals(2, md.tags().size());
    }

    @Test
    void noTagsConfiguredYieldsEmptyMap() {
        FunctionInfo info = VgiServiceImpl.scalarFunctionInfo(new Untagged(), "main");
        assertTrue(info.tags().isEmpty(), "tags should default to empty");
    }
}
