// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.client;

import farm.query.vgi.pushdown.ComparisonOperator;
import farm.query.vgi.pushdown.PushdownFilter;
import farm.query.vgi.pushdown.PushdownFilters;
import farm.query.vgi.pushdown.PushdownFiltersDecoder;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link PushdownFiltersEncoder} against {@link PushdownFiltersDecoder} — the
 * Java half of the conformance story (the Python half lives in
 * {@link PushdownFiltersPythonConformanceTest}).
 *
 * <p>Each case asserts the decoded AST rather than the bytes: operators,
 * constants, the projected column index every node repeats, and — for join
 * keys — that the out-of-band batches resolve back onto the node that named
 * them.
 */
final class PushdownFiltersEncoderTest {

    private static final ProjectedColumns COLUMNS =
            ProjectedColumns.of(List.of("n", "name", "addr"));

    @Test
    void roundTripsEveryComparisonOperator() {
        PushdownFiltersEncoder enc = PushdownFiltersEncoder.builder();
        for (ComparisonOperator op : ComparisonOperator.values()) {
            enc.filter(COLUMNS.column("n"), FilterPredicate.compare(op, 7L));
        }
        PushdownFilters decoded = decode(enc);

        assertEquals(ComparisonOperator.values().length, decoded.filters().size());
        for (int i = 0; i < ComparisonOperator.values().length; i++) {
            PushdownFilter.Constant c =
                    assertInstanceOf(PushdownFilter.Constant.class, decoded.filters().get(i));
            assertEquals(ComparisonOperator.values()[i], c.op());
            assertEquals("n", c.columnName());
            assertEquals(0, c.columnIndex());
            assertEquals(7L, c.value());
        }
    }

    @Test
    void constantsKeepTheirDeclaredTypeAndValue() {
        PushdownFilters decoded = decode(PushdownFiltersEncoder.builder()
                .filter(COLUMNS.column("n"), FilterPredicate.ge(5L))
                .filter(COLUMNS.column("name"), FilterPredicate.eq("berlin"))
                .filter(COLUMNS.column("addr"), FilterPredicate.lt(2.5d)));

        assertEquals(5L, constant(decoded, 0).value());
        assertEquals("berlin", constant(decoded, 1).value());
        assertEquals(2.5d, constant(decoded, 2).value());
        // value_ref N resolves to batch column N+1; three constants means three
        // distinct sibling columns, which is exactly what a shared-slot bug
        // would collapse.
        assertEquals(1, constant(decoded, 1).columnIndex());
        assertEquals(2, constant(decoded, 2).columnIndex());
    }

    @Test
    void roundTripsNullChecks() {
        PushdownFilters decoded = decode(PushdownFiltersEncoder.builder()
                .filter(COLUMNS.column("n"), FilterPredicate.isNull())
                .filter(COLUMNS.column("name"), FilterPredicate.isNotNull()));

        PushdownFilter.IsNull isNull =
                assertInstanceOf(PushdownFilter.IsNull.class, decoded.filters().get(0));
        assertEquals("n", isNull.columnName());
        PushdownFilter.IsNotNull isNotNull =
                assertInstanceOf(PushdownFilter.IsNotNull.class, decoded.filters().get(1));
        assertEquals(1, isNotNull.columnIndex());
    }

    @Test
    void roundTripsNestedConjunctions() {
        PushdownFilters decoded = decode(PushdownFiltersEncoder.builder()
                .filter(COLUMNS.column("n"), FilterPredicate.and(
                        FilterPredicate.ge(5L),
                        FilterPredicate.or(FilterPredicate.lt(100L), FilterPredicate.isNull()))));

        PushdownFilter.And and =
                assertInstanceOf(PushdownFilter.And.class, decoded.filters().get(0));
        assertEquals(2, and.children().size());
        assertEquals(5L, assertInstanceOf(PushdownFilter.Constant.class, and.children().get(0)).value());

        PushdownFilter.Or or =
                assertInstanceOf(PushdownFilter.Or.class, and.children().get(1));
        assertEquals(100L, assertInstanceOf(PushdownFilter.Constant.class, or.children().get(0)).value());
        assertInstanceOf(PushdownFilter.IsNull.class, or.children().get(1));

        // Column identity is repeated on every node, children included — a
        // child that lost it would decode with column_index -1.
        for (PushdownFilter child : and.children()) {
            assertEquals("n", child.columnName());
        }
        assertEquals(0, or.columnIndex());
    }

    @Test
    void roundTripsAStructFieldFilter() {
        PushdownFilters decoded = decode(PushdownFiltersEncoder.builder()
                .filter(COLUMNS.column("addr"),
                        FilterPredicate.structField(1, "city", FilterPredicate.eq("berlin"))));

        PushdownFilter.Struct s =
                assertInstanceOf(PushdownFilter.Struct.class, decoded.filters().get(0));
        assertEquals(1, s.childIndex());
        assertEquals("city", s.childName());
        assertEquals("addr", s.columnName());
        assertEquals(2, s.columnIndex());
        assertEquals("berlin",
                assertInstanceOf(PushdownFilter.Constant.class, s.childFilter()).value());
    }

    @Test
    void joinKeyValuesTravelOutOfBandAndResolveByName() {
        EncodedPushdownFilters encoded = PushdownFiltersEncoder.builder()
                .filter(COLUMNS.column("n"), FilterPredicate.joinKeys(List.of(10L, 20L, 30L)))
                .filter(COLUMNS.column("name"), FilterPredicate.joinKeys(List.of("a", "b")))
                .encode();

        assertEquals(2, encoded.joinKeys().size(), "one batch per join-key filter");

        PushdownFilters decoded =
                PushdownFiltersDecoder.decode(encoded.pushdownFilters(), encoded.joinKeys());
        PushdownFilter.In ints = assertInstanceOf(PushdownFilter.In.class, decoded.filters().get(0));
        assertEquals(List.of(10L, 20L, 30L), ints.values());
        assertEquals(0, ints.columnIndex());
        PushdownFilter.In names = assertInstanceOf(PushdownFilter.In.class, decoded.filters().get(1));
        assertEquals(List.of("a", "b"), names.values());
    }

    @Test
    void joinKeysDecodeToAnEmptySetWhenTheirBatchesAreNotSent() {
        // The two artefacts are one payload; dropping the key batches leaves the
        // node unresolvable, which is why EncodedPushdownFilters hands back both.
        EncodedPushdownFilters encoded = PushdownFiltersEncoder.builder()
                .filter(COLUMNS.column("n"), FilterPredicate.joinKeys(List.of(1L)))
                .encode();

        PushdownFilters decoded = PushdownFiltersDecoder.decode(encoded.pushdownFilters());
        assertTrue(assertInstanceOf(PushdownFilter.In.class, decoded.filters().get(0)).values().isEmpty());
    }

    @Test
    void carriesTheFilterVersionEveryDecoderDemands() {
        PushdownFilters decoded = decode(PushdownFiltersEncoder.builder()
                .filter(COLUMNS.column("n"), FilterPredicate.eq(1L)));
        assertEquals(PushdownFiltersEncoder.FILTER_VERSION, decoded.version());
    }

    @Test
    void encodesAnExplicitlyTypedConstant() {
        PushdownFilters decoded = decode(PushdownFiltersEncoder.builder()
                .filter(COLUMNS.column("n"),
                        FilterPredicate.compare(ComparisonOperator.EQ,
                                ScalarValue.of(new ArrowType.Int(32, true), 7))));
        assertEquals(7L, constant(decoded, 0).value(), "int32 reads back widened to Long");
    }

    @Test
    void rejectsAnEmptyJoinKeySet() {
        // An empty IN set is a filter that matches nothing; DuckDB never pushes
        // one, and a worker resolving it by name would silently see "no keys".
        assertThrows(IllegalArgumentException.class, () -> FilterPredicate.joinKeys(List.of()));
    }

    @Test
    void rejectsAColumnThatIsNotInTheProjection() {
        assertThrows(IllegalArgumentException.class, () -> COLUMNS.column("missing"));
    }

    private static PushdownFilters decode(PushdownFiltersEncoder encoder) {
        EncodedPushdownFilters encoded = encoder.encode();
        return PushdownFiltersDecoder.decode(encoded.pushdownFilters(), encoded.joinKeys());
    }

    private static PushdownFilter.Constant constant(PushdownFilters filters, int index) {
        return assertInstanceOf(PushdownFilter.Constant.class, filters.filters().get(index));
    }
}
