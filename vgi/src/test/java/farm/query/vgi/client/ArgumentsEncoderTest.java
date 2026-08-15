// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.client;

import farm.query.vgi.function.Arguments;
import farm.query.vgi.internal.ArgumentsParser;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link ArgumentsEncoder} against the decoder it exists to satisfy.
 *
 * <p>The assertion that matters is not "the bytes parse" but "the worker sees
 * the arguments in the slots it expects": positional order preserved, named
 * args reachable under their bare name, and each value's Java type surviving
 * the trip.
 */
final class ArgumentsEncoderTest {

    @Test
    void roundTripsPositionalScalarsOfEveryBasicType() {
        byte[] ipc = ArgumentsEncoder.builder()
                .positional(42L)
                .positional(2.5d)
                .positional("hello")
                .positional(true)
                .positional(ScalarValue.ofNull(ScalarValue.INT64))
                .encode();

        Arguments args = ArgumentsParser.parse(ipc);
        assertEquals(5, args.positional().size());
        assertEquals(42L, args.positionalAt(0));
        assertEquals(2.5d, args.positionalAt(1));
        assertEquals("hello", args.positionalAt(2));
        assertEquals(true, args.positionalAt(3));
        assertNull(args.positionalAt(4), "a typed null must decode as null, not as 0");
    }

    @Test
    void roundTripsNamedArguments() {
        byte[] ipc = ArgumentsEncoder.builder()
                .positional(5000L)
                .named("batch_size", 1000L)
                .named("label", "rows")
                .encode();

        Arguments args = ArgumentsParser.parse(ipc);
        assertEquals(List.of(5000L), args.positional());
        // The parser exposes named args under both the bare name and the
        // wire-prefixed one; the bare name is what fixtures read.
        assertEquals(1000L, args.named().get("batch_size"));
        assertEquals(1000L, args.named().get("named_batch_size"));
        assertEquals("rows", args.named().get("label"));
    }

    @Test
    void preservesTheDeclaredArrowTypePerPositional() {
        // A client that needs DuckDB's narrower width says so explicitly;
        // inference alone would widen every integer to int64.
        byte[] ipc = ArgumentsEncoder.builder()
                .positional(ScalarValue.of(ScalarValue.INT32, 7))
                .positional(7L)
                .encode();

        Arguments args = ArgumentsParser.parse(ipc);
        assertEquals(new ArrowType.Int(32, true), args.positionalTypeAt(0));
        assertEquals(new ArrowType.Int(64, true), args.positionalTypeAt(1));
        // Both still read back as Long — the parser normalises integer widths.
        assertEquals(7L, args.positionalAt(0));
        assertEquals(7L, args.positionalAt(1));
    }

    @Test
    void roundTripsNestedStructAndListArguments() {
        Map<String, Object> point = new LinkedHashMap<>();
        point.put("x", 1.5d);
        point.put("y", -2.5d);

        byte[] ipc = ArgumentsEncoder.builder()
                .positional(point)
                .positional(List.of(1L, 2L, 3L))
                .encode();

        Arguments args = ArgumentsParser.parse(ipc);
        assertEquals(point, args.positionalAt(0));
        assertEquals(List.of(1L, 2L, 3L), args.positionalAt(1));
    }

    @Test
    void positionalArgsShorthandMatchesTheBuilder() {
        assertEquals(
                List.of(3L, "x"),
                ArgumentsParser.parse(ArgumentsEncoder.positionalArgs(3L, "x")).positional());
    }

    @Test
    void encodingNoArgumentsYieldsAParseableEmptyBatch() {
        Arguments args = ArgumentsParser.parse(ArgumentsEncoder.builder().encode());
        assertTrue(args.positional().isEmpty());
        assertTrue(args.named().isEmpty());
    }

    @Test
    void rejectsAnUntypedNull() {
        // The failure has to happen here, at the call site that knows the
        // intended type — not silently on the worker as a missing argument.
        assertThrows(IllegalArgumentException.class,
                () -> ArgumentsEncoder.builder().positional((Object) null));
        assertThrows(IllegalArgumentException.class,
                () -> ArgumentsEncoder.builder().named("k", (Object) null));
    }
}
