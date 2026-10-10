// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi;

import farm.query.vgi.function.Arguments;
import farm.query.vgi.function.ParameterExtractor;
import farm.query.vgirpc.errors.Code;
import farm.query.vgirpc.errors.ErrorModel;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** The SDK's own exception types report the gRPC-style code vgi-rpc puts on the wire. */
class SdkErrorCodesTest {

    @Test
    void eachTypeDeclaresItsCodeAndNoKind() {
        assertCode(Code.INVALID_ARGUMENT, new InvalidArgumentException("x"));
        assertCode(Code.NOT_FOUND, new NotFoundException("x"));
        assertCode(Code.FAILED_PRECONDITION, new ReadOnlyCatalogException("x"));
        assertCode(Code.UNIMPLEMENTED, new UnimplementedException("x"));
    }

    @Test
    void eachTypeKeepsTheJdkTypeItReplaced() {
        assertInstanceOf(IllegalArgumentException.class, new InvalidArgumentException("x"));
        assertInstanceOf(IllegalArgumentException.class, new NotFoundException("x"));
        assertInstanceOf(UnsupportedOperationException.class, new ReadOnlyCatalogException("x"));
        assertInstanceOf(UnsupportedOperationException.class, new UnimplementedException("x"));
    }

    @Test
    void parameterConstraintFailureIsInvalidArgument() {
        ParameterExtractor p = ParameterExtractor.of(new Arguments(java.util.List.of(), Map.of("batch_size", 0L)));
        RuntimeException e = assertThrows(RuntimeException.class,
                () -> p.named("batch_size").asLong().ge(1).notNull());
        assertEquals(Code.INVALID_ARGUMENT, ErrorModel.codeOf(e));
        assertEquals("batch_size must be >= 1, got 0", e.getMessage());
    }

    private static void assertCode(Code expected, RuntimeException e) {
        assertEquals(expected, ErrorModel.codeOf(e));
        assertNull(ErrorModel.kindOf(e));
    }
}
