// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.internal;

import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.Text;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

class VectorScalarCodecTest {

    private final BufferAllocator alloc = Allocators.root();

    @Test
    void readsAndWritesPrimitives() {
        try (BigIntVector b = new BigIntVector("b", alloc);
             IntVector i = new IntVector("i", alloc);
             Float8Vector f = new Float8Vector("f", alloc);
             BitVector bit = new BitVector("bit", alloc);
             VarCharVector vc = new VarCharVector("vc", alloc);
             VarBinaryVector vb = new VarBinaryVector("vb", alloc)) {
            VectorScalarCodec.write(b, 0, 42L); b.setValueCount(1);
            VectorScalarCodec.write(i, 0, 7); i.setValueCount(1);
            VectorScalarCodec.write(f, 0, 3.14); f.setValueCount(1);
            VectorScalarCodec.write(bit, 0, true); bit.setValueCount(1);
            VectorScalarCodec.write(vc, 0, "hello"); vc.setValueCount(1);
            VectorScalarCodec.write(vb, 0, new byte[] {1, 2, 3}); vb.setValueCount(1);

            assertEquals(42L, VectorScalarCodec.read(b, 0));
            assertEquals(7L, VectorScalarCodec.read(i, 0));   // int widens to Long
            assertEquals(3.14, VectorScalarCodec.read(f, 0));
            assertEquals(true, VectorScalarCodec.read(bit, 0));
            assertEquals("hello", VectorScalarCodec.read(vc, 0));
            assertArrayEquals(new byte[] {1, 2, 3}, (byte[]) VectorScalarCodec.read(vb, 0));
        }
    }

    @Test
    void writeNullSetsNullBit() {
        try (BigIntVector b = new BigIntVector("b", alloc)) {
            b.allocateNew(1);
            VectorScalarCodec.write(b, 0, null);
            b.setValueCount(1);
            assertTrue(b.isNull(0));
            assertNull(VectorScalarCodec.read(b, 0));
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void roundtripsListOfBigInt() {
        Field item = new Field("item", new FieldType(true, new ArrowType.Int(64, true), null), null);
        Field listField = new Field("l", new FieldType(true, new ArrowType.List(), null), List.of(item));
        try (ListVector lv = (ListVector) listField.createVector(alloc)) {
            lv.allocateNew();
            VectorScalarCodec.write(lv, 0, List.of(10L, 20L, 30L));
            lv.setValueCount(1);
            List<Object> back = (List<Object>) VectorScalarCodec.read(lv, 0);
            assertEquals(List.of(10L, 20L, 30L), back);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void roundtripsStruct() {
        Field a = new Field("a", new FieldType(true, new ArrowType.Int(64, true), null), null);
        Field s = new Field("s", new FieldType(true, new ArrowType.Utf8(), null), null);
        Field struct = new Field("st",
                new FieldType(true, new ArrowType.Struct(), null),
                List.of(a, s));
        try (StructVector sv = (StructVector) struct.createVector(alloc)) {
            sv.allocateNew();
            // Pre-allocate child VarChar before writing — sv.allocateNew() may
            // not size the variable-width child for an immediate setSafe(0, …).
            sv.getChild("s").allocateNew();
            VectorScalarCodec.write(sv, 0, Map.of("a", 99L, "s", "x"));
            sv.setValueCount(1);
            Map<String, Object> back = (Map<String, Object>) VectorScalarCodec.read(sv, 0);
            assertEquals(99L, back.get("a"));
            assertEquals("x", back.get("s"));
        }
    }

    @Test
    void readStructIgnoresStructNullBit() {
        // Even when the struct itself is reported null, children are still
        // surfaced (matches ArgumentsParser semantics — DuckDB sometimes
        // leaves the outer struct null mask unset but writes valid children).
        Field a = new Field("a", new FieldType(true, new ArrowType.Int(64, true), null), null);
        Field struct = new Field("st",
                new FieldType(true, new ArrowType.Struct(), null),
                List.of(a));
        try (StructVector sv = (StructVector) struct.createVector(alloc)) {
            sv.allocateNew();
            // Don't call setIndexDefined — leave the struct null bit clear.
            ((BigIntVector) sv.getChild("a")).setSafe(0, 5L);
            sv.setValueCount(1);
            @SuppressWarnings("unchecked")
            Map<String, Object> back = (Map<String, Object>) VectorScalarCodec.read(sv, 0);
            assertEquals(5L, back.get("a"));
        }
    }

    @Test
    void integerWidthsUpcastToLongOnRead() {
        try (IntVector i = new IntVector("i", alloc)) {
            VectorScalarCodec.write(i, 0, 1000);
            i.setValueCount(1);
            Object back = VectorScalarCodec.read(i, 0);
            // Long, not Integer — settings/arguments downstream do `((Number)v).longValue()`.
            assertTrue(back instanceof Long, "expected Long, got " + back.getClass());
            assertEquals(1000L, back);
        }
    }

    @Test
    void writeVarCharAcceptsTextOrString() {
        try (VarCharVector vc = new VarCharVector("vc", alloc)) {
            VectorScalarCodec.write(vc, 0, "abc");
            VectorScalarCodec.write(vc, 1, new Text("def"));
            vc.setValueCount(2);
            assertEquals("abc", VectorScalarCodec.read(vc, 0));
            assertEquals("def", VectorScalarCodec.read(vc, 1));
        }
    }
}
