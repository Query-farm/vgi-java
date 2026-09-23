// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.TransferPair;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * An emitted batch must keep the extension tags declared at bind. Arrow's
 * {@code getTransferPair} overloads each drop them for some vector type — the
 * allocator overload for a top-level {@code FixedSizeBinary} (DuckDB's UUID),
 * the {@code Field} overload for a struct's children — and the DuckDB extension
 * refuses a column whose type no longer matches its declaration.
 */
class VectorProjectorTest {

    private static final Map<String, String> UUID_TAG = Map.of("ARROW:extension:name", "arrow.uuid");

    private static Field uuid(String name) {
        return new Field(name, new FieldType(true, new ArrowType.FixedSizeBinary(16), null, UUID_TAG), null);
    }

    private static final Schema SCHEMA = new Schema(List.of(
            uuid("u"),
            new Field("s", FieldType.nullable(ArrowType.Struct.INSTANCE), List.of(uuid("inner")))));

    private static final byte[] BYTES = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};

    private static VectorSchemaRoot filled() {
        VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, Allocators.root());
        root.allocateNew();
        ((FixedSizeBinaryVector) root.getVector("u")).setSafe(0, BYTES);
        StructVector s = (StructVector) root.getVector("s");
        s.setIndexDefined(0);
        ((FixedSizeBinaryVector) s.getChild("inner")).setSafe(0, BYTES);
        root.setRowCount(1);
        return root;
    }

    /** The premise: the hand-rolled loop this helper replaced loses the tag. */
    @Test
    void arrowsAllocatorTransferPairDropsTheUuidTag() {
        try (VectorSchemaRoot src = filled();
                FieldVector to = (FieldVector) src.getVector("u").getTransferPair(Allocators.root()).getTo()) {
            assertNotEquals(uuid("u"), to.getField());
        }
    }

    @Test
    void detachKeepsTopLevelAndNestedExtensionTags() {
        try (VectorSchemaRoot src = filled(); VectorSchemaRoot dst = VectorProjector.detach(src, null)) {
            assertEquals(SCHEMA, dst.getSchema());
            assertEquals(1, dst.getRowCount());
            assertArrayEquals(BYTES, ((FixedSizeBinaryVector) dst.getVector("u")).get(0));
            StructVector s = (StructVector) dst.getVector("s");
            assertArrayEquals(BYTES, ((FixedSizeBinaryVector) s.getChild("inner")).get(0));
        }
    }

    /**
     * Fields come from the schema, not the vectors: a transport that already
     * detached the batch may hand over vectors whose own field lost the tag.
     */
    @Test
    void detachTakesFieldsFromTheTargetSchemaNotTheVectors() {
        try (VectorSchemaRoot filled = filled()) {
            TransferPair tp = filled.getVector("u").getTransferPair(Allocators.root());
            tp.transfer();
            FieldVector stripped = (FieldVector) tp.getTo();
            try (VectorSchemaRoot src = new VectorSchemaRoot(List.of(stripped))) {
                src.setRowCount(1);
                try (VectorSchemaRoot dst = VectorProjector.detach(src, new Schema(List.of(uuid("u"))))) {
                    assertEquals(uuid("u"), dst.getVector("u").getField());
                    assertArrayEquals(BYTES, ((FixedSizeBinaryVector) dst.getVector("u")).get(0));
                }
            }
        }
    }

    @Test
    void detachNarrowsByName() {
        Schema onlyStruct = new Schema(List.of(SCHEMA.findField("s")));
        try (VectorSchemaRoot src = filled(); VectorSchemaRoot dst = VectorProjector.detach(src, onlyStruct)) {
            assertEquals(onlyStruct, dst.getSchema());
        }
    }

    @Test
    void detachNamesAMissingColumn() {
        Schema missing = new Schema(List.of(uuid("u"), uuid("absent")));
        try (VectorSchemaRoot src = filled()) {
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> VectorProjector.detach(src, missing));
            assertTrue(e.getMessage().contains("'absent'"), e.getMessage());
        }
    }

    /**
     * A DuckDB ENUM arrives dictionary-encoded: the schema calls the column
     * {@code Utf8}, the vector holds {@code UInt1} indices. The target must be
     * an index vector, still carrying the encoding.
     */
    @Test
    void detachKeepsADictionaryEncodedColumnAsIndices() {
        DictionaryEncoding enc = new DictionaryEncoding(7L, false, new ArrowType.Int(8, false));
        Field message = new Field("e", new FieldType(true, ArrowType.Utf8.INSTANCE, enc), null);
        Field memory = new Field("e", new FieldType(true, enc.getIndexType(), enc), null);
        try (UInt1Vector indices = (UInt1Vector) memory.createVector(Allocators.root())) {
            indices.allocateNew(2);
            indices.set(0, 1);
            indices.set(1, 0);
            indices.setValueCount(2);
            try (VectorSchemaRoot src = new VectorSchemaRoot(new Schema(List.of(message)), List.of(indices), 2);
                    VectorSchemaRoot dst = VectorProjector.detach(src, null)) {
                FieldVector out = dst.getVector("e");
                assertTrue(out instanceof UInt1Vector, out.getClass().getName());
                assertEquals(enc, out.getField().getDictionary());
                assertEquals(1, ((UInt1Vector) out).get(0));
                assertEquals(0, ((UInt1Vector) out).get(1));
            }
        }
    }

    @Test
    void relabelKeepsANestedTag() {
        Schema renamed = new Schema(List.of(uuid("u2"),
                new Field("s2", FieldType.nullable(ArrowType.Struct.INSTANCE), List.of(uuid("inner")))));
        try (VectorSchemaRoot dst = VectorProjector.relabel(filled(), renamed)) {
            assertEquals(renamed, dst.getSchema());
        }
    }
}
