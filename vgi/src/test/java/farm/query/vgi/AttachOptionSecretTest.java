// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi;

import farm.query.vgi.internal.AttachOptionSpecSerializer;
import farm.query.vgi.internal.BatchUtil;
import farm.query.vgi.internal.SchemaUtil;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The {@code secret} flag: declaration, wire round trip, and older peers. */
class AttachOptionSecretTest {

    private static final ArrowType UTF8 = new ArrowType.Utf8();

    @Test
    void secretRoundTripsTrue() {
        var d = AttachOptionSpecSerializer.decode(AttachOptionSpecSerializer.serialize(
                AttachOptionSpec.of("token", "Access token", UTF8, null).asSecret()));
        assertEquals("token", d.name());
        assertTrue(d.secret());
        assertFalse(d.required());
    }

    @Test
    void secretRoundTripsFalse() {
        var spec = AttachOptionSpec.of("region", "AWS region", UTF8, "us-east-1");
        assertFalse(spec.secret());
        var d = AttachOptionSpecSerializer.decode(AttachOptionSpecSerializer.serialize(spec));
        assertFalse(d.secret());
        assertFalse(d.required());
        assertNotNull(d.defaultValue());
    }

    /** The column is written explicitly, after {@code required}. */
    @Test
    void secretColumnIsAppendedAfterRequired() {
        byte[] bytes = AttachOptionSpecSerializer.serialize(
                AttachOptionSpec.requiredSecret("api_key", "API key", UTF8));
        List<String> names = BatchUtil.withReadBatch(bytes, Allocators.root(), root -> {
            List<String> out = new ArrayList<>();
            root.getSchema().getFields().forEach(f -> out.add(f.getName()));
            assertFalse(root.getVector("secret").isNull(0));
            return out;
        });
        assertEquals(List.of("name", "description", "type", "default_value", "required", "secret"), names);
    }

    @Test
    void secretCombinesWithRequired() {
        var spec = AttachOptionSpec.requiredSecret("api_key", "API key", UTF8);
        assertTrue(spec.required());
        assertTrue(spec.secret());
        assertNull(spec.defaultVector());
        var d = AttachOptionSpecSerializer.decode(AttachOptionSpecSerializer.serialize(spec));
        assertTrue(d.required());
        assertTrue(d.secret());
        assertNull(d.defaultValue());

        var viaWither = AttachOptionSpec.required("api_key", "API key", UTF8).asSecret();
        assertEquals(spec.required(), viaWither.required());
        assertEquals(spec.secret(), viaWither.secret());
    }

    /** Allowed, though a secret option normally has no default. */
    @Test
    void secretWithDefaultIsAllowed() {
        var spec = AttachOptionSpec.of("token", "Token", UTF8, "dev-token").asSecret();
        assertTrue(spec.secret());
        var d = AttachOptionSpecSerializer.decode(AttachOptionSpecSerializer.serialize(spec));
        assertTrue(d.secret());
        assertNotNull(d.defaultValue());
    }

    /** The pre-{@code secret} constructor still builds a non-secret spec. */
    @Test
    void fiveArgConstructorIsNotSecret() {
        var base = AttachOptionSpec.required("api_key", "API key", UTF8);
        var spec = new AttachOptionSpec(base.name(), base.description(), base.valueField(), null, true);
        assertFalse(spec.secret());
    }

    /** A peer that predates the column: absent reads as false. */
    @Test
    void batchWithoutSecretColumnReadsFalse() {
        var d = AttachOptionSpecSerializer.decode(legacyBatch(true, true));
        assertEquals("api_key", d.name());
        assertTrue(d.required());
        assertFalse(d.secret());
    }

    /** A peer that predates both flags. */
    @Test
    void batchWithoutRequiredOrSecretReadsFalse() {
        var d = AttachOptionSpecSerializer.decode(legacyBatch(false, false));
        assertFalse(d.required());
        assertFalse(d.secret());
    }

    /** An explicit null reads as false too. */
    @Test
    void nullSecretReadsFalse() {
        var d = AttachOptionSpecSerializer.decode(legacyBatch(true, false, true));
        assertFalse(d.secret());
    }

    private static byte[] legacyBatch(boolean withRequired, boolean required) {
        return legacyBatch(withRequired, required, false);
    }

    /** A spec batch without the {@code secret} column (or with it null). */
    private static byte[] legacyBatch(boolean withRequired, boolean required, boolean nullSecret) {
        List<Field> fields = new ArrayList<>(List.of(
                new Field("name", new FieldType(false, UTF8, null), null),
                new Field("description", new FieldType(false, UTF8, null), null),
                new Field("type", new FieldType(false, new ArrowType.Binary(), null), null),
                new Field("default_value", new FieldType(true, new ArrowType.Binary(), null), null)));
        if (withRequired) fields.add(new Field("required", new FieldType(true, new ArrowType.Bool(), null), null));
        if (nullSecret) fields.add(new Field("secret", new FieldType(true, new ArrowType.Bool(), null), null));
        byte[] typeBytes = SchemaUtil.serializeSchema(new Schema(List.of(
                new Field("value", new FieldType(true, UTF8, null), List.of()))));
        try (VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(fields), Allocators.root())) {
            root.allocateNew();
            ((VarCharVector) root.getVector("name")).setSafe(0, new Text("api_key"));
            ((VarCharVector) root.getVector("description")).setSafe(0, new Text("API key"));
            ((VarBinaryVector) root.getVector("type")).setSafe(0, typeBytes);
            ((VarBinaryVector) root.getVector("default_value")).setNull(0);
            if (withRequired) ((BitVector) root.getVector("required")).setSafe(0, required ? 1 : 0);
            if (nullSecret) ((BitVector) root.getVector("secret")).setNull(0);
            root.setRowCount(1);
            return BatchUtil.writeSingleBatch(root);
        }
    }
}
