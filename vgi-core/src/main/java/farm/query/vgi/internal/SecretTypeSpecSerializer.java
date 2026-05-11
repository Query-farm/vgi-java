// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.internal;

import farm.query.vgi.SecretTypeSpec;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.util.List;

/**
 * Serialises a {@link SecretTypeSpec} to the wire format DuckDB expects: a
 * 1-row IPC stream with schema
 * {@code {name: utf8, description: utf8, parameters_schema: binary}} where
 * {@code parameters_schema} is an IPC-encoded Arrow schema describing the
 * secret's parameters.
 */
public final class SecretTypeSpecSerializer {

    private SecretTypeSpecSerializer() {}

    private static final Schema WIRE_SCHEMA = new Schema(List.of(
            new Field("name", new FieldType(false, new ArrowType.Utf8(), null), null),
            new Field("description", new FieldType(false, new ArrowType.Utf8(), null), null),
            new Field("parameters_schema", new FieldType(false, new ArrowType.Binary(), null), null)));

    public static byte[] serialize(SecretTypeSpec spec) {
        byte[] paramSchemaBytes = SchemaUtil.serializeSchema(spec.parametersSchema());
        try (VectorSchemaRoot root = VectorSchemaRoot.create(WIRE_SCHEMA, Allocators.root())) {
            root.allocateNew();
            ((VarCharVector) root.getVector("name")).setSafe(0, new Text(spec.name()));
            ((VarCharVector) root.getVector("description")).setSafe(0, new Text(spec.description()));
            ((VarBinaryVector) root.getVector("parameters_schema")).setSafe(0, paramSchemaBytes);
            root.setRowCount(1);

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (ArrowStreamWriter w = new ArrowStreamWriter(root, null, Channels.newChannel(baos))) {
                w.start();
                w.writeBatch();
                w.end();
            }
            return baos.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException("serialize SecretTypeSpec failed", e);
        }
    }
}
