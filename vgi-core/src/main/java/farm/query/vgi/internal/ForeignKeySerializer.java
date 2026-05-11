// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.internal;

import farm.query.vgi.Worker;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
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
 * Serialises a {@link Worker.CatalogTable.ForeignKey} to wire bytes (a 1-row
 * IPC stream with schema {@code {fk_columns: list<utf8>, pk_columns:
 * list<utf8>, referenced_table: utf8, referenced_schema: utf8}}). Mirrors
 * vgi-go's {@code serializeForeignKey}.
 */
public final class ForeignKeySerializer {

    private ForeignKeySerializer() {}

    private static final ArrowType UTF8 = new ArrowType.Utf8();

    private static final Schema WIRE_SCHEMA = new Schema(List.of(
            new Field("fk_columns", new FieldType(true, new ArrowType.List(), null),
                    List.of(new Field("item", new FieldType(true, UTF8, null), null))),
            new Field("pk_columns", new FieldType(true, new ArrowType.List(), null),
                    List.of(new Field("item", new FieldType(true, UTF8, null), null))),
            new Field("referenced_table", new FieldType(true, UTF8, null), null),
            new Field("referenced_schema", new FieldType(true, UTF8, null), null)));

    public static byte[] serialize(Worker.CatalogTable.ForeignKey fk) {
        try (VectorSchemaRoot root = VectorSchemaRoot.create(WIRE_SCHEMA, Allocators.root())) {
            root.allocateNew();
            writeStringList(root, "fk_columns", fk.fkColumns());
            writeStringList(root, "pk_columns", fk.pkColumns());
            ((VarCharVector) root.getVector("referenced_table"))
                    .setSafe(0, new Text(fk.referencedTable() == null ? "" : fk.referencedTable()));
            ((VarCharVector) root.getVector("referenced_schema"))
                    .setSafe(0, new Text(fk.referencedSchema() == null ? "" : fk.referencedSchema()));
            root.setRowCount(1);

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (ArrowStreamWriter w = new ArrowStreamWriter(root, null, Channels.newChannel(baos))) {
                w.start();
                w.writeBatch();
                w.end();
            }
            return baos.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException("ForeignKeySerializer.serialize", e);
        }
    }

    private static void writeStringList(VectorSchemaRoot root, String name, List<String> values) {
        ListVector lv = (ListVector) root.getVector(name);
        UnionListWriter w = lv.getWriter();
        w.setPosition(0);
        w.startList();
        for (String s : values) {
            byte[] bytes = s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            try (org.apache.arrow.memory.ArrowBuf buf = lv.getAllocator().buffer(bytes.length)) {
                buf.setBytes(0, bytes);
                w.writeVarChar(0, bytes.length, buf);
            }
        }
        w.endList();
    }
}
