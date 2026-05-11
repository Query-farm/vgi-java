// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.internal;

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
 * Serialises a CatalogInfo discovery record (name + version specs + attach
 * options) to wire bytes — a 1-row IPC stream with schema
 * {@code {name: utf8, implementation_version: utf8?, data_version_spec: utf8?,
 *  attach_option_specs: list<binary>}}.
 */
public final class CatalogInfoSerializer {

    private CatalogInfoSerializer() {}

    private static final ArrowType UTF8 = new ArrowType.Utf8();
    private static final ArrowType BINARY = new ArrowType.Binary();

    private static final Schema WIRE_SCHEMA = new Schema(List.of(
            new Field("name", new FieldType(false, UTF8, null), null),
            new Field("implementation_version", new FieldType(true, UTF8, null), null),
            new Field("data_version_spec", new FieldType(true, UTF8, null), null),
            new Field("attach_option_specs", new FieldType(false, new ArrowType.List(), null),
                    List.of(new Field("item", new FieldType(true, BINARY, null), null)))));

    public static byte[] serialize(String name, String implementationVersion,
                                     String dataVersionSpec, List<byte[]> attachOptions) {
        try (VectorSchemaRoot root = VectorSchemaRoot.create(WIRE_SCHEMA, Allocators.root())) {
            root.allocateNew();
            ((VarCharVector) root.getVector("name")).setSafe(0, new Text(name));
            VarCharVector iv = (VarCharVector) root.getVector("implementation_version");
            if (implementationVersion == null) iv.setNull(0);
            else iv.setSafe(0, new Text(implementationVersion));
            VarCharVector dv = (VarCharVector) root.getVector("data_version_spec");
            if (dataVersionSpec == null) dv.setNull(0);
            else dv.setSafe(0, new Text(dataVersionSpec));
            ListVector lv = (ListVector) root.getVector("attach_option_specs");
            UnionListWriter w = lv.getWriter();
            w.setPosition(0);
            w.startList();
            if (attachOptions != null) {
                for (byte[] b : attachOptions) {
                    try (org.apache.arrow.memory.ArrowBuf buf = lv.getAllocator().buffer(b.length)) {
                        buf.setBytes(0, b);
                        w.writeVarBinary(0, b.length, buf);
                    }
                }
            }
            w.endList();
            root.setRowCount(1);

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (ArrowStreamWriter sw = new ArrowStreamWriter(root, null, Channels.newChannel(baos))) {
                sw.start();
                sw.writeBatch();
                sw.end();
            }
            return baos.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException("CatalogInfoSerializer.serialize", e);
        }
    }
}
