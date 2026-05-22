// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.internal;

import farm.query.vgi.CatalogDataVersionRelease;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.time.Instant;
import java.util.List;

/**
 * Serialises a CatalogInfo discovery record to wire bytes — a 1-row IPC stream
 * with schema {@code {name: utf8, implementation_version: utf8?,
 *  data_version_spec: utf8?, attach_option_specs: list<binary>,
 *  releases: list<struct<version: utf8, released_at: timestamp[us,UTC],
 *  summary: utf8, notes_url: utf8?>>, source_url: utf8?}}.
 *
 * <p>Field order and nullability must match the C++ {@code CatalogInfo} schema
 * exactly — a mismatch surfaces as "out-of-date Apache Arrow schema" at
 * {@code vgi_catalogs()}.</p>
 */
public final class CatalogInfoSerializer {

    private CatalogInfoSerializer() {}

    private static final ArrowType UTF8 = new ArrowType.Utf8();
    private static final ArrowType BINARY = new ArrowType.Binary();
    private static final ArrowType TS_US_UTC =
            new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC");

    // Child struct of the releases list. Only notes_url is nullable, matching
    // the C++ CatalogInfo schema.
    private static final Field RELEASE_STRUCT = new Field("item",
            new FieldType(true, new ArrowType.Struct(), null),
            List.of(
                    new Field("version", new FieldType(false, UTF8, null), null),
                    new Field("released_at", new FieldType(false, TS_US_UTC, null), null),
                    new Field("summary", new FieldType(false, UTF8, null), null),
                    new Field("notes_url", new FieldType(true, UTF8, null), null)));

    private static final Schema WIRE_SCHEMA = new Schema(List.of(
            new Field("name", new FieldType(false, UTF8, null), null),
            new Field("implementation_version", new FieldType(true, UTF8, null), null),
            new Field("data_version_spec", new FieldType(true, UTF8, null), null),
            new Field("attach_option_specs", new FieldType(false, new ArrowType.List(), null),
                    List.of(new Field("item", new FieldType(true, BINARY, null), null))),
            new Field("releases", new FieldType(false, new ArrowType.List(), null),
                    List.of(RELEASE_STRUCT)),
            new Field("source_url", new FieldType(true, UTF8, null), null)));

    public static byte[] serialize(String name, String implementationVersion,
                                     String dataVersionSpec, List<byte[]> attachOptions,
                                     List<CatalogDataVersionRelease> releases, String sourceUrl) {
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
            writeReleases((ListVector) root.getVector("releases"), releases);
            VarCharVector su = (VarCharVector) root.getVector("source_url");
            if (sourceUrl == null) su.setNull(0);
            else su.setSafe(0, new Text(sourceUrl));
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

    private static void writeReleases(ListVector lv, List<CatalogDataVersionRelease> releases) {
        StructVector struct = (StructVector) lv.getDataVector();
        VarCharVector version = (VarCharVector) struct.getChild("version");
        TimeStampMicroTZVector releasedAt = (TimeStampMicroTZVector) struct.getChild("released_at");
        VarCharVector summary = (VarCharVector) struct.getChild("summary");
        VarCharVector notesUrl = (VarCharVector) struct.getChild("notes_url");

        int n = releases == null ? 0 : releases.size();
        lv.startNewValue(0);
        for (int i = 0; i < n; i++) {
            CatalogDataVersionRelease r = releases.get(i);
            struct.setIndexDefined(i);
            version.setSafe(i, new Text(r.version()));
            releasedAt.setSafe(i, toMicros(r.releasedAt()));
            summary.setSafe(i, new Text(r.summary()));
            if (r.notesUrl() == null) notesUrl.setNull(i);
            else notesUrl.setSafe(i, new Text(r.notesUrl()));
        }
        struct.setValueCount(n);
        lv.endValue(0, n);
        lv.setValueCount(1);
    }

    private static long toMicros(Instant t) {
        return t.getEpochSecond() * 1_000_000L + t.getNano() / 1_000L;
    }
}
