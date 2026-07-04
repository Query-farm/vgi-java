// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import farm.query.vgi.catalog.ScanBranch;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.VarBinaryVector;
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
 * Encodes a {@code ScanBranchesResult} as the wire bytes returned by
 * {@code catalog_table_scan_branches_get}. Outer 1-row IPC stream
 * {@code {branches: list<binary>, required_extensions: list<utf8>}}; each
 * branch entry is itself a 1-row IPC stream
 * {@code {function_name: utf8, arguments: binary, branch_filter: utf8?,
 *  writable: bool}}. Field order/nullability matches the C++
 * {@code ScanBranchSchema}/{@code ScanBranchesResultSchema}.
 */
public final class ScanBranchesResultSerializer {

    private ScanBranchesResultSerializer() {}

    private static final ArrowType UTF8 = new ArrowType.Utf8();
    private static final ArrowType BINARY = new ArrowType.Binary();
    private static final ArrowType BOOL = new ArrowType.Bool();

    private static final Schema BRANCH_SCHEMA = new Schema(List.of(
            new Field("function_name", new FieldType(false, UTF8, null), null),
            new Field("arguments", new FieldType(false, BINARY, null), null),
            new Field("branch_filter", new FieldType(true, UTF8, null), null),
            new Field("writable", new FieldType(false, BOOL, null), null),
            new Field("source_catalog", new FieldType(true, UTF8, null), null),
            new Field("source_schema", new FieldType(true, UTF8, null), null),
            new Field("source_table", new FieldType(true, UTF8, null), null)));

    private static final Schema RESULT_SCHEMA = new Schema(List.of(
            new Field("branches", new FieldType(false, new ArrowType.List(), null),
                    List.of(new Field("item", new FieldType(true, BINARY, null), null))),
            new Field("required_extensions", new FieldType(false, new ArrowType.List(), null),
                    List.of(new Field("item", new FieldType(true, UTF8, null), null)))));

    /**
     * Encode a multi-branch scan result for {@code catalog_table_scan_branches_get}.
     *
     * @param branches the scan branches, each nested-encoded as its own 1-row IPC stream
     * @param requiredExtensions DuckDB extension names the branches depend on (may be {@code null})
     * @return the outer 1-row IPC stream bytes
     */
    public static byte[] serialize(List<ScanBranch> branches, List<String> requiredExtensions) {
        BufferAllocator alloc = Allocators.root();
        try (VectorSchemaRoot root = VectorSchemaRoot.create(RESULT_SCHEMA, alloc)) {
            root.allocateNew();
            ListVector branchList = (ListVector) root.getVector("branches");
            UnionListWriter bw = branchList.getWriter();
            bw.setPosition(0);
            bw.startList();
            for (ScanBranch b : branches) {
                byte[] branchBytes = encodeBranch(b, alloc);
                try (org.apache.arrow.memory.ArrowBuf buf = alloc.buffer(branchBytes.length)) {
                    buf.setBytes(0, branchBytes);
                    bw.writeVarBinary(0, branchBytes.length, buf);
                }
            }
            bw.endList();

            ListVector extList = (ListVector) root.getVector("required_extensions");
            UnionListWriter ew = extList.getWriter();
            ew.setPosition(0);
            ew.startList();
            if (requiredExtensions != null) {
                for (String s : requiredExtensions) if (s != null) ew.varChar().writeVarChar(s);
            }
            ew.endList();

            root.setRowCount(1);
            return writeStream(root);
        } catch (Exception e) {
            throw new RuntimeException("ScanBranchesResultSerializer.serialize", e);
        }
    }

    private static byte[] encodeBranch(ScanBranch b, BufferAllocator alloc) {
        byte[] argsBytes = ScanFunctionResultEncoder.encodeArguments(b.positional(), b.named());
        try (VectorSchemaRoot root = VectorSchemaRoot.create(BRANCH_SCHEMA, alloc)) {
            root.allocateNew();
            ((VarCharVector) root.getVector("function_name")).setSafe(0, new Text(b.functionName()));
            ((VarBinaryVector) root.getVector("arguments")).setSafe(0, argsBytes);
            VarCharVector bf = (VarCharVector) root.getVector("branch_filter");
            if (b.branchFilter() == null) bf.setNull(0);
            else bf.setSafe(0, new Text(b.branchFilter()));
            ((BitVector) root.getVector("writable")).setSafe(0, b.writable() ? 1 : 0);
            setNullableString(root, "source_catalog", b.sourceCatalog());
            setNullableString(root, "source_schema", b.sourceSchema());
            setNullableString(root, "source_table", b.sourceTable());
            root.setRowCount(1);
            return writeStream(root);
        } catch (Exception e) {
            throw new RuntimeException("ScanBranchesResultSerializer.encodeBranch", e);
        }
    }

    private static void setNullableString(VectorSchemaRoot root, String col, String value) {
        VarCharVector v = (VarCharVector) root.getVector(col);
        if (value == null) v.setNull(0);
        else v.setSafe(0, new Text(value));
    }

    private static byte[] writeStream(VectorSchemaRoot root) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ArrowStreamWriter sw = new ArrowStreamWriter(root, null, Channels.newChannel(baos))) {
            sw.start();
            sw.writeBatch();
            sw.end();
        }
        return baos.toByteArray();
    }
}
