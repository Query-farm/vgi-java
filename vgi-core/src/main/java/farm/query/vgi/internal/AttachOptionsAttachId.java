// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.internal;

import farm.query.vgi.AttachOptionSpec;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.TransferPair;

import java.io.ByteArrayOutputStream;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Encodes ATTACH-time option values into the returned {@code attach_id} so
 * the echo function is stateless under pool reuse / HTTP transport.
 *
 * <p>Wire format: {@code uuid(16) || 0x00 || ipc(mergedBatch)}.
 * {@code mergedBatch} is a one-row record batch with one column per declared
 * spec (declaration order). User-supplied values override the spec's default;
 * missing values fall back to the default.
 */
public final class AttachOptionsAttachId {

    private static final byte SEP = 0x00;
    private static final int UUID_BYTES = 16;

    private AttachOptionsAttachId() {}

    public static byte[] encode(List<AttachOptionSpec> specs, byte[] userOptionsIpc, SecureRandom rng) {
        BufferAllocator alloc = Allocators.root();

        // Build the merged-batch schema: one column per spec, in declaration order.
        List<Field> mergedFields = new ArrayList<>(specs.size());
        for (AttachOptionSpec spec : specs) {
            mergedFields.add(new Field(spec.name(),
                    new FieldType(true, spec.type(), null),
                    spec.children() == null ? List.of() : spec.children()));
        }
        Schema mergedSchema = new Schema(mergedFields);

        // Read user-supplied columns (if any) into a name → vector map.
        Map<String, FieldVector> userCols = new HashMap<>();
        VectorSchemaRoot userRoot = null;
        if (userOptionsIpc != null && userOptionsIpc.length > 0) {
            userRoot = BatchUtil.readSingleBatch(userOptionsIpc, alloc);
            if (userRoot != null) {
                for (FieldVector fv : userRoot.getFieldVectors()) {
                    userCols.put(fv.getField().getName(), fv);
                }
            }
        }
        try (VectorSchemaRoot mergedRoot = VectorSchemaRoot.create(mergedSchema, alloc)) {
            mergedRoot.allocateNew();
            for (int i = 0; i < specs.size(); i++) {
                AttachOptionSpec spec = specs.get(i);
                FieldVector mergedVec = mergedRoot.getVector(i);
                FieldVector userVec = userCols.get(spec.name());
                if (userVec != null && userVec.getValueCount() > 0 && !userVec.isNull(0)) {
                    // Copy user value at row 0 → merged at row 0.
                    TransferPair tp = userVec.makeTransferPair(mergedVec);
                    tp.copyValueSafe(0, 0);
                } else if (spec.defaultValue() != null) {
                    AttachOptionValueCodec.writeValue(mergedVec, 0, spec.defaultValue());
                } else {
                    mergedVec.setNull(0);
                }
            }
            mergedRoot.setRowCount(1);
            byte[] ipcBytes = BatchUtil.writeSingleBatch(mergedRoot);

            byte[] uuid = new byte[UUID_BYTES];
            rng.nextBytes(uuid);
            byte[] attachId = new byte[UUID_BYTES + 1 + ipcBytes.length];
            System.arraycopy(uuid, 0, attachId, 0, UUID_BYTES);
            attachId[UUID_BYTES] = SEP;
            System.arraycopy(ipcBytes, 0, attachId, UUID_BYTES + 1, ipcBytes.length);
            return attachId;
        } finally {
            if (userRoot != null) userRoot.close();
        }
    }

    /** Recover the merged one-row record batch IPC bytes from an attach_id. */
    public static byte[] extractBatchIpc(byte[] attachId) {
        if (attachId == null
                || attachId.length <= UUID_BYTES + 1
                || attachId[UUID_BYTES] != SEP) {
            throw new IllegalArgumentException("attach_id does not carry options payload");
        }
        byte[] out = new byte[attachId.length - UUID_BYTES - 1];
        System.arraycopy(attachId, UUID_BYTES + 1, out, 0, out.length);
        return out;
    }

    /** Read the merged record batch out of an attach_id; caller closes the root. */
    public static VectorSchemaRoot decode(byte[] attachId, BufferAllocator alloc) {
        return BatchUtil.readSingleBatch(extractBatchIpc(attachId), alloc);
    }
}
