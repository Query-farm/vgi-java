// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.internal;

import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Builds the per-batch {@code custom_metadata} that the C++ extension reads off
 * each emitted Arrow batch's {@code KeyValueMetadata}:
 *
 * <ul>
 *   <li>{@code vgi_batch_index} — decimal-string partition id for
 *       {@code supports_batch_index} table functions; the extension threads it
 *       through {@code TableFunction::get_partition_data} so ordered sinks
 *       reassemble parallel output in partition order.</li>
 *   <li>{@code vgi_partition_values#b64} — base64 of an IPC stream holding a
 *       2-row {@code (min, max)} RecordBatch over the function's
 *       {@code vgi.partition_column}-annotated fields; the extension's
 *       {@code get_partition_info} consumes it for Hive-style partitioning.</li>
 * </ul>
 *
 * <p>Mirrors vgi-python's {@code _merge_batch_index} / {@code _merge_partition_values}
 * in {@code vgi/protocol.py}.
 */
public final class EmitMetadata {

    /** Field metadata key marking an output-schema field as a partition column. */
    public static final String PARTITION_COLUMN_KEY = "vgi.partition_column";

    private EmitMetadata() {}

    /** A resolved {@code (min, max)} pair for one partition column. */
    public record Range(Object min, Object max) {}

    /** Build an output-schema {@link Field} marked as a VGI partition column —
     *  the worker-side equivalent of vgi-python's {@code partition_field()}. */
    public static Field partitionField(String name, org.apache.arrow.vector.types.pojo.ArrowType type) {
        return new Field(name,
                new org.apache.arrow.vector.types.pojo.FieldType(
                        true, type, null, Map.of(PARTITION_COLUMN_KEY, "true")),
                null);
    }

    /** Per-batch metadata for {@code supports_batch_index} functions. */
    public static Map<String, String> batchIndex(long index) {
        return Map.of("vgi_batch_index", Long.toString(index));
    }

    /**
     * Build the {@code vgi_partition_values#b64} metadata for a batch emitted by
     * a partition-declaring function.
     *
     * @param declaredSchema the function's bind/output schema — partition columns
     *                       are its fields carrying {@code vgi.partition_column=true}
     * @param emittedRoot    the batch about to be emitted (auto-extract source)
     * @param explicit       optional per-column {@code (min, max)} overrides keyed
     *                       by column name; {@code null} for pure auto-extract
     * @return the metadata map, or {@code null} when there is nothing to emit
     *         (no partition fields, or a 0-row batch)
     * @throws IllegalArgumentException on a contract violation the worker must
     *         catch before the wire (mirrors the Python framework's raises)
     */
    public static Map<String, String> partitionValues(Schema declaredSchema,
                                                        VectorSchemaRoot emittedRoot,
                                                        Map<String, Range> explicit) {
        List<Field> partitionFields = new ArrayList<>();
        for (Field f : declaredSchema.getFields()) {
            Map<String, String> md = f.getMetadata();
            if (md != null && "true".equals(md.get(PARTITION_COLUMN_KEY))) {
                partitionFields.add(f);
            }
        }
        if (partitionFields.isEmpty()) {
            if (explicit != null) {
                throw new IllegalArgumentException(
                        "out.emit(partition_values=...) requires partition-annotated fields "
                        + "in the bind schema");
            }
            return null;
        }
        // Empty batches are exempt — the C++ side skips its requirement check.
        if (emittedRoot.getRowCount() == 0) return null;

        Map<String, Range> resolved = new LinkedHashMap<>();
        for (Field f : partitionFields) {
            resolved.put(f.getName(), resolveRange(f, emittedRoot, explicit));
        }

        byte[] ipc = buildValuesBatch(partitionFields, resolved);
        Map<String, String> merged = new LinkedHashMap<>();
        merged.put("vgi_partition_values#b64", Base64.getEncoder().encodeToString(ipc));
        return merged;
    }

    private static Range resolveRange(Field field, VectorSchemaRoot root,
                                        Map<String, Range> explicit) {
        if (explicit != null && explicit.containsKey(field.getName())) {
            return explicit.get(field.getName());
        }
        FieldVector vec = root.getVector(field.getName());
        if (vec == null) {
            throw new IllegalArgumentException(
                    "column '" + field.getName() + "' is partition-annotated but absent "
                    + "from emitted batch; pass an explicit partition_values override");
        }
        Comparable<Object> min = null;
        Comparable<Object> max = null;
        for (int i = 0; i < vec.getValueCount(); i++) {
            if (vec.isNull(i)) continue;
            Object o = vec.getObject(i);
            @SuppressWarnings("unchecked")
            Comparable<Object> c = (Comparable<Object>) normalize(o);
            if (min == null || c.compareTo(min) < 0) min = c;
            if (max == null || c.compareTo(max) > 0) max = c;
        }
        return new Range(min, max);
    }

    private static Object normalize(Object o) {
        // VarCharVector.getObject returns Arrow's Text; normalise to String so
        // both auto-extracted and explicitly-supplied values share a type.
        if (o instanceof org.apache.arrow.vector.util.Text t) return t.toString();
        return o;
    }

    private static byte[] buildValuesBatch(List<Field> fields, Map<String, Range> resolved) {
        Schema schema = new Schema(fields);
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, Allocators.root())) {
            root.allocateNew();
            for (Field f : fields) {
                FieldVector v = root.getVector(f.getName());
                Range r = resolved.get(f.getName());
                setScalar(v, 0, r.min());
                setScalar(v, 1, r.max());
                v.setValueCount(2);
            }
            root.setRowCount(2);
            return BatchUtil.writeSingleBatch(root);
        }
    }

    private static void setScalar(FieldVector v, int row, Object value) {
        if (value == null) { v.setNull(row); return; }
        if (v instanceof BigIntVector bi) {
            bi.setSafe(row, ((Number) value).longValue());
        } else if (v instanceof Float8Vector f8) {
            f8.setSafe(row, ((Number) value).doubleValue());
        } else if (v instanceof VarCharVector vc) {
            vc.setSafe(row, value.toString().getBytes(StandardCharsets.UTF_8));
        } else {
            throw new IllegalArgumentException(
                    "EmitMetadata: unsupported partition column type " + v.getClass().getSimpleName());
        }
    }
}
