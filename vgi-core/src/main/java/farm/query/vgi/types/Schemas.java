// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.types;

import farm.query.vgi.internal.SchemaUtil;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

/**
 * Convenience builders for {@link Schema}s and their IPC byte encodings —
 * primarily used by scalar functions that return a single fixed-type column.
 */
public final class Schemas {

    public static final ArrowType INT64 = new ArrowType.Int(64, true);
    public static final ArrowType INT32 = new ArrowType.Int(32, true);
    public static final ArrowType UINT32 = new ArrowType.Int(32, false);
    public static final ArrowType UINT64 = new ArrowType.Int(64, false);
    public static final ArrowType FLOAT64 = new ArrowType.FloatingPoint(
            org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE);
    public static final ArrowType UTF8 = new ArrowType.Utf8();
    public static final ArrowType BOOL = new ArrowType.Bool();
    public static final ArrowType BINARY = new ArrowType.Binary();

    private Schemas() {}

    /** Single-column nullable {@code result} schema. */
    public static Schema singleResult(ArrowType t) {
        return new Schema(List.of(new Field("result", new FieldType(true, t, null), null)));
    }

    public static byte[] singleResultIpc(ArrowType t) {
        return SchemaUtil.serializeSchema(singleResult(t));
    }
}
