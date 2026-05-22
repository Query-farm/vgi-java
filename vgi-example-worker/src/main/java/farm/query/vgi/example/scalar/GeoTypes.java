// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.example.scalar;

import farm.query.vgi.internal.SchemaUtil;
import farm.query.vgi.types.Schemas;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

/** Shared Arrow types for the geo_* fixture family. */
final class GeoTypes {
    private GeoTypes() {}

    static final FieldType F64_NULLABLE = new FieldType(true, Schemas.FLOAT64, null);

    /** {@code STRUCT(lat DOUBLE, lon DOUBLE)} — argument and result element type. */
    static final Field POINT_STRUCT_FIELD = new Field(
            "result",
            new FieldType(true, ArrowType.Struct.INSTANCE, null),
            List.of(
                    new Field("lat", F64_NULLABLE, null),
                    new Field("lon", F64_NULLABLE, null)));

    /** Output schema for the centroid functions: single nullable struct column. */
    static final Schema CENTROID_OUTPUT_SCHEMA = new Schema(List.of(POINT_STRUCT_FIELD));

    static final byte[] CENTROID_OUTPUT_SCHEMA_IPC = SchemaUtil.serializeSchema(CENTROID_OUTPUT_SCHEMA);

    /** Argument type marker — DuckDB binds any compatible struct/list/fixed-list. */
    static ArrowType structArgType() { return ArrowType.Struct.INSTANCE; }
    static ArrowType listArgType() { return new ArrowType.List(); }
    static ArrowType fixedListArgType() { return new ArrowType.FixedSizeList(2); }

    /** Children for a {@code STRUCT(lat DOUBLE, lon DOUBLE)} arg field. */
    static List<Field> structPointChildren() {
        return List.of(
                new Field("lat", F64_NULLABLE, null),
                new Field("lon", F64_NULLABLE, null));
    }

    /** Children for a {@code LIST<DOUBLE>} or {@code DOUBLE[2]} arg field. */
    static List<Field> doubleElementChildren() {
        return List.of(new Field("item", F64_NULLABLE, null));
    }

    static double euclidean(double lat1, double lon1, double lat2, double lon2) {
        double dlat = lat2 - lat1;
        double dlon = lon2 - lon1;
        return Math.sqrt(dlat * dlat + dlon * dlon);
    }
}
