// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.types;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;

/**
 * DuckDB-compatible numeric type promotion rules.
 *
 * <p>Mirrors {@code vgi-go vgi.PromoteForAddition}: scalar-arithmetic functions
 * promote integer inputs one width up (TINYINT → SMALLINT → INT → BIGINT) and
 * leave floats alone. Mixed int/float promotes to float.
 */
public final class TypeRules {

    private TypeRules() {}

    /**
     * @param t an Arrow type
     * @return {@code true} if {@code t} is an integer type
     */
    public static boolean isInteger(ArrowType t) { return t instanceof ArrowType.Int; }

    /**
     * @param t an Arrow type
     * @return {@code true} if {@code t} is a floating-point type
     */
    public static boolean isFloating(ArrowType t) { return t instanceof ArrowType.FloatingPoint; }

    /**
     * @param t an Arrow type
     * @return {@code true} if {@code t} is an integer or floating-point type
     */
    public static boolean isNumeric(ArrowType t) { return isInteger(t) || isFloating(t); }

    /**
     * @param t an Arrow type
     * @return {@code true} if {@code t} can participate in arithmetic addition (numeric or decimal)
     */
    public static boolean isAddable(ArrowType t) {
        return isNumeric(t) || t instanceof ArrowType.Decimal;
    }

    /**
     * Promote a single input type one width up (for {@code double(value)}-style
     * arithmetic). Floats stay; ints climb the tier.
     *
     * @param t the input type to promote
     * @return the promoted type ({@code INT64} for non-numeric inputs)
     */
    public static ArrowType promoteForAddition(ArrowType t) {
        if (t instanceof ArrowType.Int i) {
            int next = Math.min(64, i.getBitWidth() * 2);
            return new ArrowType.Int(next, i.getIsSigned());
        }
        // FLOAT → DOUBLE for overflow safety; DOUBLE stays DOUBLE. Mirrors vgi-go.
        if (isFloating(t)) {
            return new ArrowType.FloatingPoint(
                    org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE);
        }
        // Decimal: keep the same type. Callers do value+value, which the SQL
        // rule says yields decimal(p+1, s); since p is often already 38 (the
        // cap), staying within the same precision relies on the operation
        // being effectively "double" rather than "multiply".
        if (t instanceof ArrowType.Decimal) {
            return t;
        }
        return Schemas.INT64;
    }

    /**
     * Render an Arrow type as the SQL name DuckDB users recognise
     * (e.g. {@code BIGINT}, {@code VARCHAR}, {@code DECIMAL(38,2)}). Falls back
     * to Arrow's {@code toString} for types without a clean SQL equivalent.
     *
     * @param t the Arrow type to render
     * @return the DuckDB SQL type name
     */
    public static String sqlTypeName(ArrowType t) {
        if (t instanceof ArrowType.Int i) {
            int w = i.getBitWidth();
            boolean s = i.getIsSigned();
            if (s) {
                return switch (w) { case 8 -> "TINYINT"; case 16 -> "SMALLINT";
                    case 32 -> "INTEGER"; case 64 -> "BIGINT"; default -> i.toString(); };
            }
            return switch (w) { case 8 -> "UTINYINT"; case 16 -> "USMALLINT";
                case 32 -> "UINTEGER"; case 64 -> "UBIGINT"; default -> i.toString(); };
        }
        if (t instanceof ArrowType.FloatingPoint f) {
            return switch (f.getPrecision()) {
                case HALF -> "HALF"; case SINGLE -> "FLOAT"; case DOUBLE -> "DOUBLE"; };
        }
        if (t instanceof ArrowType.Decimal d) {
            return "DECIMAL(" + d.getPrecision() + "," + d.getScale() + ")";
        }
        if (t instanceof ArrowType.Utf8) return "VARCHAR";
        if (t instanceof ArrowType.LargeUtf8) return "VARCHAR";
        if (t instanceof ArrowType.Bool) return "BOOLEAN";
        if (t instanceof ArrowType.Binary) return "BLOB";
        if (t instanceof ArrowType.LargeBinary) return "BLOB";
        if (t instanceof ArrowType.Date) return "DATE";
        if (t instanceof ArrowType.Time) return "TIME";
        if (t instanceof ArrowType.Timestamp) return "TIMESTAMP";
        if (t instanceof ArrowType.Interval) return "INTERVAL";
        if (t instanceof ArrowType.Duration) return "INTERVAL";
        if (t instanceof ArrowType.Struct) return "STRUCT";
        if (t instanceof ArrowType.List) return "LIST";
        if (t instanceof ArrowType.FixedSizeList fl) return "FIXED_LIST[" + fl.getListSize() + "]";
        return t.toString();
    }

    /**
     * Promote two operands to a common arithmetic type (used by {@code add_values}).
     * Mirrors {@code vgi.CommonTypeForAddition} — float wins, otherwise the widest
     * integer width plus one tier (capped at int64).
     *
     * @param a first operand type
     * @param b second operand type
     * @return the common arithmetic type
     */
    public static ArrowType commonTypeForAddition(ArrowType a, ArrowType b) {
        if (isFloating(a) || isFloating(b)) {
            return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
        }
        if (a instanceof ArrowType.Int ai && b instanceof ArrowType.Int bi) {
            int width = Math.max(ai.getBitWidth(), bi.getBitWidth());
            int next = Math.min(64, width * 2);
            return new ArrowType.Int(next, ai.getIsSigned() || bi.getIsSigned());
        }
        return Schemas.INT64;
    }
}
