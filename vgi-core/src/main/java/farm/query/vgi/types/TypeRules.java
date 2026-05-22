// Copyright 2025-2026 Query.Farm LLC

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

    public static boolean isInteger(ArrowType t) { return t instanceof ArrowType.Int; }

    public static boolean isFloating(ArrowType t) { return t instanceof ArrowType.FloatingPoint; }

    public static boolean isNumeric(ArrowType t) { return isInteger(t) || isFloating(t); }

    public static boolean isAddable(ArrowType t) { return isNumeric(t); }

    /**
     * Promote a single input type one width up (for {@code double(value)}-style
     * arithmetic). Floats stay; ints climb the tier.
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
     * Promote two operands to a common arithmetic type. Used by {@code add_values}.
     * Mirrors {@code vgi.CommonTypeForAddition} — float wins, otherwise the
     * widest integer width plus one tier (capped at int64).
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
