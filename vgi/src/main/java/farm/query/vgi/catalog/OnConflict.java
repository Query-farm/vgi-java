// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.catalog;

import java.util.Locale;

/**
 * What a {@code CREATE} does when the object already exists: the wire
 * {@code on_conflict} of {@code catalog_schema_create},
 * {@code catalog_table_create} and {@code catalog_view_create}.
 *
 * <p>Mirrors vgi-python's {@code OnConflict}.</p>
 */
public enum OnConflict {

    /** Fail: plain {@code CREATE}. */
    ERROR,

    /** Keep the existing object and succeed: {@code CREATE ... IF NOT EXISTS}. */
    IGNORE,

    /** Replace the existing object: {@code CREATE OR REPLACE}. */
    REPLACE;

    /**
     * Parse the wire value (case-insensitive). {@code null} or empty reads as {@link #ERROR}.
     *
     * @param wire the wire string
     * @return the policy
     * @throws IllegalArgumentException for an unknown value
     */
    public static OnConflict fromWire(String wire) {
        if (wire == null || wire.isEmpty()) return ERROR;
        return switch (wire.trim().toUpperCase(Locale.ROOT)) {
            case "ERROR" -> ERROR;
            case "IGNORE" -> IGNORE;
            case "REPLACE" -> REPLACE;
            default -> throw new IllegalArgumentException("unknown on_conflict: " + wire);
        };
    }
}
