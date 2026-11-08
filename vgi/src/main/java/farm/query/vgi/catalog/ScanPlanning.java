// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.catalog;

/**
 * Protocol 3.2 {@code TableInfo.scan_planning}: when the client resolves a
 * table's scan redirect. See {@code docs/protocol/scan-planning.md} in
 * vgi-python.
 */
public enum ScanPlanning {
    /** Resolve the redirect at bind time, with no predicates (3.1 behaviour). The default. */
    BIND("bind"),
    /**
     * Resolve it once per scan, after the client has collected the query's
     * predicates on the table and sent them as {@code pushdown_filters}. Zero
     * branches then means "no rows". A filtered table cannot inline
     * {@code scan_function}.
     */
    FILTERED("filtered");

    private final String wireValue;

    ScanPlanning(String wireValue) {
        this.wireValue = wireValue;
    }

    /**
     * The wire spelling ({@code "bind"} or {@code "filtered"}).
     *
     * @return the {@code TableInfo.scan_planning} value
     */
    public String wireValue() {
        return wireValue;
    }

    /**
     * Parse a wire value.
     *
     * @param value {@code "bind"} or {@code "filtered"}
     * @return the mode
     * @throws IllegalArgumentException for any other value, which is a protocol error
     */
    public static ScanPlanning fromWire(String value) {
        for (ScanPlanning mode : values()) {
            if (mode.wireValue.equals(value)) return mode;
        }
        throw new IllegalArgumentException(
                "scan_planning must be one of [bind, filtered], got '" + value + "'");
    }
}
