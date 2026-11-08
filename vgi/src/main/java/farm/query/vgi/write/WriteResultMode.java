// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.write;

/**
 * What a write function returns per input batch, ordered {@code COUNT < ROWS < CHANGES}. A table
 * advertises, per operation, the richest mode its write function can produce, and thereby
 * supports every lower one. DuckDB asks for one mode per statement in {@link WriteOptions}:
 *
 * <ul>
 *   <li>{@link #COUNT} for a plain INSERT / UPDATE / DELETE: one row, {@code count BIGINT NOT NULL};</li>
 *   <li>{@link #ROWS} for {@code ... RETURNING}: the written rows in the table's user columns;</li>
 *   <li>{@link #CHANGES}: old / new pairs, {@code old STRUCT(<table>)}, {@code new STRUCT(<table>)}.</li>
 * </ul>
 */
public enum WriteResultMode {
    /** One row holding the number of rows written. */
    COUNT("count"),
    /** The written rows. */
    ROWS("rows"),
    /** The written rows' before and after images. */
    CHANGES("changes");

    private final String wireName;

    WriteResultMode(String wireName) {
        this.wireName = wireName;
    }

    /** @return the lower-case name used on the wire */
    public String wireName() {
        return wireName;
    }

    /**
     * Whether a table whose maximum is this mode can serve {@code requested}.
     *
     * @param requested the mode DuckDB asked for
     * @return true when {@code requested} is this mode or a lower one
     */
    public boolean supports(WriteResultMode requested) {
        return compareTo(requested) >= 0;
    }

    /**
     * The mode with the given wire name.
     *
     * @param name {@code count}, {@code rows} or {@code changes}
     * @return the mode
     * @throws IllegalArgumentException for any other name
     */
    public static WriteResultMode fromWire(String name) {
        for (WriteResultMode m : values()) if (m.wireName.equals(name)) return m;
        throw new IllegalArgumentException("Unknown write result mode: " + name);
    }
}
