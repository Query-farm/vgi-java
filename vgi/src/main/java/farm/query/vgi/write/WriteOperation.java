// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.write;

/** A write a catalog table can accept; {@link #wireName()} is the {@code write_result_modes} key. */
public enum WriteOperation {
    /** {@code INSERT}. */
    INSERT("insert"),
    /** {@code UPDATE}. */
    UPDATE("update"),
    /** {@code DELETE}. */
    DELETE("delete");

    private final String wireName;

    WriteOperation(String wireName) {
        this.wireName = wireName;
    }

    /** @return the lower-case name used on the wire */
    public String wireName() {
        return wireName;
    }

    /**
     * The operation with the given wire name.
     *
     * @param name {@code insert}, {@code update} or {@code delete}
     * @return the operation
     * @throws IllegalArgumentException for any other name
     */
    public static WriteOperation fromWire(String name) {
        for (WriteOperation op : values()) if (op.wireName.equals(name)) return op;
        throw new IllegalArgumentException("Unknown write operation: " + name);
    }
}
