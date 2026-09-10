// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.pushdown;

/** A malformed or unsupported required VGI Filter Encoding v2 document. */
public final class FilterV2Exception extends IllegalArgumentException {
    public FilterV2Exception(String message) {
        super(message);
    }

    public FilterV2Exception(String message, Throwable cause) {
        super(message, cause);
    }
}
