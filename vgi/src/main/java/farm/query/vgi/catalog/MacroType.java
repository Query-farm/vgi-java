// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.catalog;

/** Kind of SQL macro: scalar-valued or table-valued. */
public enum MacroType {
    /** A scalar macro that expands to a single-value expression. */
    SCALAR,
    /** A table macro that expands to a table-producing expression. */
    TABLE
}
