// Copyright 2026 Query Farm LLC - https://query.farm

/**
 * Writable catalog tables: the operations a table accepts ({@link farm.query.vgi.write.WriteOperation}),
 * the result modes a write function can produce ({@link farm.query.vgi.write.WriteResultMode}),
 * the {@code write_options} DuckDB hands a write function ({@link farm.query.vgi.write.WriteOptions}),
 * and helpers for row-id columns and result batches ({@link farm.query.vgi.write.WriteResults}).
 *
 * <p>A table becomes writable by naming its write functions on the descriptor
 * ({@link farm.query.vgi.catalog.CatalogTable.Builder#updateFunction(String)} and friends). Each
 * is a {@link farm.query.vgi.tableinout.TableInOutFunction} registered in the table's catalog:
 * DuckDB streams the rows to write through it and reads one result batch per input batch, shaped
 * by the requested {@link farm.query.vgi.write.WriteResultMode}. UPDATE and DELETE identify rows
 * by the table's row-id column ({@link farm.query.vgi.write.WriteResults#rowIdField()}), which the
 * scan function must emit and DuckDB only reads through projection pushdown.
 */
package farm.query.vgi.write;
