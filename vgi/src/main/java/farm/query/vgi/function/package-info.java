// Copyright 2026 Query Farm LLC - https://query.farm

/**
 * The function argument and specification model shared across all VGI function kinds.
 *
 * <p>Scalar, table, table-in-out, and aggregate functions all declare their SQL
 * name, metadata, and argument shape through the types here, and receive their
 * call-time parameters as parsed {@link farm.query.vgi.function.Arguments}. The
 * specs are serialized into the protocol's {@code FunctionInfo} wire message so
 * DuckDB can bind calls against the worker's functions.</p>
 *
 * <ul>
 *   <li>{@link farm.query.vgi.function.FunctionDescriptor} — common surface (name, metadata, arg specs) implemented by every function kind.</li>
 *   <li>{@link farm.query.vgi.function.FunctionSpec} — the constant descriptor data, built fluently via its {@code Builder}.</li>
 *   <li>{@link farm.query.vgi.function.ArgSpec} — per-parameter specification (positional/named, const, varargs, any-typed, TABLE, nested).</li>
 *   <li>{@link farm.query.vgi.function.Arguments} — parsed call-time argument values, with source type/field metadata.</li>
 *   <li>{@link farm.query.vgi.function.ParameterExtractor} — fluent, validating accessor over {@code Arguments}.</li>
 *   <li>{@link farm.query.vgi.function.FunctionMetadata} — pushdown, stability, ordering, partition, and materialization flags.</li>
 * </ul>
 */
package farm.query.vgi.function;
