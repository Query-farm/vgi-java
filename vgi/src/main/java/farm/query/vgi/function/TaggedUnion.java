// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.function;

/**
 * A decoded Arrow union value that preserves the active member discriminator.
 *
 * <p>A plain {@code UnionVector.getObject(row)} returns only the active
 * member's value, dropping the type id that identifies <em>which</em> member
 * is active. {@code TaggedUnion} pairs the active member's field name
 * ({@code tag}) with its decoded value so callers can recover the union's
 * discriminator after the Arrow round-trip.
 *
 * <p>Mirrors the Python framework's {@code TaggedUnion}. Produced by
 * {@link farm.query.vgi.internal.VectorScalarCodec#read} for
 * {@link org.apache.arrow.vector.complex.UnionVector} (sparse, as emitted by
 * DuckDB) cells.
 *
 * @param tag   the active union member's field name (e.g. {@code "i"} or {@code "s"}).
 * @param value the active member's decoded value, or {@code null} when the active member is null.
 */
public record TaggedUnion(String tag, Object value) {}
