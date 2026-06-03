// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.util.List;

import static farm.query.vgi.internal.IpcStructBuilder.UTF8;
import static farm.query.vgi.internal.IpcStructBuilder.dict;

/**
 * One enum-shaped {@code dictionary<int16, utf8>} field with its values
 * and dictionary id. Bundles the three places a dict gets referenced
 * (schema field, provider registration, per-row index write) so they
 * stay in sync.
 *
 * @param name   the field name
 * @param id     the dictionary id (see {@link DictionaryIds})
 * @param values the ordered enum values; their index is the on-wire code
 */
public record EnumDict(String name, long id, List<String> values) {

    /**
     * Build the {@code dictionary<int16, utf8>} schema field for this enum.
     *
     * @param nullable whether the field is nullable
     * @return the schema field
     */
    public Field field(boolean nullable) {
        return dict(name, id, nullable);
    }

    /**
     * Register this enum's dictionary into {@code provider} so the IPC writer can
     * emit it.
     *
     * @param provider the provider to populate
     */
    public void register(DictionaryProvider.MapDictionaryProvider provider) {
        IpcStructBuilder.registerDict(provider, Allocators.root(), id, values);
    }

    /**
     * Write a value's dictionary index into row 0 of the index vector.
     *
     * @param v     the int16 index vector
     * @param value the enum value, or {@code null} to write null
     * @throws IllegalArgumentException if {@code value} is not a known enum value
     */
    public void write(org.apache.arrow.vector.FieldVector v, String value) {
        SmallIntVector iv = (SmallIntVector) v;
        if (value == null) { iv.setNull(0); return; }
        int idx = values.indexOf(value);
        if (idx < 0) {
            throw new IllegalArgumentException("unknown dict value '" + value + "' for " + name);
        }
        iv.setSafe(0, idx);
    }
}
