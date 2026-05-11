// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

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
 */
public record EnumDict(String name, long id, List<String> values) {

    public Field field(boolean nullable) {
        return dict(name, id, nullable);
    }

    public void register(DictionaryProvider.MapDictionaryProvider provider) {
        IpcStructBuilder.registerDict(provider, Allocators.root(), id, values);
    }

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
