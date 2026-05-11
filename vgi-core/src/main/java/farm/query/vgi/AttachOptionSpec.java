// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi;

import farm.query.vgi.internal.AttachOptionDefaultMaterializer;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.util.List;

/**
 * Declares an ATTACH-time option this worker accepts. Users supply a value at
 * ATTACH time (e.g. {@code ATTACH '…' AS x (TYPE vgi, opt_int 42)}); the
 * resolved value flows to {@code catalog_attach} via
 * {@code CatalogAttachRequest.options} as a one-row record batch keyed by
 * option name. The wire format is identical to {@link SettingSpec}.
 *
 * <p>{@code defaultVector} is a length-1 Arrow vector pre-materialised at
 * registration time. Keeping the default in vector form lets the merge path
 * (catalog_attach) copy it via {@code TransferPair} alongside user-supplied
 * values — uniform, no type dispatch on the hot path. Allocate the spec once
 * at worker startup; the vector is owned by the spec and freed when the
 * process exits.
 */
public record AttachOptionSpec(
        String name,
        String description,
        Field valueField,
        FieldVector defaultVector) {

    /** Convenience: scalar option with a Java-valued default. */
    public static AttachOptionSpec of(String name, String description,
                                       ArrowType type, Object defaultValue) {
        return of(name, description, type, List.of(), defaultValue);
    }

    /** Convenience: complex option (list/struct) with children + default. */
    public static AttachOptionSpec of(String name, String description,
                                       ArrowType type, List<Field> children,
                                       Object defaultValue) {
        Field field = new Field("value",
                new FieldType(true, type, null),
                children == null ? List.of() : children);
        FieldVector defaults = defaultValue == null
                ? null
                : AttachOptionDefaultMaterializer.materialize(field, defaultValue);
        return new AttachOptionSpec(name, description, field, defaults);
    }

    public ArrowType type() { return valueField.getType(); }

    public List<Field> children() { return valueField.getChildren(); }
}
