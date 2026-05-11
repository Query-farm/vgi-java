// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.List;

/**
 * Declares an ATTACH-time option this worker accepts. The wire format is
 * identical to {@link SettingSpec}; users supply a value at ATTACH time
 * (e.g. {@code ATTACH '…' AS x (TYPE vgi, LOCATION '…', opt_int 42)}) and
 * the resolved value flows to {@code catalog_attach} via
 * {@code CatalogAttachRequest.options} as a one-row record batch keyed by
 * option name.
 */
public record AttachOptionSpec(
        String name,
        String description,
        ArrowType type,
        List<Field> children,
        Object defaultValue) {

    public AttachOptionSpec(String name, String description, ArrowType type) {
        this(name, description, type, List.of(), null);
    }

    public AttachOptionSpec(String name, String description, ArrowType type, Object defaultValue) {
        this(name, description, type, List.of(), defaultValue);
    }

    public AttachOptionSpec(String name, String description, ArrowType type, List<Field> children) {
        this(name, description, type, children, null);
    }

    public AttachOptionSpec(String name, String description, ArrowType type,
                              List<Field> children, Object defaultValue) {
        this.name = name;
        this.description = description;
        this.type = type;
        this.children = children == null ? List.of() : children;
        this.defaultValue = defaultValue;
    }
}
