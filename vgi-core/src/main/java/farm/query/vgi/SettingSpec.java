// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi;

import org.apache.arrow.vector.types.pojo.ArrowType;

/**
 * Declares a custom DuckDB setting backed by this worker. Mirrors vgi-go
 * {@code vgi.SettingSpec}.
 *
 * <p>Settings are advertised at attach time via {@code CatalogAttachResult.settings}
 * and delivered with each {@code bind} call so functions can read them.
 */
public record SettingSpec(
        String name,
        String description,
        ArrowType type,
        Object defaultValue) {

    public SettingSpec(String name, String description, ArrowType type) {
        this(name, description, type, null);
    }
}
