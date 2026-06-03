// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.List;

/**
 * Declares a custom DuckDB setting backed by this worker. Mirrors vgi-go
 * {@code vgi.SettingSpec}.
 *
 * <p>Settings are advertised at attach time via {@code CatalogAttachResult.settings}
 * and delivered with each {@code bind} call so functions can read them.
 *
 * @param name         setting name as referenced in {@code SET}/{@code bind}
 * @param description  human-readable description for introspection
 * @param type         the setting's Arrow value type
 * @param children     child fields for complex (list/struct) settings; empty for scalars
 * @param defaultValue default value when the user has not set one; may be {@code null}
 */
public record SettingSpec(
        String name,
        String description,
        ArrowType type,
        List<Field> children,
        Object defaultValue) {

    /**
     * Scalar setting with no default.
     *
     * @param name        setting name
     * @param description human-readable description
     * @param type        the setting's Arrow value type
     */
    public SettingSpec(String name, String description, ArrowType type) {
        this(name, description, type, List.of(), null);
    }

    /**
     * Scalar setting with a default value.
     *
     * @param name         setting name
     * @param description  human-readable description
     * @param type         the setting's Arrow value type
     * @param defaultValue default value; may be {@code null}
     */
    public SettingSpec(String name, String description, ArrowType type, Object defaultValue) {
        this(name, description, type, List.of(), defaultValue);
    }

    /**
     * Complex (list/struct) setting with no default.
     *
     * @param name        setting name
     * @param description human-readable description
     * @param type        the setting's Arrow value type
     * @param children    child fields describing the nested type
     */
    public SettingSpec(String name, String description, ArrowType type, List<Field> children) {
        this(name, description, type, children, null);
    }
}
