// Copyright 2026 Query Farm LLC - https://query.farm

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
 *
 * <h2>Credentials must be declared secret</h2>
 *
 * <p>An option that carries a credential (an API key, an access token, a
 * password, a signing key) <strong>must</strong> be declared {@code secret}.
 * The flag is advertised to clients in the option's spec, and changes how they
 * and the DuckDB extension handle the value:
 *
 * <ul>
 *   <li>clients mask it in their options editors and keep it out of exported or
 *       shared configuration;</li>
 *   <li>the extension redacts it from {@code duckdb_databases()}, keeps only a
 *       salted hash of it in its result-cache key (so results stay separate per
 *       credential), and never logs it.</li>
 * </ul>
 *
 * <p>The credential is still passed inline as an ATTACH option. To keep it out
 * of the SQL text, pass an expression rather than a literal:
 *
 * <pre>{@code
 * ATTACH 'sales' (TYPE vgi, LOCATION 'https://sales.example.com',
 *                 api_key getenv('SALES_API_KEY'));
 * }</pre>
 *
 * <p>Declare one with {@link #requiredSecret(String, String, ArrowType)}, or
 * mark any spec with {@link #asSecret()}:
 *
 * <pre>{@code
 * AttachOptionSpec.requiredSecret("api_key", "API key", Schemas.UTF8);
 * AttachOptionSpec.of("token", "Optional access token", Schemas.UTF8, null).asSecret();
 * }</pre>
 *
 * <p>{@code secret} combines freely with {@code required}. It is also allowed
 * together with a default, but a secret option normally has none: a default
 * credential would ship in the worker's catalog metadata to every client.
 *
 * @param name          option name as written in the ATTACH clause / wire key
 * @param description   human-readable description for catalog introspection
 * @param valueField    Arrow field describing the option's value type (named {@code "value"})
 * @param defaultVector length-1 vector holding the pre-materialised default, or {@code null} for none
 * @param required      the caller must supply this option at ATTACH time; mutually exclusive with a
 *                      default, since an option that falls back to a value is by definition
 *                      satisfiable without the caller
 * @param secret        the value is a credential (API key, token, password): clients mask it,
 *                      and the extension redacts it from {@code duckdb_databases()}, keeps only a
 *                      salted hash of it in its cache key, and never logs it. Every
 *                      credential option must set this. Combines with {@code required}; allowed
 *                      with a default, though a secret option normally has none
 */
public record AttachOptionSpec(
        String name,
        String description,
        Field valueField,
        FieldVector defaultVector,
        boolean required,
        boolean secret) {

    /** Rejects the reserved {@code vgi_attach_ticket} name (any case) and the contradictory
     *  required-plus-default combination. */
    public AttachOptionSpec {
        if (AttachTickets.isReserved(name)) {
            throw new IllegalArgumentException(
                    "Attach option '" + name + "' uses the reserved name '" + AttachTickets.OPTION
                            + "': the framework reads it as an attach ticket before any catalog code "
                            + "runs. Rename the option.");
        }
        if (required && defaultVector != null) {
            throw new IllegalArgumentException(
                    "Attach option '" + name + "' is required but also declares a default; an option "
                            + "with a default is always satisfiable without the caller. Drop one.");
        }
    }

    /**
     * A non-secret spec; kept so declarations written before {@code secret}
     * existed still compile.
     *
     * @param name          option name
     * @param description   human-readable description
     * @param valueField    Arrow field describing the option's value type
     * @param defaultVector length-1 default vector, or {@code null} for none
     * @param required      the caller must supply this option at ATTACH time
     */
    public AttachOptionSpec(String name, String description, Field valueField,
                            FieldVector defaultVector, boolean required) {
        this(name, description, valueField, defaultVector, required, false);
    }

    /**
     * Returns a copy of this spec declared {@code secret}. The copy shares this
     * spec's default vector, so use it in place of the original.
     *
     * @return this spec with {@code secret = true}
     */
    public AttachOptionSpec asSecret() {
        return new AttachOptionSpec(name, description, valueField, defaultVector, required, true);
    }

    /**
     * Convenience: a credential the caller must supply at ATTACH time, such as
     * an API key. Equivalent to {@code required(name, description, type).asSecret()}.
     *
     * @param name        option name
     * @param description human-readable description
     * @param type        the option's Arrow value type
     * @return a spec with no default, {@code required = true} and {@code secret = true}
     */
    public static AttachOptionSpec requiredSecret(String name, String description, ArrowType type) {
        return required(name, description, type).asSecret();
    }

    /**
     * Convenience: scalar option with a Java-valued default.
     *
     * @param name         option name
     * @param description  human-readable description
     * @param type         the option's Arrow value type
     * @param defaultValue default value, materialised into a length-1 vector; may be {@code null}
     * @return a spec whose {@code valueField} is a flat field of {@code type}
     */
    public static AttachOptionSpec of(String name, String description,
                                       ArrowType type, Object defaultValue) {
        return of(name, description, type, List.of(), defaultValue);
    }

    /**
     * Convenience: an option the caller must supply at ATTACH time.
     *
     * <p>A catalog that cannot be attached without this option advertises that
     * at discovery, so a client can say so before attempting the attach rather
     * than surfacing a failure that reads like an empty catalog.
     *
     * <p>If the option is a credential, use
     * {@link #requiredSecret(String, String, ArrowType)} instead.
     *
     * @param name        option name
     * @param description human-readable description
     * @param type        the option's Arrow value type
     * @return a spec with no default and {@code required = true}
     */
    public static AttachOptionSpec required(String name, String description, ArrowType type) {
        Field field = new Field("value", new FieldType(true, type, null), List.of());
        return new AttachOptionSpec(name, description, field, null, true);
    }

    /**
     * Convenience: complex option (list/struct) with children + default.
     *
     * @param name         option name
     * @param description  human-readable description
     * @param type         the option's Arrow value type (e.g. a list or struct type)
     * @param children     child fields describing the nested type; {@code null} treated as empty
     * @param defaultValue default value, materialised into a length-1 vector; may be {@code null}
     * @return a spec whose {@code valueField} carries {@code type} and {@code children}
     */
    public static AttachOptionSpec of(String name, String description,
                                       ArrowType type, List<Field> children,
                                       Object defaultValue) {
        Field field = new Field("value",
                new FieldType(true, type, null),
                children == null ? List.of() : children);
        FieldVector defaults = defaultValue == null
                ? null
                : AttachOptionDefaultMaterializer.materialize(field, defaultValue);
        return new AttachOptionSpec(name, description, field, defaults, false);
    }

    /** {@return the option's Arrow value type, read from {@link #valueField}} */
    public ArrowType type() { return valueField.getType(); }

    /** {@return the value field's child fields (empty for flat scalar options)} */
    public List<Field> children() { return valueField.getChildren(); }
}
