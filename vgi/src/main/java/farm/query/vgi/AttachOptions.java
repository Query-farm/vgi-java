// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi;

import farm.query.vgi.internal.AttachOptionsAttachId;
import farm.query.vgi.internal.VectorScalarCodec;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * The ATTACH option values of one attach, as declared with {@link Worker#attachOptions}:
 * every declared option, set to the value the {@code ATTACH} statement gave it or else to
 * its default ({@code null} when it has neither).
 *
 * <p>Functions read them from their call parameters
 * ({@link farm.query.vgi.table.TableBindParams#attachOptions()},
 * {@link farm.query.vgi.table.TableInitParams#attachOptions()},
 * {@link farm.query.vgi.tableinout.TableInOutBindParams#attachOptions()},
 * {@link farm.query.vgi.tableinout.TableInOutInitParams#attachOptions()}), and the
 * {@link AttachHook} and {@link TransactionHooks} from their {@link AttachContext}.
 * Those views include options declared {@link AttachOptionSpec#asSecret() secret}.
 *
 * <p>The values travel inside the attach's {@code attach_opaque_data}, so every worker
 * process sees them, including one that never served the {@code ATTACH} (a pooled or
 * load-balanced deployment). Secret values never travel in plaintext there: the framework
 * encrypts them with the worker's opaque-data key ({@link Worker#signingKey(byte[])} /
 * {@code VGI_SIGNING_KEY}; over HTTP the per-process sealing key when none is configured;
 * on stdio / unix sockets without a configured key, a per-user key file shared by the
 * host's worker processes, see {@code docs/writable-tables-and-transactions.md}). A process that cannot open
 * them sees secret options as absent ({@link #secretsAvailable()} is then false).
 *
 * <p>Values are plain Java objects: {@code String} for VARCHAR, {@code Boolean},
 * {@code Long} for every integer type, {@code Double} for floating point, {@code byte[]}
 * for BLOB, {@code List} for LIST and {@code Map} for STRUCT. {@link #toString()} redacts
 * secret values.
 */
public final class AttachOptions {

    private static final AttachOptions EMPTY = new AttachOptions(Map.of(), Set.of(), true);

    private final Map<String, Object> values;
    private final Set<String> secretNames;
    private final boolean secretsAvailable;

    private AttachOptions(Map<String, Object> values, Set<String> secretNames, boolean secretsAvailable) {
        this.values = values;
        this.secretNames = secretNames;
        this.secretsAvailable = secretsAvailable;
    }

    /**
     * An attach with no options.
     *
     * @return the empty value set
     */
    public static AttachOptions empty() {
        return EMPTY;
    }

    /**
     * Builds an option set from explicit values, e.g. in a unit test of a function.
     *
     * @param values      option name to value (a {@code null} value is an option set to NULL)
     * @param secretNames which of the names are secret (redacted by {@link #toString()})
     * @return the option set
     */
    public static AttachOptions of(Map<String, ?> values, Set<String> secretNames) {
        return of(values, secretNames, true);
    }

    /**
     * Builds an option set from explicit values, recording whether the attach's secret
     * options could be opened.
     *
     * @param values           option name to value (a {@code null} value is an option set to NULL)
     * @param secretNames      which of the names are secret (redacted by {@link #toString()})
     * @param secretsAvailable false when the attach carries secret options this process could
     *                         not open (they are then absent from {@code values})
     * @return the option set
     */
    public static AttachOptions of(Map<String, ?> values, Set<String> secretNames, boolean secretsAvailable) {
        return new AttachOptions(Collections.unmodifiableMap(new LinkedHashMap<>(values)),
                Set.copyOf(secretNames), secretsAvailable);
    }

    /**
     * The non-secret option values carried by a plain attach id (the {@code attachId()} of
     * a function's parameters). Secret options are absent: they are encrypted in the attach
     * id, and only the framework holds the key, so read them from the parameters'
     * {@code attachOptions()} instead.
     *
     * @param attachId the plain attach id, or {@code null}
     * @return the options; {@link #empty()} when the attach id carries none
     */
    public static AttachOptions fromAttachId(byte[] attachId) {
        if (!AttachOptionsAttachId.carriesOptions(attachId)) return EMPTY;
        try (VectorSchemaRoot root = AttachOptionsAttachId.decode(attachId, Allocators.root())) {
            return new AttachOptions(Collections.unmodifiableMap(valuesOf(root)), Set.of(),
                    !AttachOptionsAttachId.hasSecrets(attachId));
        }
    }

    /**
     * The values of a one-row option batch, keyed by column name.
     *
     * @param root the batch, or {@code null}
     * @return name to value, in column order (mutable)
     */
    public static Map<String, Object> valuesOf(VectorSchemaRoot root) {
        Map<String, Object> out = new LinkedHashMap<>();
        if (root == null) return out;
        for (FieldVector v : root.getFieldVectors()) {
            out.put(v.getName(), root.getRowCount() == 0 ? null : VectorScalarCodec.read(v, 0));
        }
        return out;
    }

    /**
     * Whether the option is present in this set (declared, or given in a test set).
     *
     * @param name the option name
     * @return true when present, even when its value is NULL
     */
    public boolean contains(String name) {
        return values.containsKey(name);
    }

    /**
     * The option's value.
     *
     * @param name the option name
     * @return the value, or {@code null} when it is NULL or absent
     */
    public Object get(String name) {
        return values.get(name);
    }

    /**
     * The option's value as a string.
     *
     * @param name the option name
     * @return the value's string form, or {@code null} when it is NULL or absent
     */
    public String getString(String name) {
        Object v = values.get(name);
        return v == null ? null : v instanceof byte[] b
                ? new String(b, java.nio.charset.StandardCharsets.UTF_8) : v.toString();
    }

    /**
     * The option's value as a boolean.
     *
     * @param name         the option name
     * @param defaultValue returned when the value is NULL or absent
     * @return the value
     * @throws IllegalArgumentException when the value is not a boolean
     */
    public boolean getBoolean(String name, boolean defaultValue) {
        Object v = values.get(name);
        if (v == null) return defaultValue;
        if (v instanceof Boolean b) return b;
        throw new IllegalArgumentException("attach option '" + name + "' is not a BOOLEAN");
    }

    /**
     * The option's value as a long.
     *
     * @param name the option name
     * @return the value, or {@code null} when it is NULL or absent
     * @throws IllegalArgumentException when the value is not numeric
     */
    public Long getLong(String name) {
        Object v = values.get(name);
        if (v == null) return null;
        if (v instanceof Number n) return n.longValue();
        throw new IllegalArgumentException("attach option '" + name + "' is not numeric");
    }

    /**
     * The option names, in declaration order.
     *
     * @return the names
     */
    public Set<String> names() {
        return values.keySet();
    }

    /**
     * Whether {@code name} is a secret option.
     *
     * @param name the option name
     * @return true for a secret option
     */
    public boolean isSecret(String name) {
        return secretNames.contains(name);
    }

    /**
     * Whether this process could open the attach's secret options. False only when the attach
     * carries secret values this process has no key for (see the class documentation); the
     * secret options are then absent from this set.
     *
     * @return true when every secret option of the attach is present
     */
    public boolean secretsAvailable() {
        return secretsAvailable;
    }

    /**
     * Every option as an unmodifiable map, secret values included.
     *
     * @return name to value
     */
    public Map<String, Object> asMap() {
        return values;
    }

    /** @return the options with secret values shown as {@code <redacted>} */
    @Override
    public String toString() {
        StringBuilder b = new StringBuilder("AttachOptions{");
        boolean first = true;
        for (Map.Entry<String, Object> e : values.entrySet()) {
            if (!first) b.append(", ");
            first = false;
            b.append(e.getKey()).append('=');
            Object v = e.getValue();
            if (secretNames.contains(e.getKey()) && v != null) b.append("<redacted>");
            else if (v instanceof byte[] bytes) b.append("<").append(bytes.length).append(" bytes>");
            else b.append(v);
        }
        return b.append('}').toString();
    }
}
