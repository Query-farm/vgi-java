// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.client;

import farm.query.vgi.internal.BatchUtil;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Encode the extension settings a worker declared into the
 * {@code BindRequest.settings} IPC bytes.
 *
 * <p>The inverse of the worker-side {@code SettingsParser}. The wire shape is
 * flatter than the argument one: a single-row batch where <em>each column name
 * is a setting name</em> and the row-0 cell holds that setting's current value.
 * A setting the client leaves out simply doesn't appear, which is how a worker
 * distinguishes "unset" from "set to null".
 *
 * <pre>{@code
 * byte[] settings = SettingsEncoder.builder()
 *         .setting("example_multiplier", 3L)
 *         .setting("example_greeting", "hi")
 *         .encode();
 * }</pre>
 *
 * <p>Values carry their Arrow type via {@link ScalarValue}; the {@code Object}
 * overloads infer it, including a {@link Map} for a struct-valued setting.
 *
 * <p>Instances are mutable builders and are not thread-safe; build one per
 * bind call.
 */
public final class SettingsEncoder {

    private final Map<String, ScalarValue> settings = new LinkedHashMap<>();

    private SettingsEncoder() {}

    /**
     * Start a new settings batch.
     *
     * @return a fresh, empty encoder
     */
    public static SettingsEncoder builder() {
        return new SettingsEncoder();
    }

    /**
     * Encode a whole settings map in one call.
     *
     * @param settings setting name to value; values are {@link ScalarValue}s or
     *                 values with an inferable Arrow type
     * @return the {@code BindRequest.settings} IPC bytes
     */
    public static byte[] of(Map<String, ?> settings) {
        SettingsEncoder e = builder();
        for (Map.Entry<String, ?> entry : settings.entrySet()) {
            e.setting(entry.getKey(), entry.getValue());
        }
        return e.encode();
    }

    /**
     * Set one setting's value.
     *
     * @param name  the setting name exactly as the worker declared it
     * @param value a {@link ScalarValue}, or a value whose Arrow type is inferable
     * @return this encoder
     */
    public SettingsEncoder setting(String name, Object value) {
        settings.put(requireName(name), ScalarValue.of(value));
        return this;
    }

    /**
     * Set one setting's value with an explicit type — the route for a null.
     *
     * @param name  the setting name exactly as the worker declared it
     * @param value the typed value
     * @return this encoder
     */
    public SettingsEncoder setting(String name, ScalarValue value) {
        settings.put(requireName(name), value);
        return this;
    }

    /**
     * Serialise the accumulated settings.
     *
     * @return one-row IPC stream bytes suitable for {@code BindRequest.settings}
     */
    public byte[] encode() {
        List<Field> fields = new ArrayList<>(settings.size());
        List<ScalarValue> values = new ArrayList<>(settings.size());
        for (Map.Entry<String, ScalarValue> e : settings.entrySet()) {
            fields.add(e.getValue().field(e.getKey()));
            values.add(e.getValue());
        }
        Schema schema = new Schema(fields);
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, Allocators.root())) {
            root.allocateNew();
            for (int i = 0; i < fields.size(); i++) {
                FieldVector v = root.getVector(fields.get(i).getName());
                values.get(i).write(v, 0);
                v.setValueCount(1);
            }
            root.setRowCount(1);
            return BatchUtil.writeSingleBatch(root);
        }
    }

    private static String requireName(String name) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("setting requires a non-empty name");
        }
        return name;
    }
}
