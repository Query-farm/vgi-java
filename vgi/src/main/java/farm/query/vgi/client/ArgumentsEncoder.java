// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.client;

import farm.query.vgi.internal.BatchUtil;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Encode a table/scalar function's bind-time arguments into the
 * {@code BindRequest.arguments} IPC bytes a VGI worker expects.
 *
 * <p>The inverse of the worker-side {@code ArgumentsParser}. DuckDB sends a
 * one-row batch holding a single {@code args} <em>struct</em> column whose
 * children are {@code positional_0}, {@code positional_1}, … in call order and
 * {@code named_&lt;name&gt;} for each named argument. (The parser also accepts a
 * flat batch whose columns are named directly by parameter name; this encoder
 * emits the struct form, because that is what an engine actually sends and
 * therefore what workers are tested against.)
 *
 * <p>Typical use from a JVM consumer building a {@code BindRequest}:
 *
 * <pre>{@code
 * byte[] args = ArgumentsEncoder.builder()
 *         .positional(5000L)                 // seq(5000, batch_size := 1000)
 *         .named("batch_size", 1000L)
 *         .encode();
 * }</pre>
 *
 * <p>Values carry their Arrow type via {@link ScalarValue}; the {@code Object}
 * overloads infer it. A {@code null} argument has no inferable type, so pass
 * {@link ScalarValue#ofNull(ArrowType)} for it.
 *
 * <p>Instances are mutable builders and are not thread-safe; build one per
 * bind call.
 */
public final class ArgumentsEncoder {

    private final List<ScalarValue> positional = new ArrayList<>();
    private final Map<String, ScalarValue> named = new LinkedHashMap<>();

    private ArgumentsEncoder() {}

    /**
     * Start a new argument list.
     *
     * @return a fresh, empty encoder
     */
    public static ArgumentsEncoder builder() {
        return new ArgumentsEncoder();
    }

    /**
     * Encode positional arguments only — the common shape.
     *
     * @param values the positional arguments in call order; each is either a
     *               {@link ScalarValue} or a value with an inferable type
     * @return the {@code BindRequest.arguments} IPC bytes
     */
    public static byte[] positionalArgs(Object... values) {
        ArgumentsEncoder e = builder();
        for (Object v : values) e.positional(v);
        return e.encode();
    }

    /**
     * Append the next positional argument.
     *
     * @param value a {@link ScalarValue}, or a value whose Arrow type is inferable
     * @return this encoder
     */
    public ArgumentsEncoder positional(Object value) {
        positional.add(ScalarValue.of(value));
        return this;
    }

    /**
     * Append the next positional argument with an explicit type.
     *
     * @param value the typed value
     * @return this encoder
     */
    public ArgumentsEncoder positional(ScalarValue value) {
        positional.add(value);
        return this;
    }

    /**
     * Set a named argument. The wire child is {@code named_<name>}; pass the
     * bare parameter name here.
     *
     * @param name  the parameter name as declared by the worker
     * @param value a {@link ScalarValue}, or a value whose Arrow type is inferable
     * @return this encoder
     */
    public ArgumentsEncoder named(String name, Object value) {
        named.put(requireName(name), ScalarValue.of(value));
        return this;
    }

    /**
     * Set a named argument with an explicit type.
     *
     * @param name  the parameter name as declared by the worker
     * @param value the typed value
     * @return this encoder
     */
    public ArgumentsEncoder named(String name, ScalarValue value) {
        named.put(requireName(name), value);
        return this;
    }

    /**
     * Serialise the accumulated arguments.
     *
     * @return one-row IPC stream bytes suitable for {@code BindRequest.arguments}
     */
    public byte[] encode() {
        List<Field> children = new ArrayList<>(positional.size() + named.size());
        List<ScalarValue> values = new ArrayList<>(positional.size() + named.size());
        for (int i = 0; i < positional.size(); i++) {
            children.add(positional.get(i).field("positional_" + i));
            values.add(positional.get(i));
        }
        for (Map.Entry<String, ScalarValue> e : named.entrySet()) {
            children.add(e.getValue().field("named_" + e.getKey()));
            values.add(e.getValue());
        }

        Field args = new Field("args", new FieldType(true, new ArrowType.Struct(), null), children);
        Schema schema = new Schema(List.of(args));
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, Allocators.root())) {
            root.allocateNew();
            StructVector struct = (StructVector) root.getVector("args");
            // setIndexDefined marks the struct itself non-null; without it the
            // whole row reads back as a null struct and every argument is lost.
            struct.setIndexDefined(0);
            for (int i = 0; i < children.size(); i++) {
                FieldVector child = struct.getChild(children.get(i).getName());
                values.get(i).write(child, 0);
            }
            struct.setValueCount(1);
            root.setRowCount(1);
            return BatchUtil.writeSingleBatch(root);
        }
    }

    private static String requireName(String name) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("named argument requires a non-empty name");
        }
        return name;
    }
}
