// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.client;

import farm.query.vgi.internal.VectorScalarCodec;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.Field;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Read the bound arguments a worker returns for a catalog table's scan
 * function, and re-encode them as {@code BindRequest.arguments}.
 *
 * <p>The two are <em>not</em> the same encoding, which is the whole reason this
 * class exists. {@code TableScanFunctionGetResponse.arguments} is a flat
 * one-row batch whose columns are {@code arg_0}, {@code arg_1}, … for the
 * positional arguments plus one column per named argument. Bind arguments are a
 * one-row batch holding a single {@code args} <em>struct</em> whose children are
 * {@code positional_N} / {@code named_&lt;name&gt;} (see
 * {@link ArgumentsEncoder}). Feeding the former straight into a
 * {@code BindRequest} looks plausible and fails at the worker — the C++
 * extension decodes to typed values and re-encodes at bind time, and a JVM
 * consumer has to do the same.
 *
 * <pre>{@code
 * TableScanFunctionGetResponse scan =
 *         vgi.catalog_table_scan_function_get(handle, "data", "numbers", null, null, null, null);
 * BindRequest bind = new BindRequest(
 *         scan.function_name(),
 *         ScanFunctionArguments.toBindArguments(scan.arguments()),
 *         "TABLE", ...);
 * }</pre>
 *
 * <p>Types are preserved: each column is read as a {@link ScalarValue} carrying
 * the Arrow type the worker declared, so an {@code int32} argument does not
 * silently widen on its way back out.
 */
public final class ScanFunctionArguments {

    /**
     * A ceiling on the positional index a worker may name. The index sizes a
     * list, so an unbounded {@code arg_9999999999} would be an allocation
     * request rather than an argument. Matches the C++ extension's limit.
     */
    private static final int MAX_POSITIONAL = 1000;

    private static final String POSITIONAL_PREFIX = "arg_";

    private ScanFunctionArguments() {}

    /**
     * The decoded arguments: positional in call order, named by name.
     *
     * @param positional the positional arguments, in index order
     * @param named      the named arguments, in the worker's column order
     */
    public record Decoded(List<ScalarValue> positional, Map<String, ScalarValue> named) {}

    /**
     * Decode a scan function's bound arguments.
     *
     * @param arguments the {@code TableScanFunctionGetResponse.arguments} bytes;
     *                  {@code null} or empty means "no arguments"
     * @return the decoded arguments
     * @throws IllegalStateException if the bytes are malformed, a positional
     *         index is out of range, or the positional indices have a gap
     */
    public static Decoded decode(byte[] arguments) {
        if (arguments == null || arguments.length == 0) {
            return new Decoded(List.of(), Map.of());
        }
        Map<Integer, ScalarValue> positional = new LinkedHashMap<>();
        Map<String, ScalarValue> named = new LinkedHashMap<>();
        try (ByteArrayInputStream in = new ByteArrayInputStream(arguments);
             ArrowStreamReader reader = new ArrowStreamReader(in, Allocators.root())) {
            if (!reader.loadNextBatch()) return new Decoded(List.of(), Map.of());
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            for (Field f : root.getSchema().getFields()) {
                FieldVector v = root.getVector(f.getName());
                ScalarValue value = new ScalarValue(f.getType(), VectorScalarCodec.read(v, 0));
                Integer index = positionalIndex(f.getName());
                if (index == null) {
                    named.put(f.getName(), value);
                } else {
                    positional.put(index, value);
                }
            }
        } catch (Exception e) {
            if (e instanceof IllegalStateException ise) throw ise;
            throw new IllegalStateException("ScanFunctionArguments.decode failed", e);
        }
        return new Decoded(orderedPositional(positional), Collections.unmodifiableMap(named));
    }

    /**
     * Decode a scan function's bound arguments and re-encode them as bind
     * arguments.
     *
     * @param arguments the {@code TableScanFunctionGetResponse.arguments} bytes
     * @return IPC bytes for {@code BindRequest.arguments}
     */
    public static byte[] toBindArguments(byte[] arguments) {
        Decoded decoded = decode(arguments);
        ArgumentsEncoder encoder = ArgumentsEncoder.builder();
        for (ScalarValue v : decoded.positional()) encoder.positional(v);
        for (Map.Entry<String, ScalarValue> e : decoded.named().entrySet()) {
            encoder.named(e.getKey(), e.getValue());
        }
        return encoder.encode();
    }

    /**
     * The positional index a column name encodes, or {@code null} when the
     * column names a keyword argument.
     *
     * <p>A column that starts with {@code arg_} but does not continue with a
     * number is a named argument that happens to share the prefix, not a
     * malformed positional one — same reading the C++ extension takes.
     */
    private static Integer positionalIndex(String columnName) {
        if (!columnName.startsWith(POSITIONAL_PREFIX)) return null;
        String suffix = columnName.substring(POSITIONAL_PREFIX.length());
        if (suffix.isEmpty()) return null;
        for (int i = 0; i < suffix.length(); i++) {
            if (!Character.isDigit(suffix.charAt(i))) return null;
        }
        long index;
        try {
            index = Long.parseLong(suffix);
        } catch (NumberFormatException overflow) {
            throw new IllegalStateException("scan function argument '" + columnName
                    + "' has an out-of-range positional index");
        }
        if (index >= MAX_POSITIONAL) {
            throw new IllegalStateException("scan function positional argument index " + index
                    + " exceeds the maximum of " + MAX_POSITIONAL);
        }
        return (int) index;
    }

    private static List<ScalarValue> orderedPositional(Map<Integer, ScalarValue> byIndex) {
        List<ScalarValue> out = new ArrayList<>(byIndex.size());
        for (int i = 0; i < byIndex.size(); i++) {
            ScalarValue v = byIndex.get(i);
            if (v == null) {
                // A gap would mean guessing both a value and a type for the
                // missing slot, and no worker produces one — say so instead.
                throw new IllegalStateException("scan function arguments skip positional index " + i
                        + "; indices present: " + byIndex.keySet());
            }
            out.add(v);
        }
        return Collections.unmodifiableList(out);
    }
}
