// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi;

import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.Field;

import java.io.ByteArrayInputStream;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Resolved secrets passed to a worker, keyed by each secret's unique DuckDB
 * secret name (not by type) so several secrets of the same type (e.g. one per S3
 * bucket) coexist. Each secret carries its connector-serialized {@code type} (the
 * DuckDB secret type) and {@code scope} (newline-joined scope prefixes) fields,
 * plus type-specific fields like {@code key_id}.
 *
 * <p>Mirrors {@code vgi::Secrets} in the Rust SDK. Parse the {@code byte[]} blob
 * carried on the params with {@link #parse(byte[])}, then select by name, type,
 * or scope.</p>
 */
public final class Secrets {

    /** name -> { field -> value-as-string }. */
    private final Map<String, Map<String, String>> byName;

    private Secrets(Map<String, Map<String, String>> byName) {
        this.byName = byName;
    }

    /**
     * Build directly from a name -> fields map (for tests / non-IPC callers).
     *
     * @param byName the resolved secrets keyed by secret name
     * @return a {@code Secrets} over a copy of {@code byName}
     */
    public static Secrets of(Map<String, Map<String, String>> byName) {
        Map<String, Map<String, String>> copy = new LinkedHashMap<>();
        byName.forEach((k, v) -> copy.put(k, new LinkedHashMap<>(v)));
        return new Secrets(copy);
    }

    /**
     * Parse the IPC secrets blob. Each column is a secret (named by its DuckDB
     * secret name) holding a struct of its fields, including {@code type} and
     * {@code scope}. Empty/null blob yields empty secrets.
     *
     * @param bytes the IPC batch, or {@code null}/empty
     * @return the parsed secrets
     */
    public static Secrets parse(byte[] bytes) {
        Map<String, Map<String, String>> byName = new LinkedHashMap<>();
        if (bytes == null || bytes.length == 0) {
            return new Secrets(byName);
        }
        try (ByteArrayInputStream in = new ByteArrayInputStream(bytes);
             ArrowStreamReader r = new ArrowStreamReader(in, Allocators.root())) {
            if (r.loadNextBatch()) {
                VectorSchemaRoot root = r.getVectorSchemaRoot();
                for (Field f : root.getSchema().getFields()) {
                    FieldVector col = root.getVector(f.getName());
                    if (col == null || col.isNull(0)) {
                        continue;
                    }
                    Map<String, String> fields = new LinkedHashMap<>();
                    if (col instanceof StructVector sv) {
                        for (Field child : f.getChildren()) {
                            FieldVector cv = sv.getChild(child.getName());
                            if (cv != null && !cv.isNull(0)) {
                                fields.put(child.getName(), render(cv));
                            }
                        }
                    } else {
                        fields.put(f.getName(), render(col));
                    }
                    if (!fields.isEmpty()) {
                        byName.put(f.getName(), fields);
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse secrets IPC batch", e);
        }
        return new Secrets(byName);
    }

    /** A field value from the first secret carrying it (any name). */
    public Optional<String> field(String field) {
        return byName.values().stream()
                .map(m -> m.get(field))
                .filter(java.util.Objects::nonNull)
                .findFirst();
    }

    /** A named secret's field. */
    public Optional<String> namedField(String name, String field) {
        Map<String, String> m = byName.get(name);
        return m == null ? Optional.empty() : Optional.ofNullable(m.get(field));
    }

    /** Every resolved secret as (name -> fields). */
    public Map<String, Map<String, String>> byName() {
        return byName;
    }

    /** The DuckDB secret type of the named secret (its {@code type} field). */
    public Optional<String> secretType(String name) {
        return namedField(name, "type");
    }

    /** Every resolved secret whose {@code type} field matches {@code secretType}. */
    public List<Map<String, String>> ofType(String secretType) {
        return byName.values().stream()
                .filter(m -> secretType.equals(m.get("type")))
                .toList();
    }

    /**
     * The fields of the secret whose {@code scope} is the longest prefix of
     * {@code path}. The connector serializes each secret's scope as a
     * newline-joined list of prefixes; a secret with no (or empty) scope matches
     * as a last-resort fallback. Empty only when there are no candidate secrets.
     *
     * @param path the path to match (e.g. {@code s3://bucket/data/x.dat})
     * @return the best-matching secret's fields
     */
    public Optional<Map<String, String>> forScope(String path) {
        return selectForScope(path, null);
    }

    /** Like {@link #forScope} but only over secrets of {@code secretType}. */
    public Optional<Map<String, String>> forScopeOfType(String path, String secretType) {
        return selectForScope(path, secretType);
    }

    /** A field of the best scope-matching secret for {@code path}. */
    public Optional<String> fieldFor(String path, String field) {
        return forScope(path).map(m -> m.get(field)).filter(java.util.Objects::nonNull);
    }

    private Optional<Map<String, String>> selectForScope(String path, String secretType) {
        Map<String, String> best = null;
        int bestLen = -1;
        Map<String, String> fallback = null;
        for (Map<String, String> fields : byName.values()) {
            if (secretType != null && !secretType.equals(fields.get("type"))) {
                continue;
            }
            String scope = fields.get("scope");
            if (scope == null || scope.isEmpty()) {
                if (fallback == null) {
                    fallback = fields;
                }
                continue;
            }
            for (String prefix : scope.split("\n")) {
                if (!prefix.isEmpty() && path.startsWith(prefix) && prefix.length() > bestLen) {
                    bestLen = prefix.length();
                    best = fields;
                }
            }
        }
        return Optional.ofNullable(best != null ? best : fallback);
    }

    private static String render(FieldVector v) {
        if (v.isNull(0)) {
            return "";
        }
        if (v instanceof VarCharVector vc) {
            return new String(vc.get(0), java.nio.charset.StandardCharsets.UTF_8);
        }
        if (v instanceof BigIntVector iv) {
            return String.valueOf(iv.get(0));
        }
        if (v instanceof IntVector iv) {
            return String.valueOf(iv.get(0));
        }
        if (v instanceof BitVector bv) {
            return String.valueOf(bv.get(0) != 0);
        }
        Object o = v.getObject(0);
        return o == null ? "" : o.toString();
    }
}
