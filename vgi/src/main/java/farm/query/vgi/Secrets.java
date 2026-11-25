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
 *
 * <p><b>Select by type with the type selectors, never by column name.</b> The
 * column is the secret's name: an unnamed {@code CREATE SECRET (TYPE brave, ...)}
 * arrives as {@code __default_brave}, a named one under whatever its author
 * called it. {@link #lookup(String)}, {@link #ofType(String)},
 * {@link #require(String)} and {@link #forScopeOfType(String, String)} match the
 * secret's {@code type} field (falling back to the column name only for a secret
 * that carries no {@code type} field). The selection rules follow
 * {@code vgi-secrets.md} section 5:</p>
 * <ul>
 *   <li><b>By name</b> ({@link #lookupNamed}): the secret of that name, if its
 *       type matches.</li>
 *   <li><b>By scope</b> ({@link #forScopeOfType}): among secrets of the type, the
 *       one with the longest scope prefix of the path (matched as DuckDB does, see
 *       {@link #scopeMatches}); failing that, one with an empty scope; failing
 *       that, none.</li>
 *   <li><b>By type alone</b> ({@link #lookup}): a secret of the type with an
 *       empty scope; failing that, the first secret of the type.</li>
 * </ul>
 *
 * <p>Every selected field map also carries a {@code name} entry (the secret's
 * name), filled from the column name when the client did not send one.</p>
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
                        fields.putIfAbsent("name", f.getName());
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

    /**
     * Every resolved secret of {@code secretType}, in delivery order. A secret
     * matches on its {@code type} field; one that carries no {@code type} field
     * matches when its column name equals {@code secretType}.
     *
     * @param secretType the DuckDB secret type
     * @return the matching secrets' fields (possibly empty)
     */
    public List<Map<String, String>> ofType(String secretType) {
        return byName.entrySet().stream()
                .filter(e -> isOfType(e.getKey(), e.getValue(), secretType))
                .map(Map.Entry::getValue)
                .toList();
    }

    /**
     * The secret of {@code secretType} to use when no path chooses between
     * several: one with an empty scope, failing that the first of the type.
     * Matches on the secret's {@code type} field, not its name, so an unnamed
     * {@code __default_<type>} secret and one named unlike its type are both
     * found.
     *
     * @param secretType the DuckDB secret type
     * @return the selected secret's fields, or empty when none of the type was resolved
     */
    public Optional<Map<String, String>> lookup(String secretType) {
        Map<String, String> first = null;
        for (Map.Entry<String, Map<String, String>> e : byName.entrySet()) {
            if (!isOfType(e.getKey(), e.getValue(), secretType)) {
                continue;
            }
            String scope = e.getValue().get("scope");
            if (scope == null || scope.isEmpty()) {
                return Optional.of(e.getValue());
            }
            if (first == null) {
                first = e.getValue();
            }
        }
        return Optional.ofNullable(first);
    }

    /**
     * The secret named {@code name}, only if it is of {@code secretType}.
     *
     * @param secretType the DuckDB secret type the secret must have
     * @param name the secret's unique DuckDB name (e.g. {@code __default_s3})
     * @return its fields, or empty when no such secret, or it has another type
     */
    public Optional<Map<String, String>> lookupNamed(String secretType, String name) {
        Map<String, String> m = byName.get(name);
        return m != null && isOfType(name, m, secretType) ? Optional.of(m) : Optional.empty();
    }

    /**
     * Like {@link #lookup(String)} but a missing secret is an error. The message
     * names the type and never a secret value, so it is safe to surface from a
     * bind.
     *
     * @param secretType the DuckDB secret type
     * @return the selected secret's fields
     * @throws IllegalStateException when no secret of {@code secretType} was resolved
     */
    public Map<String, String> require(String secretType) {
        return lookup(secretType).orElseThrow(() -> new IllegalStateException(
                "required secret of type '" + secretType + "' was not found; create one with "
                        + "CREATE SECRET (TYPE " + secretType + ", ...)"));
    }

    private static boolean isOfType(String name, Map<String, String> fields, String secretType) {
        String type = fields.get("type");
        return type != null ? type.equals(secretType) : name.equals(secretType);
    }

    /**
     * The fields of the secret whose {@code scope} is the longest prefix of
     * {@code path}, matched as DuckDB matches it (see {@link #scopeMatches}): an
     * authority-only scope such as {@code snmp://10.0.0.1} covers only that host
     * and paths under it, never {@code snmp://10.0.0.15} or
     * {@code snmp://10.0.0.1:161}. The connector serializes each secret's scope as a
     * newline-joined list of prefixes; a secret with no (or empty) scope matches
     * as a last-resort fallback. Empty when no scope matches and no candidate is
     * unscoped.
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
        for (Map.Entry<String, Map<String, String>> e : byName.entrySet()) {
            Map<String, String> fields = e.getValue();
            if (secretType != null && !isOfType(e.getKey(), fields, secretType)) {
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
                if (scopeMatches(prefix, path) && prefix.length() > bestLen) {
                    bestLen = prefix.length();
                    best = fields;
                }
            }
        }
        return Optional.ofNullable(best != null ? best : fallback);
    }

    /**
     * The URI authority of {@code text}, or {@code ""}; mirrors DuckDB's
     * {@code StringUtil::GetURIAuthority}. {@code text} counts as a URI only if it
     * contains {@code ://} after a non-empty scheme and its first {@code /} or
     * backslash is the one that starts that {@code ://}. The authority runs from
     * after {@code ://} to the first {@code /}, backslash, {@code ?} or {@code #}
     * (or the end).
     */
    private static String uriAuthority(String text) {
        int schemeEnd = text.indexOf("://");
        if (schemeEnd <= 0) {
            return "";
        }
        int firstSlash = -1;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == '/' || c == '\\') {
                firstSlash = i;
                break;
            }
        }
        if (firstSlash != schemeEnd + 1) {
            return "";
        }
        int start = schemeEnd + 3;
        int end = text.length();
        for (int i = start; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == '/' || c == '\\' || c == '?' || c == '#') {
                end = i;
                break;
            }
        }
        return text.substring(start, end);
    }

    /**
     * Whether a secret scope {@code prefix} covers {@code path}, exactly as DuckDB
     * decides.
     *
     * <p>A scope matches when it is a string prefix of {@code path}, except that
     * an authority-only URI scope ({@code scheme://authority} with nothing after
     * the authority) has an implicit trailing {@code /}: {@code snmp://10.0.0.1}
     * covers {@code snmp://10.0.0.1} and {@code snmp://10.0.0.1/x} but not
     * {@code snmp://10.0.0.15} or {@code snmp://10.0.0.1:161}, and
     * {@code s3://bucket} no longer covers {@code s3://bucket-evil/}. This is
     * DuckDB's {@code BaseSecret::MatchScore} as of the Haybarn 2.0.1 engine
     * (upstream duckdb#28684). Matching it keeps a worker from choosing a secret
     * DuckDB itself would not resolve for that path. An empty {@code prefix} is
     * not a scope; the selectors treat unscoped secrets as the fallback
     * separately.</p>
     *
     * <p>DuckDB indexes UTF-8 bytes; this indexes UTF-16 chars, and the two agree.
     * A UTF-8 prefix is a UTF-16 prefix of the same code points (both encodings
     * are prefix-free per code point). Every other check is either the relative
     * order of ASCII delimiters ({@code /}, backslash, {@code ?}, {@code #},
     * {@code ://}), which neither encoding changes, or "is the next code unit
     * after the prefix a {@code /}", and an ASCII character is the same single
     * unit at the same code-point position in both. (Strings with unpaired
     * surrogates have no UTF-8 form and cannot come from DuckDB.)</p>
     *
     * @param prefix one scope prefix of a secret
     * @param path the path being resolved
     * @return whether DuckDB would consider {@code prefix} to cover {@code path}
     */
    public static boolean scopeMatches(String prefix, String path) {
        if (prefix == null || path == null || prefix.isEmpty() || !path.startsWith(prefix)) {
            return false;
        }
        String authority = uriAuthority(prefix);
        boolean authorityOnly = !authority.isEmpty()
                && prefix.length() == prefix.indexOf("://") + 3 + authority.length();
        if (!authorityOnly) {
            return true;
        }
        return path.length() == prefix.length() || path.charAt(prefix.length()) == '/';
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
