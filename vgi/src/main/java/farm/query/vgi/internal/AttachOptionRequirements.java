// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import farm.query.vgi.AttachOptionSpec;
import farm.query.vgirpc.RpcError;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Enforces {@link AttachOptionSpec#required()} at {@code catalog_attach}.
 *
 * <p>A catalog that cannot be attached without a given option advertises that
 * at discovery; this is the matching refusal, so an omission fails on its own
 * terms instead of producing a catalog that reads as empty.
 */
public final class AttachOptionRequirements {

    private AttachOptionRequirements() {}

    /**
     * Throw when an option declared {@code required} has no corresponding entry
     * in the supplied ATTACH options.
     *
     * <p>Names compare case-insensitively, mirroring DuckDB's handling of
     * ATTACH option keys. The message matches the Python and Go
     * implementations — the extension's integration suite matches on its text.
     *
     * @param catalogName the catalog being attached, for the message
     * @param specs       the option specs this worker declares
     * @param optionsIpc  the supplied options as received: each option is a column of a
     *                    one-row batch, so the column names are the keys; may be {@code null}
     * @throws RpcError when a required option was not supplied
     */
    public static void validate(String catalogName, List<AttachOptionSpec> specs,
                                 byte[] optionsIpc) {
        if (specs == null || specs.isEmpty()) return;
        boolean anyRequired = specs.stream().anyMatch(AttachOptionSpec::required);
        if (!anyRequired) return;
        Set<String> supplied = suppliedNames(optionsIpc);
        if (supplied == null) return;
        List<String> missing = new ArrayList<>();
        for (AttachOptionSpec spec : specs) {
            if (spec.required() && !supplied.contains(spec.name().toLowerCase(Locale.ROOT))) {
                missing.add(spec.name());
            }
        }
        if (missing.isEmpty()) return;
        String quoted = missing.stream().map(n -> "'" + n + "'").collect(Collectors.joining(", "));
        throw new RpcError("ValueError",
                "Catalog '" + catalogName + "' cannot be attached without the required option"
                        + (missing.size() > 1 ? "s" : "") + " " + quoted + ".", "");
    }

    /** Column names of the supplied options batch, lower-cased. */
    private static Set<String> suppliedNames(byte[] optionsIpc) {
        if (optionsIpc == null || optionsIpc.length == 0) return Set.of();
        Set<String> names = new HashSet<>();
        try (VectorSchemaRoot root = BatchUtil.readSingleBatch(optionsIpc, Allocators.root())) {
            for (Field f : root.getSchema().getFields()) {
                names.add(f.getName().toLowerCase(Locale.ROOT));
            }
        } catch (RuntimeException e) {
            // Unreadable options are not the same as absent ones; let the attach
            // proceed and fail on its own terms rather than reporting a missing
            // option that may well have been supplied.
            return null;
        }
        return names;
    }
}
