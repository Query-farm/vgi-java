// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.pushdown;

import java.util.regex.Pattern;

/** Stable namespace/name/version identity used by v2 calls and artifacts. */
public record FilterIdentity(String namespace, String name, long version) {
    private static final Pattern NAMESPACE =
            Pattern.compile("[a-z][a-z0-9]*(?:\\.[a-z][a-z0-9_]*)*");
    private static final Pattern NAME = Pattern.compile("[a-z][a-z0-9_]*");

    public FilterIdentity {
        if (namespace == null || !NAMESPACE.matcher(namespace).matches()) {
            throw new IllegalArgumentException("invalid filter namespace: " + namespace);
        }
        if (name == null || !NAME.matcher(name).matches()) {
            throw new IllegalArgumentException("invalid filter name: " + name);
        }
        if (version == 0) {
            throw new IllegalArgumentException("filter version must be a positive uint64");
        }
    }
}
