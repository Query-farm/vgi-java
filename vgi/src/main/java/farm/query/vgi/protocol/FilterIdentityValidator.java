// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.protocol;

import java.util.regex.Pattern;

final class FilterIdentityValidator {
    private static final Pattern NAMESPACE =
            Pattern.compile("[a-z][a-z0-9]*(?:\\.[a-z][a-z0-9_]*)*");
    private static final Pattern NAME = Pattern.compile("[a-z][a-z0-9_]*");

    private FilterIdentityValidator() {}

    static void validate(String namespace, String name, long version) {
        if (namespace == null || !NAMESPACE.matcher(namespace).matches()) {
            throw new IllegalArgumentException("invalid filter capability namespace: " + namespace);
        }
        if (name == null || !NAME.matcher(name).matches()) {
            throw new IllegalArgumentException("invalid filter capability name: " + name);
        }
        if (version == 0) {
            throw new IllegalArgumentException("filter capability version must be a positive uint64");
        }
    }
}
