// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.internal;

/**
 * Central registry of dictionary IDs used by the {@code *InfoSerializer}
 * family. Each {@link EnumDict} field claims one of these constants;
 * adding a new dict-encoded serializer means appending one constant here
 * so collisions are caught by the compiler, not by silent IPC corruption.
 *
 * <p>IDs are scoped per {@link org.apache.arrow.vector.dictionary.DictionaryProvider}
 * instance — different serializers each create their own provider, so
 * IDs only need to be unique across dicts that share a provider. We
 * allocate them globally regardless to keep the registry trivial to
 * audit.</p>
 */
final class DictionaryIds {

    // FunctionInfoSerializer
    static final long FUNCTION_TYPE = 1;
    static final long STABILITY = 2;
    static final long NULL_HANDLING = 3;
    static final long ORDER_PRESERVATION = 4;
    static final long ORDER_DEPENDENT = 5;
    static final long DISTINCT_DEPENDENT = 6;

    // MacroInfoSerializer
    static final long MACRO_TYPE = 7;

    private DictionaryIds() {}
}
