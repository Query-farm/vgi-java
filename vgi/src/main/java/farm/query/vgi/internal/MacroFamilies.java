// Copyright 2026 Query Farm LLC - https://query.farm
package farm.query.vgi.internal;

import farm.query.vgi.catalog.Macro;
import farm.query.vgi.catalog.MacroType;
import farm.query.vgi.protocol.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** Converts simple provider descriptors to one canonical overload with explicit SQL context. */
public final class MacroFamilies {
    private MacroFamilies() {}

    public static MacroInfo fromDescriptor(Macro macro) {
        var owner = List.of(macro.schema());
        var kind = macro.macroType() == MacroType.SCALAR ? "scalar" : "table";
        var context = new QueryEvaluationContext("UTC", "binary", "asc", "nulls_last", true);
        var defaults = macro.parameterDefaults() == null ? Map.<String, String>of() : macro.parameterDefaults();
        var docs = macro.parameterDocs() == null ? Map.<String, String>of() : macro.parameterDocs();
        for (var name : defaults.keySet()) {
            if (!macro.parameters().contains(name)) throw new IllegalArgumentException("unknown macro default parameter: " + name);
        }
        var names = new HashSet<String>();
        var parameters = new ArrayList<MacroParameter>();
        boolean seenDefault = false;
        for (var name : macro.parameters()) {
            if (name.isEmpty() || name.indexOf('\0') >= 0 || !names.add(name.toLowerCase(Locale.ROOT)))
                throw new IllegalArgumentException("invalid or duplicate macro parameter");
            boolean hasDefault = defaults.containsKey(name);
            if (seenDefault && !hasDefault) throw new IllegalArgumentException("required parameter cannot follow a default");
            seenDefault |= hasDefault;
            byte[] value = hasDefault ? MacroDefaultsEncoder.encode(java.util.Collections.singletonMap(name, defaults.get(name))) : null;
            parameters.add(new MacroParameter(name, null, value, null, docs.get(name)));
        }
        if (macro.definition() == null || macro.definition().isBlank() || macro.definition().indexOf('\0') >= 0)
            throw new IllegalArgumentException("macro definition must be nonempty SQL without NUL");
        var overload = new MacroOverload(parameters, macro.definition(), "duckdb.macro.v1", owner, kind, context);
        return new MacroInfo(macro.comment(), macro.tags() == null ? Map.of() : macro.tags(), macro.name(), owner, kind.toUpperCase(Locale.ROOT), List.of(overload));
    }
}
