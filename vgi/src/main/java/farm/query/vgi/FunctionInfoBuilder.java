// Copyright 2026 Query Farm LLC - https://query.farm
package farm.query.vgi;

import farm.query.vgi.aggregate.AggregateFunction;
import farm.query.vgi.function.Arguments;
import farm.query.vgi.function.FunctionDescriptor;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.internal.ArgumentSpecSerializer;
import farm.query.vgi.internal.BatchUtil;
import farm.query.vgi.internal.SchemaUtil;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.protocol.FunctionInfo;
import farm.query.vgi.scalar.ScalarBindParams;
import farm.query.vgi.scalar.ScalarFunction;
import farm.query.vgi.table.TableBindParams;
import farm.query.vgi.table.TableFunction;
import farm.query.vgi.tableinout.TableInOutBindParams;
import farm.query.vgi.tableinout.TableInOutFunction;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;
import java.util.Map;

/**
 * Builds the {@code catalog_schema_contents_functions} entry ({@link FunctionInfo}) for a
 * function descriptor: arguments, output schema (from an empty-argument bind), parameter
 * defaults, metadata-driven capabilities (pushdown, ordering, partitioning, splits),
 * {@code has_finalize}, {@code max_workers} and {@code required_secrets}.
 *
 * <p>The single implementation behind the public {@link CatalogInterface#functionInfoFor}; the
 * worker's own catalog listing ({@code farm.query.vgi.internal.VgiServiceImpl}) goes through
 * that same method, so code-implemented catalogs and registered functions advertise identical
 * entries.
 */
final class FunctionInfoBuilder {
    private FunctionInfoBuilder() {}

    /**
     * Build the {@code FunctionInfo} for a scalar function exactly as the
     * catalog-enumeration path does (binds with empty args, maps metadata).
     * Tests reach it through the {@code VgiServiceImpl} delegate of the same name.
     *
     * @param fn the scalar function.
     * @param schemaName the owning schema name.
     * @return the wire {@link FunctionInfo}.
     */
    private static FunctionInfo scalarFunctionInfo(ScalarFunction fn, String schemaName) {
        byte[] out = catalogBindOutput(fn.name(),
                () -> fn.onBind(new ScalarBindParams(fn.name(), Arguments.empty(), null, Map.of())));
        return withRequiredSecrets(baseFunctionInfo(fn, schemaName, "SCALAR", out, false),
                fn.requiredSecrets());
    }

    /**
     * Build the {@code FunctionInfo} for a table function exactly as the
     * catalog-enumeration path does. 
     *
     * @param fn the table function.
     * @param schemaName the owning schema name.
     * @return the wire {@link FunctionInfo}.
     */
    private static FunctionInfo tableFunctionInfo(TableFunction fn, String schemaName) {
        if (fn instanceof farm.query.vgi.table.CopyFromFunction) {
            // COPY-FROM readers have no static output schema — it's the COPY
            // target's columns, supplied per-statement via the copy_from bind
            // context. Calling onBind here would (intentionally) throw, so
            // advertise an empty output schema, mirroring vgi-python's
            // _function_to_info (which emits an empty schema for table funcs).
            return withRequiredSecrets(baseFunctionInfo(fn, schemaName, "TABLE",
                    SchemaUtil.serializeSchema(new Schema(List.of())), false), fn.requiredSecrets());
        }
        byte[] out = catalogBindOutput(fn.name(),
                () -> fn.onBind(new TableBindParams(fn.name(), Arguments.empty(), null, Map.of())));
        return withRequiredSecrets(baseFunctionInfo(fn, schemaName, "TABLE", out, false),
                fn.requiredSecrets());
    }

    private static FunctionInfo toTableInOutFunctionInfo(TableInOutFunction fn, String schemaName) {
        byte[] out = catalogBindOutput(fn.name(),
                () -> fn.onBind(new TableInOutBindParams(fn.name(), Arguments.empty(), null, Map.of())));
        // has_finalize drives the C++ per-substream FINALIZE-phase init after
        // input EOS (parallel streaming finalize). Blended (RowTransformFunction)
        // registrations additionally advertise input_from_args, entering the
        // C++ in-out registration branch with real-typed positional args; the
        // C++ rejects input_from_args && has_finalize, and RowTransformFunction
        // pins hasFinalize() to false, so the pair stays consistent.
        FunctionInfo base = baseFunctionInfo(fn, schemaName, "TABLE", out, fn.hasFinalize());
        // Re-stamp the TIO-specific fields baseFunctionInfo hardcodes:
        // max_workers (0 = the C++ parallel-by-default per-substream fan-out;
        // 1 would be the A3 serial opt-out AND disqualify the exchange-mode
        // result cache) and input_from_args for blended registrations.
        return withRequiredSecrets(new FunctionInfo(
                base.comment(), base.tags(), base.name(), base.schema_path(), base.function_type(),
                base.arguments(), base.output_schema(), base.parameter_default_values(),
                base.stability(), base.null_handling(), base.argument_monotonicity(),
                base.description(), base.examples(), base.categories(), base.projection_pushdown(),
                base.filter_pushdown(), base.sampling_pushdown(), base.late_materialization(),
                base.filter_semantic_profiles(), base.additional_filter_functions(),
                base.runtime_filter_algorithms(), base.filter_evaluation_contexts(),
                base.order_preservation(), (int) fn.maxWorkers(), base.supports_batch_index(),
                base.supports_splits(), base.filters_exactly_applied(),
                base.supports_positions(), base.split_token_ttl_seconds(),
                base.partition_kind(), base.order_dependent(), base.distinct_dependent(),
                base.supports_window(), base.streaming_partitioned(), base.has_finalize(),
                base.source_order_dependent(), base.sink_order_dependent(),
                base.requires_input_batch_index(),
                fn instanceof farm.query.vgi.tableinout.RowTransformFunction,
                fn instanceof farm.query.vgi.tableinout.RowTransformFunction rt
                        ? rt.inputRowsPerCall() : null,
                base.required_settings(), base.required_secrets()), fn.requiredSecrets());
    }

    /**
     * The {@code FunctionInfo} this worker's own listing sends for {@code fn} in a schema, before
     * {@code visibility} is stamped: the builder behind {@link CatalogInterface#functionInfoFor}.
     *
     * @param fn         a scalar, table, table in-out, table-buffering or aggregate function
     * @param schemaPath the schema the function is listed in
     * @return the wire {@link FunctionInfo}, with {@code visibility} unset
     * @throws IllegalArgumentException for any other descriptor, or an empty schema path
     */
    static FunctionInfo describe(FunctionDescriptor fn, List<String> schemaPath) {
        if (fn == null) throw new IllegalArgumentException("functionInfoFor: function is null");
        if (schemaPath == null || schemaPath.isEmpty()) {
            throw new IllegalArgumentException("functionInfoFor: " + fn.name() + " needs a schema path");
        }
        String schema = schemaPath.get(schemaPath.size() - 1);
        FunctionInfo info;
        if (fn instanceof ScalarFunction f) info = scalarFunctionInfo(f, schema);
        else if (fn instanceof farm.query.vgi.buffering.TableBufferingFunction f) info = toBufferingFunctionInfo(f, schema);
        else if (fn instanceof TableInOutFunction f) info = toTableInOutFunctionInfo(f, schema);
        else if (fn instanceof TableFunction f) info = tableFunctionInfo(f, schema);
        else if (fn instanceof AggregateFunction<?> f) info = aggregateFunctionInfo(f, schema);
        else throw new IllegalArgumentException("functionInfoFor: " + fn.name() + " is a "
                + fn.getClass().getName() + ", not a scalar, table, table in-out, table-buffering or"
                + " aggregate function");
        if (schemaPath.size() == 1) return info;
        return new FunctionInfo(info.comment(), info.tags(), info.name(), List.copyOf(schemaPath),
                info.function_type(), info.arguments(), info.output_schema(), info.parameter_default_values(),
                info.stability(), info.null_handling(), info.argument_monotonicity(), info.description(),
                info.examples(), info.categories(), info.projection_pushdown(), info.filter_pushdown(),
                info.sampling_pushdown(), info.late_materialization(), info.filter_semantic_profiles(),
                info.additional_filter_functions(), info.runtime_filter_algorithms(),
                info.filter_evaluation_contexts(), info.order_preservation(), info.max_workers(),
                info.supports_batch_index(), info.supports_splits(), info.filters_exactly_applied(),
                info.supports_positions(), info.split_token_ttl_seconds(), info.partition_kind(),
                info.order_dependent(), info.distinct_dependent(), info.supports_window(),
                info.streaming_partitioned(), info.has_finalize(), info.source_order_dependent(),
                info.sink_order_dependent(), info.requires_input_batch_index(), info.input_from_args(),
                info.input_rows_per_call(), info.required_settings(), info.required_secrets(),
                info.visibility());
    }

    private static FunctionInfo aggregateFunctionInfo(AggregateFunction<?> fn, String schemaName) {
        // The C++ extension pre-resolves the declared secrets and delivers them
        // on AggregateBindRequest.secrets.
        return withRequiredSecrets(baseFunctionInfo(fn, schemaName, "AGGREGATE",
                SchemaUtil.serializeSchema(fn.outputSchema()), false), fn.requiredSecrets());
    }

    /**
     * Re-stamp {@code required_secrets}, which {@code baseFunctionInfo}
     * hardcodes empty. The extension resolves each declared secret before the
     * first bind rather than waiting for a two-phase request.
     */
    private static FunctionInfo withRequiredSecrets(
            FunctionInfo base, List<farm.query.vgi.protocol.FunctionRequiredSecret> required) {
        if (required == null || required.isEmpty()) return base;
        for (int i = 0; i < required.size(); i++) {
            if (required.get(i) == null) {
                // A dropped entry would mean the extension never resolves the
                // secret the function depends on; fail the listing instead.
                throw new IllegalArgumentException("function '" + base.name()
                        + "' declares a null required secret at index " + i
                        + "; use FunctionRequiredSecret.ofType/scoped/named");
            }
        }
        return new FunctionInfo(
                base.comment(), base.tags(), base.name(), base.schema_path(), base.function_type(),
                base.arguments(), base.output_schema(), base.parameter_default_values(),
                base.stability(), base.null_handling(), base.argument_monotonicity(),
                base.description(), base.examples(), base.categories(), base.projection_pushdown(),
                base.filter_pushdown(), base.sampling_pushdown(), base.late_materialization(),
                base.filter_semantic_profiles(), base.additional_filter_functions(),
                base.runtime_filter_algorithms(), base.filter_evaluation_contexts(),
                base.order_preservation(), base.max_workers(), base.supports_batch_index(),
                base.supports_splits(), base.filters_exactly_applied(),
                base.supports_positions(), base.split_token_ttl_seconds(),
                base.partition_kind(), base.order_dependent(), base.distinct_dependent(),
                base.supports_window(), base.streaming_partitioned(), base.has_finalize(),
                base.source_order_dependent(), base.sink_order_dependent(),
                base.requires_input_batch_index(), base.input_from_args(), base.input_rows_per_call(),
                base.required_settings(), required);
    }

    private static FunctionInfo toBufferingFunctionInfo(
            farm.query.vgi.buffering.TableBufferingFunction fn, String schemaName) {
        byte[] out = catalogBindOutput(fn.name(),
                () -> fn.onBind(new TableInOutBindParams(fn.name(), Arguments.empty(), null, Map.of())));
        FunctionInfo base = baseFunctionInfo(fn.name(), fn.metadata(), schemaName, "TABLE_BUFFERING",
                ArgumentSpecSerializer.toIpcBytes(fn.argumentSpecs()), out,
                encodeParameterDefaults(fn), /*hasFinalize=*/true, 1, null);
        // Re-stamp the buffering-specific ordering flags (baseFunctionInfo
        // hardcodes them false; they're only meaningful for TableBuffering).
        return withRequiredSecrets(new FunctionInfo(
                base.comment(), base.tags(), base.name(), base.schema_path(), base.function_type(),
                base.arguments(), base.output_schema(), base.parameter_default_values(),
                base.stability(), base.null_handling(), base.argument_monotonicity(),
                base.description(), base.examples(), base.categories(), base.projection_pushdown(),
                base.filter_pushdown(), base.sampling_pushdown(), base.late_materialization(),
                base.filter_semantic_profiles(), base.additional_filter_functions(),
                base.runtime_filter_algorithms(), base.filter_evaluation_contexts(),
                base.order_preservation(), base.max_workers(), base.supports_batch_index(),
                base.supports_splits(), base.filters_exactly_applied(),
                base.supports_positions(), base.split_token_ttl_seconds(),
                base.partition_kind(), base.order_dependent(), base.distinct_dependent(),
                base.supports_window(), base.streaming_partitioned(), base.has_finalize(),
                fn.sourceOrderDependent(), fn.sinkOrderDependent(), fn.requiresInputBatchIndex(),
                base.input_from_args(), base.input_rows_per_call(),
                base.required_settings(), base.required_secrets()), fn.requiredSecrets());
    }

    private static byte[] bindOutput(BindResponse r) {
        return r.output_schema() != null ? r.output_schema() : new byte[0];
    }

    private static final org.slf4j.Logger CATALOG_LOG =
            org.slf4j.LoggerFactory.getLogger(FunctionInfoBuilder.class);

    /**
     * Run a catalog-listing bind (empty arguments, no input schema) and return its
     * output schema bytes. A function whose {@code onBind} rejects the empty-argument
     * probe (validating required arguments, needing an attach, ...) must not break
     * ATTACH for the whole catalog: the failure is swallowed and an empty output
     * schema is advertised instead, which is what vgi-python always advertises for
     * table functions. The real bind at call time still runs {@code onBind} with the
     * caller's arguments. Constraint enforcement is a no-op here: every declared
     * argument is absent, and absent values skip their constraints.
     */
    private static byte[] catalogBindOutput(String name, java.util.function.Supplier<BindResponse> bind) {
        try {
            return bindOutput(bind.get());
        } catch (RuntimeException e) {
            CATALOG_LOG.debug("catalog listing: onBind of '{}' with empty arguments threw ({}); "
                    + "advertising an empty output schema", name, e.toString());
            return SchemaUtil.serializeSchema(new Schema(List.of()));
        }
    }

    private static FunctionInfo baseFunctionInfo(FunctionDescriptor fn,
                                           String schemaName, String type,
                                           byte[] outputSchema, boolean hasFinalize) {
        // Only TableFunction has a meaningful maxWorkers; everything else
        // is single-worker by definition.
        int maxWorkers = fn instanceof TableFunction tf ? (int) tf.maxWorkers() : 1;
        List<String> argumentMonotonicity = null;
        if (fn.argumentMonotonicity() != null) {
            if (!"SCALAR".equals(type)) {
                throw new IllegalArgumentException(
                        "argumentMonotonicity is only valid for scalar functions");
            }
            if (fn.argumentMonotonicity().size() != fn.argumentSpecs().size()) {
                throw new IllegalArgumentException("argumentMonotonicity has "
                        + fn.argumentMonotonicity().size() + " entries, expected "
                        + fn.argumentSpecs().size() + " declaration slots");
            }
            argumentMonotonicity = fn.argumentMonotonicity().stream()
                    .map(value -> {
                        if (value == null) {
                            throw new IllegalArgumentException(
                                    "argumentMonotonicity must not contain null entries");
                        }
                        return value.wireName();
                    })
                    .toList();
        }
        return baseFunctionInfo(fn.name(), fn.metadata(), schemaName, type,
                ArgumentSpecSerializer.toIpcBytes(fn.argumentSpecs()), outputSchema,
                encodeParameterDefaults(fn), hasFinalize,
                maxWorkers, argumentMonotonicity);
    }

    private static FunctionInfo baseFunctionInfo(String name, FunctionMetadata md, String schemaName,
                                           String type, byte[] arguments, byte[] outputSchema,
                                           byte[] parameterDefaultValues,
                                           boolean hasFinalize, int maxWorkers,
                                           List<String> argumentMonotonicity) {
        return new FunctionInfo(
                null,
                md.tags() == null ? Map.of() : md.tags(),
                name,
                List.of(schemaName),
                type,
                arguments,
                outputSchema,
                parameterDefaultValues,
                stabilityWire(md.stability()),
                nullHandlingWire(md.nullHandling()),
                argumentMonotonicity,
                md.description(),
                md.examples() == null ? List.of() : md.examples(),
                md.categories() == null ? List.of() : md.categories(),
                md.projectionPushdown(),
                md.filterPushdown() ? Boolean.TRUE : null,
                md.samplingPushdown() ? Boolean.TRUE : null,
                md.lateMaterialization() ? Boolean.TRUE : null,
                md.filterSemanticProfiles(),
                md.additionalFilterFunctions(),
                md.runtimeFilterAlgorithms(),
                md.filterEvaluationContexts(),
                md.orderPreservation() == null ? null : md.orderPreservation().wireName(),
                maxWorkers,
                md.supportsBatchIndex(),
                md.supportsSplits(),
                md.filtersExactlyApplied(),
                md.supportsPositions(),
                md.splitTokenTtlSeconds(),
                md.partitionKind() == null ? "NOT_PARTITIONED" : md.partitionKind().name(),
                "NOT_ORDER_DEPENDENT",
                "NOT_DISTINCT_DEPENDENT",
                false,
                false,  // streaming_partitioned
                hasFinalize,
                false,  // source_order_dependent — only meaningful for TableBuffering
                false,  // sink_order_dependent
                false,  // requires_input_batch_index
                false,  // input_from_args — re-stamped true for RowTransformFunction
                null,   // input_rows_per_call — re-stamped for RowTransformFunction
                List.of(),
                List.of());
    }

    private static byte[] encodeParameterDefaults(
            FunctionDescriptor fn) {
        var defaults = fn.parameterDefaultValues();
        if (defaults == null) return null;
        if (defaults.getRowCount() != 1) {
            throw new IllegalArgumentException(
                    "parameterDefaultValues must contain exactly one row, got "
                            + defaults.getRowCount());
        }
        var arguments = ArgumentSpecSerializer.toSchema(fn.argumentSpecs()).getFields();
        int argumentIndex = 0;
        for (var defaultField : defaults.getSchema().getFields()) {
            while (argumentIndex < arguments.size()
                    && !arguments.get(argumentIndex).getName().equals(defaultField.getName())) {
                argumentIndex++;
            }
            if (argumentIndex == arguments.size()) {
                throw new IllegalArgumentException(
                        "parameterDefaultValues field '" + defaultField.getName()
                                + "' is not in argument signature order");
            }
            var argumentField = arguments.get(argumentIndex);
            if (!argumentField.getType().equals(defaultField.getType())) {
                throw new IllegalArgumentException(
                        "parameterDefaultValues field '" + defaultField.getName()
                                + "' has type " + defaultField.getType()
                                + ", expected " + argumentField.getType());
            }
            argumentIndex++;
        }
        return BatchUtil.writeSingleBatch(defaults);
    }

    private static String stabilityWire(farm.query.vgi.function.Stability s) {
        return switch (s) {
            case CONSISTENT -> "CONSISTENT";
            case VOLATILE -> "VOLATILE";
            case CONSISTENT_WITHIN_QUERY -> "CONSISTENT_WITHIN_QUERY";
        };
    }

    private static String nullHandlingWire(farm.query.vgi.function.NullHandling n) {
        return switch (n) {
            case DEFAULT -> "DEFAULT";
            case SPECIAL -> "SPECIAL";
        };
    }
}
