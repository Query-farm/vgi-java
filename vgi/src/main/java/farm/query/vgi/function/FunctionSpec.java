// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.function;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.ArrayList;
import java.util.List;

/**
 * The constant descriptor data a VGI function declares once: its SQL name, its
 * {@link FunctionMetadata}, and its {@link ArgSpec} list. Returned from
 * {@link FunctionDescriptor#spec()}; the {@code name()}/{@code metadata()}/
 * {@code argumentSpecs()} accessors default to reading this record, so a
 * function implements one method instead of three.
 *
 * <p>Does not carry an output schema — output is declared per function kind
 * ({@code AggregateFunction.outputSchema()} / {@code onBind(...)}), so there is
 * nothing shareable to hoist here.
 *
 * <p>Build with {@link #builder(String)}: positional adders ({@link Builder#arg},
 * {@link Builder#constArg}, {@link Builder#nested}, {@link Builder#varargs},
 * {@link Builder#any}, {@link Builder#table}) auto-assign positions 0, 1, 2, …
 * in call order, so the author never writes — and so cannot transpose — an
 * index. {@link Builder#named} declares a kwarg-style argument with no positional
 * slot ({@code position = -1}). {@link Builder#arg(ArgSpec)} is an escape hatch
 * for exotic combinations (e.g. varargs + any-typed) the fluent methods don't cover.
 *
 * @param name          the function's SQL name.
 * @param metadata      the function's {@link FunctionMetadata}.
 * @param argumentSpecs the function's argument specifications (defensively copied; never {@code null}).
 */
public record FunctionSpec(String name, FunctionMetadata metadata, List<ArgSpec> argumentSpecs) {

    /** Canonical constructor: normalizes a {@code null} argument list to empty and defensively copies. */
    public FunctionSpec {
        argumentSpecs = argumentSpecs == null ? List.of() : List.copyOf(argumentSpecs);
    }

    /**
     * name + metadata, no arguments.
     *
     * @param name     the function's SQL name.
     * @param metadata the function's {@link FunctionMetadata}.
     */
    public FunctionSpec(String name, FunctionMetadata metadata) {
        this(name, metadata, List.of());
    }

    /**
     * Start a fluent builder for the named function.
     *
     * @param name the function's SQL name.
     * @return a new {@link Builder}.
     */
    public static Builder builder(String name) {
        return new Builder(name);
    }

    /** Fluent builder; positional adders auto-number, {@link #named} does not. */
    public static final class Builder {
        private final String name;
        private FunctionMetadata metadata;
        private final List<ArgSpec> args = new ArrayList<>();
        private int nextPosition = 0;

        private Builder(String name) {
            this.name = name;
        }

        /** Shorthand for {@code metadata(FunctionMetadata.describe(description))}.
         *
         *  @param description the function description.
         *  @return this builder. */
        public Builder description(String description) {
            this.metadata = FunctionMetadata.describe(description);
            return this;
        }

        /**
         * Set the function metadata.
         *
         * @param metadata the {@link FunctionMetadata}.
         * @return this builder.
         */
        public Builder metadata(FunctionMetadata metadata) {
            this.metadata = metadata;
            return this;
        }

        /** Positional runtime-column argument (non-const).
         *
         *  @param argName argument name.
         *  @param type    Arrow type.
         *  @return this builder. */
        public Builder arg(String argName, ArrowType type) {
            args.add(new ArgSpec(argName, nextPosition++, type));
            return this;
        }

        /** Positional compile-time-constant argument (bind-validated).
         *
         *  @param argName argument name.
         *  @param type    Arrow type.
         *  @return this builder. */
        public Builder constArg(String argName, ArrowType type) {
            args.add(ArgSpec.positional(argName, nextPosition++, type));
            return this;
        }

        /** Positional nested-type argument (struct/list/map) with explicit children.
         *
         *  @param argName  argument name.
         *  @param type     nested Arrow type.
         *  @param children child {@link Field}s describing the nested shape.
         *  @return this builder. */
        public Builder nested(String argName, ArrowType type, List<Field> children) {
            args.add(ArgSpec.nested(argName, nextPosition++, type, children, false));
            return this;
        }

        /** Positional varargs argument.
         *
         *  @param argName argument name.
         *  @param type    Arrow type each vararg element takes.
         *  @return this builder. */
        public Builder varargs(String argName, ArrowType type) {
            args.add(ArgSpec.varargs(argName, nextPosition++, type));
            return this;
        }

        /** Positional "any"-typed argument matched at bind time against {@code bounds}.
         *
         *  @param argName argument name.
         *  @param bounds  bind-time type predicates.
         *  @return this builder. */
        public Builder any(String argName, TypeBoundPredicate... bounds) {
            args.add(ArgSpec.any(argName, nextPosition++, List.of(bounds)));
            return this;
        }

        /** Positional TABLE-typed input (table-in-out functions).
         *
         *  @param argName argument name.
         *  @return this builder. */
        public Builder table(String argName) {
            args.add(ArgSpec.table(argName, nextPosition++));
            return this;
        }

        /** Named-only (kwarg) argument with a default; consumes no positional slot.
         *
         *  @param argName      kwarg name.
         *  @param type         Arrow type.
         *  @param defaultValue default literal when the kwarg is omitted.
         *  @return this builder. */
        public Builder named(String argName, ArrowType type, String defaultValue) {
            args.add(ArgSpec.named(argName, type, defaultValue));
            return this;
        }

        /**
         * Escape hatch: append a fully-formed {@link ArgSpec} verbatim (no
         * auto-numbering). For shapes the fluent methods don't compose, e.g.
         * varargs + any-typed. Position, if any, must be set on the spec.
         *
         * @param spec the fully-formed argument spec.
         * @return this builder.
         */
        public Builder arg(ArgSpec spec) {
            args.add(spec);
            return this;
        }

        /**
         * Build the immutable {@link FunctionSpec}.
         *
         * @return the assembled spec.
         */
        public FunctionSpec build() {
            return new FunctionSpec(name, metadata, args);
        }
    }
}
