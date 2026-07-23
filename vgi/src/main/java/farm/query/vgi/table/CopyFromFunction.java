// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.table;

import farm.query.vgi.function.Arguments;
import farm.query.vgi.internal.SchemaUtil;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.protocol.CopyFromContext;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

/**
 * Base class for custom {@code COPY ... FROM} format readers. Mirrors
 * vgi-python's {@code vgi.copy_from_function.CopyFromFunction}.
 *
 * <p>A {@code CopyFromFunction} lets a VGI catalog act as a remote file-format
 * reader: the user runs {@code COPY target FROM 'path' (FORMAT <name>, opt val, ...)}
 * and the worker parses the source and streams Arrow batches that DuckDB inserts
 * into the local {@code target} table.
 *
 * <p>Mechanically it is an ordinary producer-mode {@link TableFunction} (so it
 * reuses the whole bind/init/scan path). What makes it a COPY format is twofold:
 * <ul>
 *   <li>it returns its {@code FORMAT} identifier from {@link #copyFromFormat()}, and
 *   <li>the catalog advertises it through {@code catalog_copy_from_formats}, so the
 *       VGI DuckDB extension registers a DuckDB {@code CopyFunction} for it.
 * </ul>
 *
 * <p>The COPY statement's file path and the target table's schema arrive on the
 * bind via {@link CopyFromContext} ({@code params.copyFrom()}); the COPY options
 * arrive as the function's normal {@link #argumentSpecs() argument specs}
 * (declare them like any other function — their {@code doc} becomes the option
 * description). The {@code file_path} is supplied by COPY, not as an option.
 *
 * <p>Subclasses implement {@link #read}, emitting batches whose schema matches
 * {@code expectedSchema} exactly — DuckDB inserts <b>no</b> cast between the scan
 * and the INSERT, so a type/arity mismatch is rejected at COPY bind.
 */
public abstract class CopyFromFunction implements TableFunction {

    /** Sink for the batches a {@link #read} call produces. */
    @FunctionalInterface
    public interface Emitter {
        /**
         * Emit one batch. Its schema must equal the COPY target's
         * {@code expectedSchema} exactly. Ownership transfers to the framework.
         *
         * @param batch the batch to stream into the COPY target
         */
        void emit(VectorSchemaRoot batch);
    }

    /**
     * The SQL {@code FORMAT} identifier users type, e.g. {@code COPY t FROM 'x'
     * (FORMAT myfmt)}. Mirrors vgi-python's {@code COPY_FROM_FORMAT}.
     *
     * @return the format name (without any attach-alias prefix)
     */
    public abstract String copyFromFormat();

    /**
     * Optional free-text comment surfaced by {@code vgi_copy_formats()}.
     *
     * @return the comment, or {@code null} (the default)
     */
    public String copyFromComment() { return null; }

    /**
     * The COPY direction. Only {@code "from"} is supported today; reserved for a
     * future {@code COPY ... TO}.
     *
     * @return {@code "from"}
     */
    public String copyFromDirection() { return "from"; }

    /**
     * Bind the output schema to the COPY target's schema. DuckDB forces the
     * scan's output types to the target table's columns, so a COPY-FROM reader
     * must produce exactly {@code expected_schema}. Final — subclasses customise
     * {@link #read}, not the bind.
     *
     * @param params the bind-time parameters (carries {@code copyFrom()})
     * @return a bind response carrying the COPY target's expected schema
     */
    @Override
    public final BindResponse onBind(TableBindParams params) {
        CopyFromContext cf = params.copyFrom();
        if (cf == null) {
            throw new IllegalArgumentException(name()
                    + " is a COPY FROM format reader; invoke it via "
                    + "COPY <table> FROM '<path>' (FORMAT " + copyFromFormat()
                    + "), not as a table function.");
        }
        // On the first bind pass, forward any requested secrets as a two-phase
        // secret-scope request; the resolved values reach read() via params.secrets().
        if (!params.resolvedSecretsProvided()) {
            List<CopySecretLookup> lookups = secretLookups(params);
            if (lookups != null && !lookups.isEmpty()) {
                List<String> types = new java.util.ArrayList<>(lookups.size());
                List<String> scopes = new java.util.ArrayList<>(lookups.size());
                List<String> names = new java.util.ArrayList<>(lookups.size());
                for (CopySecretLookup l : lookups) {
                    types.add(l.secretType());
                    scopes.add(l.scope() == null ? "" : l.scope());
                    names.add(l.name() == null ? "" : l.name());
                }
                return new BindResponse(cf.expected_schema(), new byte[0], types, scopes, names);
            }
        }
        return BindResponse.forSchema(cf.expected_schema());
    }

    /**
     * Secret-bind hook: forward {@code CREATE SECRET} credentials for
     * secret-backed cloud sources (S3/GCS/HTTP/…). Override to request the secrets
     * the reader needs — typically scoped by the source path
     * ({@code params.copyFrom().file_path()}). The framework's two-phase secret
     * bind resolves each lookup and surfaces the resolved values on
     * {@code params.secrets()} at {@link #read} time. Defaults to none. Mirrors
     * vgi-python's {@code CopyFromFunction.on_secrets}.
     *
     * @param params the bind-time parameters (carries {@code copyFrom()})
     * @return the secrets to resolve; empty (the default) requests none
     */
    public List<CopySecretLookup> secretLookups(TableBindParams params) {
        return List.of();
    }

    /**
     * Build the single-shot producer that drives {@link #read} once and streams
     * its batches. Final — subclasses customise {@link #read}.
     *
     * @param params the per-execution init parameters (carries {@code copyFrom()})
     * @return a producer that reads the source on its first tick
     */
    @Override
    public final TableProducerState createProducer(TableInitParams params) {
        CopyFromContext cf = params.copyFrom();
        if (cf == null) {
            throw new IllegalStateException(name()
                    + ": missing COPY FROM context at init time");
        }
        return new CopyFromProducerState(this, params, cf);
    }

    /**
     * Parse {@code path} and emit Arrow batches via {@code out.emit(...)}.
     *
     * @param path           source path from the {@code COPY ... FROM 'path'} statement
     * @param options        parsed COPY options (the function's {@link Arguments})
     * @param expectedSchema the COPY target's schema; every emitted batch must match
     *     it exactly (names + types, in order)
     * @param params         the full init parameters (settings, secrets, storage, allocator)
     * @param out            collector to emit batches to; {@code finish()} is handled for you
     * @param ctx            the per-call context (logging, cancellation)
     */
    public abstract void read(String path, Arguments options, Schema expectedSchema,
                               TableInitParams params, Emitter out, CallContext ctx);

    /** Producer that reads the whole source on its first tick, then drains. */
    private static final class CopyFromProducerState extends TableProducerState {
        private final CopyFromFunction fn;
        private final TableInitParams params;
        private final CopyFromContext cf;
        private final Schema expectedSchema;
        private Deque<VectorSchemaRoot> pending;

        CopyFromProducerState(CopyFromFunction fn, TableInitParams params, CopyFromContext cf) {
            super(params);
            this.fn = fn;
            this.params = params;
            this.cf = cf;
            // The COPY target schema is the canonical emit schema. Prefer the
            // framework-provided output schema (already the target columns,
            // never projected — CopyFromFunction never opts into projection
            // pushdown); fall back to the bind context's expected schema.
            Schema fromCtx = SchemaUtil.deserializeSchema(cf.expected_schema());
            this.expectedSchema = params.outputSchema() != null ? params.outputSchema() : fromCtx;
        }

        @Override
        public void produceTick(OutputCollector out, CallContext ctx) {
            if (pending == null) {
                pending = new ArrayDeque<>();
                fn.read(cf.file_path(), params.arguments(), expectedSchema, params, pending::add, ctx);
            }
            if (pending.isEmpty()) {
                out.finish();
                return;
            }
            out.emit(pending.poll());
            // Finish in the SAME tick as the last batch. The read already
            // buffered everything, so there is nothing left to come back for —
            // and on the stateless HTTP transport an unfinished producer forces
            // a continuation token, which means serializing this state: it
            // holds the live init params (allocator, storage view), so that
            // serialization throws and a whole-file-in-one-batch COPY that
            // could have completed here fails instead.
            if (pending.isEmpty()) {
                out.finish();
            }
        }
    }
}
