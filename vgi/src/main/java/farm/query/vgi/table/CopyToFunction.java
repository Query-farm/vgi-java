// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.table;

import farm.query.vgi.buffering.BufferingFinalizeProducer;
import farm.query.vgi.buffering.TableBufferingCombineParams;
import farm.query.vgi.buffering.TableBufferingFinalizeParams;
import farm.query.vgi.buffering.TableBufferingFunction;
import farm.query.vgi.buffering.TableBufferingProcessParams;
import farm.query.vgi.function.Arguments;
import farm.query.vgi.internal.SchemaUtil;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.protocol.CopyToContext;
import farm.query.vgi.tableinout.TableInOutBindParams;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

/**
 * Base class for custom {@code COPY ... TO} format writers. Mirrors vgi-python's
 * {@code vgi.copy_to_function.CopyToFunction}.
 *
 * <p>A {@code CopyToFunction} lets a VGI catalog act as a remote sink: the user
 * runs {@code COPY (query|table) TO 'path' (FORMAT <name>, opt val, ...)} and
 * DuckDB streams the source rows out to the worker, which writes them to a
 * destination (a proprietary format, a remote API/object store, a custom sink).
 *
 * <p>Mechanically a {@code CopyToFunction} is a buffered (Sink+Combine)
 * {@link TableBufferingFunction} with <b>no Source phase</b> — it reuses the
 * whole {@code table_buffering_process} / {@code table_buffering_combine}
 * machinery on both sides:
 * <ul>
 *   <li>{@link #write} is called once per input batch (the buffered
 *       {@code process()} step, fanned out across DuckDB's sink threads /
 *       per-thread workers). Persist the batch to a shard via
 *       {@code params.storage()} ({@code execution_id}-scoped — see below).</li>
 *   <li>{@link #close} is called <b>exactly once</b> on the coordinator worker
 *       (the buffered {@code combine()} step, driven by DuckDB's once-only
 *       {@code copy_to_finalize}). Read the shards back and perform the terminal
 *       write+flush+close of the destination.</li>
 * </ul>
 *
 * <p>There is no finalize/drain phase, so the destination MUST be fully written
 * and closed inside {@link #close} — a writer that forgets leaves a silent
 * partial file.
 *
 * <p><b>Cross-process invariant.</b> {@link #write} and {@link #close} may run on
 * different worker processes (pool rotation / HTTP). Any shard state
 * {@link #close} needs MUST live in cross-process storage scoped by
 * {@code params.executionId()} ({@code params.storage()} is the canonical choice)
 * or be written to a destination that tolerates concurrent writers. Buffering on
 * {@code this} / static fields silently breaks under rotation.
 *
 * <p>The destination {@code path} + {@code format} arrive via the bind's
 * {@code copy_to} context ({@code params.copyTo()}); the COPY options arrive as
 * the function's normal {@link #argumentSpecs() argument specs}. The source
 * schema rides the bind's input schema ({@code params.inputSchema()}); each
 * {@link #write} also receives the batch directly.
 *
 * <p><b>Ordering.</b> By default the sink is parallel (per-thread workers write
 * shards, {@link #close} merges) and rows arrive in no particular order. To
 * require source order, override {@link #sinkOrderDependent()} to return
 * {@code true} — discovery surfaces {@code ordered=true} and the extension then
 * uses a single-threaded sink ({@code REGULAR_COPY_TO_FILE}).
 */
public abstract class CopyToFunction implements TableBufferingFunction {

    /**
     * The SQL {@code FORMAT} identifier users type, e.g. {@code COPY t TO 'x'
     * (FORMAT myfmt)}. Mirrors vgi-python's {@code COPY_TO_FORMAT}.
     *
     * @return the format name (without any attach-alias prefix)
     */
    public abstract String copyToFormat();

    /**
     * Optional free-text comment surfaced by {@code vgi_copy_formats()}.
     *
     * @return the comment, or {@code null} (the default)
     */
    public String copyToComment() { return null; }

    /**
     * The COPY direction. Always {@code "to"} for this base.
     *
     * @return {@code "to"}
     */
    public String copyToDirection() { return "to"; }

    /**
     * Secret-bind hook: forward {@code CREATE SECRET} credentials for
     * secret-backed cloud writes (S3/GCS/HTTP/…). Override to request the secrets
     * the writer needs — typically scoped by the destination path
     * ({@code params.copyTo().file_path()}). The framework's two-phase secret bind
     * resolves each lookup from the caller's secret store and surfaces the resolved
     * values on {@code params.secrets()} at {@link #write} / {@link #close} time.
     * Defaults to none, so a writer that never touched credentials is unaffected.
     * Mirrors vgi-python's {@code CopyToFunction.on_secrets}.
     *
     * @param params the bind-time parameters (carries {@code copyTo()})
     * @return the secrets to resolve; empty (the default) requests none
     */
    public List<CopySecretLookup> secretLookups(TableInOutBindParams params) {
        return List.of();
    }

    /**
     * A sink produces no rows — bind to an empty output schema. Final; subclasses
     * customise {@link #write} / {@link #close} / {@link #secretLookups}, not the
     * bind itself. On the first bind pass this forwards any {@link #secretLookups}
     * as a two-phase secret-scope request.
     *
     * @param params the bind-time parameters (input schema = the source columns)
     * @return a bind response (empty output schema, or a secret-scope request)
     */
    @Override
    public final BindResponse onBind(TableInOutBindParams params) {
        byte[] emptySchema = SchemaUtil.serializeSchema(new Schema(List.of()));
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
                return new BindResponse(emptySchema, new byte[0], types, scopes, names);
            }
        }
        return BindResponse.forSchema(emptySchema);
    }

    /**
     * Sink one input batch (delegates to {@link #write}) and return the
     * {@code execution_id} bucket so every batch of a query lands together.
     *
     * @param batch  one input batch to write
     * @param params the process-phase context (storage, execution id, copy-to context)
     * @return the {@code execution_id} as the opaque {@code state_id}
     */
    @Override
    public final byte[] process(VectorSchemaRoot batch, TableBufferingProcessParams params) {
        write(batch, params.args(), copyToPath(params.copyTo()), params);
        return params.executionId();
    }

    /**
     * Terminal write (delegates to {@link #close}), once on the coordinator. No
     * Source phase — returns an empty finalize-id list.
     *
     * @param stateIds the per-batch state ids (all the {@code execution_id})
     * @param params   the combine-phase context (storage, copy-to context, source schema)
     * @return an empty list (the COPY path never drains output)
     */
    @Override
    public final List<byte[]> combine(List<byte[]> stateIds, TableBufferingCombineParams params) {
        close(params.args(), copyToPath(params.copyTo()), params);
        return List.of();
    }

    /**
     * Never invoked on the COPY-TO path ({@link #combine} returns no finalize
     * ids). Returns an immediately-finishing producer for safety.
     *
     * @param params the finalize-phase context
     * @return a producer that finishes on first tick
     */
    @Override
    public final TableProducerState createFinalizeProducer(TableBufferingFinalizeParams params) {
        return new NoOutputFinalizeProducer(params);
    }

    /** A Source-phase producer that emits nothing — the COPY-TO path never drains. */
    private static final class NoOutputFinalizeProducer extends BufferingFinalizeProducer {
        NoOutputFinalizeProducer(TableBufferingFinalizeParams params) { super(params); }

        @Override public void produceTick(OutputCollector out, CallContext ctx) { out.finish(); }
    }

    /**
     * Persist one input {@code batch} to a shard (called per sink batch).
     *
     * <p>Store the batch in cross-process storage scoped by
     * {@code params.executionId()} ({@code params.storage()}) so {@link #close} —
     * which may run on a different worker process — can read it back; or write
     * directly to a concurrency-tolerant destination. Do NOT buffer on
     * {@code this}.
     *
     * @param batch    one input batch to buffer
     * @param options  parsed COPY options (the function's {@link Arguments})
     * @param filePath destination path from the {@code COPY ... TO 'path'} statement
     * @param params   the process-phase context (storage, execution id, source schema)
     */
    public abstract void write(VectorSchemaRoot batch, Arguments options, String filePath,
                                TableBufferingProcessParams params);

    /**
     * Write the destination and close it, exactly once.
     *
     * <p>Read the shards persisted by {@link #write} (via {@code params.storage()})
     * and perform the terminal write + flush + close of {@code filePath}. Called
     * even when zero rows were written (empty COPY) — produce an empty or
     * header-only file. The returned count is informational (DuckDB reports its
     * own row count); return the number of rows written.
     *
     * @param options  parsed COPY options (the function's {@link Arguments})
     * @param filePath destination path from the {@code COPY ... TO 'path'} statement
     * @param params   the combine-phase context (storage, source schema via {@code inputSchema()})
     * @return the number of rows written (informational)
     */
    public abstract long close(Arguments options, String filePath, TableBufferingCombineParams params);

    private String copyToPath(CopyToContext cf) {
        if (cf == null) {
            throw new IllegalStateException(name()
                    + " is a COPY TO format writer; invoke it via "
                    + "COPY <source> TO '<path>' (FORMAT " + copyToFormat() + ").");
        }
        return cf.file_path();
    }
}
