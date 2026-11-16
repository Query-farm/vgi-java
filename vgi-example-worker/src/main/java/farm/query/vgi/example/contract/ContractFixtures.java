// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.contract;

import farm.query.vgi.Worker;
import farm.query.vgi.buffering.BufferingStorageHolder;
import farm.query.vgi.catalog.CatalogTable;
import farm.query.vgi.catalog.ConstraintRecord;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.function.Stability;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.storage.BoundStorage;
import farm.query.vgi.table.TableBindParams;
import farm.query.vgi.table.TableFunction;
import farm.query.vgi.table.TableInitParams;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.tableinout.TableInOutBindParams;
import farm.query.vgi.tableinout.TableInOutExchangeState;
import farm.query.vgi.tableinout.TableInOutFunction;
import farm.query.vgi.tableinout.TableInOutInitParams;
import farm.query.vgi.types.Schemas;
import farm.query.vgi.write.WriteOperation;
import farm.query.vgi.write.WriteResultMode;
import farm.query.vgi.write.WriteResults;
import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The {@code example.contract} cross-SDK fixtures (protocol {@code vgi.v3} / 3.3), backing
 * {@code visibility/example_table_only.test} and {@code write_contract/example_insert_finalize.test}.
 *
 * <ul>
 *   <li>{@code good}: read-only, a row id, scanned by the {@code table_only}
 *       {@code contract_good_scan} with projection pushdown.</li>
 *   <li>{@code ranged}: read-only, scanned by a {@code table_only} function named exactly
 *       {@code range}, so the worker's function binds rather than DuckDB's {@code range()}.</li>
 *   <li>{@code notes}: INSERT only, {@code count} result mode, a NOT_NULL constraint record the
 *       worker enforces; the count comes from the insert's finalize alone.</li>
 * </ul>
 *
 * <p>{@code contract.amounts} (Decimal32 / Decimal64 columns) is not declared: arrow-java has no
 * 32- or 64-bit decimal vectors, so {@code write_contract/example_decimal_width.test} is excluded
 * from this SDK's lanes.
 *
 * <p>The schema carries no comment: {@code table/database_tags.test} pins exactly the commented
 * schemas. Nothing is stored; the insert validates, counts and discards.
 */
public final class ContractFixtures {

    /** The schema every fixture lives in. */
    public static final String SCHEMA = "contract";

    static final Schema GOOD = new Schema(List.of(
            Schemas.nullable("id", Schemas.INT64),
            Schemas.nullable("label", Schemas.UTF8),
            WriteResults.rowIdField()));
    static final Schema RANGED = new Schema(List.of(Schemas.nullable("who", Schemas.UTF8)));
    static final Schema NOTES = new Schema(List.of(
            Schemas.nonNull("id", Schemas.INT64),
            Schemas.nullable("note", Schemas.UTF8)));

    private static final byte[] REVISION = "contract.v1".getBytes(StandardCharsets.UTF_8);
    private static final SecureRandom RNG = new SecureRandom();

    private ContractFixtures() {}

    /**
     * Register the {@code contract} schema's tables and their functions on {@code w}.
     *
     * @param w the example worker
     */
    public static void register(Worker w) {
        w.registerTable(SCHEMA, new Scan(GOOD_SCAN, GOOD, "Scan contract.good (row id = id)", true))
         .registerTable(SCHEMA, new Scan(RANGED_SCAN, RANGED,
                 "Scan contract.ranged: a table_only scan named range", false))
         .registerTable(SCHEMA, new Scan(NOTES_SCAN, NOTES, "Scan contract.notes: always empty", false))
         .registerTableInOut(SCHEMA, new NotesInsert())
         .registerCatalogTable(CatalogTable.builder(SCHEMA, "good", Schemas.serialize(GOOD))
                 .comment("Read-only; table_only scan with a row id")
                 .scanFunction(GOOD_SCAN)
                 .build())
         .registerCatalogTable(CatalogTable.builder(SCHEMA, "ranged", Schemas.serialize(RANGED))
                 .comment("Read-only; its table_only scan is named range")
                 .scanFunction(RANGED_SCAN)
                 .build())
         .registerCatalogTable(CatalogTable.builder(SCHEMA, "notes", Schemas.serialize(NOTES))
                 .comment("INSERT only; the count comes from finalize")
                 .scanFunction(NOTES_SCAN)
                 .insertFunction(NotesInsert.NAME)
                 .writeResultMode(WriteOperation.INSERT, WriteResultMode.COUNT)
                 .constraints(List.of(ConstraintRecord.builder(SCHEMA, "notes", "notes_id_not_null")
                         .notNull("id")
                         .enforcement(ConstraintRecord.Enforcement.ENFORCED)
                         .validation(ConstraintRecord.Validation.ALL_ROWS)
                         .build()), REVISION)
                 .build());
    }

    private static FunctionMetadata volatileMeta(String description) {
        return FunctionMetadata.describe(description).withStability(Stability.VOLATILE);
    }

    // ------------------------------------------------------------------ scans

    /** Scan function names; each is table_only, since no table sets {@code exposeScan}. */
    static final String GOOD_SCAN = "contract_good_scan";
    static final String RANGED_SCAN = "range";
    static final String NOTES_SCAN = "contract_notes_scan";

    /** A no-argument scan emitting a fixed row set ({@link RowsState#rows}). */
    static final class Scan implements TableFunction {
        private final String name;
        private final Schema schema;
        private final String description;
        private final boolean projection;

        Scan(String name, Schema schema, String description, boolean projection) {
            this.name = name;
            this.schema = schema;
            this.description = description;
            this.projection = projection;
        }

        @Override
        public FunctionSpec spec() {
            FunctionMetadata m = volatileMeta(description);
            if (projection) m = m.withPushdown(true, false, false);
            return FunctionSpec.builder(name).metadata(m).build();
        }

        @Override
        public BindResponse onBind(TableBindParams params) {
            return BindResponse.forSchema(Schemas.serialize(schema));
        }

        @Override
        public TableProducerState createProducer(TableInitParams params) {
            RowsState s = new RowsState();
            // The projection-narrowed schema: only the requested columns are emitted.
            s.emitSchema = params.outputSchema();
            s.function = name;
            return s;
        }
    }

    /** Producer for {@link Scan}: one batch of the function's rows, then end of stream. */
    public static final class RowsState extends TableProducerState {
        /** The schema to emit (projection applied). */
        public Schema emitSchema;
        /** Which scan this is. */
        public String function;
        /** Whether the batch was emitted. */
        public boolean done;

        /** No-arg constructor for HTTP state-token deserialization. */
        public RowsState() {}

        static List<Map<String, Object>> rows(String function) {
            List<Map<String, Object>> out = new ArrayList<>();
            switch (function) {
                case GOOD_SCAN -> {
                    String[] labels = {"a", "b", "c"};
                    for (int i = 0; i < labels.length; i++) {
                        Map<String, Object> row = new LinkedHashMap<>();
                        row.put("id", (long) (i + 1));
                        row.put("label", labels[i]);
                        row.put("rowid", (long) (i + 1));
                        out.add(row);
                    }
                }
                case RANGED_SCAN -> out.add(new LinkedHashMap<>(Map.of("who", "vgi example worker")));
                default -> { }
            }
            return out;
        }

        @Override
        public void produceTick(OutputCollector out, CallContext ctx) {
            if (done) {
                out.finish();
                return;
            }
            done = true;
            out.emit(WriteResults.rowsBatch(emitSchema, rows(function), Allocators.root()));
        }
    }

    // ------------------------------------------------------------------ insert

    /** Storage namespace of the per-call row counters. */
    static final byte[] NS_COUNT = "contract.notes_insert.count".getBytes(StandardCharsets.UTF_8);

    /**
     * {@code contract_notes_insert}: validates each batch, adds its rows to a counter in
     * execution-scoped shared storage keyed by the call id it bound (its bind opaque data, as
     * txfix keys its write calls), answers each batch with an empty count batch, and reports the
     * call's total from its finalize. Pooled worker processes may serve one statement, so the
     * counter is never a per-process global.
     */
    static final class NotesInsert implements TableInOutFunction {
        static final String NAME = "contract_notes_insert";

        @Override
        public FunctionSpec spec() {
            return FunctionSpec.builder(NAME)
                    .metadata(volatileMeta("Backs INSERT into contract.notes; count from finalize"))
                    .table("data").build();
        }

        @Override
        public boolean hasFinalize() {
            return true;
        }

        @Override
        public BindResponse onBind(TableInOutBindParams params) {
            byte[] callId = new byte[16];
            RNG.nextBytes(callId);
            return new BindResponse(Schemas.serialize(WriteResults.resultSchema(WriteResultMode.COUNT, NOTES)),
                    callId, List.of(), List.of(), List.of());
        }

        @Override
        public TableInOutExchangeState createExchange(TableInOutInitParams params) {
            NotesInsertState s = new NotesInsertState();
            s.executionId = params.storage().executionId();
            s.attachId = params.attachId();
            s.callId = params.bindOpaqueData();
            s.emitSchema = params.outputSchema();
            return s;
        }

        @Override
        public List<VectorSchemaRoot> finish(TableInOutInitParams params) {
            BoundStorage storage = params.storage();
            byte[] callId = params.bindOpaqueData();
            long total = storage.counterGet(NS_COUNT, callId);
            storage.counterDelete(NS_COUNT, callId);
            return List.of(WriteResults.countBatch(total, Allocators.root()));
        }
    }

    /**
     * {@code contract_notes_insert} exchange. Its fields ride the HTTP state token; the storage
     * view (a live connection) is re-bound from the execution id it serialized.
     */
    public static final class NotesInsertState extends TableInOutExchangeState {
        /** The execution the counter is scoped to. */
        public byte[] executionId;
        /** The attach plaintext (storage shard routing). */
        public byte[] attachId;
        /** The call id bound at bind time: the counter's key. */
        public byte[] callId;
        /** The bound count result schema. */
        public Schema emitSchema;
        private transient BoundStorage storage;

        /** No-arg constructor for HTTP state-token deserialization. */
        public NotesInsertState() {}

        private BoundStorage storage() {
            if (storage == null) storage = BufferingStorageHolder.bind(executionId, attachId);
            return storage;
        }

        @Override
        public void onInputBatch(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
            VectorSchemaRoot batch = input.root();
            for (Field f : batch.getSchema().getFields()) {
                if (!f.isNullable()) {
                    throw new IllegalArgumentException(
                            "contract: notes_insert input field " + f.getName() + " is non-nullable");
                }
            }
            FieldVector ids = batch.getVector("id");
            int n = batch.getRowCount();
            for (int i = 0; i < n; i++) {
                if (ids == null || ids.isNull(i)) {
                    throw new IllegalArgumentException("contract: notes.id required");
                }
            }
            storage().counterAdd(NS_COUNT, callId, n);
            // The count is finalize's to report: every batch is answered with an empty one.
            VectorSchemaRoot empty = VectorSchemaRoot.create(emitSchema, Allocators.root());
            empty.setRowCount(0);
            out.emit(empty);
        }
    }
}
