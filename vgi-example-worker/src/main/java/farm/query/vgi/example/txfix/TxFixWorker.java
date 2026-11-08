// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.txfix;

import farm.query.vgi.AttachContext;
import farm.query.vgi.AttachOptionSpec;
import farm.query.vgi.AttachOptions;
import farm.query.vgi.AttachSettings;
import farm.query.vgi.TransactionHooks;
import farm.query.vgi.Worker;
import farm.query.vgi.catalog.CatalogTable;
import farm.query.vgi.catalog.ConstraintRecord;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.function.Stability;
import farm.query.vgi.protocol.BindResponse;
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
import farm.query.vgi.write.WriteOptions;
import farm.query.vgi.write.WriteResultMode;
import farm.query.vgi.write.WriteResults;
import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * The writable-table and transaction-hook fixture (vgi {@code docs/writable_tx_fixtures.md}):
 * catalog {@code txfix}, schema {@code main}, built only with the SDK's public API.
 *
 * <ul>
 *   <li>ATTACH options {@code transactional} (BOOLEAN, default false) and {@code token}
 *       (VARCHAR, secret). An {@link farm.query.vgi.AttachHook} turns
 *       {@code supports_transactions} on per attach from {@code transactional}; every function
 *       and hook reads {@code token} from its {@link AttachOptions}.</li>
 *   <li>{@code items} (id, name, qty + rowid) is writable through the {@code items_update}
 *       table in-out function declared on its descriptor; {@code labels} is read-only. Both
 *       carry constraint records, so the framework negotiates {@code vgi.constraint.enforce.v1}.</li>
 *   <li>{@link TransactionHooks}: begin mints the id, commit applies the staged changes (or
 *       refuses a negative qty, failing COMMIT), rollback discards them.</li>
 * </ul>
 *
 * State is per token, in files shared by every worker process ({@link TxFixState}).
 */
public final class TxFixWorker {

    /** The catalog name the tests attach. */
    public static final String CATALOG_NAME = "txfix";

    static final Schema ITEMS_USER = new Schema(List.of(
            Schemas.nonNull("id", Schemas.INT32),
            Schemas.nonNull("name", Schemas.UTF8),
            Schemas.nullable("qty", Schemas.INT64)));
    static final Schema ITEMS_SCAN = withRowId(ITEMS_USER);
    static final Schema LABELS = new Schema(List.of(
            Schemas.nonNull("id", Schemas.INT32),
            Schemas.nullable("label", Schemas.UTF8)));

    private static final byte[] REVISION = "txfix.v1".getBytes(StandardCharsets.UTF_8);
    private static final SecureRandom RNG = new SecureRandom();

    private TxFixWorker() {}

    private static Schema withRowId(Schema user) {
        List<Field> fields = new ArrayList<>(user.getFields());
        fields.add(WriteResults.rowIdField());
        return new Schema(fields);
    }

    /** Whether {@code needle} occurs in {@code haystack}: the never-plaintext check whoami reports. */
    static boolean contains(byte[] haystack, byte[] needle) {
        if (haystack == null || needle.length == 0) return false;
        outer:
        for (int i = 0; i + needle.length <= haystack.length; i++) {
            for (int j = 0; j < needle.length; j++) if (haystack[i + j] != needle[j]) continue outer;
            return true;
        }
        return false;
    }

    static String hex(byte[] b) {
        return b == null ? null : HexFormat.of().formatHex(b);
    }

    static String token(AttachOptions o) {
        return o.getString("token");
    }

    /** The state partition of the attach's token (a digest; the token itself is never stored). */
    static String partition(AttachOptions o) {
        return TxFixState.partition(token(o));
    }

    /** The transaction a call stages under: its id on a transactional attach, else none. */
    static String stagingTx(AttachOptions o, byte[] transactionId) {
        return o.getBoolean("transactional", false) ? hex(transactionId) : null;
    }

    private static FunctionMetadata volatileMeta(String description) {
        return FunctionMetadata.describe(description).withStability(Stability.VOLATILE);
    }

    private static List<ConstraintRecord> constraints(String table, boolean name) {
        List<ConstraintRecord> out = new ArrayList<>();
        out.add(enforced(ConstraintRecord.builder("main", table, "id_not_null").notNull("id")));
        if (name) out.add(enforced(ConstraintRecord.builder("main", table, "name_not_null").notNull("name")));
        out.add(enforced(ConstraintRecord.builder("main", table, "primary").primaryKey("id")));
        return out;
    }

    private static ConstraintRecord enforced(ConstraintRecord.Builder b) {
        return b.enforcement(ConstraintRecord.Enforcement.ENFORCED)
                .validation(ConstraintRecord.Validation.ALL_ROWS).build();
    }

    /**
     * The fixture worker.
     *
     * @return the configured worker
     */
    public static Worker build() {
        return Worker.builder()
                .catalogName(CATALOG_NAME)
                .catalogComment("Writable-table and transaction-hook fixture")
                .defaultSchema("main")
                .supportsTimeTravel(false)
                .attachOptions(
                        AttachOptionSpec.of("transactional",
                                "Stage changes in DuckDB transactions (supports_transactions)",
                                new ArrowType.Bool(), false),
                        AttachOptionSpec.of("token", "Partitions the fixture state", Schemas.UTF8, null)
                                .asSecret())
                .onAttach((attach, request) ->
                        AttachSettings.transactional(attach.options().getBoolean("transactional", false)))
                .transactionHooks(new Hooks())
                .registerTable("main", new Reset())
                .registerTable("main", new WhoAmI())
                .registerTable("main", new CurrentTx())
                .registerTable("main", new Events())
                .registerTable("main", new ItemsScan())
                .registerTable("main", new LabelsScan())
                .registerTableInOut("main", new ItemsUpdate())
                .registerTableInOut("main", new TagTx())
                .registerCatalogTable(CatalogTable.builder("main", "items", Schemas.serialize(ITEMS_SCAN))
                        .comment("Writable through items_update")
                        .scanFunction(ItemsScan.NAME)
                        .updateFunction(ItemsUpdate.NAME)
                        .writeResultMode(WriteOperation.UPDATE, WriteResultMode.CHANGES)
                        .constraints(constraints("items", true), REVISION)
                        .build())
                .registerCatalogTable(CatalogTable.builder("main", "labels", Schemas.serialize(LABELS))
                        .comment("Read-only")
                        .scanFunction(LabelsScan.NAME)
                        .constraints(constraints("labels", false), REVISION)
                        .build());
    }

    /**
     * Serve the fixture: stdio by default, {@code --unix PATH} for the launcher, {@code --http}.
     *
     * @param args the worker arguments
     */
    public static void main(String[] args) {
        build().runFromArgs(args);
    }

    // ------------------------------------------------------------------ hooks

    static final class Hooks implements TransactionHooks {
        @Override
        public byte[] begin(AttachContext attach) {
            byte[] id = new byte[16];
            RNG.nextBytes(id);
            TxFixState.withPartition(partition(attach.options()), p -> {
                p.event("begin", hex(id), true);
                return null;
            });
            return id;
        }

        @Override
        public void commit(AttachContext attach, byte[] transactionId) {
            String tx = hex(transactionId);
            String refused = TxFixState.withPartition(partition(attach.options()), p -> {
                TreeMap<Integer, TxFixState.Row> staged = p.staged.remove(tx);
                if (staged != null) {
                    for (TxFixState.Row r : staged.values()) {
                        if (r.qty() != null && r.qty() < 0) {
                            p.event("commit", tx, false);
                            return "txfix: commit refused: qty " + r.qty() + " for id " + r.id();
                        }
                    }
                    p.committed.putAll(staged);
                }
                p.event("commit", tx, true);
                return null;
            });
            if (refused != null) throw new IllegalStateException(refused);
        }

        @Override
        public void rollback(AttachContext attach, byte[] transactionId) {
            String tx = hex(transactionId);
            TxFixState.withPartition(partition(attach.options()), p -> {
                p.staged.remove(tx);
                p.event("rollback", tx, true);
                return null;
            });
        }
    }

    // ------------------------------------------------------- table functions

    /** A table function emitting rows computed at init (or at produce time, by override). */
    abstract static class RowsFunction implements TableFunction {
        abstract String name0();

        abstract Schema schema();

        abstract String description();

        boolean projection() {
            return false;
        }

        @Override
        public FunctionSpec spec() {
            FunctionMetadata m = volatileMeta(description());
            if (projection()) m = m.withPushdown(true, false, false);
            return FunctionSpec.builder(name0()).metadata(m).build();
        }

        @Override
        public BindResponse onBind(TableBindParams params) {
            return BindResponse.forSchema(Schemas.serialize(schema()));
        }
    }

    /** Producer emitting one batch built by {@link #rows()} on its first tick. */
    public abstract static class OnceState extends TableProducerState {
        public Schema outputSchema;
        public boolean done;

        abstract List<Map<String, Object>> rows();

        @Override
        public void produceTick(OutputCollector out, CallContext ctx) {
            if (done) {
                out.finish();
                return;
            }
            done = true;
            out.emit(WriteResults.rowsBatch(outputSchema, rows(), Allocators.root()));
        }
    }

    static final class Reset extends RowsFunction {
        @Override String name0() { return "reset"; }
        @Override Schema schema() { return new Schema(List.of(Schemas.nullable("ok", Schemas.BOOL))); }
        @Override String description() { return "Restore the token's initial rows; clear staged changes and events"; }

        @Override
        public TableProducerState createProducer(TableInitParams params) {
            ResetState s = new ResetState();
            s.outputSchema = params.outputSchema();
            s.partition = partition(params.attachOptions());
            return s;
        }
    }

    /** {@code reset()} producer. */
    public static final class ResetState extends OnceState {
        public String partition;

        @Override
        List<Map<String, Object>> rows() {
            TxFixState.withPartition(partition, p -> {
                p.reset();
                return null;
            });
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("ok", true);
            return List.of(row);
        }
    }

    static final class WhoAmI extends RowsFunction {
        @Override String name0() { return "whoami"; }
        @Override Schema schema() {
            return new Schema(List.of(Schemas.nullable("token", Schemas.UTF8),
                    Schemas.nullable("transactional", Schemas.BOOL),
                    Schemas.nullable("token_in_attach_data", Schemas.BOOL)));
        }
        @Override String description() { return "The attach's options as the function sees them"; }

        @Override
        public TableProducerState createProducer(TableInitParams params) {
            WhoAmIState s = new WhoAmIState();
            s.outputSchema = params.outputSchema();
            // Only the attach id is kept: the token is opened again while producing.
            s.attachId = params.attachId();
            return s;
        }
    }

    /** {@code whoami()} producer: re-opens the options from the attach id; no secret in state. */
    public static final class WhoAmIState extends OnceState {
        public byte[] attachId;

        @Override
        List<Map<String, Object>> rows() {
            AttachOptions options = AttachOptions.open(attachId);
            String token = token(options);
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("token", token);
            row.put("transactional", options.getBoolean("transactional", false));
            row.put("token_in_attach_data",
                    token != null && contains(attachId, token.getBytes(StandardCharsets.UTF_8)));
            return List.of(row);
        }
    }

    static final class CurrentTx extends RowsFunction {
        @Override String name0() { return "current_tx"; }
        @Override Schema schema() {
            return new Schema(List.of(Schemas.nullable("in_tx", Schemas.BOOL),
                    Schemas.nullable("tx_id", Schemas.UTF8)));
        }
        @Override String description() { return "The plaintext transaction id this call received"; }

        @Override
        public TableProducerState createProducer(TableInitParams params) {
            CurrentTxState s = new CurrentTxState();
            s.outputSchema = params.outputSchema();
            s.txId = hex(params.transactionId());
            return s;
        }
    }

    /** {@code current_tx()} producer: the id captured at init, emitted while producing. */
    public static final class CurrentTxState extends OnceState {
        public String txId;

        @Override
        List<Map<String, Object>> rows() {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("in_tx", txId != null);
            row.put("tx_id", txId);
            return List.of(row);
        }
    }

    static final class Events extends RowsFunction {
        @Override String name0() { return "events"; }
        @Override Schema schema() {
            return new Schema(List.of(Schemas.nullable("seq", Schemas.INT64),
                    Schemas.nullable("kind", Schemas.UTF8), Schemas.nullable("tx_id", Schemas.UTF8),
                    Schemas.nullable("ok", Schemas.BOOL)));
        }
        @Override String description() { return "The token's transaction-hook event log"; }

        @Override
        public TableProducerState createProducer(TableInitParams params) {
            EventsState s = new EventsState();
            s.outputSchema = params.outputSchema();
            s.partition = partition(params.attachOptions());
            return s;
        }
    }

    /** {@code events()} producer. */
    public static final class EventsState extends OnceState {
        public String partition;

        @Override
        List<Map<String, Object>> rows() {
            return TxFixState.withPartition(partition, p -> {
                List<Map<String, Object>> out = new ArrayList<>();
                for (TxFixState.Event e : p.events) {
                    Map<String, Object> row = new LinkedHashMap<>();
                    row.put("seq", e.seq());
                    row.put("kind", e.kind());
                    row.put("tx_id", e.txId());
                    row.put("ok", e.ok());
                    out.add(row);
                }
                return out;
            });
        }
    }

    static final class ItemsScan extends RowsFunction {
        static final String NAME = "items_scan";
        @Override String name0() { return NAME; }
        @Override Schema schema() { return ITEMS_SCAN; }
        @Override String description() { return "Scan items (row id = id), reading the transaction's staged changes"; }
        @Override boolean projection() { return true; }

        @Override
        public TableProducerState createProducer(TableInitParams params) {
            ItemsState s = new ItemsState();
            s.outputSchema = params.outputSchema();
            s.partition = partition(params.attachOptions());
            s.txId = stagingTx(params.attachOptions(), params.transactionId());
            return s;
        }
    }

    /** {@code items_scan()} producer: committed rows overlaid with the transaction's staged ones. */
    public static final class ItemsState extends OnceState {
        public String partition;
        public String txId;

        @Override
        List<Map<String, Object>> rows() {
            return TxFixState.withPartition(partition, p -> {
                List<Map<String, Object>> out = new ArrayList<>();
                for (TxFixState.Row r : p.view(txId).values()) {
                    Map<String, Object> row = r.asMap();
                    row.put("rowid", (long) r.id());
                    out.add(row);
                }
                return out;
            });
        }
    }

    static final class LabelsScan extends RowsFunction {
        static final String NAME = "labels_scan";
        @Override String name0() { return NAME; }
        @Override Schema schema() { return LABELS; }
        @Override String description() { return "Scan the read-only labels table"; }
        @Override boolean projection() { return true; }

        @Override
        public TableProducerState createProducer(TableInitParams params) {
            LabelsState s = new LabelsState();
            s.outputSchema = params.outputSchema();
            return s;
        }
    }

    /** {@code labels_scan()} producer. */
    public static final class LabelsState extends OnceState {
        @Override
        List<Map<String, Object>> rows() {
            List<Map<String, Object>> out = new ArrayList<>();
            for (Object[] r : new Object[][] {{1, "x"}, {2, "y"}}) {
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("id", r[0]);
                row.put("label", r[1]);
                out.add(row);
            }
            return out;
        }
    }

    // ---------------------------------------------------- table in-out functions

    /** An input cell as a plain Java value. */
    static Object cell(FieldVector v, int row) {
        if (v.isNull(row)) return null;
        Object o = v.getObject(row);
        return o instanceof Text t ? t.toString() : o;
    }

    static final class ItemsUpdate implements TableInOutFunction {
        static final String NAME = "items_update";

        @Override
        public FunctionSpec spec() {
            return FunctionSpec.builder(NAME).metadata(volatileMeta("Backs UPDATE txfix.main.items"))
                    .table("data").build();
        }

        @Override
        public BindResponse onBind(TableInOutBindParams params) {
            WriteResultMode mode = WriteOptions.from(params.arguments()).resultMode();
            return BindResponse.forSchema(Schemas.serialize(WriteResults.resultSchema(mode, ITEMS_USER)));
        }

        @Override
        public TableInOutExchangeState createExchange(TableInOutInitParams params) {
            UpdateState s = new UpdateState();
            s.partition = partition(params.attachOptions());
            s.transactional = params.attachOptions().getBoolean("transactional", false);
            s.txId = stagingTx(params.attachOptions(), params.transactionId());
            s.mode = WriteOptions.from(params.arguments()).resultMode().wireName();
            return s;
        }
    }

    /** {@code items_update} exchange: validates every row, then applies or stages them. */
    public static final class UpdateState extends TableInOutExchangeState {
        public String partition;
        public boolean transactional;
        public String txId;
        public String mode;

        @Override
        public void onInputBatch(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
            VectorSchemaRoot batch = input.root();
            FieldVector rowids = batch.getVector("rowid");
            if (rowids == null) throw new IllegalArgumentException("txfix: UPDATE input carries no rowid");
            List<Map<String, Object>> before = new ArrayList<>();
            List<Map<String, Object>> after = new ArrayList<>();
            TxFixState.withPartition(partition, p -> {
                TreeMap<Integer, TxFixState.Row> current = p.view(txId);
                List<TxFixState.Row> updated = new ArrayList<>();
                for (int i = 0; i < batch.getRowCount(); i++) {
                    int id = ((Number) cell(rowids, i)).intValue();
                    TxFixState.Row old = current.get(id);
                    if (old == null) throw new IllegalArgumentException("txfix: no row with id " + id);
                    String name = old.name();
                    Long qty = old.qty();
                    for (FieldVector v : batch.getFieldVectors()) {
                        Object value = cell(v, i);
                        switch (v.getName()) {
                            case "id" -> {
                                if (value == null || ((Number) value).intValue() != id) {
                                    throw new IllegalArgumentException("txfix: id is not updatable");
                                }
                            }
                            case "name" -> name = (String) value;
                            case "qty" -> qty = value == null ? null : ((Number) value).longValue();
                            default -> { }
                        }
                    }
                    if (name == null) throw new IllegalArgumentException("txfix: name must not be NULL");
                    if (!transactional && qty != null && qty < 0) {
                        throw new IllegalArgumentException("txfix: qty must be >= 0");
                    }
                    before.add(old.asMap());
                    TxFixState.Row row = new TxFixState.Row(id, name, qty);
                    updated.add(row);
                    after.add(row.asMap());
                }
                for (TxFixState.Row row : updated) {
                    if (txId != null) p.staged.computeIfAbsent(txId, k -> new TreeMap<>()).put(row.id(), row);
                    else p.committed.put(row.id(), row);
                }
                return null;
            });
            out.emit(WriteResults.resultBatch(WriteResultMode.fromWire(mode), ITEMS_USER, before, after,
                    Allocators.root()));
        }
    }

    static final class TagTx implements TableInOutFunction {
        @Override
        public FunctionSpec spec() {
            return FunctionSpec.builder("tag_tx")
                    .metadata(volatileMeta("Pass the input through, adding the transaction id seen while processing"))
                    .table("data").build();
        }

        @Override
        public BindResponse onBind(TableInOutBindParams params) {
            List<Field> fields = new ArrayList<>(params.inputSchema().getFields());
            fields.add(Schemas.nullable("tx_id", Schemas.UTF8));
            return BindResponse.forSchema(Schemas.serialize(new Schema(fields)));
        }

        @Override
        public TableInOutExchangeState createExchange(TableInOutInitParams params) {
            TagState s = new TagState();
            s.outputSchema = params.outputSchema();
            s.txId = hex(params.transactionId());
            return s;
        }
    }

    /** {@code tag_tx} exchange: moves each input column into the output and appends {@code tx_id}. */
    public static final class TagState extends TableInOutExchangeState {
        public Schema outputSchema;
        public String txId;

        @Override
        public void onInputBatch(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
            VectorSchemaRoot in = input.root();
            int n = in.getRowCount();
            List<FieldVector> vectors = new ArrayList<>();
            List<Field> fields = outputSchema.getFields();
            for (int c = 0; c < fields.size() - 1; c++) {
                FieldVector target = fields.get(c).createVector(Allocators.root());
                in.getVector(c).makeTransferPair(target).transfer();
                vectors.add(target);
            }
            VarCharVector tx = (VarCharVector) fields.get(fields.size() - 1).createVector(Allocators.root());
            tx.allocateNew(n);
            for (int i = 0; i < n; i++) {
                if (txId == null) tx.setNull(i);
                else tx.setSafe(i, txId.getBytes(StandardCharsets.UTF_8));
            }
            tx.setValueCount(n);
            vectors.add(tx);
            out.emit(new VectorSchemaRoot(fields, vectors, n));
        }
    }
}
