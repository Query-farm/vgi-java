// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import farm.query.vgi.AttachContext;
import farm.query.vgi.AttachOptionSpec;
import farm.query.vgi.AttachOptions;
import farm.query.vgi.AttachSettings;
import farm.query.vgi.CatalogProfiles;
import farm.query.vgi.TransactionHooks;
import farm.query.vgi.Worker;
import farm.query.vgi.catalog.CatalogTable;
import farm.query.vgi.catalog.ConstraintRecord;
import farm.query.vgi.client.TableInfoDecoder;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.protocol.BindRequest;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.protocol.CatalogAttachRequest;
import farm.query.vgi.protocol.CatalogAttachResult;
import farm.query.vgi.protocol.CatalogCapabilities;
import farm.query.vgi.protocol.InitRequest;
import farm.query.vgi.protocol.TableInfo;
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
import farm.query.vgirpc.MethodNotImplementedError;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.marshal.RecordCodec;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Writable catalog tables, per-attach {@code supports_transactions}, transaction hooks, and the
 * transaction id and ATTACH options (secret ones included) reaching functions -- with and
 * without opaque-data sealing, and across service instances standing in for worker processes.
 */
class WritableTransactionsTest {

    private static final byte[] KEY = new byte[32];
    private static final String TOKEN = "tok-SECRET-0123456789";

    static {
        for (int i = 0; i < KEY.length; i++) KEY[i] = (byte) (0x21 + i);
    }

    /** Everything the functions and hooks saw, in order. */
    static final List<String> SEEN = new CopyOnWriteArrayList<>();
    static final List<byte[]> HOOK_IDS = new CopyOnWriteArrayList<>();
    static volatile String refuseCommit;

    @BeforeEach
    void clear() {
        SEEN.clear();
        HOOK_IDS.clear();
        refuseCommit = null;
    }

    static final Schema ITEMS = new Schema(List.of(
            Schemas.nonNull("id", Schemas.INT32), Schemas.nullable("qty", Schemas.INT64), WriteResults.rowIdField()));

    static String hex(byte[] b) {
        return b == null ? "null" : java.util.HexFormat.of().formatHex(b);
    }

    /** {@code probe()}: records the transaction id and token at bind and at init. */
    static final class Probe implements TableFunction {
        @Override public FunctionSpec spec() {
            return FunctionSpec.builder("probe").metadata(FunctionMetadata.describe("probe")).build();
        }
        @Override public BindResponse onBind(TableBindParams p) {
            SEEN.add("bind tx=" + hex(p.transactionId()) + " token=" + p.attachOptions().getString("token"));
            return BindResponse.forSchema(Schemas.serialize(new Schema(List.of(Schemas.nullable("x", Schemas.INT64)))));
        }
        @Override public TableProducerState createProducer(TableInitParams p) {
            SEEN.add("init tx=" + hex(p.transactionId()) + " token=" + p.attachOptions().getString("token"));
            return new TableProducerState() {
                @Override public void produceTick(OutputCollector out, CallContext ctx) { out.finish(); }
            };
        }
    }

    /** {@code probe_io(TABLE)}: records the same at bind and init, plus attach id and bind data. */
    static final class ProbeIo implements TableInOutFunction {
        @Override public FunctionSpec spec() {
            return FunctionSpec.builder("probe_io").metadata(FunctionMetadata.describe("probe")).table("data").build();
        }
        @Override public BindResponse onBind(TableInOutBindParams p) {
            SEEN.add("io-bind tx=" + hex(p.transactionId()) + " token=" + p.attachOptions().getString("token")
                    + " attach=" + hex(p.attachOpaqueData()));
            return new BindResponse(Schemas.serialize(p.inputSchema()), "bind-state".getBytes(StandardCharsets.UTF_8),
                    List.of(), List.of(), List.of());
        }
        @Override public TableInOutExchangeState createExchange(TableInOutInitParams p) {
            SEEN.add("io-init tx=" + hex(p.transactionId()) + " token=" + p.attachOptions().getString("token")
                    + " attach=" + hex(p.attachId()) + " bind=" + new String(p.bindOpaqueData(), StandardCharsets.UTF_8));
            return new TableInOutExchangeState() {
                @Override public void onInputBatch(AnnotatedBatch in, OutputCollector out, CallContext ctx) {}
            };
        }
    }

    static final class Hooks implements TransactionHooks {
        @Override public byte[] begin(AttachContext attach) {
            byte[] id = TransactionHooks.super.begin(attach);
            HOOK_IDS.add(id);
            SEEN.add("begin tx=" + hex(id) + " token=" + attach.options().getString("token"));
            return id;
        }
        @Override public void commit(AttachContext attach, byte[] id) {
            SEEN.add("commit tx=" + hex(id) + " token=" + attach.options().getString("token"));
            if (refuseCommit != null) throw new IllegalStateException(refuseCommit);
        }
        @Override public void rollback(AttachContext attach, byte[] id) {
            SEEN.add("rollback tx=" + hex(id) + " token=" + attach.options().getString("token"));
        }
    }

    static List<ConstraintRecord> records(String table) {
        return List.of(ConstraintRecord.builder("main", table, "id_nn").notNull("id")
                .enforcement(ConstraintRecord.Enforcement.ENFORCED).validation(ConstraintRecord.Validation.ALL_ROWS).build());
    }

    static Worker worker() {
        return Worker.builder().catalogName("wtx")
                .attachOptions(
                        AttachOptionSpec.of("transactional", "t", new ArrowType.Bool(), false),
                        AttachOptionSpec.of("token", "secret", Schemas.UTF8, null).asSecret())
                .onAttach((attach, request) ->
                        AttachSettings.transactional(attach.options().getBoolean("transactional", false)))
                .transactionHooks(new Hooks())
                .registerTable("main", new Probe())
                .registerTableInOut("main", new ProbeIo())
                .registerCatalogTable(CatalogTable.builder("main", "items", Schemas.serialize(ITEMS))
                        .scanFunction("probe").updateFunction("probe_io")
                        .writeResultMode(WriteOperation.UPDATE, WriteResultMode.CHANGES)
                        .constraints(records("items"), "r1".getBytes(StandardCharsets.UTF_8)).build())
                .registerCatalogTable(CatalogTable.builder("main", "labels", Schemas.serialize(ITEMS))
                        .scanFunction("probe")
                        .constraints(records("labels"), "r1".getBytes(StandardCharsets.UTF_8)).build());
    }

    static VgiServiceImpl service(Worker w, boolean sealed) {
        return new VgiServiceImpl(w, w.scalars(), w.tables(), w.tableInOuts(), w.aggregates(), sealed,
                sealed ? KEY : null);
    }

    static byte[] options(String token, Boolean transactional) {
        List<Field> fields = new ArrayList<>();
        if (token != null) fields.add(new Field("token", FieldType.nullable(new ArrowType.Utf8()), null));
        if (transactional != null) fields.add(new Field("transactional", FieldType.nullable(new ArrowType.Bool()), null));
        try (VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(fields), Allocators.root())) {
            root.allocateNew();
            if (token != null) ((VarCharVector) root.getVector("token")).setSafe(0, token.getBytes(StandardCharsets.UTF_8));
            if (transactional != null) ((BitVector) root.getVector("transactional")).setSafe(0, transactional ? 1 : 0);
            root.setRowCount(1);
            return BatchUtil.writeSingleBatch(root);
        }
    }

    static byte[] offer(List<String> read, List<String> write) {
        return CatalogProfiles.clientOffer("test", new CatalogCapabilities(read, List.of(), write, List.of()));
    }

    static BindRequest bind(String fn, byte[] attach, byte[] tx, Schema input) {
        return new BindRequest(fn, null, "TABLE", input == null ? null : Schemas.serialize(input), null, null,
                attach, tx, false, null, null, null, null, List.of("main"), null);
    }

    static InitRequest inlineInit(BindRequest bind, Schema out, String phase) {
        return new InitRequest(RecordCodec.serializeToBytes(bind), Schemas.serialize(out), null,
                null, null, null, phase, null, null, null, null, null, null, null, null, null, null, null, null);
    }

    @ParameterizedTest(name = "sealed={0}")
    @ValueSource(booleans = {false, true})
    void functionsAndHooksSeeOnePlaintextTransactionIdAndTheSecret(boolean sealed) {
        Worker w = worker();
        VgiServiceImpl svc = service(w, sealed);
        CatalogAttachResult plain = svc.catalog_attach(
                CatalogAttachRequest.of("wtx", options(TOKEN, null), "", ""), null);
        CatalogAttachResult tx = svc.catalog_attach(
                CatalogAttachRequest.of("wtx", options(TOKEN, true), "", ""), null);
        assertFalse(plain.supports_transactions(), "supports_transactions follows the option, per attach");
        assertTrue(tx.supports_transactions());

        byte[] attach = tx.attach_opaque_data();
        byte[] sealedTx = svc.catalog_transaction_begin(attach, null).transaction_opaque_data();
        assertEquals(1, HOOK_IDS.size());
        String id = hex(HOOK_IDS.get(0));
        if (sealed) assertFalse(hex(sealedTx).equals(id), "the wire value is sealed on HTTP");
        else assertEquals(id, hex(sealedTx));

        Schema out = new Schema(List.of(Schemas.nullable("x", Schemas.INT64)));
        svc.init(inlineInit(bind("probe", attach, sealedTx, null), out, null), null);
        Schema in = new Schema(List.of(Schemas.nullable("v", Schemas.INT64)));
        svc.init(inlineInit(bind("probe_io", attach, sealedTx, in), in, null), null);
        svc.catalog_transaction_commit(attach, sealedTx, null);

        String expect = "tx=" + id + " token=" + TOKEN;
        assertEquals("begin " + expect, SEEN.get(0));
        assertEquals("bind " + expect, SEEN.get(1));
        assertEquals("init " + expect, SEEN.get(2));
        assertTrue(SEEN.get(3).startsWith("io-bind " + expect + " attach="), SEEN.get(3));
        String attachId = SEEN.get(3).substring(SEEN.get(3).indexOf("attach=") + 7);
        assertEquals("io-init " + expect + " attach=" + attachId + " bind=bind-state", SEEN.get(4),
                "table in-out init sees the attach id, the bind's opaque data and the transaction");
        assertEquals("commit " + expect, SEEN.get(5));
    }

    @ParameterizedTest(name = "sealed={0}")
    @ValueSource(booleans = {false, true})
    void aFailingCommitHookFailsCommitAndRollbackGetsTheId(boolean sealed) {
        VgiServiceImpl svc = service(worker(), sealed);
        byte[] attach = svc.catalog_attach(CatalogAttachRequest.of("wtx", options(TOKEN, true), "", ""), null)
                .attach_opaque_data();
        byte[] t1 = svc.catalog_transaction_begin(attach, null).transaction_opaque_data();
        refuseCommit = "commit refused: qty -5 for id 2";
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> svc.catalog_transaction_commit(attach, t1, null));
        assertEquals("commit refused: qty -5 for id 2", e.getMessage());
        refuseCommit = null;
        byte[] t2 = svc.catalog_transaction_begin(attach, null).transaction_opaque_data();
        svc.catalog_transaction_rollback(attach, t2, null);
        assertEquals("rollback tx=" + hex(HOOK_IDS.get(1)) + " token=" + TOKEN, SEEN.get(SEEN.size() - 1));
    }

    /** Another service instance (another worker process) sees the options, secret included. */
    @ParameterizedTest(name = "sealed={0}")
    @ValueSource(booleans = {false, true})
    void anotherProcessReadsTheSecretFromTheAttachValue(boolean sealed) {
        Worker w = worker();
        byte[] attach = service(w, sealed).catalog_attach(
                CatalogAttachRequest.of("wtx", options(TOKEN, true), "", ""), null).attach_opaque_data();
        VgiServiceImpl other = service(w, sealed);
        other.bind(bind("probe", attach, null, null), null);
        assertEquals("bind tx=null token=" + TOKEN, SEEN.get(0));
        assertFalse(new String(attach, StandardCharsets.ISO_8859_1).contains(TOKEN),
                "the secret is never in plaintext in the attach value");
    }

    /** A worker whose key does not open the blob refuses the attach uniformly, never "absent". */
    @Test
    void aWorkerWithoutTheKeyRefusesTheAttachUniformly() {
        Worker w = worker();
        VgiServiceImpl a = new VgiServiceImpl(w, w.scalars(), w.tables(), w.tableInOuts(), w.aggregates(),
                false, null, KEY);
        byte[] attachId = a.catalog_attach(CatalogAttachRequest.of("wtx", options(TOKEN, true), "", ""), null)
                .attach_opaque_data();
        VgiServiceImpl b = new VgiServiceImpl(w, w.scalars(), w.tables(), w.tableInOuts(), w.aggregates(),
                false, null, new byte[32]);
        for (org.junit.jupiter.api.function.Executable call : List.<org.junit.jupiter.api.function.Executable>of(
                () -> b.attachOptionsOf(attachId),
                () -> b.bind(bind("probe", attachId, null, null), null),
                () -> b.catalog_transaction_begin(attachId, null))) {
            farm.query.vgirpc.errors.StatusError e = assertThrows(farm.query.vgirpc.errors.StatusError.class, call);
            assertEquals("attach_opaque_data not recognized", e.getMessage());
            assertEquals(OpaqueDataSealer.NOT_RECOGNIZED_KIND, e.errorKind());
            assertEquals(farm.query.vgirpc.errors.Code.INVALID_ARGUMENT, e.errorCode());
        }
        AttachOptions own = a.attachOptionsOf(attachId);
        assertTrue(own.secretsAvailable());
        assertEquals(TOKEN, own.getString("token"));
        assertTrue(own.isSecret("token"));
        assertFalse(own.toString().contains(TOKEN), "toString redacts secrets");
        AttachOptions publicView = AttachOptions.fromAttachId(attachId);
        assertNull(publicView.getString("token"), "the public decode never sees secrets");
        assertFalse(publicView.secretsAvailable());
        // An attach that supplied no secret value carries no blob and needs no key.
        byte[] noSecret = a.catalog_attach(CatalogAttachRequest.of("wtx", options(null, true), "", ""), null)
                .attach_opaque_data();
        assertTrue(b.attachOptionsOf(noSecret).getBoolean("transactional", false));
    }

    /** A producer keeps only the attach id and re-opens the options while producing. */
    @ParameterizedTest(name = "sealed={0}")
    @ValueSource(booleans = {false, true})
    void attachOptionsOpenReturnsTheSecretFromThePlainAttachId(boolean sealed) {
        VgiServiceImpl svc = service(worker(), sealed);
        byte[] wire = svc.catalog_attach(CatalogAttachRequest.of("wtx", options(TOKEN, true), "", ""), null)
                .attach_opaque_data();
        byte[] plain = sealed ? new OpaqueDataSealer(KEY).unsealAttach(wire, farm.query.vgirpc.AuthContext.ANONYMOUS)
                : wire;
        AttachOptions opened = AttachOptions.open(plain);
        assertEquals(TOKEN, opened.getString("token"));
        assertTrue(opened.getBoolean("transactional", false));
    }

    /** On HTTP the secret is absent from the opened (inner) attach bytes too, not only the wire. */
    @Test
    void theSecretIsAbsentFromTheInnerBytesOnHttp() {
        byte[] wire = service(worker(), true).catalog_attach(
                CatalogAttachRequest.of("wtx", options(TOKEN, true), "", ""), null).attach_opaque_data();
        byte[] inner = new OpaqueDataSealer(KEY).unsealAttach(wire, farm.query.vgirpc.AuthContext.ANONYMOUS);
        assertFalse(new String(inner, StandardCharsets.ISO_8859_1).contains(TOKEN));
        assertFalse(new String(wire, StandardCharsets.ISO_8859_1).contains(TOKEN));
    }

    @Test
    void withoutAHookTheMainCatalogKeepsItsDefaults() {
        Worker w = Worker.builder().catalogName("plain").registerTable(new Probe());
        VgiServiceImpl svc = service(w, false);
        CatalogAttachResult r = svc.catalog_attach(CatalogAttachRequest.of("plain", null, "", ""), null);
        assertTrue(r.supports_transactions());
        assertTrue(r.supports_time_travel());
        Worker off = Worker.builder().catalogName("plain").supportsTransactions(false).supportsTimeTravel(false)
                .registerTable(new Probe());
        CatalogAttachResult r2 = service(off, false).catalog_attach(CatalogAttachRequest.of("plain", null, "", ""), null);
        assertFalse(r2.supports_transactions());
        assertFalse(r2.supports_time_travel());
        // Without hooks a transaction still gets a random id, and commit succeeds.
        byte[] attach = service(w, true).catalog_attach(CatalogAttachRequest.of("plain", null, "", ""), null)
                .attach_opaque_data();
        assertNotNull(svc.catalog_transaction_begin(r.attach_opaque_data(), null).transaction_opaque_data());
        assertNotNull(attach);
    }

    @Test
    void aWritableTableAdvertisesItsWriteFunctionAndModes() {
        VgiServiceImpl svc = service(worker(), false);
        byte[] attach = svc.catalog_attach(CatalogAttachRequest.of("wtx", options(null, null), "", ""), null)
                .attach_opaque_data();
        TableInfo items = TableInfoDecoder.decode(
                svc.catalog_table_get(attach, List.of("main"), "items", null, null, null, null).items().get(0));
        assertEquals(Map.of("update", "changes"), items.write_result_modes());
        assertTrue(absent(items.insert_function()), "no insert function");
        assertTrue(absent(items.delete_function()), "no delete function");
        assertNotNull(items.update_function());
        assertEquals("probe_io", BatchUtil.withReadBatch(items.update_function(), Allocators.root(),
                root -> ((VarCharVector) root.getVector("function_name")).getObject(0).toString()));
        assertArrayEquals(items.update_function(),
                svc.catalog_table_update_function_get(attach, List.of("main"), "items", null, null));
        MethodNotImplementedError e = assertThrows(MethodNotImplementedError.class,
                () -> svc.catalog_table_insert_function_get(attach, List.of("main"), "items", null, null, null));
        assertTrue(e.getMessage().contains("does not support INSERT"), e.getMessage());
        TableInfo labels = TableInfoDecoder.decode(
                svc.catalog_table_get(attach, List.of("main"), "labels", null, null, null, null).items().get(0));
        assertTrue(labels.write_result_modes().isEmpty());
        assertTrue(absent(labels.update_function()));
        assertThrows(MethodNotImplementedError.class,
                () -> svc.catalog_table_update_function_get(attach, List.of("main"), "labels", null, null));
    }

    /** The serializer writes an absent function as empty bytes, which the extension reads as none. */
    static boolean absent(byte[] fn) {
        return fn == null || fn.length == 0;
    }

    @Test
    void aWritableConstrainedTableNegotiatesTheEnforceProfile() {
        VgiServiceImpl svc = service(worker(), false);
        byte[] offer = offer(List.of(CatalogProfiles.CONSTRAINT_RECORDS_READ), List.of(CatalogProfiles.CONSTRAINT_ENFORCE));
        CatalogAttachResult r = svc.catalog_attach(
                new CatalogAttachRequest("wtx", options(null, null), "", "", offer), null);
        CatalogCapabilities selected = CatalogProfiles.decode(r.selected_capabilities());
        assertEquals(List.of(CatalogProfiles.CONSTRAINT_ENFORCE), selected.write());
        assertTrue(selected.read().contains(CatalogProfiles.CONSTRAINT_RECORDS_READ));

        // Read-only constrained tables only: nothing to enforce.
        Worker ro = Worker.builder().catalogName("ro").registerTable(new Probe())
                .registerCatalogTable(CatalogTable.builder("main", "labels", Schemas.serialize(ITEMS))
                        .scanFunction("probe").constraints(records("labels"), null).build());
        CatalogAttachResult r2 = service(ro, false).catalog_attach(new CatalogAttachRequest("ro", null, "", "", offer), null);
        assertTrue(CatalogProfiles.decode(r2.catalog_capabilities()).write().isEmpty());
    }

    /** A provider advertising only the write profile no longer loses it to the read prerequisite. */
    @Test
    void anExplicitWriteProfileSurvivesTheReadPrerequisite() {
        byte[] offer = offer(List.of(CatalogProfiles.CONSTRAINT_RECORDS_READ), List.of(CatalogProfiles.CONSTRAINT_ENFORCE));
        Worker w = Worker.builder().catalogName("x").registerTable(new Probe())
                .catalogCapabilities(req -> CatalogProfiles.negotiate(new CatalogCapabilities(List.of(), List.of(),
                        List.of(CatalogProfiles.CONSTRAINT_ENFORCE), List.of()), req.client_capabilities()))
                .registerCatalogTable(CatalogTable.builder("main", "labels", Schemas.serialize(ITEMS))
                        .scanFunction("probe").constraints(records("labels"), null).build());
        CatalogAttachResult r = service(w, false).catalog_attach(new CatalogAttachRequest("x", null, "", "", offer), null);
        assertEquals(List.of(CatalogProfiles.CONSTRAINT_ENFORCE), CatalogProfiles.decode(r.selected_capabilities()).write());
    }

    @Test
    void writeDeclarationsAreValidated() {
        assertThrows(IllegalArgumentException.class, () -> CatalogTable.builder("main", "t", Schemas.serialize(ITEMS))
                .updateFunction("u").build(), "update needs a scan function for row ids");
        assertThrows(IllegalArgumentException.class, () -> CatalogTable.builder("main", "t", Schemas.serialize(ITEMS))
                .scanFunction("s").writeResultMode(WriteOperation.INSERT, WriteResultMode.ROWS).build(),
                "a mode needs its function");
        CatalogTable t = CatalogTable.builder("main", "t", Schemas.serialize(ITEMS)).scanFunction("s")
                .insertFunction("i").deleteFunction("d").writeResultMode(WriteOperation.DELETE, WriteResultMode.ROWS)
                .build();
        assertEquals(Map.of("insert", "count", "delete", "rows"), t.wireWriteResultModes());
        assertEquals("i", t.withStatistics(List.of()).insertFunctionName(), "withers keep the write declaration");
    }

    /** The main catalog's resolved data version travels in the attach value and is recovered elsewhere. */
    @Test
    void theResolvedDataVersionSurvivesAnotherProcess() {
        Worker w = Worker.builder().catalogName("dv").dataVersionSpec(">=1.0.0,<4.0.0").registerTable(new Probe());
        byte[] attach = service(w, false).catalog_attach(CatalogAttachRequest.of("dv", null, "2.0.0", ""), null)
                .attach_opaque_data();
        CatalogRegistry fresh = new CatalogRegistry(w);
        byte[] attachId = fresh.restoreAttach(attach);
        assertEquals("2.0.0", fresh.dataVersion(attachId));
        assertEquals(16, attachId.length, "functions still see the plain 16-byte attach id");
    }
}
