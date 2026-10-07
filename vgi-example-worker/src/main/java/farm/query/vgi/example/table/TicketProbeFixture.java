// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.table;

import farm.query.vgi.AttachOptionSpec;
import farm.query.vgi.Worker;
import farm.query.vgi.catalog.CatalogTable;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.internal.SchemaUtil;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.table.TableBindParams;
import farm.query.vgi.table.TableFunction;
import farm.query.vgi.table.TableInitParams;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * {@code ticket_probe}: the cross-SDK fixture catalog for attach tickets
 * ({@code docs/protocol/vgi-attach-tickets.md} §7). Every SDK's fixture worker serves it
 * identically, and the extension's {@code attach_ticket/*.test} sqllogictests run against each.
 *
 * <ul>
 *   <li>Attach options, in this order: {@code region} (VARCHAR, default {@code 'us-east-1'}) and
 *       {@code api_key} (VARCHAR, required, secret).</li>
 *   <li>Table {@code main.probe}, backed by the table function {@code main.ticket_probe} (no
 *       arguments): one row, {@code region} and {@code api_key_sha256}, the first 12 lowercase hex
 *       characters of {@code SHA-256(UTF-8(api_key))}. The key itself is never returned.</li>
 * </ul>
 *
 * <p>So a reattach with nothing but {@code vgi_attach_ticket} reading the same row proves the
 * secret took effect without travelling again.
 */
public final class TicketProbeFixture {

    /** The catalog name. */
    public static final String CATALOG_NAME = "ticket_probe";
    /** The {@code region} default. */
    public static final String DEFAULT_REGION = "us-east-1";

    static final Schema OUTPUT = new Schema(List.of(
            new Field("region", FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("api_key_sha256", FieldType.nullable(new ArrowType.Utf8()), null)));

    private TicketProbeFixture() {}

    /**
     * The declared attach options, in contract order.
     *
     * @return {@code region} then {@code api_key}
     */
    public static List<AttachOptionSpec> specs() {
        return List.of(
                AttachOptionSpec.of("region", "Region the probe reports back", new ArrowType.Utf8(),
                        DEFAULT_REGION),
                AttachOptionSpec.requiredSecret("api_key", "API key; only its digest is ever returned",
                        new ArrowType.Utf8()));
    }

    /**
     * The first 12 lowercase hex characters of {@code SHA-256(UTF-8(apiKey))}.
     *
     * @param apiKey the key
     * @return its digest prefix
     */
    public static String apiKeyDigest(String apiKey) {
        try {
            byte[] d = MessageDigest.getInstance("SHA-256").digest(apiKey.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(d).substring(0, 12);
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Serve the catalog from {@code w}.
     *
     * @param w the fixture worker
     * @return {@code w}
     */
    public static Worker register(Worker w) {
        return w.registerExtraCatalog(new Worker.ExtraCatalog(
                        CATALOG_NAME, null, null,
                        "Attach-ticket probe",
                        specs(),
                        "Attach-ticket probe: one plain and one secret attach option",
                        Map.of(), false, null, null, null))
                // The attach carries region and the key's digest, never the key: the value
                // travels unsealed on stdio (vgi-opaque-data-sealing.md rule 5).
                .extraCatalogAttachData(CATALOG_NAME, TicketProbeFixture::attachData)
                .registerExtraCatalogTableFunction(CATALOG_NAME, "main", new ProbeFunction())
                .registerExtraCatalogTable(CATALOG_NAME, CatalogTable.builder("main", "probe",
                                SchemaUtil.serializeSchema(OUTPUT))
                        .comment("The options this attach was made with")
                        .scanFunction("ticket_probe")
                        .build());
    }

    /**
     * The catalog's own attach bytes: {@code region || 0x00 || sha256(api_key)[:12]}, read from the
     * attach options (names compared case-insensitively). The key itself is not kept.
     *
     * @param request the attach request
     * @return the bytes the probe reads back
     */
    static byte[] attachData(farm.query.vgi.protocol.CatalogAttachRequest request) {
        String region = DEFAULT_REGION;
        String apiKey = null;
        byte[] ipc = request.options();
        if (ipc != null && ipc.length > 0) {
            String[] found = farm.query.vgi.internal.BatchUtil.withReadBatch(ipc, Allocators.root(), root -> {
                String[] out = new String[2];
                if (root == null || root.getRowCount() == 0) return out;
                for (FieldVector v : root.getFieldVectors()) {
                    String name = v.getField().getName().toLowerCase(Locale.ROOT);
                    if (!(v instanceof VarCharVector s) || s.isNull(0)) continue;
                    String value = new String(s.get(0), StandardCharsets.UTF_8);
                    if (name.equals("region")) out[0] = value;
                    else if (name.equals("api_key")) out[1] = value;
                }
                return out;
            });
            if (found[0] != null) region = found[0];
            apiKey = found[1];
        }
        if (apiKey == null) {
            throw new IllegalArgumentException("Catalog '" + CATALOG_NAME
                    + "' cannot be attached without the required option 'api_key'.");
        }
        return (region + "\0" + apiKeyDigest(apiKey)).getBytes(StandardCharsets.UTF_8);
    }

    /** {@code ticket_probe()}: the attached {@code region} and a digest of the attached {@code api_key}. */
    public static final class ProbeFunction implements TableFunction {

        private static final byte[] OUTPUT_IPC = SchemaUtil.serializeSchema(OUTPUT);
        private static final FunctionSpec SPEC = FunctionSpec.builder("ticket_probe")
                .metadata(FunctionMetadata.describe(
                                "Report the attach options of this ticket_probe attach (the api_key only as a digest)")
                        .withCategories("generator", "testing"))
                .build();

        @Override public FunctionSpec spec() { return SPEC; }

        @Override public BindResponse onBind(TableBindParams params) {
            return BindResponse.forSchema(OUTPUT_IPC);
        }

        @Override public long cardinality(TableBindParams p) { return 1L; }

        @Override public TableProducerState createProducer(TableInitParams params) {
            if (params.attachId() == null) {
                throw new IllegalStateException(
                        "ticket_probe must be read through an attach of the ticket_probe catalog");
            }
            byte[] id = params.attachId();
            String own = id.length > 16 ? new String(id, 16, id.length - 16, StandardCharsets.UTF_8) : "";
            int sep = own.indexOf('\0');
            if (sep < 0) {
                throw new IllegalStateException(
                        "ticket_probe must be read through an attach of the ticket_probe catalog");
            }
            return new State(own.substring(0, sep), own.substring(sep + 1));
        }
    }

    /** Producer state: the one row, emitted once. Public fields so it survives an HTTP state token. */
    public static final class State extends TableProducerState {
        public String region;
        public String digest;
        public boolean emitted;

        public State() {}

        State(String region, String digest) {
            this.region = region;
            this.digest = digest;
        }

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            if (emitted) { out.finish(); return; }
            emitted = true;
            VectorSchemaRoot root = VectorSchemaRoot.create(OUTPUT, Allocators.root());
            root.allocateNew();
            ((VarCharVector) root.getVector(0)).setSafe(0, region.getBytes(StandardCharsets.UTF_8));
            ((VarCharVector) root.getVector(1)).setSafe(0, digest.getBytes(StandardCharsets.UTF_8));
            root.setRowCount(1);
            out.emit(root);
        }
    }
}
