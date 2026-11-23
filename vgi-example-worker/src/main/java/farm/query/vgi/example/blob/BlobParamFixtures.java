// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.blob;

import static farm.query.vgi.example.blob.BlobTestFixtures.PAYLOAD_LEN;
import static farm.query.vgi.example.blob.BlobTestFixtures.PAYLOAD_SHA256;
import static farm.query.vgi.example.blob.BlobTestFixtures.sha256Hex;

import farm.query.vgi.Worker;
import farm.query.vgi.catalog.CatalogTable;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.function.ParameterExtractor;
import farm.query.vgi.internal.BatchUtil;
import farm.query.vgi.internal.SchemaUtil;
import farm.query.vgi.protocol.BindRepeatability;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.protocol.ScanSplit;
import farm.query.vgi.scalar.Const;
import farm.query.vgi.scalar.ScalarFn;
import farm.query.vgi.scalar.Vector;
import farm.query.vgi.table.PlanRequest;
import farm.query.vgi.table.PlanResult;
import farm.query.vgi.table.SimpleTableFunction;
import farm.query.vgi.table.TableBindParams;
import farm.query.vgi.table.TableInitParams;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.Schema;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The protocol 3.4 large-value fixtures of the {@code blob_test} catalog (vgi
 * {@code docs/testing/blob-test-catalog.md}, "Protocol 3.4 large values"), backing the
 * extension's {@code test/sql/external_storage/arguments_ref*.test}. vgi-python's
 * {@code vgi/_test_fixtures/blob_catalog.py} (779dc67) is the reference.
 *
 * <ul>
 *   <li>{@code blob_param_batches(payload, batches)}: one one-row batch per stream turn;</li>
 *   <li>{@code blob_param_streams(payload, streams)}: one split per stream, each emitting
 *       {@code (stream, strlen, sha256)};</li>
 *   <li>{@code blob_param_scalar(i, payload)}: {@code sha256(payload)} per non-NULL {@code i},
 *       {@code payload} a constant parameter so it rides the bind;</li>
 *   <li>{@code blob_param_table}: a catalog table whose scan ({@code blob_param_table_scan}, the
 *       {@code blob_param_echo} twin) binds {@code repeat('t', 65536)}, reports
 *       {@code STABLE_WITHIN_QUERY} and is not inline-bound.</li>
 * </ul>
 */
public final class BlobParamFixtures {

    private BlobParamFixtures() {}

    /** The fixed argument {@code blob_param_table} binds: {@code repeat('t', 65536)}. */
    public static final String BLOB_PARAM_TABLE_ARGUMENT = "t".repeat(65536);

    /**
     * Register the four fixtures into the {@code blob_test} catalog.
     *
     * @param w the worker (the catalog already registered)
     * @return {@code w}
     */
    public static Worker register(Worker w) {
        String c = BlobTestFixtures.CATALOG_NAME;
        String s = BlobTestFixtures.SCHEMA;
        return w.registerExtraCatalogTableFunction(c, s, new BlobParamBatches())
                .registerExtraCatalogTableFunction(c, s, new BlobParamStreams())
                .registerExtraCatalogScalar(c, s, new BlobParamScalar())
                .registerExtraCatalogTableFunction(c, s, new BlobParamTableScan())
                .registerExtraCatalogTable(c, CatalogTable.builder(s, "blob_param_table",
                                SchemaUtil.serializeSchema(BlobParamTableScan.OUTPUT))
                        .comment("blob_param_echo(repeat('t', 65536)), STABLE_WITHIN_QUERY, bound over RPC on every scan")
                        .scanFunction("blob_param_table_scan", List.of(BLOB_PARAM_TABLE_ARGUMENT), Map.of())
                        .build());
    }

    private static String payload(ParameterExtractor p) {
        return p.positional(0, "payload").asString().required();
    }

    /** {@code blob_param_batches(payload, batches)}: row {@code i} per turn, {@code (i, strlen, sha256)}. */
    public static final class BlobParamBatches extends SimpleTableFunction {
        static final Schema OUTPUT = Schemas.of(Schemas.nullable("i", Schemas.INT64), PAYLOAD_LEN, PAYLOAD_SHA256);
        private static final FunctionSpec SPEC = FunctionSpec.builder("blob_param_batches")
                .metadata(FunctionMetadata.describe(
                        "Length and SHA-256 of a parameter, once per batch (argument references over many turns)"))
                .arg("payload", Schemas.UTF8)
                .arg("batches", Schemas.INT64)
                .build();

        @Override public FunctionSpec spec() { return SPEC; }
        @Override protected Schema outputSchema() { return OUTPUT; }

        @Override public long cardinality(TableBindParams params) {
            Object n = params.arguments().positional().size() < 2 ? null : params.arguments().positional().get(1);
            return n instanceof Number num ? num.longValue() : -1L;
        }

        @Override public TableProducerState createProducer(TableInitParams params) {
            ParameterExtractor p = ParameterExtractor.of(params.arguments());
            byte[] raw = payload(p).getBytes(StandardCharsets.UTF_8);
            long batches = p.positional(1, "batches").asLong().ge(0).required();
            return new State(raw.length, sha256Hex(raw), batches);
        }

        /** One one-row batch per tick. Public fields + no-arg ctor for the HTTP state token. */
        public static final class State extends TableProducerState {
            /** UTF-8 byte length of the parameter. */
            public long length;
            /** Lowercase hex SHA-256 of the parameter. */
            public String sha256;
            /** Batches to emit. */
            public long batches;
            /** Next batch. */
            public long next;

            /** No-arg constructor for state deserialization. */
            public State() {}

            State(long length, String sha256, long batches) {
                this.length = length;
                this.sha256 = sha256;
                this.batches = batches;
            }

            @Override public void produceTick(OutputCollector out, CallContext ctx) {
                if (next >= batches) {
                    out.finish();
                    return;
                }
                long i = next++;
                BatchUtil.emit(OUTPUT, 1, out, (root, rows, ignored) -> {
                    ((BigIntVector) root.getVector(0)).setSafe(0, i);
                    ((BigIntVector) root.getVector(1)).setSafe(0, length);
                    ((VarCharVector) root.getVector(2)).setSafe(0, sha256.getBytes(StandardCharsets.US_ASCII));
                });
            }
        }
    }

    /** {@code blob_param_streams(payload, streams)}: one bind planned as {@code streams} splits. */
    public static final class BlobParamStreams extends SimpleTableFunction {
        static final Schema OUTPUT = Schemas.of(Schemas.nullable("stream", Schemas.INT64), PAYLOAD_LEN, PAYLOAD_SHA256);
        private static final FunctionSpec SPEC = FunctionSpec.builder("blob_param_streams")
                .metadata(FunctionMetadata.describe(
                        "Length and SHA-256 of a parameter from parallel streams (argument references)").withSplits())
                .arg("payload", Schemas.UTF8)
                .arg("streams", Schemas.INT64)
                .build();

        @Override public FunctionSpec spec() { return SPEC; }
        @Override protected Schema outputSchema() { return OUTPUT; }

        private static long streams(TableBindParams params) {
            return ParameterExtractor.of(params.arguments()).positional(1, "streams").asLong().ge(1).required();
        }

        @Override public long cardinality(TableBindParams params) { return streams(params); }

        @Override public PlanResult plan(TableBindParams params, PlanRequest request) {
            long count = streams(params);
            List<ScanSplit> splits = new ArrayList<>();
            for (long i = 0; i < count; i++) {
                splits.add(ScanSplit.of(ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(i).array()));
            }
            return PlanResult.of(splits);
        }

        @Override public TableProducerState createProducer(TableInitParams params) {
            List<byte[]> payloads = params.splitPayloads();
            if (payloads == null) {
                throw new IllegalStateException("blob_param_streams is split-only; vgi_split_scans is probably off");
            }
            byte[] raw = payload(ParameterExtractor.of(params.arguments())).getBytes(StandardCharsets.UTF_8);
            List<Long> claimed = new ArrayList<>();
            for (byte[] p : payloads) claimed.add(ByteBuffer.wrap(p).order(ByteOrder.LITTLE_ENDIAN).getLong());
            return new State(raw.length, sha256Hex(raw), claimed);
        }

        /** One row per claimed stream. Public fields + no-arg ctor for the HTTP state token. */
        public static final class State extends TableProducerState {
            /** UTF-8 byte length of the parameter. */
            public long length;
            /** Lowercase hex SHA-256 of the parameter. */
            public String sha256;
            /** Stream indices this reader claimed. */
            public List<Long> streams = new ArrayList<>();
            /** Whether the rows were emitted. */
            public boolean done;

            /** No-arg constructor for state deserialization. */
            public State() {}

            State(long length, String sha256, List<Long> streams) {
                this.length = length;
                this.sha256 = sha256;
                this.streams = streams;
            }

            @Override public void produceTick(OutputCollector out, CallContext ctx) {
                if (done || streams.isEmpty()) {
                    out.finish();
                    return;
                }
                done = true;
                int n = streams.size();
                BatchUtil.emit(OUTPUT, n, out, (root, rows, ignored) -> {
                    for (int r = 0; r < n; r++) {
                        ((BigIntVector) root.getVector(0)).setSafe(r, streams.get(r));
                        ((BigIntVector) root.getVector(1)).setSafe(r, length);
                        ((VarCharVector) root.getVector(2)).setSafe(r, sha256.getBytes(StandardCharsets.US_ASCII));
                    }
                });
            }
        }
    }

    /** {@code blob_param_scalar(i, payload) -> VARCHAR}: {@code sha256(payload)} per non-NULL {@code i}. */
    public static final class BlobParamScalar extends ScalarFn {
        @Override public String name() { return "blob_param_scalar"; }
        @Override public String description() {
            return "SHA-256 of a constant parameter for every row (argument references for scalars)";
        }

        /**
         * {@code sha256(payload)} for each row whose {@code i} is not NULL.
         *
         * @param i       row values; NULL gives NULL
         * @param payload the constant parameter (rides the bind)
         * @param result  the output
         */
        public void compute(@Vector(doc = "Row value; NULL gives NULL") BigIntVector i,
                            @Const(doc = "Constant parameter to hash") String payload,
                            VarCharVector result) {
            int rows = i.getValueCount();
            byte[] digest = payload == null ? null
                    : sha256Hex(payload.getBytes(StandardCharsets.UTF_8)).getBytes(StandardCharsets.US_ASCII);
            result.allocateNew(rows);
            for (int r = 0; r < rows; r++) {
                if (i.isNull(r) || digest == null) result.setNull(r);
                else result.setSafe(r, digest);
            }
            result.setValueCount(rows);
        }
    }

    /**
     * The scan behind {@code blob_param_table}: {@code blob_param_echo} with a fixed argument. It
     * reports {@code STABLE_WITHIN_QUERY} and is never inline-bound, so every scan binds over RPC
     * and a serialized-plan rebind must reproduce the same bind bytes.
     */
    public static final class BlobParamTableScan extends SimpleTableFunction {
        static final Schema OUTPUT = Schemas.of(PAYLOAD_LEN, PAYLOAD_SHA256);
        private static final FunctionSpec SPEC = FunctionSpec.builder("blob_param_table_scan")
                .metadata(FunctionMetadata.describe(
                        "Length and SHA-256 of blob_param_table's fixed parameter (STABLE_WITHIN_QUERY)"))
                .arg("payload", Schemas.UTF8)
                .build();

        @Override public FunctionSpec spec() { return SPEC; }
        @Override protected Schema outputSchema() { return OUTPUT; }
        @Override public long cardinality(TableBindParams params) { return 1L; }

        @Override public BindResponse onBind(TableBindParams params) {
            return new BindResponse(SchemaUtil.serializeSchema(OUTPUT), new byte[0], List.of(), List.of(),
                    List.of(), List.of(), BindRepeatability.STABLE_WITHIN_QUERY);
        }

        @Override public TableProducerState createProducer(TableInitParams params) {
            byte[] raw = payload(ParameterExtractor.of(params.arguments())).getBytes(StandardCharsets.UTF_8);
            return new BlobTestFixtures.BlobParamEcho.State(raw.length, sha256Hex(raw));
        }
    }
}
