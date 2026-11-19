// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import farm.query.vgi.Worker;
import farm.query.vgi.aggregate.AggregateFunction;
import farm.query.vgi.aggregate.StateCodec;
import farm.query.vgi.client.ArgumentsEncoder;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.internal.BatchUtil;
import farm.query.vgi.internal.VgiServiceImpl;
import farm.query.vgi.protocol.AggregateBindRequest;
import farm.query.vgi.protocol.AggregateBindResponse;
import farm.query.vgi.protocol.AggregateCombineRequest;
import farm.query.vgi.protocol.AggregateDestructorRequest;
import farm.query.vgi.protocol.AggregateFinalizeRequest;
import farm.query.vgi.protocol.AggregateFinalizeResponse;
import farm.query.vgi.protocol.AggregateUpdateRequest;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.wire.Allocators;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

/**
 * Aggregates on the Cloudflare Durable Object tier: every lifecycle RPC is
 * routed to the attach's shard (the mock, like the real Worker, rejects a
 * missing or malformed {@code shard_key}). Through 0.48 the aggregate store used
 * the raw backend and sent {@code shard_key: ""}, which the server rejects.
 */
class CfdoAggregateTest {

    private static final byte[] ATTACH = "attach-cfdo-0123456789abcdef".getBytes(StandardCharsets.UTF_8);
    private static final List<String> MAIN = List.of("main");

    static final class SumAgg implements AggregateFunction<long[]> {
        private static final FunctionSpec SPEC = FunctionSpec.builder("t_sum").description("doc").build();
        @Override public FunctionSpec spec() { return SPEC; }
        @Override public Schema outputSchema() { return Schemas.of(Schemas.nullable("result", Schemas.INT64)); }
        @Override public long[] newState() { return new long[1]; }
        @Override public void update(Map<Long, long[]> states, long[] gids, VectorSchemaRoot input) {
            BigIntVector v = (BigIntVector) input.getVector(0);
            for (int i = 0; i < input.getRowCount(); i++) {
                states.computeIfAbsent(gids[i], k -> new long[1])[0] += v.get(i);
            }
        }
        @Override public void combine(long[] t, long[] s) { t[0] += s[0]; }
        @Override public void finalize(FieldVector result, int row, long[] state) {
            ((BigIntVector) result).setSafe(row, state[0]);
        }
        @Override public byte[] serializeState(long[] s) {
            return StateCodec.encode(out -> out.writeLong(s[0]));
        }
        @Override public long[] deserializeState(byte[] b) {
            return StateCodec.decode(b, in -> new long[] {in.readLong()});
        }
    }

    private static byte[] longBatch(String[] names, long[][] cols) {
        List<Field> fields = new ArrayList<>();
        for (String n : names) fields.add(new Field(n, FieldType.nullable(new ArrowType.Int(64, true)), null));
        try (VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(fields), Allocators.root())) {
            root.allocateNew();
            for (int c = 0; c < names.length; c++) {
                BigIntVector v = (BigIntVector) root.getVector(c);
                for (int r = 0; r < cols[0].length; r++) v.setSafe(r, cols[c][r]);
            }
            root.setRowCount(cols[0].length);
            return BatchUtil.writeSingleBatch(root);
        }
    }

    private static VgiServiceImpl service(FunctionStorage storage) {
        Worker w = Worker.builder().registerAggregate(new SumAgg());
        return new VgiServiceImpl(w, List.of(), List.of(), List.of(), w.aggregates(), false, null, null,
                "k".getBytes(StandardCharsets.UTF_8), storage);
    }

    @Test
    void aggregateLifecycleIsShardedPerAttach() throws IOException {
        try (MockDoServer mock = new MockDoServer()) {
            runLifecycle(new CfdoStorage(mock.baseUrl(), null), ATTACH);
            assertEquals(0, mock.missingShard, "a request reached the DO without a valid shard_key");
            assertEquals(0, mock.missingAttempt);
            assertEquals(Set.of(ShardKey.derive(ATTACH)), mock.shardKeys);
        }
    }

    /** The same lifecycle against a real Worker + Durable Object ({@code wrangler dev} or deployed). */
    @Test
    @org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable(named = "VGI_CF_DO_INTEGRATION_URL", matches = ".+")
    void aggregateLifecycleOnRealDurableObject() {
        byte[] attach = new byte[24];
        new java.security.SecureRandom().nextBytes(attach); // fresh shard per run
        runLifecycle(new CfdoStorage(System.getenv("VGI_CF_DO_INTEGRATION_URL"), System.getenv("VGI_CF_DO_TOKEN")),
                attach);
    }

    private static void runLifecycle(CfdoStorage backend, byte[] attachId) {
        {
            VgiServiceImpl svc = service(backend);
            AggregateBindResponse b = svc.aggregate_bind(new AggregateBindRequest("t_sum",
                    ArgumentsEncoder.positionalArgs(5L), null, null, null, attachId, MAIN, null), null);
            byte[] exec = b.execution_id();
            byte[] in = longBatch(new String[] {"value", "__vgi_group_id"}, new long[][] {{1, 2, 3, 4}, {0, 1, 0, 1}});
            svc.aggregate_update(new AggregateUpdateRequest("t_sum", exec, in, attachId, MAIN), null);
            // A second worker process serves the next batch and the combine.
            VgiServiceImpl other = service(backend);
            other.aggregate_update(new AggregateUpdateRequest("t_sum", exec, in, attachId, MAIN), null);
            other.aggregate_combine(new AggregateCombineRequest("t_sum", exec,
                    longBatch(new String[] {"source_group_id", "target_group_id"}, new long[][] {{1}, {2}}),
                    attachId, MAIN), null);
            AggregateFinalizeResponse r = svc.aggregate_finalize(new AggregateFinalizeRequest("t_sum", exec,
                    longBatch(new String[] {"group_id"}, new long[][] {{0, 1, 2}}), b.output_schema(), attachId, MAIN),
                    null);
            try (VectorSchemaRoot root = BatchUtil.readSingleBatch(r.result_batch(), Allocators.root())) {
                BigIntVector v = (BigIntVector) root.getVector(0);
                assertEquals(8, v.get(0));
                assertEquals(12, v.get(1));
                assertEquals(12, v.get(2));
            }
            svc.aggregate_destructor(new AggregateDestructorRequest("t_sum", exec,
                    longBatch(new String[] {"group_id"}, new long[][] {{0, 1, 2}}), attachId, MAIN), null);
        }
    }

    @Test
    void aggregateWithoutAttachIsRefusedOnTheShardedTier() throws IOException {
        try (MockDoServer mock = new MockDoServer()) {
            VgiServiceImpl svc = service(new CfdoStorage(mock.baseUrl(), null));
            assertThrows(IllegalStateException.class, () -> svc.aggregate_bind(new AggregateBindRequest("t_sum",
                    ArgumentsEncoder.positionalArgs(5L), null, null, null, null, MAIN, null), null));
            assertEquals(0, mock.requestLog.size(), "nothing may be sent with an empty shard_key");
        }
    }
}
