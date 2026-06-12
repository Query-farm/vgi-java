// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.accumulate;

import farm.query.vgi.buffering.BufferingFinalizeProducer;
import farm.query.vgi.buffering.TableBufferingCombineParams;
import farm.query.vgi.buffering.TableBufferingFinalizeParams;
import farm.query.vgi.buffering.TableBufferingFunction;
import farm.query.vgi.buffering.TableBufferingProcessParams;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.internal.BatchUtil;
import farm.query.vgi.internal.SchemaUtil;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.storage.BoundStorage;
import farm.query.vgi.storage.FunctionStorage;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.tableinout.TableInOutBindParams;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.PeriodDuration;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * {@code accumulate(name, <rows>, ttl, max_row_size, result)} — append rows to
 * a named, ATTACH-scoped persistent collection and optionally return its
 * contents. A buffering (Sink → Combine → Source) operator: the input stages
 * across the parallel sink, {@code combine} runs once to stamp the rows with a
 * single {@code _timestamp}, append them to the collection, apply
 * ttl/max_row_size eviction, and stage the requested result, and the source
 * streams it back. Mirrors vgi-python's accumulate fixture.
 */
public final class AccumulateFunction implements TableBufferingFunction {

    private static final byte[] NS_IN = "in".getBytes(StandardCharsets.UTF_8);
    private static final byte[] NS_OUT = "out".getBytes(StandardCharsets.UTF_8);
    private static final byte[] KEY = new byte[0];

    private static final FunctionSpec SPEC = FunctionSpec.builder("accumulate")
            .metadata(FunctionMetadata.describe(
                    "Append rows to a named collection; return all/new/no rows with a _timestamp column")
                    .withCategories("stateful", "utility"))
            .constArg("name", Schemas.UTF8)
            .table("data")
            .named("ttl", new ArrowType.Interval(IntervalUnit.MONTH_DAY_NANO), null)
            .named("max_row_size", Schemas.INT64, "0")
            .named("result", Schemas.UTF8, "all")
            .build();

    @Override public FunctionSpec spec() { return SPEC; }

    @Override
    public BindResponse onBind(TableInOutBindParams params) {
        if (params.attachStorage() == null) {
            // Catalog enumeration: no attach context, no real bind to validate.
            return BindResponse.forSchema(SchemaUtil.serializeSchema(
                    Schemas.of(Field.nullable(AccumulateStore.TIMESTAMP_COLUMN,
                            AccumulateStore.TIMESTAMP_TYPE))));
        }
        String name = params.arguments().positionalAt(0) instanceof String s ? s : "";
        AccumulateStore.validateName(name);
        Schema inputSchema = params.inputSchema();
        if (inputSchema == null || inputSchema.getFields().isEmpty()) {
            throw new IllegalArgumentException("accumulate requires a table input");
        }
        for (Field f : inputSchema.getFields()) {
            if (AccumulateStore.TIMESTAMP_COLUMN.equals(f.getName())) {
                throw new IllegalArgumentException(
                        "input may not contain a reserved '" + AccumulateStore.TIMESTAMP_COLUMN
                                + "' column; accumulate adds this column to its output");
            }
        }
        BoundStorage ps = params.attachStorage();
        byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
        Schema outSchema = AccumulateStore.outputSchema(inputSchema);
        Schema existing = AccumulateStore.getSchema(ps, nameBytes);
        if (existing == null) {
            AccumulateStore.putSchema(ps, nameBytes, outSchema);
        } else if (!AccumulateStore.schemasMatch(AccumulateStore.inputSchemaOf(existing), inputSchema)) {
            throw new IllegalArgumentException(
                    "input schema for accumulate('" + name + "', ...) does not match the "
                            + "schema already accumulated under that name.\n"
                            + "  accumulated: " + AccumulateStore.inputSchemaOf(existing) + "\n"
                            + "  received:    " + inputSchema);
        }
        return BindResponse.forSchema(SchemaUtil.serializeSchema(outSchema));
    }

    @Override
    public byte[] process(VectorSchemaRoot batch, TableBufferingProcessParams params) {
        params.storage().stateAppend(NS_IN, KEY, BatchUtil.writeSingleBatch(batch));
        return params.executionId();
    }

    private static long intervalToMicros(PeriodDuration pd) {
        // Calendar months have no fixed length; approximate each as 30 days
        // (Python parity — INTERVAL '1 month' evicts older than 30 days).
        long days = pd.getPeriod().toTotalMonths() * 30 + pd.getPeriod().getDays();
        return days * 24L * 3600L * 1_000_000L + pd.getDuration().toNanos() / 1_000L;
    }

    @Override
    public List<byte[]> combine(List<byte[]> stateIds, TableBufferingCombineParams params) {
        BoundStorage ps = params.storage().rescope(params.attachOpaqueData());
        String name = params.args().positionalAt(0) instanceof String s ? s : "";
        byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
        Object ttlRaw = params.args().named().get("ttl");
        long maxRowSize = params.args().namedLong("max_row_size", 0L);
        String resultMode = params.args().namedString("result", "all");
        Schema outputSchema = params.outputSchema();

        long callTsUs = AccumulateStore.nowUs();
        List<byte[]> stamped = new ArrayList<>();
        long newRows = 0;
        for (FunctionStorage.LogEntry e : params.storage().stateLogScan(NS_IN, KEY, 0, 0)) {
            try (VectorSchemaRoot in = BatchUtil.readSingleBatch(e.value(), Allocators.root())) {
                if (in.getRowCount() == 0) continue;
                newRows += in.getRowCount();
                stamped.add(AccumulateStore.stampBatch(in, outputSchema, callTsUs));
            }
        }

        for (byte[] segment : stamped) {
            try (VectorSchemaRoot seg = BatchUtil.readSingleBatch(segment, Allocators.root())) {
                AccumulateStore.appendSegment(ps, nameBytes, segment, seg.getRowCount(), callTsUs);
            }
        }

        if (ttlRaw instanceof PeriodDuration pd) {
            AccumulateStore.evictTtl(ps, nameBytes, callTsUs - intervalToMicros(pd));
        }
        if (maxRowSize > 0) {
            long total = AccumulateStore.getCount(ps, nameBytes);
            if (total > maxRowSize) {
                AccumulateStore.evictMaxRows(ps, nameBytes, total, maxRowSize);
            }
        }

        List<byte[]> toEmit = switch (resultMode) {
            case "new" -> stamped;  // the rows this call added (pre-eviction)
            case "none" -> List.of();
            default -> AccumulateStore.readSegments(ps, nameBytes);
        };
        for (byte[] blob : toEmit) {
            params.storage().stateAppend(NS_OUT, KEY, blob);
        }
        return List.of(params.executionId());
    }

    @Override
    public TableProducerState createFinalizeProducer(TableBufferingFinalizeParams params) {
        return new OutDrainProducer(params);
    }

    /** Drains the staged {@code out} log, one batch per tick. */
    private static final class OutDrainProducer extends BufferingFinalizeProducer {
        private long afterId = 0;

        OutDrainProducer(TableBufferingFinalizeParams params) {
            super(params);
        }

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            List<FunctionStorage.LogEntry> rows = storage.stateLogScan(NS_OUT, KEY, afterId, 1);
            if (rows.isEmpty()) {
                out.finish();
                return;
            }
            FunctionStorage.LogEntry e = rows.get(0);
            try (VectorSchemaRoot full = BatchUtil.readSingleBatch(e.value(), Allocators.root())) {
                emitProjected(full, out);
            }
            afterId = e.id();
        }
    }
}
