// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.accumulate;

import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.internal.BatchUtil;
import farm.query.vgi.internal.SchemaUtil;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.storage.BoundStorage;
import farm.query.vgi.table.TableBindParams;
import farm.query.vgi.table.TableFunction;
import farm.query.vgi.table.TableInitParams;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * {@code accumulate_read(name)} — return a collection's accumulated rows
 * (input columns + {@code _timestamp}) without modifying it. Reading a name
 * never accumulated in this ATTACH session raises at bind time. Mirrors
 * vgi-python's {@code AccumulateReadFunction}.
 */
public final class AccumulateReadFunction implements TableFunction {

    private static final FunctionSpec SPEC = FunctionSpec.builder("accumulate_read")
            .metadata(FunctionMetadata.describe(
                    "Read an accumulated collection's rows without modifying it")
                    .withCategories("stateful", "utility"))
            .constArg("name", Schemas.UTF8)
            .build();

    @Override public FunctionSpec spec() { return SPEC; }

    @Override
    public BindResponse onBind(TableBindParams params) {
        if (params.attachStorage() == null) {
            // Catalog enumeration: no attach context, no collection to resolve.
            return BindResponse.forSchema(SchemaUtil.serializeSchema(
                    Schemas.of(Field.nullable(AccumulateStore.TIMESTAMP_COLUMN,
                            AccumulateStore.TIMESTAMP_TYPE))));
        }
        String name = params.arguments().positionalAt(0) instanceof String s ? s : "";
        AccumulateStore.validateName(name);
        Schema schema = AccumulateStore.getSchema(
                params.attachStorage(), name.getBytes(StandardCharsets.UTF_8));
        if (schema == null) {
            throw new IllegalArgumentException(
                    "no accumulation named '" + name + "' in this session");
        }
        return BindResponse.forSchema(SchemaUtil.serializeSchema(schema));
    }

    @Override public long maxWorkers() { return 1L; }

    @Override
    public TableProducerState createProducer(TableInitParams params) {
        BoundStorage ps = params.storage().rescope(params.attachId());
        String name = params.arguments().positionalAt(0) instanceof String s ? s : "";
        List<byte[]> segments = AccumulateStore.readSegments(
                ps, name.getBytes(StandardCharsets.UTF_8));
        return new TableProducerState() {
            private int next = 0;

            @Override public void produceTick(OutputCollector out, CallContext ctx) {
                if (next >= segments.size()) {
                    out.finish();
                    return;
                }
                out.emit(BatchUtil.readSingleBatch(segments.get(next++), params.allocator()));
            }
        };
    }
}
