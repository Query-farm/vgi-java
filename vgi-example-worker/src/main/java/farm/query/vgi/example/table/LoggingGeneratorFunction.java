// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.example.table;

import farm.query.vgi.internal.SchemaUtil;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.table.TableBindParams;
import farm.query.vgi.table.TableFunction;
import farm.query.vgi.table.TableInitParams;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.log.Level;
import farm.query.vgirpc.log.Message;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

/**
 * {@code logging_generator(count)} — generates {@code count} integer rows
 * (one per tick) and emits diagnostic log messages on start, every 10 rows,
 * and on completion. Used by logging_generator.test to verify that worker
 * logs appear in DuckDB's {@code duckdb_logs()} table.
 */
public final class LoggingGeneratorFunction implements TableFunction {

    private static final Schema OUTPUT_SCHEMA = new Schema(List.of(
            Schemas.nullable("n", Schemas.INT64)));
    private static final byte[] OUTPUT_SCHEMA_IPC =
            SchemaUtil.serializeSchema(OUTPUT_SCHEMA);

    private static final FunctionSpec SPEC = FunctionSpec.builder("logging_generator")
            .description("Emits log messages during generation")
            .constArg("count", Schemas.INT64)
            .build();

    @Override public FunctionSpec spec() { return SPEC; }
    @Override public BindResponse onBind(TableBindParams params) {
        return BindResponse.forSchema(OUTPUT_SCHEMA_IPC);
    }
    @Override public TableProducerState createProducer(TableInitParams params) {
        long count = ((Number) params.arguments().positionalAt(0)).longValue();
        return new State(count);
    }

    public static final class State extends TableProducerState {
        public long count;
        public long index;

        public State() {}
        State(long count) { this.count = count; }

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            if (index == 0) {
                out.clientLog(new Message(Level.INFO, "Starting generation of " + count + " values", null));
            }
            if (index >= count) {
                out.clientLog(new Message(Level.INFO, "Generation complete", null));
                out.finish();
                return;
            }
            VectorSchemaRoot root = VectorSchemaRoot.create(OUTPUT_SCHEMA, Allocators.root());
            root.allocateNew();
            BigIntVector v = (BigIntVector) root.getVector("n");
            v.setSafe(0, index);
            root.setRowCount(1);
            out.emit(root);
            index++;
        }
    }
}
