// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.example.table;

import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.internal.AttachOptionsAttachId;
import farm.query.vgi.internal.SchemaUtil;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.table.TableBindParams;
import farm.query.vgi.table.TableFunction;
import farm.query.vgi.table.TableInitParams;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.List;

/**
 * {@code echo_attach_options()} — emits a one-row batch with the merged
 * {defaults + user-supplied} ATTACH option values that catalog_attach
 * encoded into {@code attach_id}.
 *
 * <p>Stateless: the row data lives entirely in {@code attach_id}, so the
 * function is safe under pool reuse and stateless HTTP transport.
 */
public final class EchoAttachOptionsFunction implements TableFunction {

    private final Schema outputSchema;
    private final byte[] outputSchemaIpc;

    public EchoAttachOptionsFunction() {
        List<Field> fields = new ArrayList<>();
        for (farm.query.vgi.AttachOptionSpec spec : AttachOptionsFixture.declaredSpecs()) {
            fields.add(new Field(spec.name(),
                    new FieldType(true, spec.type(), null),
                    spec.children() == null ? List.of() : spec.children()));
        }
        this.outputSchema = new Schema(fields);
        this.outputSchemaIpc = SchemaUtil.serializeSchema(outputSchema);
    }

    private static final FunctionSpec SPEC = FunctionSpec.builder("echo_attach_options")
            .metadata(FunctionMetadata.describe("Echo the attach-time option values carried in attach_id")
                    .withCategories("generator", "testing"))
            .build();

    @Override public FunctionSpec spec() { return SPEC; }

    @Override public BindResponse onBind(TableBindParams params) {
        return BindResponse.forSchema(outputSchemaIpc);
    }

    @Override public long cardinality(TableBindParams p) { return 1L; }

    @Override public TableProducerState createProducer(TableInitParams params) {
        if (params.attachId() == null) {
            throw new IllegalStateException("echo_attach_options requires an attach_id");
        }
        return new State(params.attachId(), outputSchema);
    }

    public static final class State extends TableProducerState {
        public byte[] attachId;
        public Schema outputSchema;
        public boolean emitted;

        public State() {}

        State(byte[] attachId, Schema outputSchema) {
            this.attachId = attachId;
            this.outputSchema = outputSchema;
        }

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            if (emitted) { out.finish(); return; }
            emitted = true;
            VectorSchemaRoot src = AttachOptionsAttachId.decode(attachId, Allocators.root());
            out.emit(src);
        }
    }
}
