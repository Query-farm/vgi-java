// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.example.table;

import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.internal.SchemaUtil;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.table.TableBindParams;
import farm.query.vgi.table.TableFunction;
import farm.query.vgi.table.TableInitParams;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.io.Serializable;
import java.util.List;

/**
 * {@code scoped_secret_demo(path STRING [const])} — two-phase bind with a
 * scoped secret lookup. Emits one row: {@code (scope, found)} where
 * {@code found} is true iff a vgi_example secret matched the path.
 */
public final class ScopedSecretDemoFunction implements TableFunction {

    private static final Schema OUTPUT_SCHEMA = new Schema(List.of(
            Schemas.nullable("scope", Schemas.UTF8),
            Schemas.nullable("found", Schemas.BOOL)));
    private static final byte[] OUTPUT_SCHEMA_IPC =
            SchemaUtil.serializeSchema(OUTPUT_SCHEMA);

    @Override public String name() { return "scoped_secret_demo"; }

    @Override public FunctionMetadata metadata() {
        return FunctionMetadata.describe("Two-phase bind with scoped secret lookup");
    }

    @Override public List<ArgSpec> argumentSpecs() {
        return List.of(ArgSpec.positional("path", 0, Schemas.UTF8));
    }

    @Override public BindResponse onBind(TableBindParams p) {
        Object pathObj = p.arguments().positional().isEmpty()
                ? null : p.arguments().positionalAt(0);
        String path = pathObj == null ? "" : pathObj.toString();
        if (!p.resolvedSecretsProvided()) {
            // Phase-1: request a vgi_example secret scoped to the path.
            return new BindResponse(OUTPUT_SCHEMA_IPC, new byte[0],
                    List.of("vgi_example"), List.of(path), List.of(""));
        }
        return BindResponse.forSchema(OUTPUT_SCHEMA_IPC);
    }

    @Override public TableProducerState createProducer(TableInitParams params) {
        Object pathObj = params.arguments().positional().isEmpty()
                ? null : params.arguments().positionalAt(0);
        String path = pathObj == null ? "" : pathObj.toString();
        boolean found = params.secrets() != null && params.secrets().length > 0;
        return new State(path, found);
    }

    public static final class State extends TableProducerState implements Serializable {
        private static final long serialVersionUID = 1L;
        public String path;
        public boolean found;
        public boolean done;

        public State() {}
        State(String path, boolean found) { this.path = path; this.found = found; }

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            if (done) { out.finish(); return; }
            done = true;
            VectorSchemaRoot root = VectorSchemaRoot.create(OUTPUT_SCHEMA, Allocators.root());
            root.allocateNew();
            ((VarCharVector) root.getVector("scope")).setSafe(0, new Text(path));
            ((BitVector) root.getVector("found")).setSafe(0, found ? 1 : 0);
            root.setRowCount(1);
            out.emit(root);
            out.finish();
        }
    }
}
