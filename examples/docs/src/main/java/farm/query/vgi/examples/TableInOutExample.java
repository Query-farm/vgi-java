// VGI-Java example: a table-in-out (TIO) function.
//
// A TIO function consumes a relation and streams a relation back — a row-by-row
// (really batch-by-batch) transform. DuckDB feeds you input batches; you emit
// output batches. Use it for streaming reshapes, enrichment, or filtering that
// you'd rather express in Java than SQL.
//
// This example is the canonical `echo`: output schema == input schema, every
// input batch passes through unchanged. `PassthroughTIOFunction` supplies the
// "output schema = input schema" bind, so you only write the exchange.
//
//   ATTACH 'demo' AS demo (TYPE vgi, LOCATION 'launch:/abs/path/bin/runTableInOut');
//   SELECT * FROM demo.echo((SELECT * FROM range(3) t(x)));   -- 0,1,2
package farm.query.vgi.examples;

import farm.query.vgi.Worker;
import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.tableinout.PassthroughTIOFunction;
import farm.query.vgi.tableinout.TableInOutExchangeState;
import farm.query.vgi.tableinout.TableInOutInitParams;
import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.util.TransferPair;

import java.util.ArrayList;
import java.util.List;

/** {@code echo(data TABLE) -> *}: passes every input batch through unchanged. */
public final class TableInOutExample extends PassthroughTIOFunction {

    @Override public String name() { return "echo"; }

    @Override public FunctionMetadata metadata() {
        return FunctionMetadata.describe("Emit each input batch unchanged")
                .withCategories("utility");
    }

    // Declare the single table-valued argument. TIO functions take a relation.
    @Override public List<ArgSpec> argumentSpecs() {
        return List.of(ArgSpec.table("data", 0));
    }

    @Override public TableInOutExchangeState createExchange(TableInOutInitParams params) {
        return new EchoState();
    }

    /** One exchange instance per execution; `onInputBatch` runs per input batch. */
    public static final class EchoState extends TableInOutExchangeState {
        @Override
        public void onInputBatch(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
            // Transfer the input vectors into a fresh root before emitting.
            //
            // Why not just `out.emit(input.root())`? The framework close()s each
            // emitted root after writing it. The input root is owned by the
            // reader and reused for the NEXT batch — closing it would corrupt the
            // stream. TransferPair moves the buffers into a root we own, leaving
            // the reader intact. (TransferPair, not a row copy, also preserves
            // dictionary-encoded children.)
            VectorSchemaRoot in = input.root();
            List<FieldVector> outVectors = new ArrayList<>();
            for (FieldVector v : in.getFieldVectors()) {
                TransferPair tp = v.getTransferPair(Allocators.root());
                tp.transfer();
                outVectors.add((FieldVector) tp.getTo());
            }
            VectorSchemaRoot copy = new VectorSchemaRoot(outVectors);
            copy.setRowCount(in.getRowCount());
            out.emit(copy);   // emit() takes ownership; do not close `copy` yourself
        }
    }

    public static void main(String[] args) {
        Worker.builder()
                .catalogName("demo")
                .registerTableInOut(new TableInOutExample())
                .runFromArgs(args);
    }
}
