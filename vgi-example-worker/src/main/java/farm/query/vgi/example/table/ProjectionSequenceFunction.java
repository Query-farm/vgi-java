// Copyright 2026 Query Farm LLC - https://query.farm
package farm.query.vgi.example.table;

import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.internal.ProjectionExpressions;
import farm.query.vgi.internal.SchemaUtil;
import farm.query.vgi.protocol.*;
import farm.query.vgi.table.*;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.marshal.RecordCodec;
import farm.query.vgirpc.wire.Allocators;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

/** Portable, pure widening-cast projection example. */
public final class ProjectionSequenceFunction extends SimpleTableFunction {
  public static final Schema OUTPUT =
      new Schema(List.of(Schemas.nullable("n", Schemas.INT32)));
  private static final FunctionSpec SPEC =
      FunctionSpec.builder("projection_sequence")
          .description("Optional widening-cast projection")
          .build();
  @Override
  public FunctionSpec spec() {
    return SPEC;
  }
  @Override
  protected Schema outputSchema() {
    return OUTPUT;
  }
  @Override
  public BindResponse onBind(TableBindParams params) {
    return new BindResponse(SchemaUtil.serializeSchema(OUTPUT), new byte[0],
                            List.of(), List.of(), List.of(),
                            List.of(ProjectionExpressions.PROFILE));
  }
  @Override
  public TableFunctionProjectionResponse
  projection(TableFunctionProjectionRequest request) {
    var original = RecordCodec.deserializeFromBytes(request.bind_result(),
                                                    BindResponse.class);
    var result = SchemaUtil.deserializeSchema(request.result_schema());
    var indices = new ArrayList<Long>();
    for (int i = 0; i < request.expressions().size(); i++) {
      var expression = RecordCodec.deserializeFromBytes(
          request.expressions().get(i), SqlExpression.class);
      var context = expression.evaluation_context();
      if (expression.sql().equals("CAST(\"n\" AS BIGINT)") &&
          context.equals(new QueryEvaluationContext("UTC", "binary", "asc",
                                                    "nulls_last", true)) &&
          result.getFields().get(i).getType().equals(Schemas.INT64) &&
          result.getFields().get(i).isNullable())
        indices.add((long)i);
    }
    if (indices.isEmpty())
      return TableFunctionProjectionResponse.decline();
    var source = SchemaUtil.deserializeSchema(original.output_schema());
    var fields = new ArrayList<>(source.getFields());
    for (long i : indices)
      fields.add(result.getFields().get((int)i));
    var accepted = new BindResponse(
        SchemaUtil.serializeSchema(
            new Schema(fields, source.getCustomMetadata())),
        Integer.toString(indices.size()).getBytes(StandardCharsets.US_ASCII),
        List.of(), List.of(), List.of(),
        original.projection_expression_profiles());
    return new TableFunctionProjectionResponse(
        indices, RecordCodec.serializeToBytes(accepted));
  }
  @Override
  public TableProducerState createProducer(TableInitParams params) {
    int appended =
        params.bindOpaqueData().length == 0
            ? 0
            : Integer.parseInt(new String(params.bindOpaqueData(),
                                          StandardCharsets.US_ASCII));
    if (params.outputSchema().getFields().size() != 1 + appended)
      throw new IllegalArgumentException(
          "Projection state differs from output schema");
    return new State(SchemaUtil.serializeSchema(params.outputSchema()));
  }
  public static final class State extends TableProducerState {
    public byte[] schema;
    public int offset;
    public State() {}
    State(byte[] schema) { this.schema = schema; }
    @Override
    public void produceTick(OutputCollector out, CallContext ctx) {
      if (offset == 10) {
        out.finish();
        return;
      }
      var root = VectorSchemaRoot.create(SchemaUtil.deserializeSchema(schema),
                                         Allocators.root());
      root.allocateNew();
      for (int column = 0; column < root.getFieldVectors().size(); column++) {
        var vector = root.getVector(column);
        for (int i = 0; i < 5; i++) {
          if (column == 0)
            ((IntVector)vector).setSafe(i, offset + i);
          else
            ((BigIntVector)vector).setSafe(i, offset + i);
        }
      }
      root.setRowCount(5);
      offset += 5;
      out.emit(root);
    }
  }
}
