// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.example.scalar;

import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.TypeBoundPredicate;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.scalar.ScalarBindParams;
import farm.query.vgi.scalar.ScalarFunction;
import farm.query.vgi.scalar.ScalarProcessParams;
import farm.query.vgi.types.ScalarHelpers;
import farm.query.vgi.types.Schemas;
import farm.query.vgi.types.TypeRules;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.math.BigDecimal;

import java.util.List;

/**
 * {@code double(value)} — returns {@code value * 2}, promoted one width up
 * for integers (TINYINT→SMALLINT→INTEGER→BIGINT) and float-stable for floats.
 */
public final class DoubleFunction implements ScalarFunction {

    @Override public String name() { return "double"; }

    @Override public FunctionMetadata metadata() {
        return FunctionMetadata.describe("Doubles a numeric value");
    }

    @Override public List<ArgSpec> argumentSpecs() {
        return List.of(ArgSpec.any("value", 0, List.of(TypeBoundPredicate.IS_ADDABLE)));
    }

    @Override public BindResponse onBind(ScalarBindParams params) {
        ArrowType in = (params.inputSchema() == null
                || params.inputSchema().getFields().isEmpty())
                ? Schemas.INT64
                : params.inputSchema().getFields().get(0).getType();
        // Reject non-multipliable types up-front. The error message must
        // contain the predicate name so the test can pin the failure to the
        // bind layer rather than a downstream Arrow kernel exception.
        if (in instanceof ArrowType.Date
                || in instanceof ArrowType.Time
                || in instanceof ArrowType.Timestamp
                || in instanceof ArrowType.Interval
                || in instanceof ArrowType.Duration
                || in instanceof ArrowType.Bool
                || in instanceof ArrowType.Utf8
                || in instanceof ArrowType.LargeUtf8
                || in instanceof ArrowType.Binary
                || in instanceof ArrowType.LargeBinary) {
            throw new IllegalArgumentException(
                    "double: _is_multipliable_type rejects " + in
                            + " — only numeric types (int/float/decimal) are supported");
        }
        return BindResponse.forSchema(Schemas.singleResultIpc(TypeRules.promoteForAddition(in)));
    }

    @Override
    public VectorSchemaRoot process(ScalarProcessParams params, VectorSchemaRoot input, BufferAllocator alloc) {
        FieldVector value = input.getFieldVectors().get(0);
        Schema outSchema = params.outputSchema();
        ArrowType outType = outSchema.getFields().get(0).getType();
        int rows = input.getRowCount();
        VectorSchemaRoot out = VectorSchemaRoot.create(outSchema, alloc);
        out.allocateNew();
        FieldVector v = out.getVector("result");
        for (int i = 0; i < rows; i++) {
            if (value.isNull(i)) {
                v.setNull(i);
                continue;
            }
            switch (v) {
                case Float8Vector f -> f.setSafe(i, ScalarHelpers.toDouble(value, i) * 2);
                case Float4Vector f -> f.setSafe(i, (float) (ScalarHelpers.toDouble(value, i) * 2));
                case BigIntVector b -> b.setSafe(i, ScalarHelpers.toLong(value, i) * 2);
                case IntVector iv -> iv.setSafe(i, (int) (ScalarHelpers.toLong(value, i) * 2));
                case SmallIntVector s -> s.setSafe(i, (short) (ScalarHelpers.toLong(value, i) * 2));
                case TinyIntVector t -> t.setSafe(i, (byte) (ScalarHelpers.toLong(value, i) * 2));
                case DecimalVector d -> {
                    BigDecimal bd = ((DecimalVector) value).getObject(i);
                    BigDecimal doubled = bd.add(bd);
                    int precision = ((ArrowType.Decimal) d.getField().getType()).getPrecision();
                    if (doubled.precision() > precision) {
                        throw new IllegalArgumentException(
                                "value " + doubled + " does not fit in precision " + precision);
                    }
                    d.setSafe(i, doubled);
                }
                default -> throw new IllegalStateException("unexpected output type: " + outType);
            }
        }
        out.setRowCount(rows);
        return out;
    }
}
