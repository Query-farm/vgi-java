// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.example.scalar;

import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.scalar.ScalarBindParams;
import farm.query.vgi.scalar.ScalarFunction;
import farm.query.vgi.scalar.ScalarProcessParams;
import farm.query.vgi.types.ScalarHelpers;
import farm.query.vgi.types.Schemas;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.util.Text;

import java.util.List;

/**
 * {@code any_mixed(a, b)} — two overloads where {@code a} accepts any Arrow
 * type and {@code b} is dispatched on type ({@code int64} vs {@code utf8}).
 */
public final class AnyMixedFunctions {

    private AnyMixedFunctions() {}

    private static final byte[] OUT = Schemas.singleResultIpc(Schemas.UTF8);

    public static final class IntVariant implements ScalarFunction {
        @Override public String name() { return "any_mixed"; }
        @Override public FunctionMetadata metadata() {
            return FunctionMetadata.describe("Any+int dispatch");
        }
        @Override public List<ArgSpec> argumentSpecs() {
            return List.of(
                    ArgSpec.any("a", 0, List.of()),
                    new ArgSpec("b", 1, Schemas.INT64));
        }
        @Override public BindResponse onBind(ScalarBindParams p) { return BindResponse.forSchema(OUT); }
        @Override public VectorSchemaRoot process(ScalarProcessParams p, VectorSchemaRoot input,
                                                    BufferAllocator alloc) {
            int rows = input.getRowCount();
            FieldVector a = input.getFieldVectors().get(0);
            FieldVector b = input.getFieldVectors().get(1);
            VectorSchemaRoot out = VectorSchemaRoot.create(p.outputSchema(), alloc);
            out.allocateNew();
            VarCharVector r = (VarCharVector) out.getVector("result");
            for (int i = 0; i < rows; i++) {
                if (a.isNull(i) || b.isNull(i)) r.setNull(i);
                else r.setSafe(i, new Text("any+int: " + ScalarHelpers.toLong(b, i)));
            }
            out.setRowCount(rows);
            return out;
        }
    }

    public static final class StrVariant implements ScalarFunction {
        @Override public String name() { return "any_mixed"; }
        @Override public FunctionMetadata metadata() {
            return FunctionMetadata.describe("Any+str dispatch");
        }
        @Override public List<ArgSpec> argumentSpecs() {
            return List.of(
                    ArgSpec.any("a", 0, List.of()),
                    new ArgSpec("b", 1, Schemas.UTF8));
        }
        @Override public BindResponse onBind(ScalarBindParams p) { return BindResponse.forSchema(OUT); }
        @Override public VectorSchemaRoot process(ScalarProcessParams p, VectorSchemaRoot input,
                                                    BufferAllocator alloc) {
            int rows = input.getRowCount();
            FieldVector a = input.getFieldVectors().get(0);
            FieldVector b = input.getFieldVectors().get(1);
            VectorSchemaRoot out = VectorSchemaRoot.create(p.outputSchema(), alloc);
            out.allocateNew();
            VarCharVector r = (VarCharVector) out.getVector("result");
            for (int i = 0; i < rows; i++) {
                if (a.isNull(i) || b.isNull(i)) r.setNull(i);
                else {
                    String s = ((VarCharVector) b).getObject(i).toString();
                    r.setSafe(i, new Text("any+str: " + s));
                }
            }
            out.setRowCount(rows);
            return out;
        }
    }

    public static final class SmartFormatInt implements ScalarFunction {
        @Override public String name() { return "smart_format"; }
        @Override public FunctionMetadata metadata() {
            return FunctionMetadata.describe("Format double with width prefix");
        }
        @Override public List<ArgSpec> argumentSpecs() {
            return List.of(
                    new ArgSpec("width", 0, Schemas.INT64, /*isConst=*/true),
                    new ArgSpec("value", 1, Schemas.FLOAT64));
        }
        @Override public BindResponse onBind(ScalarBindParams p) { return BindResponse.forSchema(OUT); }
        @Override public VectorSchemaRoot process(ScalarProcessParams p, VectorSchemaRoot input,
                                                    BufferAllocator alloc) {
            int width = ((Number) p.arguments().positionalAt(0)).intValue();
            int rows = input.getRowCount();
            FieldVector v = input.getFieldVectors().get(0);
            VectorSchemaRoot out = VectorSchemaRoot.create(p.outputSchema(), alloc);
            out.allocateNew();
            VarCharVector r = (VarCharVector) out.getVector("result");
            for (int i = 0; i < rows; i++) {
                if (v.isNull(i)) r.setNull(i);
                else r.setSafe(i, new Text(String.format("%" + width + "s", ScalarHelpers.toDouble(v, i))));
            }
            out.setRowCount(rows);
            return out;
        }
    }

    public static final class SmartFormatStr implements ScalarFunction {
        @Override public String name() { return "smart_format"; }
        @Override public FunctionMetadata metadata() {
            return FunctionMetadata.describe("Format double with string prefix");
        }
        @Override public List<ArgSpec> argumentSpecs() {
            return List.of(
                    new ArgSpec("prefix", 0, Schemas.UTF8, /*isConst=*/true),
                    new ArgSpec("value", 1, Schemas.FLOAT64));
        }
        @Override public BindResponse onBind(ScalarBindParams p) { return BindResponse.forSchema(OUT); }
        @Override public VectorSchemaRoot process(ScalarProcessParams p, VectorSchemaRoot input,
                                                    BufferAllocator alloc) {
            String prefix = (String) p.arguments().positionalAt(0);
            int rows = input.getRowCount();
            FieldVector v = input.getFieldVectors().get(0);
            VectorSchemaRoot out = VectorSchemaRoot.create(p.outputSchema(), alloc);
            out.allocateNew();
            VarCharVector r = (VarCharVector) out.getVector("result");
            for (int i = 0; i < rows; i++) {
                if (v.isNull(i)) r.setNull(i);
                else r.setSafe(i, new Text(prefix + ScalarHelpers.toDouble(v, i)));
            }
            out.setRowCount(rows);
            return out;
        }
    }
}
