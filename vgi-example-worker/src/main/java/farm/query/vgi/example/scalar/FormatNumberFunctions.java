// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.example.scalar;

import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.scalar.ScalarBindParams;
import farm.query.vgi.scalar.ScalarFunction;
import farm.query.vgi.scalar.ScalarProcessParams;
import farm.query.vgi.types.Schemas;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.util.Text;

/**
 * {@code format_number} — three overloads:
 * <ol>
 *   <li>{@code format_number(value DOUBLE)} → {@code "%.0f"}</li>
 *   <li>{@code format_number(precision INT64 [const], value DOUBLE)} → {@code "%.<precision>f"}</li>
 *   <li>{@code format_number(precision INT64 [const], prefix VARCHAR [const], value DOUBLE)}
 *       → {@code "<prefix>%.<precision>f"}</li>
 * </ol>
 *
 * <p>Each variant is a separate {@link ScalarFunction} all named
 * {@code "format_number"}; the framework dispatches by argument arity at
 * bind time.</p>
 */
public final class FormatNumberFunctions {

    private FormatNumberFunctions() {}

    private static final byte[] OUTPUT_SCHEMA_IPC = Schemas.singleResultIpc(Schemas.UTF8);

    /** {@code format_number(value)} — default precision 0. */
    public static final class Default implements ScalarFunction {
        private static final FunctionSpec SPEC = FunctionSpec.builder("format_number")
                .description("Format number with default precision (0 decimals)")
                .arg("value", Schemas.FLOAT64)
                .build();

        @Override public FunctionSpec spec() { return SPEC; }
        @Override public BindResponse onBind(ScalarBindParams params) {
            return BindResponse.forSchema(OUTPUT_SCHEMA_IPC);
        }
        @Override
        public VectorSchemaRoot process(ScalarProcessParams params, VectorSchemaRoot input, BufferAllocator alloc) {
            return mapFormat(params, input, alloc, 0, "");
        }
    }

    /** {@code format_number(precision, value)}. */
    public static final class WithPrecision implements ScalarFunction {
        private static final FunctionSpec SPEC = FunctionSpec.builder("format_number")
                .description("Format number with specified precision")
                .constArg("precision", Schemas.INT64)
                .arg("value", Schemas.FLOAT64)
                .build();

        @Override public FunctionSpec spec() { return SPEC; }
        @Override public BindResponse onBind(ScalarBindParams params) {
            return BindResponse.forSchema(OUTPUT_SCHEMA_IPC);
        }
        @Override
        public VectorSchemaRoot process(ScalarProcessParams params, VectorSchemaRoot input, BufferAllocator alloc) {
            int precision = ((Number) params.arguments().positionalAt(0)).intValue();
            return mapFormat(params, input, alloc, precision, "");
        }
    }

    /** {@code format_number(precision, prefix, value)}. */
    public static final class Full implements ScalarFunction {
        private static final FunctionSpec SPEC = FunctionSpec.builder("format_number")
                .description("Format number with precision and prefix")
                .constArg("precision", Schemas.INT64)
                .constArg("prefix", Schemas.UTF8)
                .arg("value", Schemas.FLOAT64)
                .build();

        @Override public FunctionSpec spec() { return SPEC; }
        @Override public BindResponse onBind(ScalarBindParams params) {
            return BindResponse.forSchema(OUTPUT_SCHEMA_IPC);
        }
        @Override
        public VectorSchemaRoot process(ScalarProcessParams params, VectorSchemaRoot input, BufferAllocator alloc) {
            int precision = ((Number) params.arguments().positionalAt(0)).intValue();
            String prefix = (String) params.arguments().positionalAt(1);
            return mapFormat(params, input, alloc, precision, prefix == null ? "" : prefix);
        }
    }

    private static VectorSchemaRoot mapFormat(ScalarProcessParams params, VectorSchemaRoot input,
                                                 BufferAllocator alloc, int precision, String prefix) {
        FieldVector valueCol = input.getFieldVectors().get(0);
        VectorSchemaRoot out = VectorSchemaRoot.create(params.outputSchema(), alloc);
        out.allocateNew();
        VarCharVector v = (VarCharVector) out.getVector("result");
        int rows = input.getRowCount();
        for (int i = 0; i < rows; i++) {
            if (valueCol.isNull(i)) {
                v.setNull(i);
            } else {
                double val = ((Float8Vector) valueCol).get(i);
                String formatted = String.format("%." + precision + "f", val);
                v.setSafe(i, new Text(prefix + formatted));
            }
        }
        out.setRowCount(rows);
        return out;
    }
}
