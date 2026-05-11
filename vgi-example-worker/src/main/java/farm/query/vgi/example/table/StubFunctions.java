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
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.Serializable;
import java.util.List;

/**
 * Lightweight stub table functions registered for {@code function_registration.test}
 * count assertions. Each emits zero rows. Real implementations live in the
 * corresponding feature-specific tests (which are skipped here).
 */
public final class StubFunctions {

    private StubFunctions() {}

    /** Base for zero-row stubs with a configurable name + output schema. */
    private static abstract class Stub implements TableFunction {
        final String name;
        final String description;
        final Schema outputSchema;
        final byte[] outputSchemaIpc;
        final List<ArgSpec> argSpecs;

        Stub(String name, String description, Schema outputSchema, List<ArgSpec> argSpecs) {
            this.name = name;
            this.description = description;
            this.outputSchema = outputSchema;
            this.outputSchemaIpc = SchemaUtil.serializeSchema(outputSchema);
            this.argSpecs = argSpecs;
        }

        @Override public String name() { return name; }
        @Override public FunctionMetadata metadata() {
            return FunctionMetadata.describe(description);
        }
        @Override public List<ArgSpec> argumentSpecs() { return argSpecs; }
        @Override public BindResponse onBind(TableBindParams p) { return BindResponse.forSchema(outputSchemaIpc); }
        @Override public TableProducerState createProducer(TableInitParams params) { return new EmptyState(); }
    }

    public static final class EmptyState extends TableProducerState implements Serializable {
        private static final long serialVersionUID = 1L;
        public EmptyState() {}
        @Override public void produceTick(OutputCollector out, CallContext ctx) { out.finish(); }
    }

    /** {@code expression_filter_test(count BIGINT)} — sequence-shaped stub. */
    public static final class ExpressionFilterTest extends Stub {
        public ExpressionFilterTest() {
            super("expression_filter_test",
                    "Filter pushdown reproducer with expression filters",
                    new Schema(List.of(Schemas.nullable("n", Schemas.INT64))),
                    List.of(ArgSpec.positional("count", 0, Schemas.INT64)));
        }
    }

    /** {@code spatial_filter_example(count BIGINT)} — spatial filter reproducer stub. */
    public static final class SpatialFilterExample extends Stub {
        public SpatialFilterExample() {
            super("spatial_filter_example",
                    "Spatial filter pushdown reproducer",
                    new Schema(List.of(
                            Schemas.nullable("id", Schemas.INT64),
                            new Field("geom", new FieldType(true, new org.apache.arrow.vector.types.pojo.ArrowType.Binary(), null), null))),
                    List.of(ArgSpec.positional("count", 0, Schemas.INT64)));
        }
    }

    /** {@code versioned_data_scan(version BIGINT)} — versioned scan stub. */
    public static final class VersionedDataScan extends Stub {
        public VersionedDataScan() {
            super("versioned_data_scan",
                    "Versioned data scan function",
                    new Schema(List.of(
                            Schemas.nullable("id", Schemas.INT64),
                            Schemas.nullable("value", Schemas.UTF8))),
                    List.of(ArgSpec.positional("version", 0, Schemas.INT64)));
        }
    }

    /** {@code colors_scan()} — stub for the colors catalog table scan. */
    public static final class ColorsScan extends Stub {
        public ColorsScan() {
            super("colors_scan",
                    "Colors table scan function",
                    new Schema(List.of(
                            Schemas.nullable("id", Schemas.INT64),
                            Schemas.nullable("color", Schemas.UTF8),
                            Schemas.nullable("hex_code", Schemas.UTF8))),
                    List.of());
        }
    }

    /** {@code departments_scan()} — stub for departments catalog table scan. */
    public static final class DepartmentsScan extends Stub {
        public DepartmentsScan() {
            super("departments_scan",
                    "Departments table scan function",
                    new Schema(List.of(
                            Schemas.nullable("id", Schemas.INT64),
                            Schemas.nullable("name", Schemas.UTF8),
                            Schemas.nullable("budget", Schemas.FLOAT64))),
                    List.of());
        }
    }

    /** {@code employees_scan()} — stub for employees catalog table scan. */
    public static final class EmployeesScan extends Stub {
        public EmployeesScan() {
            super("employees_scan",
                    "Employees table scan function",
                    new Schema(List.of(
                            Schemas.nullable("id", Schemas.INT64),
                            Schemas.nullable("name", Schemas.UTF8),
                            Schemas.nullable("email", Schemas.UTF8),
                            Schemas.nullable("department_id", Schemas.INT64))),
                    List.of());
        }
    }

    /** {@code products_scan()} — stub for products catalog table scan. */
    public static final class ProductsScan extends Stub {
        public ProductsScan() {
            super("products_scan",
                    "Products table scan function",
                    new Schema(List.of(
                            Schemas.nullable("id", Schemas.INT64),
                            Schemas.nullable("name", Schemas.UTF8),
                            Schemas.nullable("price", Schemas.FLOAT64),
                            Schemas.nullable("quantity", Schemas.INT64))),
                    List.of());
        }
    }

    /** {@code projects_scan()} — stub for projects catalog table scan. */
    public static final class ProjectsScan extends Stub {
        public ProjectsScan() {
            super("projects_scan",
                    "Projects table scan function",
                    new Schema(List.of(
                            Schemas.nullable("department_id", Schemas.INT64),
                            Schemas.nullable("project_code", Schemas.UTF8),
                            Schemas.nullable("title", Schemas.UTF8))),
                    List.of());
        }
    }
}
