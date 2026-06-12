// Copyright 2026 Query Farm LLC - https://query.farm

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

    // expression_filter_test and spatial_filter_example are now real generators
    // (ExpressionFilterTestFunction / SpatialFilterExampleFunction) — they back
    // table/expression_filter.test, which exercises expression-filter pushdown.

    /** {@code versioned_data_scan(version BIGINT)} — versioned scan stub. */
    public static final class VersionedDataScan extends Stub {
        public VersionedDataScan() {
            super("versioned_data_scan",
                    "Returns versioned data with schema evolution",
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
                    "Scan colors table (ENUM column)",
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
                    "Scan departments table",
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
                    "Scan employees table",
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
                    "Scan products table",
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
                    "Scan projects table",
                    new Schema(List.of(
                            Schemas.nullable("department_id", Schemas.INT64),
                            Schemas.nullable("project_code", Schemas.UTF8),
                            Schemas.nullable("title", Schemas.UTF8))),
                    List.of());
        }
    }

    private static Field structOf(String name, Field... children) {
        return new Field(name,
                new FieldType(true, new org.apache.arrow.vector.types.pojo.ArrowType.Struct(), null),
                List.of(children));
    }

    /** {@code rff_simple_scan()} — flat (a, b) for required_field_filter_paths. */
    public static final class RffSimpleScan extends Stub {
        public RffSimpleScan() {
            super("rff_simple_scan",
                    "rff_simple — flat columns (a, b) for required_field_filter_paths tests",
                    new Schema(List.of(
                            Schemas.nullable("a", Schemas.INT64),
                            Schemas.nullable("b", Schemas.INT64))),
                    List.of());
        }
    }

    /** {@code rff_struct_scan()} — STRUCT(s.a, s.b) + other. */
    public static final class RffStructScan extends Stub {
        public RffStructScan() {
            super("rff_struct_scan",
                    "rff_struct — STRUCT(s.a, s.b) + other for required_field_filter_paths tests",
                    new Schema(List.of(
                            structOf("s",
                                    Schemas.nullable("a", Schemas.INT64),
                                    Schemas.nullable("b", Schemas.INT64)),
                            Schemas.nullable("other", Schemas.INT64))),
                    List.of());
        }
    }

    /** {@code rff_nested_scan()} — nested STRUCT(wrapper.mid.leaf). */
    public static final class RffNestedScan extends Stub {
        public RffNestedScan() {
            super("rff_nested_scan",
                    "rff_nested — nested STRUCT(wrapper.mid.leaf) for required_field_filter_paths tests",
                    new Schema(List.of(
                            structOf("wrapper",
                                    structOf("mid",
                                            Schemas.nullable("leaf", Schemas.INT64))))),
                    List.of());
        }
    }

    /** {@code rff_multi_scan()} — top-level + struct subfield required paths. */
    public static final class RffMultiScan extends Stub {
        public RffMultiScan() {
            super("rff_multi_scan",
                    "rff_multi — top-level + struct subfield required paths",
                    new Schema(List.of(
                            structOf("s",
                                    Schemas.nullable("a", Schemas.INT64),
                                    Schemas.nullable("b", Schemas.INT64)),
                            Schemas.nullable("top", Schemas.INT64))),
                    List.of());
        }
    }

    /** {@code rff_none_scan()} — control table with no required paths. */
    public static final class RffNoneScan extends Stub {
        public RffNoneScan() {
            super("rff_none_scan",
                    "rff_none — control table with no required_field_filter_paths",
                    new Schema(List.of(
                            Schemas.nullable("a", Schemas.INT64),
                            Schemas.nullable("b", Schemas.INT64))),
                    List.of());
        }
    }
}
