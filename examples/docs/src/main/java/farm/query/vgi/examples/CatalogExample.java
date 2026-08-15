// VGI-Java example: constructing a catalog (schema · table · view · macros).
//
// Functions are one half of what a worker can serve; the other is the catalog —
// the schemas, tables, views, and macros SQL users expect. None of these extend
// a base class. You build small descriptor objects and register them on the
// Worker builder, and DuckDB surfaces them like any local catalog object.
//
// This builds a `catalog` schema with:
//   - a table `first_five`, whose rows come from the `numbers` table function
//   - a view `evens` over that table
//   - a scalar macro `triple` and a table macro `below`
//
//   ATTACH 'demo' AS demo (TYPE vgi, LOCATION 'launch:/abs/path/bin/runCatalog');
//   SELECT * FROM demo.catalog.first_five;     -- 0,1,2,3,4
//   SELECT * FROM demo.catalog.evens;          -- 0,2,4
package farm.query.vgi.examples;

import farm.query.vgi.Worker;
import farm.query.vgi.catalog.CatalogTable;
import farm.query.vgi.catalog.Macro;
import farm.query.vgi.catalog.MacroType;
import farm.query.vgi.catalog.View;
import farm.query.vgi.internal.SchemaUtil;
import farm.query.vgi.types.Schemas;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;
import java.util.Map;

public final class CatalogExample {

    // A table's columns are an Arrow schema, serialized to IPC bytes.
    private static final byte[] FIRST_FIVE_COLUMNS = SchemaUtil.serializeSchema(
            new Schema(List.of(Schemas.nullable("n", Schemas.INT64))));

    /**
     * Register the catalog objects onto an existing worker. The worker must also
     * register the {@code numbers} table function ({@link TableExample}) that
     * backs the table.
     */
    public static Worker register(Worker w) {
        return w
                .schemaComment("catalog", "Tables, views, and macros built by hand")

                // A table whose rows are produced by a function. `scanFunction`
                // names the backing function and its arguments — here `numbers(5)`,
                // which the worker itself serves. Its one INT64 output column maps
                // (by position) onto the table's declared `n` column.
                .registerCatalogTable(CatalogTable.builder("catalog", "first_five", FIRST_FIVE_COLUMNS)
                        .comment("The integers 0..4")
                        .scanFunction("numbers", List.of((Object) 5L), Map.of())
                        .cardinality(5, 5)
                        .build())

                // A view is SQL the engine expands at query time.
                .registerView(new View("catalog", "evens",
                        "SELECT n FROM catalog.first_five WHERE n % 2 = 0",
                        "Even values from first_five"))

                // Macros are parameterized SQL the engine inlines at the call site.
                .registerMacro(new Macro("catalog", "triple", MacroType.SCALAR,
                        List.of("x"), "x * 3", "Triple a value"))
                .registerMacro(new Macro("catalog", "series", MacroType.TABLE,
                        List.of("k"), "SELECT n FROM range(k) t(n)",
                        "The integers 0..k-1"));
    }

    public static void main(String[] args) {
        Worker w = Worker.builder()
                .catalogName("demo")
                .registerTable(new TableExample());   // the `numbers` function the table is backed by
        register(w);
        w.runFromArgs(args);
    }
}
