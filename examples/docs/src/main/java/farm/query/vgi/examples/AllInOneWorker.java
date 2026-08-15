// VGI-Java example: one worker serving all five function kinds at once.
//
// This is the artifact the quickstart and the integration test attach to. It
// registers every example function under the catalog `demo`, so a single ATTACH
// exposes upper_case (scalar), numbers (table), echo (table-in-out), vgi_sum
// (aggregate), and collect (buffering).
//
//   ./gradlew installDist
//   ATTACH 'demo' AS demo (TYPE vgi,
//       LOCATION 'launch:/abs/path/build/install/vgi-java-examples/bin/vgi-java-examples');
package farm.query.vgi.examples;

import farm.query.vgi.Worker;

/** A single worker process exposing one function of each kind. */
public final class AllInOneWorker {

    public static void main(String[] args) {
        Worker w = Worker.builder()
                .catalogName("demo")
                .catalogComment("VGI-Java introductory examples")
                .registerScalar(new ScalarExample())          // upper_case
                .registerTable(new TableExample())             // numbers (parallel-safe via storage)
                .registerTableInOut(new TableInOutExample())   // echo
                .registerAggregate(new AggregateExample())     // vgi_sum
                .registerTableBuffering(new BufferingExample());// collect
        CatalogExample.register(w);                            // catalog: schema, table, view, macros
        w.runFromArgs(args);
    }
}
