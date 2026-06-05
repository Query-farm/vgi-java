// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.table;

import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.internal.VectorProjector;
import farm.query.vgi.pushdown.FilterApplier;
import farm.query.vgi.pushdown.PushdownFilters;
import farm.query.vgi.pushdown.PushdownFiltersDecoder;
import farm.query.vgi.table.SimpleTableFunction;
import farm.query.vgi.table.TableInitParams;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.types.CachedSchema;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.util.List;

/**
 * {@code filter_echo_table_scan()} — no-arg catalog-table scan echoing the
 * pushed-down filters it received. Backs {@code example.data.filter_echo_table}
 * and {@code test/sql/integration/table/filter_pushdown_through_view.test}, which
 * characterizes filter pushdown directly and through a VIEW.
 *
 * <p>Like {@link FilterEchoFunction} the {@code pushed_filters} column shows the
 * SQL-like representation of whatever DuckDB pushed down; the framework
 * auto-applies the filters so the result set stays correct. Unlike
 * {@code filter_echo} it is a no-arg <em>table</em> scan emitting a fixed 100-row
 * dataset ({@code n} in 0..99, {@code s = 'row_<n>'}), so a
 * {@code LIKE 'prefix%'} predicate (which DuckDB lowers to a constant-prefix
 * RANGE filter) is observable through a view over this table.
 */
public final class FilterEchoTableScanFunction extends SimpleTableFunction {

    private static final Schema OUTPUT_SCHEMA = Schemas.of(
            Schemas.nullable("n", Schemas.INT64),
            Schemas.nullable("s", Schemas.UTF8),
            Schemas.nullable("pushed_filters", Schemas.UTF8));

    private static final int ROWS = 100;

    @Override public String name() { return "filter_echo_table_scan"; }

    @Override public List<farm.query.vgi.function.ArgSpec> argumentSpecs() { return List.of(); }

    @Override public FunctionMetadata metadata() {
        return FunctionMetadata.describe(
                "Catalog-table scan echoing pushed-down filters (backs example.data.filter_echo_table)")
                .withPushdown(/*projection=*/true, /*filter=*/true, /*autoApply=*/true)
                .withCategories("generator", "diagnostic", "testing");
    }

    @Override protected Schema outputSchema() { return OUTPUT_SCHEMA; }

    @Override public TableProducerState createProducer(TableInitParams params) {
        byte[] pf = params.pushdownFilters();
        PushdownFilters filters = pf == null
                ? PushdownFilters.empty()
                : PushdownFiltersDecoder.decode(pf, params.joinKeys());
        return new State(filters.formatInline(), pf, new CachedSchema(params.outputSchema()),
                params.joinKeys());
    }

    public static final class State extends TableProducerState {
        public String filterStr;
        public byte[] filterBytes;
        public CachedSchema projected;
        public List<byte[]> joinKeysIpc;
        public boolean done;

        public State() {}

        State(String filterStr, byte[] filterBytes, CachedSchema projected, List<byte[]> joinKeysIpc) {
            this.filterStr = filterStr;
            this.filterBytes = filterBytes;
            this.projected = projected;
            this.joinKeysIpc = joinKeysIpc;
        }

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            if (done) { out.finish(); return; }
            done = true;
            VectorSchemaRoot work = VectorSchemaRoot.create(OUTPUT_SCHEMA, Allocators.root());
            work.allocateNew();
            BigIntVector nv = (BigIntVector) work.getVector("n");
            VarCharVector sv = (VarCharVector) work.getVector("s");
            VarCharVector pv = (VarCharVector) work.getVector("pushed_filters");
            Text filterText = new Text(filterStr);
            for (int i = 0; i < ROWS; i++) {
                nv.setSafe(i, i);
                sv.setSafe(i, new Text("row_" + i));
                pv.setSafe(i, filterText);
            }
            work.setRowCount(ROWS);
            if (filterBytes != null) {
                work = FilterApplier.from(filterBytes, joinKeysIpc).apply(work);
            }
            out.emit(VectorProjector.project(work, projected.get()));
        }
    }
}
