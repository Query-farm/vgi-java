// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.table;

import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.ParameterExtractor;
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
 * Time-travel + filter-pushdown fixtures backing
 * {@code test/sql/integration/table/time_travel_pushdown.test}: a table can be
 * partition-pruned (filter pushdown) AND time-travelled ({@code AT (VERSION|
 * TIMESTAMP ...)}) in the same query.
 *
 * <ul>
 *   <li>{@link TimeTravelPushdown} ({@code tt_pushdown_scan}) — function-backed.
 *       Reads the AT clause from the bind request threaded onto init
 *       ({@link TableInitParams#atUnit}). Backs {@code example.data.tt_pushdown_fn}.</li>
 *   <li>{@link TtPushdownCols} ({@code tt_pushdown_cols_scan}) — columns-based.
 *       Receives the resolved version as a scan-function argument (the worker's
 *       {@code catalog_table_scan_function_get} resolves AT -> version). Backs
 *       {@code example.data.tt_pushdown_cols}.</li>
 * </ul>
 *
 * <p>Both echo {@code seen_version} (the version actually scanned) and
 * {@code pushed_filters} (the SQL-like predicate DuckDB pushed down), so one
 * query asserts both signals at once. {@code auto_apply_filters} keeps the
 * result set correct.
 */
public final class TimeTravelPushdownFunctions {

    private TimeTravelPushdownFunctions() {}

    static final Schema OUTPUT_SCHEMA = Schemas.of(
            Schemas.nullable("id", Schemas.INT64),
            Schemas.nullable("val", Schemas.INT64),
            Schemas.nullable("seen_version", Schemas.INT64),
            Schemas.nullable("pushed_filters", Schemas.UTF8));

    /** v2 is a strict superset of v1, so a row-count difference proves which
     *  version was scanned. */
    private static long[] versionIds(int version) {
        return version == 1
                ? new long[]{1, 2, 3, 4, 5}
                : new long[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    }

    /** Resolve an AT clause to one of the fixture's versions (1 or 2). */
    static int resolveVersion(String atUnit, String atValue) {
        if (atUnit == null || atUnit.isEmpty()) return 2;
        if ("version".equalsIgnoreCase(atUnit)) return Integer.parseInt(atValue);
        if ("timestamp".equalsIgnoreCase(atUnit)) {
            int year = Integer.parseInt(atValue.substring(0, Math.min(4, atValue.length())));
            return year <= 2020 ? 1 : 2;
        }
        throw new IllegalArgumentException("Unsupported at_unit: " + atUnit);
    }

    private static String pushedFilters(TableInitParams params) {
        byte[] pf = params.pushdownFilters();
        PushdownFilters filters = pf == null
                ? PushdownFilters.empty()
                : PushdownFiltersDecoder.decode(pf, params.joinKeys());
        return filters.formatInline();
    }

    /** Shared one-shot producer: emit one batch for {@code seenVersion},
     *  applying pushdown filters and projecting to the requested columns. */
    static final class State extends TableProducerState {
        public int seenVersion;
        public String filterStr;
        public byte[] filterBytes;
        public CachedSchema projected;
        public List<byte[]> joinKeysIpc;
        public boolean done;

        public State() {}

        State(int seenVersion, String filterStr, byte[] filterBytes, CachedSchema projected,
                List<byte[]> joinKeysIpc) {
            this.seenVersion = seenVersion;
            this.filterStr = filterStr;
            this.filterBytes = filterBytes;
            this.projected = projected;
            this.joinKeysIpc = joinKeysIpc;
        }

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            if (done) { out.finish(); return; }
            done = true;
            long[] ids = versionIds(seenVersion);
            VectorSchemaRoot work = VectorSchemaRoot.create(OUTPUT_SCHEMA, Allocators.root());
            work.allocateNew();
            BigIntVector idv = (BigIntVector) work.getVector("id");
            BigIntVector valv = (BigIntVector) work.getVector("val");
            BigIntVector sv = (BigIntVector) work.getVector("seen_version");
            VarCharVector pv = (VarCharVector) work.getVector("pushed_filters");
            Text filterText = new Text(filterStr);
            for (int i = 0; i < ids.length; i++) {
                idv.setSafe(i, ids[i]);
                valv.setSafe(i, ids[i] * 10);
                sv.setSafe(i, seenVersion);
                pv.setSafe(i, filterText);
            }
            work.setRowCount(ids.length);
            if (filterBytes != null) {
                work = FilterApplier.from(filterBytes, joinKeysIpc).apply(work);
            }
            out.emit(VectorProjector.project(work, projected.get()));
        }
    }

    /** {@code tt_pushdown_scan} — function-backed; reads AT at init. */
    public static final class TimeTravelPushdown extends SimpleTableFunction {
        @Override public String name() { return "tt_pushdown_scan"; }

        @Override public List<farm.query.vgi.function.ArgSpec> argumentSpecs() { return List.of(); }

        @Override public FunctionMetadata metadata() {
            return FunctionMetadata.describe(
                    "Function-backed time-travel + filter-pushdown scan (reads AT at init).")
                    .withPushdown(/*projection=*/true, /*filter=*/true, /*autoApply=*/true)
                    .withCategories("generator", "diagnostic", "testing");
        }

        @Override protected Schema outputSchema() { return OUTPUT_SCHEMA; }

        @Override public TableProducerState createProducer(TableInitParams params) {
            int version = resolveVersion(params.atUnit(), params.atValue());
            return new State(version, pushedFilters(params), params.pushdownFilters(),
                    new CachedSchema(params.outputSchema()), params.joinKeys());
        }
    }

    /** {@code tt_pushdown_cols_scan} — columns-based; version via positional arg. */
    public static final class TtPushdownCols extends SimpleTableFunction {
        @Override public String name() { return "tt_pushdown_cols_scan"; }

        @Override public FunctionMetadata metadata() {
            return FunctionMetadata.describe(
                    "Columns-based time-travel + filter-pushdown scan (version via arg).")
                    .withPushdown(/*projection=*/true, /*filter=*/true, /*autoApply=*/true)
                    .withCategories("generator", "diagnostic", "testing");
        }

        @Override public List<farm.query.vgi.function.ArgSpec> argumentSpecs() {
            return List.of(new farm.query.vgi.function.ArgSpec("version", 0, Schemas.INT64, true));
        }

        @Override protected Schema outputSchema() { return OUTPUT_SCHEMA; }

        @Override public TableProducerState createProducer(TableInitParams params) {
            int version = (int) ParameterExtractor.of(params.arguments())
                    .positional(0, "version").asLong().required();
            return new State(version, pushedFilters(params), params.pushdownFilters(),
                    new CachedSchema(params.outputSchema()), params.joinKeys());
        }
    }
}
