// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.buffering;

import farm.query.vgi.buffering.TableBufferingFinalizeParams;
import farm.query.vgi.cache.CacheControl;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.types.Schemas;

import java.util.Map;

/**
 * {@code cached_sum_all(data TABLE)} — cacheable BUFFERED whole-input reducer
 * (column-wise sum) advertising {@code vgi.cache.*} on its finalize output,
 * backing the exchange-mode buffered result cache
 * ({@code cache/exchange_buffered.test}). A repeat query with the same input
 * multiset (any order) replays the cached single-row result and skips the
 * combine + finalize-drain on the worker (the Sink ingestion still runs — the
 * key is only known after all input is folded). Mirrors vgi-python's
 * {@code CachedSumAllColumnsFunction}.
 */
public final class CachedSumAllColumnsFunction extends SumAllColumnsBufferingFunction {

    private static final FunctionSpec SPEC = FunctionSpec.builder("cached_sum_all")
            .metadata(FunctionMetadata.describe(
                            "Cacheable column-wise sum across all input (advertises vgi.cache.ttl)")
                    .withCategories("aggregation", "cache", "test"))
            .table("data")
            .named("logging", Schemas.BOOL, "false")
            .build();

    @Override public FunctionSpec spec() { return SPEC; }

    @Override public TableProducerState createFinalizeProducer(TableBufferingFinalizeParams params) {
        return new CachedSumProducer(params);
    }

    /** The parent's summing producer, advertising {@code vgi.cache.ttl} on the emit. */
    static final class CachedSumProducer extends SumProducer {

        /** No-arg constructor for HTTP state-token deserialization. */
        CachedSumProducer() {}

        CachedSumProducer(TableBufferingFinalizeParams params) { super(params); }

        @Override protected Map<String, String> emitMetadata() {
            return CacheControl.ttl(300).toMetadata();
        }
    }
}
