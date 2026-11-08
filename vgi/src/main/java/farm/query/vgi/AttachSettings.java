// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi;

import farm.query.vgirpc.schema.Nullable;

/**
 * Per-attach decisions an {@link AttachHook} returns for the worker's main catalog. A
 * {@code null} component keeps the worker-wide setting ({@link Worker#supportsTransactions(boolean)},
 * {@link Worker#supportsTimeTravel(boolean)}).
 *
 * @param supportsTransactions whether this attach's result advertises
 *     {@code supports_transactions}. When true, DuckDB calls
 *     {@code catalog_transaction_begin} / {@code _commit} / {@code _rollback} for every
 *     transaction on the attach (autocommit statements included), which run the worker's
 *     {@link TransactionHooks}, and every call inside a transaction carries its id
 *     ({@link farm.query.vgi.table.TableBindParams#transactionId()} and friends). When false,
 *     DuckDB never calls them and functions see no transaction id.
 * @param supportsTimeTravel whether this attach's result advertises {@code supports_time_travel}
 *     (DuckDB then accepts {@code AT (...)} clauses on its tables)
 */
public record AttachSettings(@Nullable Boolean supportsTransactions, @Nullable Boolean supportsTimeTravel) {

    /**
     * Keep every worker-wide setting.
     *
     * @return settings with every component {@code null}
     */
    public static AttachSettings defaults() {
        return new AttachSettings(null, null);
    }

    /**
     * Settings that only decide {@code supports_transactions}.
     *
     * @param supported whether this attach supports transactions
     * @return the settings
     */
    public static AttachSettings transactional(boolean supported) {
        return new AttachSettings(supported, null);
    }

    /**
     * A copy deciding {@code supports_time_travel}.
     *
     * @param supported whether this attach supports time travel
     * @return the settings
     */
    public AttachSettings withTimeTravel(boolean supported) {
        return new AttachSettings(supportsTransactions, supported);
    }
}
