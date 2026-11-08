// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi;

import java.security.SecureRandom;

/**
 * Worker code run at {@code catalog_transaction_begin}, {@code _commit} and {@code _rollback}
 * on the worker's main catalog; registered with {@link Worker#transactionHooks(TransactionHooks)}.
 *
 * <p>DuckDB calls these only on an attach whose result advertised {@code supports_transactions}
 * ({@link Worker#supportsTransactions(boolean)}, or per attach from an {@link AttachHook}).
 * It then begins a transaction for every statement it runs against the attach: an explicit
 * {@code BEGIN ... COMMIT} block, and also the implicit transaction around each autocommit
 * statement.
 *
 * <p><b>The transaction id.</b> {@link #begin} returns the transaction's plaintext id. The
 * framework seals it on HTTP before it reaches the client, and opens it again on every later
 * call, so {@link #commit} / {@link #rollback} and every function of the transaction
 * ({@link farm.query.vgi.table.TableBindParams#transactionId()},
 * {@link farm.query.vgi.table.TableInitParams#transactionId()},
 * {@link farm.query.vgi.tableinout.TableInOutBindParams#transactionId()},
 * {@link farm.query.vgi.tableinout.TableInOutInitParams#transactionId()}) receive these same
 * bytes, on every transport. A worker can therefore stage work under the id in a function and
 * apply or discard it in the hooks.
 *
 * <p><b>Failing COMMIT.</b> An exception thrown by {@link #commit} fails the client's
 * {@code COMMIT} (or the autocommit statement) with the exception's message, and DuckDB
 * rolls the transaction back on its side. Do not rely on a {@link #rollback} call after a
 * failed commit: discard the staged work before throwing.
 *
 * <p>The hooks may run on any worker process (pooled launcher, subprocess transport, HTTP
 * replicas), not necessarily the one that ran {@code begin}: keep staged work where every
 * process can reach it (a database, a shared file, the worker's shared storage).
 */
public interface TransactionHooks {

    /**
     * Mint the transaction id. The default returns 16 random bytes.
     *
     * @param attach the attach the transaction runs on
     * @return the plaintext transaction id, or {@code null} for none (calls of the transaction
     *     then carry no id)
     */
    default byte[] begin(AttachContext attach) {
        byte[] id = new byte[16];
        DefaultTransactionIds.RNG.nextBytes(id);
        return id;
    }

    /**
     * Commit the transaction. Throw to fail the client's {@code COMMIT}.
     *
     * @param attach        the attach the transaction runs on
     * @param transactionId the id {@link #begin} returned
     */
    default void commit(AttachContext attach, byte[] transactionId) {}

    /**
     * Roll the transaction back.
     *
     * @param attach        the attach the transaction runs on
     * @param transactionId the id {@link #begin} returned
     */
    default void rollback(AttachContext attach, byte[] transactionId) {}
}

/** Source of the default transaction ids. */
final class DefaultTransactionIds {
    static final SecureRandom RNG = new SecureRandom();

    private DefaultTransactionIds() {}
}
