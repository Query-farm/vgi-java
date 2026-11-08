// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi;

import farm.query.vgi.protocol.CatalogAttachRequest;

/**
 * Called on every {@code ATTACH} of the worker's main catalog, after required options are
 * checked and before the attach result is returned; registered with
 * {@link Worker#onAttach(AttachHook)}.
 *
 * <p>The hook sees every option value, secret ones included, and decides the attach's
 * per-attach settings, for example {@code supports_transactions} from an option:
 *
 * <pre>{@code
 * worker.onAttach((attach, request) ->
 *         AttachSettings.transactional(attach.options().getBoolean("transactional", false)));
 * }</pre>
 *
 * <p>Throwing fails the {@code ATTACH} with the exception's message. Keep no per-attach
 * state in process memory: later calls of the attach may reach another worker process
 * (pooled launcher, subprocess transport, HTTP replicas). Everything a function or hook
 * needs later is in its {@link AttachOptions}.
 */
@FunctionalInterface
public interface AttachHook {

    /**
     * Decide the settings of one attach.
     *
     * @param attach  the attach being created: catalog name, plain attach id, option values
     * @param request the raw attach request
     * @return the settings, or {@code null} for {@link AttachSettings#defaults()}
     */
    AttachSettings onAttach(AttachContext attach, CatalogAttachRequest request);
}
