// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi;

import farm.query.vgi.catalog.ConstraintRecord;
import farm.query.vgi.protocol.CatalogCapabilities;
import farm.query.vgi.protocol.ClientCapabilities;
import farm.query.vgirpc.marshal.RecordCodec;
import farm.query.vgirpc.schema.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

/**
 * Encodes and negotiates the catalog profiles ({@code CatalogCapabilities}) of a
 * {@code catalog_attach}: what a catalog implements, what the client offered, and the
 * canonical selection the attach result carries.
 *
 * <p>The framework already negotiates the constraint read profiles for every catalog whose
 * tables carry {@link ConstraintRecord}s. Use this class to build an explicit decision
 * ({@link Worker#catalogCapabilities}, {@link Worker.ExtraCatalog#withCapabilities},
 * {@link CatalogInterface#catalogCapabilities}) — for example a catalog that accepts
 * {@code CREATE TABLE ... NOT NULL} selects {@link #CONSTRAINT_CREATE} and
 * {@link #CONSTRAINT_ENFORCE} — or, as a client, to build the offer an attach request carries
 * ({@link #clientOffer}).
 */
public final class CatalogProfiles {

    /** Read: canonical {@code ConstraintInfo} records. */
    public static final String CONSTRAINT_RECORDS_READ = ConstraintRecord.CONSTRAINT_RECORDS_READ;
    /** Read: {@code CHECK} records' DuckDB scalar expressions (needs {@link #CONSTRAINT_RECORDS_READ}). */
    public static final String SCALAR_EXPRESSION_READ = ConstraintRecord.SCALAR_EXPRESSION_READ;
    /** DDL: constraint definitions on {@code CREATE TABLE}. */
    public static final String CONSTRAINT_CREATE = "vgi.constraint.create.v1";
    /** DDL: {@code catalog_table_constraints_apply} (add/drop NOT NULL and friends). */
    public static final String CONSTRAINT_MUTATE = "vgi.constraint.mutate.v1";
    /** Write: the catalog enforces its constraint records on every write path. */
    public static final String CONSTRAINT_ENFORCE = "vgi.constraint.enforce.v1";
    /** Transaction: constraint DDL inside a client transaction. */
    public static final String TRANSACTION_DDL = "vgi.transaction.ddl.v1";

    private static final List<String> NEEDS_RECORDS = List.of(
            SCALAR_EXPRESSION_READ, CONSTRAINT_CREATE, CONSTRAINT_MUTATE, CONSTRAINT_ENFORCE);

    private CatalogProfiles() {}

    /**
     * An empty profile set.
     *
     * @return {@code CatalogCapabilities} with four empty lists
     */
    public static CatalogCapabilities none() {
        return new CatalogCapabilities(List.of(), List.of(), List.of(), List.of());
    }

    /**
     * Encodes a profile set as the IPC bytes {@code catalog_capabilities} /
     * {@code selected_capabilities} carry.
     *
     * @param capabilities the profiles
     * @return the IPC bytes
     */
    public static byte[] encode(CatalogCapabilities capabilities) {
        return RecordCodec.serializeToBytes(capabilities);
    }

    /**
     * Decodes {@code CatalogCapabilities} IPC bytes.
     *
     * @param bytes the IPC bytes, or null / empty
     * @return the profiles; {@link #none()} for null or empty bytes
     */
    public static CatalogCapabilities decode(@Nullable byte[] bytes) {
        return bytes == null || bytes.length == 0 ? none()
                : RecordCodec.deserializeFromBytes(bytes, CatalogCapabilities.class);
    }

    /**
     * The catalog profiles a client offered in its attach request.
     *
     * @param clientCapabilities the request's {@code ClientCapabilities} IPC bytes, or null
     * @return the offered profiles; {@link #none()} when the client sent none
     */
    public static CatalogCapabilities offered(@Nullable byte[] clientCapabilities) {
        if (clientCapabilities == null || clientCapabilities.length == 0) return none();
        ClientCapabilities client = RecordCodec.deserializeFromBytes(clientCapabilities, ClientCapabilities.class);
        return decode(client.catalog());
    }

    /**
     * The canonical selection: each category's profiles that are both implemented and offered,
     * sorted, dropping any whose prerequisite was not selected (every constraint profile
     * needs {@link #CONSTRAINT_RECORDS_READ}).
     *
     * @param available          the profiles the catalog implements
     * @param clientCapabilities the request's {@code ClientCapabilities} IPC bytes, or null
     * @return the selection
     */
    public static CatalogCapabilities select(CatalogCapabilities available, @Nullable byte[] clientCapabilities) {
        CatalogCapabilities offer = offered(clientCapabilities);
        var read = intersect(available.read(), offer.read());
        var ddl = intersect(available.ddl(), offer.ddl());
        var write = intersect(available.write(), offer.write());
        var transaction = intersect(available.transaction(), offer.transaction());
        if (!read.contains(CONSTRAINT_RECORDS_READ)) {
            read.removeAll(NEEDS_RECORDS);
            ddl.removeAll(NEEDS_RECORDS);
            write.removeAll(NEEDS_RECORDS);
        }
        return new CatalogCapabilities(List.copyOf(read), List.copyOf(ddl), List.copyOf(write),
                List.copyOf(transaction));
    }

    /**
     * Advertises {@code available} and selects from it against the client's offer: the value a
     * capability hook returns.
     *
     * @param available          the profiles the catalog implements
     * @param clientCapabilities the request's {@code ClientCapabilities} IPC bytes, or null
     * @return the advertisement and selection
     */
    public static Worker.CatalogCapabilitySelection negotiate(CatalogCapabilities available,
                                                              @Nullable byte[] clientCapabilities) {
        return new Worker.CatalogCapabilitySelection(encode(available),
                encode(select(available, clientCapabilities)));
    }

    /**
     * The union of two profile sets, sorted per category.
     *
     * @param a one set
     * @param b the other
     * @return the union
     */
    public static CatalogCapabilities union(CatalogCapabilities a, CatalogCapabilities b) {
        return new CatalogCapabilities(merged(a.read(), b.read()), merged(a.ddl(), b.ddl()),
                merged(a.write(), b.write()), merged(a.transaction(), b.transaction()));
    }

    /**
     * Whether a profile set names {@code profile} in any category.
     *
     * @param capabilities the profiles
     * @param profile      the profile name
     * @return true when present
     */
    public static boolean contains(CatalogCapabilities capabilities, String profile) {
        return capabilities.read().contains(profile) || capabilities.ddl().contains(profile)
                || capabilities.write().contains(profile) || capabilities.transaction().contains(profile);
    }

    /**
     * A {@code ClientCapabilities} offer for an attach request.
     *
     * @param engine the client engine name
     * @param offer  the catalog profiles offered
     * @return the {@code ClientCapabilities} IPC bytes
     */
    public static byte[] clientOffer(String engine, CatalogCapabilities offer) {
        return RecordCodec.serializeToBytes(new ClientCapabilities(engine, List.of(), List.of(), true,
                List.of(), encode(offer)));
    }

    /**
     * The default offer of a client that reads constraint records (and their CHECK
     * expressions) but performs no constraint DDL: what a reader such as a catalog browser
     * should send so constrained tables are served to it.
     *
     * @return the {@code ClientCapabilities} IPC bytes
     */
    public static byte[] defaultClientOffer() {
        return clientOffer("vgi-java", new CatalogCapabilities(
                List.of(CONSTRAINT_RECORDS_READ, SCALAR_EXPRESSION_READ), List.of(), List.of(), List.of()));
    }

    private static TreeSet<String> intersect(List<String> available, List<String> offered) {
        var out = new TreeSet<String>();
        for (String p : available) if (offered.contains(p)) out.add(p);
        return out;
    }

    private static List<String> merged(List<String> a, List<String> b) {
        var out = new TreeSet<>(a);
        out.addAll(b);
        return List.copyOf(new ArrayList<>(out));
    }
}
