// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;

import java.util.Map;

/**
 * A companion catalog the client should ATTACH when this VGI catalog attaches
 * (lakehouse federation). Serialised as an item in
 * {@code CatalogAttachResult.attach_catalogs}; mirrors vgi-python's
 * {@code AttachCatalogInfo}. The C++ VGI extension attaches each entry at
 * VGI-attach time so multi-branch catalog-table branches can resolve tables in
 * a companion DuckLake / Iceberg / Postgres / DuckDB.
 *
 * <p>Field order, types and nullability are part of the wire contract; the
 * Arrow schema is derived reflectively from the record components.
 *
 * @param alias      ATTACH alias; also a catalog-table {@code ScanBranch}'s
 *     {@code sourceCatalog}. Namespace it by your catalog identity so two
 *     workers don't both claim the same name
 * @param target     ATTACH target — a path or DSN (e.g.
 *     {@code "ducklake:sqlite:/data/meta.sqlite"})
 * @param db_type    DuckDB db type; empty => inferred from the target scheme prefix
 * @param options    extra ATTACH options forwarded verbatim (e.g. DuckLake {@code DATA_PATH})
 * @param hidden     attach hidden (excluded from {@code duckdb_databases()}); still resolvable
 * @param required   when {@code true}, attach failure fails the whole VGI ATTACH
 * @param secret_ref optional named secret to inject into the companion's ATTACH
 *     options (opt-in on the client via {@code attach_companion_secrets})
 */
public record AttachCatalogInfo(
        String alias,
        String target,
        String db_type,
        Map<String, String> options,
        boolean hidden,
        boolean required,
        String secret_ref) implements ArrowSerializableRecord {}
