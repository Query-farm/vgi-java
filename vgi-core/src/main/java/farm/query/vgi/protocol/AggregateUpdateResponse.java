// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;

/** Empty ack for {@code aggregate_update}. */
public record AggregateUpdateResponse() implements ArrowSerializableRecord {}
