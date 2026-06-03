// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;

/** Empty ack for {@code aggregate_update}. */
public record AggregateUpdateResponse() implements ArrowSerializableRecord {}
