// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;

/** Empty acknowledgement for the {@code aggregate_combine} request. */
public record AggregateCombineResponse() implements ArrowSerializableRecord {}
