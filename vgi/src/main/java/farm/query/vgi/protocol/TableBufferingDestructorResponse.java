// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;

/** Empty ack for {@code table_buffering_destructor}. */
public record TableBufferingDestructorResponse() implements ArrowSerializableRecord {}
