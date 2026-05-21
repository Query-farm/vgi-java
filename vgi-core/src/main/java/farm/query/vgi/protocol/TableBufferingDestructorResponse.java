// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;

/** Empty ack for {@code table_buffering_destructor}. */
public record TableBufferingDestructorResponse() implements ArrowSerializableRecord {}
