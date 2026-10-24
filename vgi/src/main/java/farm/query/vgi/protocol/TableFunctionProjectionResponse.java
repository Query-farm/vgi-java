// Copyright 2025, 2026 Query Farm LLC - https://query.farm
package farm.query.vgi.protocol;
import farm.query.vgirpc.schema.*;
import java.util.List;
/** Empty indices plus NULL bind result is an ordinary decline. */
public record TableFunctionProjectionResponse(List<Long> accepted_indices,
                                              @Nullable byte[] bind_result)
    implements ArrowSerializableRecord {
  public static TableFunctionProjectionResponse decline() {
    return new TableFunctionProjectionResponse(List.of(), null);
  }
}
