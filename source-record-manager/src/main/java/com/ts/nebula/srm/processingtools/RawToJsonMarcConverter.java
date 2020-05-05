package com.ts.nebula.srm.processingtools;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;


public interface RawToJsonMarcConverter {
  JsonObject convert(Buffer source);
}
