package com.ts.nebula.srm.processingtools;

import io.vertx.core.json.JsonObject;
import org.marc4j.marc.Record;

public interface MarcToInventoryInstanceConverter {
  JsonObject convert(Record sourceRawRecord);
}
