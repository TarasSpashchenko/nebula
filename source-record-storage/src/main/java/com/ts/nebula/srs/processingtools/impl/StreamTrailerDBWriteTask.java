package com.ts.nebula.srs.processingtools.impl;

import com.ts.nebula.srs.processingtools.DBWriteTask;
import io.vertx.core.json.JsonObject;

public class StreamTrailerDBWriteTask implements DBWriteTask {

  private final JsonObject content;

  public StreamTrailerDBWriteTask(JsonObject content) {
    this.content = content;
  }

  public JsonObject getContent() {
    return content;
  }
}
