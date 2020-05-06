package com.ts.nebula.srs.processingtools.impl;

import com.ts.nebula.srs.processingtools.DBWriteTask;
import io.vertx.core.json.JsonObject;

import java.util.List;

public class MarkBatchDBWriteTask implements DBWriteTask {
  private final List<JsonObject> marcRecords;

  public MarkBatchDBWriteTask(List<JsonObject> marcRecords) {
    this.marcRecords = marcRecords;
  }

  public List<JsonObject> getMarcRecords() {
    return marcRecords;
  }
}
