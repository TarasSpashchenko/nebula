package com.ts.nebula.srm.domain.model;

import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;
import org.marc4j.marc.Record;

@DataObject(generateConverter = true, inheritConverter = true, publicConverter = false)
public class TestData {
  private Record marcRawRecord;

  private JsonObject marcJsonRecord;

  public TestData() {
    super();
  }

  public TestData(JsonObject json) {
    super();
  }


  public Record getMarcRawRecord() {
    return marcRawRecord;
  }

  public void setMarcRawRecord(Record marcRawRecord) {
    this.marcRawRecord = marcRawRecord;
  }

  public JsonObject getMarcJsonRecord() {
    return marcJsonRecord;
  }

  public void setMarcJsonRecord(JsonObject marcJsonRecord) {
    this.marcJsonRecord = marcJsonRecord;
  }
}
