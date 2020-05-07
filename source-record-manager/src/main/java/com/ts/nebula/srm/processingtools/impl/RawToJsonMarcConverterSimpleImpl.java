package com.ts.nebula.srm.processingtools.impl;

import com.ts.nebula.srm.processingtools.RawToJsonMarcConverter;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import org.marc4j.MarcJsonWriter;
import org.marc4j.marc.Record;

import java.io.ByteArrayOutputStream;
import java.io.IOException;


public class RawToJsonMarcConverterSimpleImpl implements RawToJsonMarcConverter {

  private static final RawToJsonMarcConverter instance = new RawToJsonMarcConverterSimpleImpl();

  public static RawToJsonMarcConverter getInstance() {
    return instance;
  }

  @Override
  public JsonObject convert(Record sourceRawRecord) {
    try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
      MarcJsonWriter writer = new MarcJsonWriter(os);
      writer.write(sourceRawRecord);
      return new JsonObject(Buffer.buffer(os.toByteArray()));
    } catch (IOException e) {
      e.printStackTrace();
      return new JsonObject(); //Ugly but just to suppress
    }
  }
}
