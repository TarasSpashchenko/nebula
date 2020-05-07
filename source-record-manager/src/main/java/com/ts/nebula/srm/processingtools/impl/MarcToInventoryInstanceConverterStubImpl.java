package com.ts.nebula.srm.processingtools.impl;

import com.ts.nebula.srm.processingtools.MarcToInventoryInstanceConverter;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import org.apache.commons.io.IOUtils;
import org.marc4j.marc.Record;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

public class MarcToInventoryInstanceConverterStubImpl implements MarcToInventoryInstanceConverter {
  @Override
  public JsonObject convert(Record sourceRawRecord) {
    try (InputStream stubInstanceInputStream = getClass().getResourceAsStream("/stubInstance.json")) {
      ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      IOUtils.copy(stubInstanceInputStream, outputStream);
      return new JsonObject(Buffer.buffer(outputStream.toByteArray()));
    } catch (Exception e) {
      e.printStackTrace();
      return new JsonObject();
    }
  }
}
