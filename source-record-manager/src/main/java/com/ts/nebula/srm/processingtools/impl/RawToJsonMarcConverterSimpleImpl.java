package com.ts.nebula.srm.processingtools.impl;

import com.ts.nebula.srm.processingtools.RawToJsonMarcConverter;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import org.marc4j.MarcJsonWriter;
import org.marc4j.MarcReader;
import org.marc4j.MarcStreamReader;
import org.marc4j.marc.Record;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;


public class RawToJsonMarcConverterSimpleImpl implements RawToJsonMarcConverter {

  private static final RawToJsonMarcConverter isntance = new RawToJsonMarcConverterSimpleImpl();

  public static RawToJsonMarcConverter getInstance() {
    return isntance;
  }

  @Override
  public JsonObject convert(Buffer source) {
    ByteArrayInputStream inputStream = new ByteArrayInputStream(source.getBytes());
    MarcReader marcReader = new MarcStreamReader(inputStream, StandardCharsets.UTF_8.name());
    if (marcReader.hasNext()) {
      Record next = marcReader.next();

      try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
        MarcJsonWriter writer = new MarcJsonWriter(os);
        writer.write(next);
        return new JsonObject(Buffer.buffer(os.toByteArray()));
      } catch (IOException e) {
        e.printStackTrace();
        return new JsonObject(); //Ugly but just to suppress
      }
    } else {
      return new JsonObject();
    }
    //TODO: Error handling and all other boring stuff
  }
}
