package com.ts.nebula.srm.processingtools.impl;

import com.ts.nebula.srm.processingtools.RawToJsonMarcConverter;
import io.vertx.codegen.annotations.Nullable;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.streams.WriteStream;
import org.marc4j.marc.Record;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

public class MarcRecordWriteStreamImpl implements WriteStream<Record> {
  // All stuff within an instance of this class can be stateful
  private final WriteStream<Buffer> srsWriteStream;
  private final WriteStream<Buffer> inventoryWriteStream;
  private final RawToJsonMarcConverter rawToJsonMarcConverter;

  private final Handler<Throwable> proxyExceptionHandler =
    event -> {
      Handler<Throwable> handler = this.exceptionHandler;
      if (handler != null) {
        handler.handle(event);
      }
    };
  private final Handler<Void> proxyDrainHandler =
    event -> {
      Handler<Void> handler = this.drainHandler;

      if (handler != null && !writeQueueFull()) {
        handler.handle(event);
      }
    };

  private Handler<Throwable> exceptionHandler;
  private Handler<Void> drainHandler;

  private final Map<WriteStream<?>, Boolean> writeQueueFullMap = new HashMap<>();

  public MarcRecordWriteStreamImpl(WriteStream<Buffer> srsWriteStream, WriteStream<Buffer> inventoryWriteStream) {
    this.srsWriteStream = srsWriteStream;
    this.inventoryWriteStream = inventoryWriteStream;
    rawToJsonMarcConverter = new RawToJsonMarcConverterSimpleImpl();
    init();
  }

  //TODO: init delegates
  private void init() {
    setupExceptionHandlers();
    setupDrainHandlers();
  }

  private void setupExceptionHandlers() {
    srsWriteStream.exceptionHandler(proxyExceptionHandler);
    inventoryWriteStream.exceptionHandler(proxyExceptionHandler);
  }

  private void setupDrainHandlers() {
    srsWriteStream.drainHandler(event -> {
      writeQueueFullMap.put(srsWriteStream, Boolean.FALSE);
      proxyDrainHandler.handle(event);
    });
    inventoryWriteStream.drainHandler(event -> {
      writeQueueFullMap.put(inventoryWriteStream, Boolean.FALSE);
      proxyDrainHandler.handle(event);
    });

  }

  @Override
  public WriteStream<Record> exceptionHandler(Handler<Throwable> handler) {
    exceptionHandler = handler;
    return this;
  }

  @Override
  public WriteStream<Record> write(Record data) {
    return write(data, null);
  }

  private void doWrite(WriteStream<Buffer> delegateWriteStream, Supplier<JsonObject> payloadSupplier) {
    delegateWriteStream.write(payloadSupplier.get().toBuffer());
    boolean queueFull = delegateWriteStream.writeQueueFull();
    writeQueueFullMap.put(delegateWriteStream, queueFull);
//    if (!queueFull) {
//      proxyDrainHandler.handle(null);
//    }
  }

  @Override
  public WriteStream<Record> write(Record data, Handler<AsyncResult<Void>> handler) {
    doWrite(srsWriteStream, () -> rawToJsonMarcConverter.convert(data));
    doWrite(inventoryWriteStream, () -> new MarcToInventoryInstanceConverterStubImpl().convert(data));

    return this;
  }

  @Override
  public void end() {
    end((Handler<AsyncResult<Void>>) null);
  }

  @Override
  public void end(Handler<AsyncResult<Void>> handler) {
    if (handler != null) {
      handler.handle(Future.succeededFuture());
    }
  }

  @Override
  public WriteStream<Record> setWriteQueueMaxSize(int maxSize) {
    srsWriteStream.setWriteQueueMaxSize(maxSize);
    inventoryWriteStream.setWriteQueueMaxSize(maxSize);
    return this;
  }

  @Override
  public boolean writeQueueFull() {
    return writeQueueFullMap.values().stream().filter(Boolean::booleanValue).findFirst().orElse(Boolean.FALSE);
  }

  @Override
  public WriteStream<Record> drainHandler(@Nullable Handler<Void> handler) {
    drainHandler = handler;
    return this;
  }
}
