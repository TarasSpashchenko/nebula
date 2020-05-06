package com.ts.nebula.srs.processingtools.impl;

import io.vertx.codegen.annotations.Nullable;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.core.parsetools.JsonEvent;
import io.vertx.core.streams.WriteStream;

public class MarcRecordsWriteStreamWrapper implements WriteStream<JsonEvent> {
  private final WriteStream<JsonObject> delegate;

  public MarcRecordsWriteStreamWrapper(WriteStream<JsonObject> delegate) {
    this.delegate = delegate;
  }

  @Override
  public WriteStream<JsonEvent> exceptionHandler(Handler<Throwable> handler) {
    delegate.exceptionHandler(handler);
    return this;
  }

  @Override
  public WriteStream<JsonEvent> write(JsonEvent data) {
    delegate.write(data.objectValue());
    return this;
  }

  @Override
  public WriteStream<JsonEvent> write(JsonEvent data, Handler<AsyncResult<Void>> handler) {
    delegate.write(data.objectValue(), handler);
    return this;
  }

  @Override
  public void end() {
    delegate.end();
  }

  @Override
  public void end(Handler<AsyncResult<Void>> handler) {
    delegate.end(handler);
  }

  @Override
  public WriteStream<JsonEvent> setWriteQueueMaxSize(int maxSize) {
    delegate.setWriteQueueMaxSize(maxSize);
    return this;
  }

  @Override
  public boolean writeQueueFull() {
    return delegate.writeQueueFull();
  }

  @Override
  public WriteStream<JsonEvent> drainHandler(@Nullable Handler<Void> handler) {
    delegate.drainHandler(handler);
    return this;
  }
}
