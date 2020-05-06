package com.ts.nebula.srs.processingtools.impl;

import com.ts.nebula.srs.processingtools.DBWriteTask;
import io.vertx.codegen.annotations.Nullable;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.streams.WriteStream;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class DummyMarcRecordsBatchDBWriteStreamImpl implements WriteStream<DBWriteTask> {
  private final Vertx vertx;
  private final AtomicInteger activeWritesCounter = new AtomicInteger();
  private Handler<Void> drainHandler;
  private Handler<AsyncResult<Void>> endHandler;
  private volatile int writeQueueMaxSize = 10;//Integer.MAX_VALUE;

  private volatile boolean streamEnded = false;

  public DummyMarcRecordsBatchDBWriteStreamImpl(Vertx vertx) {
    this.vertx = vertx;
  }

  @Override
  public WriteStream<DBWriteTask> exceptionHandler(Handler<Throwable> handler) {
    return this;
  }

  @Override
  public WriteStream<DBWriteTask> write(DBWriteTask data) {
    return write(data, null);
  }

  @Override
  public WriteStream<DBWriteTask> write(DBWriteTask data, Handler<AsyncResult<Void>> handler) {
    vertx.executeBlocking(promise -> {
      //This is an async database interaction when a batch of MARC records is being saved into the database
      try {
        activeWritesCounter.incrementAndGet();
        Thread.sleep(new Random().nextInt(500) + 500);
        System.out.println("> DBBatch has been saved into the database! activeWritesCounter: " + activeWritesCounter.get());
      } catch (InterruptedException e) {
        e.printStackTrace();
        Thread.currentThread().interrupt();
      } finally {
        activeWritesCounter.decrementAndGet();
      }
      promise.complete();
    }, false, ar -> {
      if (handler != null) {
        handler.handle(ar.mapEmpty());
      }

      if ((endHandler != null) && (activeWritesCounter.get() == 0)) {
        System.out.println("> DummyMarcRecordsBatchDBWriteStreamImpl calling endHandler... activeWritesCounter: " + activeWritesCounter.get());
        endHandler.handle(Future.succeededFuture());
      }
      if (!streamEnded && activeWritesCounter.get() < writeQueueMaxSize) {
        System.out.println("> DummyMarcRecordsBatchDBWriteStreamImpl calling drainHandler... activeWritesCounter: " + activeWritesCounter.get());
        drainHandler.handle(null);
      }
    });
    return this;
  }

  @Override
  public void end() {
    streamEnded = true;
  }

  @Override
  public void end(Handler<AsyncResult<Void>> handler) {
    streamEnded = true;
    if (handler != null) {
      endHandler = handler;
    }
  }

  @Override
  public WriteStream<DBWriteTask> setWriteQueueMaxSize(int maxSize) {
    writeQueueMaxSize = maxSize;
    return this;
  }

  @Override
  public boolean writeQueueFull() {
    return activeWritesCounter.get() > writeQueueMaxSize;
  }

  @Override
  public WriteStream<DBWriteTask> drainHandler(@Nullable Handler<Void> handler) {
    drainHandler = handler;
    return this;
  }
}
