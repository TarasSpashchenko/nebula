package com.ts.nebula.srs.processingtools.impl;

import com.ts.nebula.srs.processingtools.DBWriteTask;
import io.vertx.codegen.annotations.Nullable;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.core.streams.WriteStream;

import java.util.ArrayList;
import java.util.List;

public class MarcRecordsDBWriteStream implements WriteStream<JsonObject> {

  private final WriteStream<DBWriteTask> marcRecordsBatchDBWriteStream;

  private Handler<Throwable> exceptionHandler;
  private Handler<Void> drainHandler;

  private int writeBatchSize = 100;

  //even volatile is not needed here because each instance of this class is supposed to be run
  //in the scope of a particular verticle so it will be processed by the same event-loop thread
  private /*volatile*/ List<JsonObject> nextMarcBatch = new ArrayList<>(writeBatchSize);
  private /*volatile*/ List<Handler<AsyncResult<Void>>> nextMarcBatchHandlers = new ArrayList<>(writeBatchSize);

  private /*volatile*/ boolean streamEnded = false;

  public MarcRecordsDBWriteStream(WriteStream<DBWriteTask> marcRecordsBatchDBWriteStream) {
    this.marcRecordsBatchDBWriteStream = marcRecordsBatchDBWriteStream;
    init();
  }

  private void init() {
    //TODO
    marcRecordsBatchDBWriteStream
      .exceptionHandler(event -> {
        Handler<Throwable> handler = this.exceptionHandler;
        if (handler != null) {
          handler.handle(event);
        }
      })
      .drainHandler(event -> {
        Handler<Void> handler = this.drainHandler;
        if (handler != null) {
          handler.handle(event);

        }
      });
  }

  @Override
  public WriteStream<JsonObject> exceptionHandler(Handler<Throwable> handler) {
    exceptionHandler = handler;
    return this;
  }

  @Override
  public WriteStream<JsonObject> write(JsonObject data) {
    return write(data, null);
  }

  @Override
  public WriteStream<JsonObject> write(JsonObject data, Handler<AsyncResult<Void>> handler) {
    if (streamEnded) {
      return this;
    }

    DBWriteTaskHolder dbWriteTaskHolder = null;
    if (data.getBoolean("streamHeader", false)) {
      dbWriteTaskHolder = new DBWriteTaskHolder(streamHeaderToDBWriteTask(data), handler);
    } else if (data.getBoolean("streamTrailer", false)) {
      dbWriteTaskHolder = new DBWriteTaskHolder(streamTrailerToDBWriteTask(data), handler);
    } else {
      List<JsonObject> localNextMarcBatch = null;
      List<Handler<AsyncResult<Void>>> localNextMarcBatchHandlers = null;

//      synchronized (this) {
      nextMarcBatch.add(data);
      if (handler != null) {
        nextMarcBatchHandlers.add(handler);
      }
      if (nextMarcBatch.size() >= writeBatchSize) {
        localNextMarcBatch = nextMarcBatch;
        nextMarcBatch = new ArrayList<>(writeBatchSize);
        localNextMarcBatchHandlers = nextMarcBatchHandlers;
        nextMarcBatchHandlers = new ArrayList<>(writeBatchSize);
      }
//      }
      if (localNextMarcBatch != null) {
        final List<Handler<AsyncResult<Void>>> finalNextMarcBatchHandlers = localNextMarcBatchHandlers;
        Handler<AsyncResult<Void>> batchHandler =
          event -> finalNextMarcBatchHandlers.forEach(asyncResultHandler -> asyncResultHandler.handle(event));

        dbWriteTaskHolder = new DBWriteTaskHolder(marcBatchToDBWriteTask(localNextMarcBatch), batchHandler);
      }
    }

    if (dbWriteTaskHolder != null) {
      marcRecordsBatchDBWriteStream.write(dbWriteTaskHolder.dbWriteTask, dbWriteTaskHolder.handler);
    }

    return this;
  }

  @Override
  public void end() {
    end((Handler<AsyncResult<Void>>) null);
  }

  @Override
  public void end(Handler<AsyncResult<Void>> handler) {
    streamEnded = true;

    //Just push the last batch
    if (nextMarcBatch.size() > 0) {
      final List<Handler<AsyncResult<Void>>> finalNextMarcBatchHandlers = nextMarcBatchHandlers;
      Handler<AsyncResult<Void>> batchHandler =
        event -> finalNextMarcBatchHandlers.forEach(asyncResultHandler -> asyncResultHandler.handle(event));
      marcRecordsBatchDBWriteStream.write(marcBatchToDBWriteTask(nextMarcBatch), batchHandler);
    }

    marcRecordsBatchDBWriteStream.end(handler);
  }

  @Override
  public WriteStream<JsonObject> setWriteQueueMaxSize(int maxSize) {
    marcRecordsBatchDBWriteStream.setWriteQueueMaxSize(maxSize);
    return this;
  }

  @Override
  public boolean writeQueueFull() {
    return marcRecordsBatchDBWriteStream.writeQueueFull();
  }

  @Override
  public WriteStream<JsonObject> drainHandler(@Nullable Handler<Void> handler) {
    drainHandler = handler;
    return this;
  }

  public int getWriteBatchSize() {
    return writeBatchSize;
  }

  public void setWriteBatchSize(int writeBatchSize) {
    this.writeBatchSize = writeBatchSize;
  }

  private DBWriteTask streamHeaderToDBWriteTask(JsonObject streamHeader) {
    return new StreamHeaderDBWriteTask(streamHeader);
  }

  private DBWriteTask streamTrailerToDBWriteTask(JsonObject streamHeader) {
    return new StreamTrailerDBWriteTask(streamHeader);
  }

  private DBWriteTask marcBatchToDBWriteTask(List<JsonObject> nextMarcBatch) {
    return new MarkBatchDBWriteTask(nextMarcBatch);
  }

  private static class DBWriteTaskHolder {
    private final DBWriteTask dbWriteTask;
    private final Handler<AsyncResult<Void>> handler;

    public DBWriteTaskHolder(DBWriteTask dbWriteTask, Handler<AsyncResult<Void>> handler) {
      this.dbWriteTask = dbWriteTask;
      this.handler = handler;
    }
  }
}
