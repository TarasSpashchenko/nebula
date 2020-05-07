package com.ts.nebula.srm.processingtools;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.ReadStream;
import io.vertx.core.streams.WriteStream;
import org.marc4j.marc.Record;

public interface MarcStreamParser extends Handler<Buffer>, ReadStream<Record> {

  void processAsynchronously(WriteStream<Record> destination, Handler<AsyncResult<Void>> completionHandler);

  /**
   * This method is called to provide the parser with data.
   *
   * @param buffer a chunk of data
   */
  @Override
  void handle(Buffer buffer);

  @Override
  MarcStreamParser exceptionHandler(Handler<Throwable> handler);

  @Override
  MarcStreamParser handler(Handler<Record> handler);

  @Override
  MarcStreamParser pause();

  @Override
  MarcStreamParser fetch(long amount);

  @Override
  MarcStreamParser resume();

  @Override
  MarcStreamParser endHandler(Handler<Void> endHandler);

  void terminateOnError(Throwable terminatedCause);

  boolean isTerminatedOnError();

  Throwable getTerminationOnErrorCause();

  boolean isTerminated();
}
