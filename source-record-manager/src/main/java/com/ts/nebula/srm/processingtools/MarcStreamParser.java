package com.ts.nebula.srm.processingtools;

import com.ts.nebula.srm.processingtools.impl.MarcStreamParserImpl;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.ReadStream;
import io.vertx.core.streams.WriteStream;

import java.util.concurrent.Executor;

public interface MarcStreamParser extends Handler<Buffer>, ReadStream<Buffer> {

  static MarcStreamParser newMarcParser(ReadStream<Buffer> stream, Executor asyncExecutor) {
    return MarcStreamParserImpl.newMarcParser(stream, asyncExecutor);
  }

  void processAsynchronously(WriteStream<Buffer> destination, Handler<AsyncResult<Void>> handler);

  /**
   * This method is called to provide the parser with data.
   *
   * @param buffer a chunk of data
   */
  void handle(Buffer buffer);

  @Override
  MarcStreamParser exceptionHandler(Handler<Throwable> handler);

  @Override
  MarcStreamParser handler(Handler<Buffer> handler);

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
}
