package com.ts.nebula.srm.processingtools;

import com.ts.nebula.srm.processingtools.impl.MarcProcessorImpl;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.ReadStream;
import io.vertx.core.streams.WriteStream;

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

public interface MarcProcessor extends Handler<Buffer>, ReadStream<Buffer> {

  static MarcProcessor newMarcParser(ReadStream<Buffer> stream) {
    return MarcProcessorImpl.newMarcParser(stream);
  }

  void processAsynchronously(WriteStream<Buffer> destination, Executor asyncExecutor, Handler<AsyncResult<Void>> handler);

  void startAsyncProcessing(WriteStream<Buffer> destination, Consumer<Callable<Integer>> asyncMarcReaderExecutor, Handler<AsyncResult<Void>> handler);

  /**
   * This method is called to provide the parser with data.
   *
   * @param buffer a chunk of data
   */
  void handle(Buffer buffer);

  @Override
  MarcProcessor exceptionHandler(Handler<Throwable> handler);

  @Override
  MarcProcessor handler(Handler<Buffer> handler);

  @Override
  MarcProcessor pause();

  @Override
  MarcProcessor fetch(long amount);

  @Override
  MarcProcessor resume();

  @Override
  MarcProcessor endHandler(Handler<Void> endHandler);

}
