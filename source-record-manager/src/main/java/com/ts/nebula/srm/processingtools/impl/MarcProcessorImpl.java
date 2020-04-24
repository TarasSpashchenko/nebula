package com.ts.nebula.srm.processingtools.impl;

import com.ts.nebula.srm.processingtools.MarcProcessor;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.impl.Arguments;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.streams.ReadStream;
import io.vertx.core.streams.WriteStream;
import org.marc4j.MarcPermissiveStreamReader;
import org.marc4j.MarcReader;
import org.marc4j.MarcStreamWriter;
import org.marc4j.marc.Record;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

public class MarcProcessorImpl implements MarcProcessor {

  private long demand = Long.MAX_VALUE;
  private final ReadStream<Buffer> stream;

  private boolean processing;
  private boolean streamEnded;

  private Handler<Buffer> eventHandler;
  private Handler<Void> endHandler;
  private Handler<Throwable> exceptionHandler;
  private Handler<Void> resumeHandler;

  private final VertxBufferInputStream bufferInputStream;
  private final MarcReader marcReader;

  private static final Logger log = LoggerFactory.getLogger(MarcProcessorImpl.class);

  public static MarcProcessor newMarcParser(ReadStream<Buffer> stream) {
    return new MarcProcessorImpl(stream);
  }

  private MarcProcessorImpl(ReadStream<Buffer> stream) {
    super();
    this.stream = stream;
    bufferInputStream = new VertxBufferInputStream();
    marcReader = new MarcPermissiveStreamReader(bufferInputStream, true, true, "BESTGUESS");
  }

  private Buffer wrapIntoBuffer(Record rawRecord) {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    MarcStreamWriter streamWriter = new MarcStreamWriter(bos, StandardCharsets.UTF_8.name());
    streamWriter.write(rawRecord);
    streamWriter.close();
    return Buffer.buffer(bos.toByteArray());
  }

  private void doProcessAsynchronously(Executor asyncExecutor) {
    asyncExecutor.execute(() -> {
      if (marcReader.hasNext()) {
        eventHandler.handle(wrapIntoBuffer(marcReader.next()));
        log.debug("message sent...");

        int remainingBuffersCapacity = bufferInputStream.remainingBuffersCapacity();
        //TODO: get rid of magic numbers
        if (remainingBuffersCapacity > 40 && demand > 0) {
          ReadStream<Buffer> s = stream;
          if (s != null) {
            s.resume();
          }
        }
        if (demand > 0) {
          doProcessAsynchronously(asyncExecutor);
        }
      } else {
        end();
      }
    });
  }

  @Override
  public void processAsynchronously(WriteStream<Buffer> destination, Executor asyncExecutor, Handler<AsyncResult<Void>> handler) {
    pause();
    resumeHandler = (event) -> doProcessAsynchronously(asyncExecutor);
    this.pipeTo(destination, handler);
//    doProcessAsynchronously(asyncExecutor);
  }


  @Override
  public void startAsyncProcessing(WriteStream<Buffer> destination, Consumer<Callable<Integer>> asyncMarcReaderExecutor, Handler<AsyncResult<Void>> handler) {
    throw new RuntimeException("Method is deprecated!");
//    asyncMarcReaderExecutor.accept(() -> {
//      this.pipeTo(destination, handler);
//
//      int count = 0;
//      try {
//        while (marcReader.hasNext()) {
//          eventHandler.handle(wrapIntoBuffer(marcReader.next()));
//          count++;
//        }
//      } finally {
//        end();
//      }
//      return count;
//    });
  }

  @Override
  public void handle(Buffer buffer) {
    bufferInputStream.populate(buffer);
    processStream();

    int remainingBuffersCapacity = bufferInputStream.remainingBuffersCapacity();
    //TODO: get rid of magic numbers
    if (remainingBuffersCapacity < 10) {
      ReadStream<Buffer> s = stream;
      if (s != null) {
        s.pause();
      }
    }
  }

  @Override
  public MarcProcessor exceptionHandler(Handler<Throwable> handler) {
    exceptionHandler = handler;
    return this;
  }

  @Override
  public MarcProcessor handler(Handler<Buffer> handler) {
    eventHandler = handler;
    if (stream != null) {
      if (handler != null) {
        stream.endHandler(v -> {
          streamEnded = true;
          processStream();
        });
        stream.exceptionHandler(err -> {
          if (exceptionHandler != null) {
            exceptionHandler.handle(err);
          }
        });
        stream.handler(this);
      } else {
        stream.handler(null);
        stream.endHandler(null);
        stream.exceptionHandler(null);
      }
    }
    return this;
  }

  @Override
  public MarcProcessor pause() {
    demand = 0L;
    processStream();
    return this;
  }

  @Override
  public MarcProcessor fetch(long amount) {
    Arguments.require(amount > 0, "Fetch amount must be > 0");
    demand += amount;
    if (demand < 0L) {
      demand = Long.MAX_VALUE;
    }
    processStream();
    Handler<Void> resumeHandler = this.resumeHandler;
    if (resumeHandler != null) {
      resumeHandler.handle(null);
    }
    return this;
  }

  @Override
  public MarcProcessor resume() {
    return fetch(Long.MAX_VALUE);
  }

  @Override
  public MarcProcessor endHandler(Handler<Void> endHandler) {
    this.endHandler = endHandler;
    return this;
  }

  private void endStream() {
    bufferInputStream.end();
  }

  private void end() {
    try {
      Handler<Void> handler = endHandler;
      if (handler != null) {
        handler.handle(null);
      }
    } finally {
      bufferInputStream.close();
    }
  }

  private void processStream() {
    if (processing) {
      return;
    }
    processing = true;
    try {
      if (demand > 0) {
        if (!streamEnded) {
          ReadStream<Buffer> s = stream;
          if (s != null) {
            s.resume();
          }
        } else {
          endStream();
        }
      } else {
        ReadStream<Buffer> s = stream;
        if (s != null) {
          s.pause();
        }
      }

    } finally {
      processing = false;
    }
  }
}
