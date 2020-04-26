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
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executor;

public class MarcProcessorImpl implements MarcProcessor {

  private volatile long demand = Long.MAX_VALUE;
  private final ReadStream<Buffer> stream;

  private volatile boolean processing;
  private volatile boolean streamEnded;

  private Handler<Buffer> eventHandler;
  private Handler<Void> endHandler;
  private Handler<Throwable> exceptionHandler;
  private Handler<Void> resumeHandler;

  private final VertxBufferInputStream bufferInputStream;
  private final MarcReader marcReader;

  private volatile boolean terminated;
  private volatile boolean terminationOnErrorRequested;
  private volatile boolean terminationOnErrorCompleted;

  private volatile Throwable terminationOnErrorCause;

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

  @Override
  public void processAsynchronously(WriteStream<Buffer> destination, Executor asyncExecutor, Handler<AsyncResult<Void>> handler) {
    pause();
    resumeHandler = (event) -> doProcessAsynchronously(asyncExecutor);
    this.pipeTo(destination, handler);
//    doProcessAsynchronously(asyncExecutor);
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
        log.debug("Source stream is paused in handle(Buffer buffer).");
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
    log.debug("MarcProcessor.pause()");
    demand = 0L;
    processStream();
    return this;
  }

  @Override
  public MarcProcessor fetch(long amount) {
    if (terminated || terminationOnErrorRequested) {
      log.warn("MarcProcessor.fetch(long amount) - MarcProcessor is already terminated...");
      return this;
    }

    log.debug("MarcProcessor.fetch(long amount)");
    demand = amount;
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
    if (terminated || terminationOnErrorRequested) {
      log.warn("MarcProcessor.resume() - MarcProcessor is already terminated...");
      return this;
    }

    boolean pausedNow = demand <= 0;
    if (log.isDebugEnabled() && pausedNow) {
      log.debug("MarcProcessor.resume()");
    }
    return pausedNow ? fetch(Long.MAX_VALUE) : this;
  }

  @Override
  public MarcProcessor endHandler(Handler<Void> endHandler) {
    this.endHandler = endHandler;
    return this;
  }

  @Override
  public synchronized void terminateOnError(Throwable terminatedCause) {
    log.warn("MarcProcessor.terminateOnError(Throwable terminatedCause): " + terminatedCause);
    terminatedCause.printStackTrace();
    if (!terminationOnErrorRequested) {
      this.terminationOnErrorCause = terminatedCause;
      terminationOnErrorCompleted = false;
      terminationOnErrorRequested = true;

      //We don't need to process stream so Just push resumeHandler to doTerminate()!
      Handler<Void> resumeHandler = this.resumeHandler;
      if (resumeHandler != null) {
        resumeHandler.handle(null);
      }
    }
  }

  @Override
  public boolean isTerminatedOnError() {
    return terminationOnErrorRequested;
  }

  @Override
  public Throwable getTerminationOnErrorCause() {
    return terminationOnErrorCause;
  }

  private void endStream() {
    bufferInputStream.end();
  }

  private void end() {
    log.debug("MarcProcessor.end() - starting...");
    try {
      Handler<Void> handler = endHandler;
      if (handler != null) {
        handler.handle(null);
      }
    } finally {
      bufferInputStream.close();
      terminated = true;
      log.debug("MarcProcessor.end() - completed.");
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
            log.debug("Source stream is resumed.");
          }
        } else {
          endStream();
        }
      } else {
        ReadStream<Buffer> s = stream;
        if (s != null) {
          s.pause();
          log.debug("Source stream is paused in processStream().");
        }
      }

    } finally {
      processing = false;
    }
  }

  private synchronized void doTerminate() {
    if (!terminationOnErrorCompleted) {
      log.debug("MarcProcessor.doTerminate() - starting...");
      pause();

      Handler<Throwable> exHandler = this.exceptionHandler;
      if (exHandler != null) {
        exHandler.handle(terminationOnErrorCause);
      }

      end();
      terminationOnErrorCompleted = true;
      log.debug("MarcProcessor.doTerminate() - completed.");
    }
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
      if (terminated) {
        log.warn("MarcProcessor is already terminated...");
      } else if (terminationOnErrorRequested) {
        doTerminate();
      } else if (marcReader.hasNext()) {
        eventHandler.handle(wrapIntoBuffer(marcReader.next()));

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

}
