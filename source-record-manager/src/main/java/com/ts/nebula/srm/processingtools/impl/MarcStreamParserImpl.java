package com.ts.nebula.srm.processingtools.impl;

import com.ts.nebula.srm.processingtools.MarcStreamParser;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
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

public class MarcStreamParserImpl implements MarcStreamParser {

  private final ReadStream<Buffer> stream;
  private final Executor asyncExecutor;

  private volatile boolean demand = false;
  private volatile boolean streamEnded;

  private Handler<Buffer> eventHandler;
  private Handler<Void> endHandler;
  private Handler<Throwable> exceptionHandler;

  private final VertxBufferInputStream bufferInputStream;
  private final MarcReader marcReader;

  private volatile boolean terminated;
  private volatile boolean terminationOnErrorRequested;
  private volatile boolean terminationOnErrorCompleted;

  private volatile Throwable terminationOnErrorCause;

  private static final Logger log = LoggerFactory.getLogger(MarcStreamParserImpl.class);

  public static MarcStreamParser newMarcParser(ReadStream<Buffer> stream, Executor asyncExecutor) {
    return new MarcStreamParserImpl(stream, asyncExecutor);
  }

  private MarcStreamParserImpl(ReadStream<Buffer> stream, Executor asyncExecutor) {
    super();
    this.stream = stream;
    this.asyncExecutor = asyncExecutor;
    bufferInputStream = new VertxBufferInputStream();
    marcReader = new MarcPermissiveStreamReader(bufferInputStream, true, true, "BESTGUESS");
  }

  @Override
  public void processAsynchronously(WriteStream<Buffer> destination, Handler<AsyncResult<Void>> handler) {
    pause();
    this.pipeTo(destination, handler);
  }

  @Override
  public void handle(Buffer buffer) {
    bufferInputStream.populate(buffer);

    int remainingBuffersCapacity = bufferInputStream.remainingBuffersCapacity();
    //TODO: get rid of magic numbers
    if (remainingBuffersCapacity < 10) {
      ReadStream<Buffer> s = stream;
      if (s != null) {
        s.pause();
        log.debug("Source stream is paused in handle(Buffer buffer).");
      }
      doProcessAsynchronously();
    }
  }

  @Override
  public MarcStreamParser exceptionHandler(Handler<Throwable> handler) {
    exceptionHandler = handler;
    return this;
  }

  @Override
  public MarcStreamParser handler(Handler<Buffer> handler) {
    eventHandler = handler;
    if (stream != null) {
      if (handler != null) {
        stream.endHandler(v -> {
          streamEnded = true;
          endStream();
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
  public MarcStreamParser pause() {
    log.debug("MarcStreamParser.pause()");
    if (demand) {

      asyncExecutor.execute(() -> demand = false);

      if (!streamEnded) {
        ReadStream<Buffer> s = stream;
        if (s != null) {
          s.pause();
          log.debug("Source stream is paused in pause().");
        }
      }
    }
    return this;
  }

  @Override
  public synchronized MarcStreamParser fetch(long amount) {
    if (terminated || terminationOnErrorRequested) {
      log.warn("MarcStreamParser.fetch(long amount) - MarcStreamParser is already terminated...");
      return this;
    }
    if (!demand) {
      demand = true;
      log.debug("MarcStreamParser.fetch(long amount)");

      doProcessAsynchronously();
      if (!streamEnded) {
        ReadStream<Buffer> s = stream;
        if (s != null) {
          s.resume();
          log.debug("Source stream is resumed.");
        }
      }
    }
    return this;
  }

  @Override
  public MarcStreamParser resume() {
    if (terminated || terminationOnErrorRequested) {
      log.warn("MarcStreamParser.resume() - MarcStreamParser is already terminated...");
      return this;
    }

    boolean pausedNow = !demand;
    if (log.isDebugEnabled() && pausedNow) {
      log.debug("MarcStreamParser.resume()");
    }
    return pausedNow ? fetch(Long.MAX_VALUE) : this;
  }

  @Override
  public MarcStreamParser endHandler(Handler<Void> endHandler) {
    this.endHandler = endHandler;
    return this;
  }

  @Override
  public synchronized void terminateOnError(Throwable terminatedCause) {
    log.warn("MarcStreamParser.terminateOnError(Throwable terminatedCause): " + terminatedCause);
    terminatedCause.printStackTrace();
    if (!terminationOnErrorRequested) {
      this.terminationOnErrorCause = terminatedCause;
      terminationOnErrorCompleted = false;
      terminationOnErrorRequested = true;

      //We don't need to process stream so Just push resumeHandler to doTerminate()!
      doProcessAsynchronously();
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

  public synchronized boolean isTerminated() {
    return terminated;
  }

  private void endStream() {
    bufferInputStream.end();
    log.debug("MarcStreamParser.endStream() - completed.");
  }

  private synchronized void end() {
    if (!terminated) {
      terminated = true;
      log.debug("MarcStreamParser.end() - starting...");
      try {
        Handler<Void> handler = endHandler;
        if (handler != null) {
          handler.handle(null);
        }
      } finally {
        bufferInputStream.close();
        log.debug("MarcStreamParser.end() - completed.");
      }
    }
  }

  private void abort() {
    log.debug("MarcStreamParser.abort()");
    demand = false;
    streamEnded = true;

    ReadStream<Buffer> s = stream;
    if (s != null) {
      s.pause();
      log.debug("Source stream is paused in abort().");
    }
  }

  private synchronized void doTerminate() {
    if (!terminationOnErrorCompleted) {
      log.debug("MarcStreamParser.doTerminate() - starting...");
      abort();

      Handler<Throwable> exHandler = this.exceptionHandler;
      if (exHandler != null) {
        exHandler.handle(terminationOnErrorCause);
      }

      end();
      terminationOnErrorCompleted = true;
      log.debug("MarcStreamParser.doTerminate() - completed.");
    }
  }

  private Buffer wrapIntoBuffer(Record rawRecord) {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    MarcStreamWriter streamWriter = new MarcStreamWriter(bos, StandardCharsets.UTF_8.name());
    streamWriter.write(rawRecord);
    streamWriter.close();
    return Buffer.buffer(bos.toByteArray());
  }

  private void doProcessAsynchronously() {
    asyncExecutor.execute(() -> {
      boolean initialDemand = this.demand;
      if (terminated) {
        log.warn("MarcStreamParser is already terminated...");
      } else if (terminationOnErrorRequested) {
        doTerminate();
      } else if (marcReader.hasNext()) {
        Handler<Buffer> localEventHandler = eventHandler;
        if (localEventHandler != null) {
          localEventHandler.handle(wrapIntoBuffer(marcReader.next()));

          int remainingBuffersCapacity = bufferInputStream.remainingBuffersCapacity();
          //TODO: get rid of magic numbers
          if (remainingBuffersCapacity > 40 && (demand || initialDemand)) {
            ReadStream<Buffer> s = stream;
            if (s != null) {
              s.resume();
            }
          }

          if (demand || initialDemand) {
            doProcessAsynchronously();
          }
        }
      } else {
        end();
      }
    });
  }

}
