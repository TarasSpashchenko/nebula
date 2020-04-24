package com.ts.nebula.srm.http;

import com.ts.nebula.srm.processingtools.MarcProcessor;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.eventbus.MessageProducer;
import io.vertx.core.eventbus.impl.MessageProducerImpl;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.parsetools.RecordParser;
import io.vertx.core.streams.WriteStream;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.marc4j.marc.Record;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class HttpServerVerticle extends AbstractVerticle {

  public static final String CONFIG_HTTP_SERVER_PORT = "http.server.port";

  private static final Logger LOGGER = LoggerFactory.getLogger(HttpServerVerticle.class);

  @Override
  public void start(Promise<Void> promise) {

    Integer portNumber = config().getInteger(CONFIG_HTTP_SERVER_PORT, 8080);
    HttpServer server =
      vertx.createHttpServer(new HttpServerOptions().setHost("localhost").setPort(portNumber).setLogActivity(true));

    Router router = Router.router(vertx);
//    router.post().handler(BodyHandler.create());
    router.post("/import/rawMARCv6").handler(this::importRawMARCSHandler6);
    router.post("/import/rawMARCv7").handler(this::importRawMARCSHandler7);

    server
      .requestHandler(router)
      .listen(ar -> {
        if (ar.succeeded()) {
          LOGGER.info("HTTP server running on portNumber " + portNumber);
          promise.complete();
        } else {
          LOGGER.error("Could not start a HTTP server", ar.cause());
          promise.fail(ar.cause());
        }
      });
  }


  private List<MessageConsumer<Buffer>> createRawMarcMessageConsumer(String addressSuffix) {
    List<MessageConsumer<Buffer>> result = new ArrayList<>();
    MessageConsumer<Buffer> messageConsumer1 =
      vertx.eventBus().consumer("RawMarc_" + addressSuffix,
        event -> vertx.executeBlocking(promise -> {
//        try {
//          Thread.sleep(1000);
          System.out.println(Thread.currentThread().getName() + " Consumer1 : " + event.body());
//        } catch (InterruptedException e) {
//          e.printStackTrace();
//        }
        }, false, null));
    messageConsumer1.setMaxBufferedMessages(100);
    result.add(messageConsumer1);

//    MessageConsumer<Buffer> messageConsumer2 = vertx.eventBus().consumer("RawMarc_" + addressSuffix, event -> {
//      vertx.executeBlocking(promise -> {
//        try {
//          Thread.sleep(1000);
//          System.out.println(Thread.currentThread().getName() + " Consumer2 : " + event.body());
//        } catch (InterruptedException e) {
//          e.printStackTrace();
//        }
//      }, false, null);
//    });
//    messageConsumer2.setMaxBufferedMessages(100);
//    result.add(messageConsumer2);

    return result;
  }

  private MessageProducer<Buffer> createRawMarcMessageProducer(String addressSuffix) {
    String address = "RawMarc_" + addressSuffix;
    MessageProducer<Buffer> sender = new MessageProducerImpl<Buffer>(vertx, address, true, new DeliveryOptions()) {

      @Override
      public void end(Handler<AsyncResult<Void>> handler) {
        close(handler);
      }
    };

    return sender.setWriteQueueMaxSize(200);
  }

  private void importRawMARCSHandler7(RoutingContext context) {
    HttpServerRequest request = context.request();
    request.pause();

    MessageProducer<Buffer> messageProducer = createRawMarcMessageProducer("XSuffix_01");
    List<MessageConsumer<Buffer>> messageConsumerList = createRawMarcMessageConsumer("XSuffix_01");

    MarcProcessor marcProcessor = MarcProcessor.newMarcParser(request);
    marcProcessor.pause();

    marcProcessor.processAsynchronously(messageProducer,
      command -> vertx.executeBlocking(promise -> {
        try {
          command.run();
          promise.complete();
        } catch (Exception e) {
          e.printStackTrace();
          LOGGER.error("Could not process stream " + e);
          promise.fail(e);
        }
      }, false, ar -> {
        if (ar.succeeded()) {
          System.out.println("vertx.executeBlocking - succeeded completion");
        } else {
          System.out.println("vertx.executeBlocking - error. e: " + ar.cause());
        }
      }), ar -> {
        if (ar.succeeded()) {
          System.out.println("marcParser.processAsynchronously - succeeded completion");
        } else {
          System.out.println("marcParser.processAsynchronously - error. e: " + ar.cause());
        }
        messageProducer.end();
        messageConsumerList.forEach(MessageConsumer::unregister);
        context.response().end("RecordParser Request has been handled.");
      });
  }


  private void importRawMARCSHandler6(RoutingContext context) {
    HttpServerRequest request = context.request();
    request.pause();

    MarcProcessor marcProcessor = MarcProcessor.newMarcParser(request);
    marcProcessor.pause();

    marcProcessor.startAsyncProcessing(dummyWriter2,
      asyncReaderCallable -> vertx.<Integer>executeBlocking(promise -> {
        try {
          Integer recordCount = asyncReaderCallable.call();
          promise.complete(recordCount);
        } catch (Exception e) {
          e.printStackTrace();
          LOGGER.error("Could not process stream " + e);
          promise.fail(e);
        }
      }, false, ar -> {
        if (ar.succeeded()) {
          int recordCount = ar.result();
          System.out.println("executeBlocking completed. recordCount: " + recordCount);
        } else {
          System.out.println("executeBlocking error. e: " + ar.cause());
        }
        context.response().end("RecordParser Request has been handled.");
      }), ar -> {
        if (ar.succeeded()) {
          System.out.println("marcParser.startProcessing - succeeded completion");
        } else {
          System.out.println("marcParser.startProcessing - error. e: " + ar.cause());
        }
      }
    );

  }

//  private void importRawMARCSHandler5(RoutingContext context) {
//    HttpServerRequest request = context.request();
//    request.pause();
//
//    MarcParser marcParser = MarcParser.newMarcParser(request, asyncReaderCallable -> {
//      vertx.<Integer>executeBlocking(promise -> {
//        try {
//          Integer recordCount = asyncReaderCallable.call();
//          promise.complete(recordCount);
//        } catch (Exception e) {
//          e.printStackTrace();
//          LOGGER.error("Could not process stream " + e);
//          promise.fail(e);
//        }
//      }, false, ar -> {
//        if (ar.succeeded()) {
//          int recordCount = ar.result();
//          System.out.println("asyncReaderCallable completed. recordCount: " + recordCount);
//        } else {
//          System.out.println("asyncReaderCallable error. e: " + ar.cause());
//        }
//      });
//    });
//
//    marcParser.pause();
//
//    vertx.executeBlocking(promise -> {
//      marcParser.pipeTo(new WriteStream<Record>() {
//        @Override
//        public WriteStream<Record> exceptionHandler(Handler<Throwable> handler) {
//          System.out.println("WriteStream<Buffer> exceptionHandler(Handler<Throwable> handler)");
//          return this;
//        }
//
//        @Override
//        public WriteStream<Record> write(Record data) {
//          System.out.println("WriteStream<Buffer> write(Buffer data)");
//          System.out.println("This is the next record: " + data.toString());
//          return this;
//        }
//
//        @Override
//        public WriteStream<Record> write(Record data, Handler<AsyncResult<Void>> handler) {
//          System.out.println("WriteStream<Buffer> write(Buffer data, Handler<AsyncResult<Void>> handler)");
//          System.out.println("This is the next record: " + data.toString());
//          handler.handle(Future.succeededFuture());
//          return this;
//        }
//
//        @Override
//        public void end() {
//          System.out.println("end()");
//        }
//
//        @Override
//        public void end(Handler<AsyncResult<Void>> handler) {
//          System.out.println("end(Handler<AsyncResult<Void>> handler)");
//          handler.handle(Future.succeededFuture());
//        }
//
//        @Override
//        public WriteStream<Record> setWriteQueueMaxSize(int maxSize) {
//          System.out.println("WriteStream<Buffer> setWriteQueueMaxSize(int maxSize)");
//          return this;
//        }
//
//        @Override
//        public boolean writeQueueFull() {
//          System.out.println("boolean writeQueueFull()");
//          return false;
//        }
//
//        @Override
//        public WriteStream<Record> drainHandler(@io.vertx.codegen.annotations.Nullable Handler<Void> handler) {
//          System.out.println("WriteStream<Buffer> drainHandler(@io.vertx.codegen.annotations.Nullable Handler<Void> handler)");
//          if (Objects.nonNull(handler)) {
//            handler.handle(null);
//          }
//          return this;
//        }
//      }, ar -> {
//        System.out.println("This is the end: " + ar.succeeded());
//        promise.complete(ar.result());
//      });
//    }, false, ar -> {
//      context.response().end("RecordParser Request has been handled.");
//    });
//
//  }

  private void importRawMARCSHandler4(RoutingContext context) {
    HttpServerRequest request = context.request();
    request.pause();

    RecordParser recordParser = RecordParser.newDelimited(Buffer.buffer(new byte[]{0x1d}), request);
    recordParser.pause();

    vertx.executeBlocking(
      promise -> recordParser.pipeTo(dummyWriter2, event -> {
        System.out.println("This is the end: " + event.succeeded());
        promise.complete(event.result());
      }), false, result -> context.response().end("RecordParser Request has been handled."));


  }


  private void importRawMARCSHandler3(RoutingContext context) {
    HttpServerRequest request = context.request();
    request.pause();

    RecordParser recordParser = RecordParser.newDelimited("\n", request);
    recordParser.pause();

    recordParser.pipeTo(dummyWriter2, event -> {
      System.out.println("This is the end: " + event.succeeded());

      context.response().end("RecordParser Request has been handled.");
    });

  }


  private void importRawMARCSHandler2(RoutingContext context) {
    HttpServerRequest request = context.request();
    request.pause();

    RecordParser recordParser = RecordParser.newDelimited("\n", request);
    recordParser.pause();

    recordParser.handler(h -> System.out.println("This is the next record: " + h.toString()));

    recordParser.endHandler(event -> {
      System.out.println("This is the end");
      context.response().end("RecordParser Request has been handled.");
    });

    recordParser.resume();
  }

  private void importRawMARCSHandler(RoutingContext context) {
    HttpServerRequest request = context.request();
    request.pause();

    final RecordParser parser = RecordParser.newDelimited("\n", h -> System.out.println("This is the next record: " + h.toString())).endHandler(event -> {
        System.out.println("This is the end");
        context.response().end("RecordParser Request has been handled.");
      }
    );


    request.handler(parser);
    request.endHandler(event -> parser.handle(Buffer.buffer("\n")));

    request.resume();
//
//    System.out.println("request.bytesRead():" + request.bytesRead());
//
//    request.bodyHandler(processingtools).endHandler(
//      event -> context.response().end("Request has been handled.")
//    );
////    processingtools.pipeTo(context.response());

  }


  private WriteStream<Record> dummyWriter = new WriteStream<Record>() {
    private int count;

    @Override
    public WriteStream<Record> exceptionHandler(Handler<Throwable> handler) {
      System.out.println("WriteStream<Buffer> exceptionHandler(Handler<Throwable> handler)");
      return this;
    }

    @Override
    public WriteStream<Record> write(Record data) {
      System.out.println("WriteStream<Buffer> write(Buffer data)");
      System.out.println("This is the next record: " + data.toString());
      System.out.println("------------Record number: " + ++count);
      return this;
    }

    @Override
    public WriteStream<Record> write(Record data, Handler<AsyncResult<Void>> handler) {
      System.out.println("WriteStream<Buffer> write(Buffer data, Handler<AsyncResult<Void>> handler)");
      System.out.println("This is the next record: " + data.toString());
      handler.handle(Future.succeededFuture());
      return this;
    }

    @Override
    public void end() {
      System.out.println("end()");
    }

    @Override
    public void end(Handler<AsyncResult<Void>> handler) {
      System.out.println("end(Handler<AsyncResult<Void>> handler)");
      handler.handle(Future.succeededFuture());
    }

    @Override
    public WriteStream<Record> setWriteQueueMaxSize(int maxSize) {
      System.out.println("WriteStream<Buffer> setWriteQueueMaxSize(int maxSize)");
      return this;
    }

    @Override
    public boolean writeQueueFull() {
      System.out.println("boolean writeQueueFull()");
      return false;
    }

    @Override
    public WriteStream<Record> drainHandler(@io.vertx.codegen.annotations.Nullable Handler<Void> handler) {
      System.out.println("WriteStream<Buffer> drainHandler(@io.vertx.codegen.annotations.Nullable Handler<Void> handler)");
      if (Objects.nonNull(handler)) {
        handler.handle(null);
      }
      return this;
    }
  };


  private WriteStream<Buffer> dummyWriter2 = new WriteStream<Buffer>() {
    @Override
    public WriteStream<Buffer> exceptionHandler(Handler<Throwable> handler) {
      System.out.println("WriteStream<Buffer> exceptionHandler(Handler<Throwable> handler)");
      return this;
    }

    @Override
    public WriteStream<Buffer> write(Buffer data) {
      System.out.println("WriteStream<Buffer> write(Buffer data)");
      System.out.println("This is the next record: " + data.toString());
      return this;
    }

    @Override
    public WriteStream<Buffer> write(Buffer data, Handler<AsyncResult<Void>> handler) {
      System.out.println("WriteStream<Buffer> write(Buffer data, Handler<AsyncResult<Void>> handler)");
      System.out.println("This is the next record: " + data.toString());
      handler.handle(Future.succeededFuture());
      return this;
    }

    @Override
    public void end() {
      System.out.println("end()");
    }

    @Override
    public void end(Handler<AsyncResult<Void>> handler) {
      System.out.println("end(Handler<AsyncResult<Void>> handler)");
      handler.handle(Future.succeededFuture());
    }

    @Override
    public WriteStream<Buffer> setWriteQueueMaxSize(int maxSize) {
      System.out.println("WriteStream<Buffer> setWriteQueueMaxSize(int maxSize)");
      return this;
    }

    @Override
    public boolean writeQueueFull() {
      System.out.println("boolean writeQueueFull()");
      return false;
    }

    @Override
    public WriteStream<Buffer> drainHandler(@io.vertx.codegen.annotations.Nullable Handler<Void> handler) {
      System.out.println("WriteStream<Buffer> drainHandler(@io.vertx.codegen.annotations.Nullable Handler<Void> handler)");
      if (Objects.nonNull(handler)) {
        handler.handle(null);
      }
      return this;
    }
  };
}
