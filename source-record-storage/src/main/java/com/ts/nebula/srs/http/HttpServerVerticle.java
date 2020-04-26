package com.ts.nebula.srs.http;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.parsetools.RecordParser;
import io.vertx.core.streams.WriteStream;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

import java.util.Objects;

public class HttpServerVerticle extends AbstractVerticle {
  public static final String CONFIG_HTTP_SERVER_PORT = "http.server.port";

  private static final Logger LOGGER = LoggerFactory.getLogger(HttpServerVerticle.class);

  @Override
  public void start(Promise<Void> promise) {
    Integer portNumber = config().getInteger(CONFIG_HTTP_SERVER_PORT, 8090);
    HttpServer server =
      vertx.createHttpServer(new HttpServerOptions().setHost("localhost").setPort(portNumber).setLogActivity(true));

    Router router = Router.router(vertx);
//    router.post().handler(BodyHandler.create());
    router.post("/import/storeSRSRecords").handler(this::dummyStoreSRSRecords);

    server
      .requestHandler(router)
      .listen(ar -> {
        if (ar.succeeded()) {
          LOGGER.info("HTTP server running on portNumber " + portNumber);
          promise.complete();
        } else {
          ar.cause().printStackTrace();
          LOGGER.error("Could not start a HTTP server", ar.cause());
          promise.fail(ar.cause());
        }
      });
  }

  private void dummyStoreSRSRecords(RoutingContext context) {
    HttpServerRequest request = context.request();
    request.pause();

    RecordParser recordParser = RecordParser.newFixed(1024, request);
    recordParser.pause();

    vertx.executeBlocking(
      promise -> recordParser.pipeTo(dummyWriter2, event -> {
        if (event.failed()) {
          event.cause().printStackTrace();
        }
        System.out.println("This is the end: " + event.succeeded());
        promise.complete(event.result());
      }), false, result -> context.response().end("RecordParser Request has been handled."));
  }

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
