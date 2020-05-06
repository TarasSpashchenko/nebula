package com.ts.nebula.srs.http;

import com.ts.nebula.srs.processingtools.impl.DummyMarcRecordsBatchDBWriteStreamImpl;
import com.ts.nebula.srs.processingtools.impl.MarcRecordsDBWriteStream;
import com.ts.nebula.srs.processingtools.impl.MarcRecordsWriteStreamWrapper;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.parsetools.JsonParser;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

import java.util.UUID;

public class HttpServerVerticle extends AbstractVerticle {
  public static final String CONFIG_HTTP_SERVER_PORT = "http.server.port";

  private static final Logger LOGGER = LoggerFactory.getLogger(HttpServerVerticle.class);

  @Override
  public void start(Promise<Void> promise) {
    Integer portNumber = config().getInteger(CONFIG_HTTP_SERVER_PORT, 8090);
    HttpServer server =
      vertx.createHttpServer(new HttpServerOptions().setHost("localhost").setPort(portNumber).setLogActivity(true));

    Router router = Router.router(vertx);
    router.post("/import/storeSRSRecords").handler(this::storeSRSRecordsHandler3);

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

  private void storeSRSRecordsHandler3(RoutingContext context) {
    String id = UUID.randomUUID().toString().replaceAll("-", "");

    HttpServerRequest request = context.request();
    request.pause();

    JsonParser jsonParser = JsonParser.newParser(request);
    jsonParser.pause();
    jsonParser.objectValueMode();

    MarcProcessingExecutionContext executionContext = new MarcProcessingExecutionContext(id);
    executionContext.request = request;

    DummyMarcRecordsBatchDBWriteStreamImpl dummyMarcRecordsDBWriteStream = new DummyMarcRecordsBatchDBWriteStreamImpl(vertx);

    MarcRecordsDBWriteStream marcRecordsDBWriteStream = new MarcRecordsDBWriteStream(dummyMarcRecordsDBWriteStream);

    jsonParser.pipeTo(new MarcRecordsWriteStreamWrapper(marcRecordsDBWriteStream), ar -> {
System.out.println("jsonParser.pipeTo - completionHandler: " + ar);
      request.response().end("All MARC records have been saved!");
    });
  }

  private static class MarcProcessingExecutionContext {
    private String id;
    private HttpServerRequest request;

    private MarcProcessingExecutionContext(String id) {
      this.id = id;
    }

  }

}
