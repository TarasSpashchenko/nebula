package com.ts.nebula.srm.http;

import com.ts.nebula.srm.processingtools.MarcStreamParser;
import com.ts.nebula.srm.processingtools.impl.HttpClientRequestWriteStreamWrapper;
import com.ts.nebula.srm.processingtools.impl.MarcRecordWriteStreamImpl;
import com.ts.nebula.srm.processingtools.impl.MarcStreamParserImpl;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.streams.WriteStream;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.marc4j.marc.Record;

import java.util.UUID;

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
    router.post("/import/rawMARCv7").handler(this::importRawMARCSHandler9);

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

  private Handler<AsyncResult<CompositeFuture>> compositeCompletionHandler(MarcStreamParser marcStreamParser, HttpServerResponse response) {
    return ar -> {
      if (ar.failed()) {
        marcStreamParser.terminateOnError(ar.cause());
        response.setStatusCode(500).end(ar.cause().toString());
      } else {
        CompositeFuture compositeFuture = ar.result();

        String srsResult = compositeFuture.resultAt(0);
        String inventoryResult = compositeFuture.resultAt(1);
        String completionResult = compositeFuture.resultAt(2);

        response.end("All processed successfully :\n" + srsResult + "\n" + inventoryResult + "\n" + completionResult);
      }
    };
  }

  private void importRawMARCSHandler9(RoutingContext context) {
    HttpServerRequest request = context.request();
    request.pause();

    MarcStreamParser marcStreamParser = MarcStreamParserImpl.newMarcParser(vertx, request);
    marcStreamParser.pause();

    String id = UUID.randomUUID().toString().replaceAll("-", "");

    Promise<String> srsHttpClientPromise = Promise.promise();
    WriteStream<Buffer> srsWriteStream =
      new HttpClientRequestWriteStreamWrapper(vertx.createHttpClient(), "localhost", 8090, "/import/storeSRSRecords", srsHttpClientPromise);

    Promise<String> inventoryHttpClientPromise = Promise.promise();
    WriteStream<Buffer>
      inventoryWriteStream = new HttpClientRequestWriteStreamWrapper(vertx.createHttpClient(), "localhost", 8090, "/import/storeSRSRecords", inventoryHttpClientPromise);

    Promise<String> completionPromise = Promise.promise();

    CompositeFuture.join(
      srsHttpClientPromise.future(),
      inventoryHttpClientPromise.future(),
      completionPromise.future()).onComplete(compositeCompletionHandler(marcStreamParser, request.response()));

    Buffer header = Buffer.buffer("{\"streamHeader\": true, \"JobExecutionId\":\"" + id + "\", \"someOtherStuff\":\"blah... blah... blah... \"}");
    srsWriteStream.write(header);
    inventoryWriteStream.write(header);

    WriteStream<Record> marcRecordWriteStream = new MarcRecordWriteStreamImpl(srsWriteStream, inventoryWriteStream);

    // This part looks ugly - it is a point for improvement
    marcStreamParser.processAsynchronously(marcRecordWriteStream, ar -> {
      String trailerStr = ar.succeeded() ?
        "{\"streamTrailer\": true, \"succeeded\": true, \"failed\": false, \"JobExecutionId\":\"" + id + "\", \"someOtherStuff\":\"blah... blah... blah... \"}" :
        "{\"streamTrailer\": true, \"succeeded\": false, \"failed\": true, \"JobExecutionId\":\"" + id + "\", \"someOtherStuff\":\"blah... blah... blah... \"}";
      Buffer trailer = Buffer.buffer(trailerStr);

      if (ar.succeeded()) {
        completionPromise.complete(trailerStr); //just put something here
      } else {
        completionPromise.fail(ar.cause());
      }

      //You should be care with default void end(T data, Handler<AsyncResult<Void>> handler)
      //because that method will hang in such circumstances
      srsWriteStream
        .write(trailer)
        .end(asr -> {
          if (asr.failed()) {
            srsHttpClientPromise.tryFail(asr.cause());
          }
        });
      inventoryWriteStream
        .write(trailer)
        .end(asr -> {
          if (asr.failed()) {
            inventoryHttpClientPromise.tryFail(asr.cause());
          }
        });
    });

  }

}
