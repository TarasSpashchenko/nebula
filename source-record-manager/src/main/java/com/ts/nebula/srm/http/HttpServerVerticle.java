package com.ts.nebula.srm.http;

import com.ts.nebula.srm.processingtools.MarcStreamParser;
import com.ts.nebula.srm.processingtools.impl.RawToJsonMarcConverterSimpleImpl;
import com.ts.nebula.srm.processingtools.impl.ResponsibleMessageProducerImpl;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.eventbus.MessageProducer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.RequestOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

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
    router.post("/import/rawMARCv7").handler(this::importRawMARCSHandler7v2);

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

  private void createMarcProcessor(MarcProcessingExecutionContext executionContext, HttpServerRequest request) {
    executionContext.request = request;
    request.pause();

    MarcStreamParser marcStreamParser = MarcStreamParser.newMarcParser(request, command -> vertx.executeBlocking(promise -> {
      try {
        command.run();
        promise.complete();
      } catch (Exception e) {
        e.printStackTrace();
        LOGGER.error("Could not process stream " + e);
        promise.fail(e);
      }
    }, true, ar -> {
      if (!ar.succeeded()) {
        System.out.println("vertx.executeBlocking - error. e: " + ar.cause());
      }
    }));

    marcStreamParser.pause();

    executionContext.marcStreamParser = marcStreamParser;
  }

  private void createRawMarcMessageProducer(MarcProcessingExecutionContext executionContext, String addressSuffix) {
    String address = "RawMarc_" + addressSuffix;
    MessageProducer<Buffer> rawMarcRecordsSender = new ResponsibleMessageProducerImpl<>(vertx, address, true, new DeliveryOptions());

    rawMarcRecordsSender.setWriteQueueMaxSize(2);

    executionContext.rawMarcRecordsSender = rawMarcRecordsSender;
  }

  private void createSrsHttpClient(MarcProcessingExecutionContext executionContext) {
    HttpClient srsHttpClient = vertx.createHttpClient(
      new HttpClientOptions()
//        .setProtocolVersion(HttpVersion.HTTP_2) //because of netty bug https://github.com/netty/netty/issues/7485
        .setDefaultHost("localhost")
        .setDefaultPort(8090))
      .connectionHandler(connection -> {
        connection.closeHandler(closeEvent -> {
          executionContext.connectionAlreadyClosed = true;
        });
        executionContext.wasConnected = true;
      });

    executionContext.srsHttpClientRequest =
      srsHttpClient.post(
        new RequestOptions()
          .setHost("localhost")
          .setPort(8090)
          .setURI("/import/storeSRSRecords"))
        .setChunked(true)
        .setWriteQueueMaxSize(2)
        .handler(response ->
          response.bodyHandler(
            buffer -> {
              System.out.println("SRS Response: " + buffer.toString());
              //TODO: it is a wrong place it should not be done here
              executionContext.request.response().end("RecordParser Request has been handled.");
            }
          ));

    executionContext.srsHttpClientRequest.drainHandler(event -> {
      executionContext.marcStreamParser.resume();
    });

    executionContext.srsHttpClientRequest.exceptionHandler(e -> {
      e.printStackTrace();
      executionContext.marcStreamParser.terminateOnError(e);
    });

  }

  private void createRawMarcMessageConsumer(MarcProcessingExecutionContext executionContext, String addressSuffix) {
    createSrsHttpClient(executionContext);
    MessageConsumer<Buffer> rawMarcMessageConsumer = vertx.eventBus().consumer("RawMarc_" + addressSuffix, message -> {

      JsonObject marcRecord = RawToJsonMarcConverterSimpleImpl.getInstance().convert(message.body());

      Buffer marcRecordForSRS = marcRecord.toBuffer();

      executionContext.srsHttpClientRequest.write(marcRecordForSRS);


      if (executionContext.srsHttpClientRequest.writeQueueFull()) {
        executionContext.marcStreamParser.pause();
      }

    });


    rawMarcMessageConsumer.exceptionHandler(e -> {
      e.printStackTrace();
      executionContext.marcStreamParser.terminateOnError(e);
    });
    rawMarcMessageConsumer.setMaxBufferedMessages(1);

    executionContext.rawMarcMessageConsumer = rawMarcMessageConsumer;
  }

  private void startRequestProcessing(MarcProcessingExecutionContext executionContext) {
    executionContext.marcStreamParser.processAsynchronously(executionContext.rawMarcRecordsSender,
      ar -> {
        if (ar.succeeded()) {
          System.out.println("marcParser.processAsynchronously - succeeded completion");
        } else {
          System.out.println("marcParser.processAsynchronously - error. e: " + ar.cause());
        }

        executionContext.rawMarcRecordsSender.end();
        executionContext.rawMarcMessageConsumer.unregister(uar -> {
          if (uar.succeeded()) {
            System.out.println("executionContext.rawMarcMessageConsumer succeeded.");
          } else {
            System.out.println("executionContext.rawMarcMessageConsumer failed. " + uar.cause());
          }

          if (executionContext.wasConnected && !executionContext.connectionAlreadyClosed) {
            executionContext.srsHttpClientRequest.end(arRequest -> {
              if (arRequest.succeeded()) {
                System.out.println("executionContext.srsHttpClientRequest succeeded.");
              } else {
                System.out.println("executionContext.srsHttpClientRequest failed. " + arRequest.cause());
              }
            });
          } else {
            //TODO: it is a wrong place it should not be done here
            executionContext.request.response().setStatusCode(500).end(executionContext.marcStreamParser.getTerminationOnErrorCause().toString());
          }
        });
      });
  }

  private void importRawMARCSHandler7v2(RoutingContext context) {
    String suffix = UUID.randomUUID().toString().replaceAll("-", "");

    MarcProcessingExecutionContext executionContext = new MarcProcessingExecutionContext();
    HttpServerRequest request = context.request();

    createMarcProcessor(executionContext, request);
    createRawMarcMessageProducer(executionContext, suffix);
    createRawMarcMessageConsumer(executionContext, suffix);
    startRequestProcessing(executionContext);
  }


  private static class MarcProcessingExecutionContext {
    private HttpServerRequest request;
    private MarcStreamParser marcStreamParser;
    private MessageProducer<Buffer> rawMarcRecordsSender;

    private HttpClientRequest srsHttpClientRequest;
    private MessageConsumer<Buffer> rawMarcMessageConsumer;

    private boolean connectionAlreadyClosed;
    private boolean wasConnected;

  }

}
