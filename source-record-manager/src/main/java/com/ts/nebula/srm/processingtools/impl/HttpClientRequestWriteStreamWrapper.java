package com.ts.nebula.srm.processingtools.impl;

import io.vertx.codegen.annotations.Nullable;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.RequestOptions;
import io.vertx.core.streams.WriteStream;


public class HttpClientRequestWriteStreamWrapper implements WriteStream<Buffer> {
  private final WriteStream<Buffer> httpClientRequest;
  private boolean wasConnected = false;
  private boolean connectionAlreadyClosed = false;

  public HttpClientRequestWriteStreamWrapper(HttpClient httpClient, String host, int port, String uri, Promise<String> httpClientResponsePromise) {
    httpClient.connectionHandler(connection -> {
      wasConnected = true;
      connection.closeHandler(closeEvent -> connectionAlreadyClosed = true);
    });

    this.httpClientRequest = httpClient.post(
      new RequestOptions()
        .setHost(host)
        .setPort(port)
        .setURI(uri))
      .setChunked(true)
      .setWriteQueueMaxSize(2)
      .handler(response -> response.bodyHandler(buffer -> {
        int statusCode = response.statusCode();
        if (statusCode >= 200 && statusCode < 300) {
          httpClientResponsePromise.tryComplete(buffer.toString());
        } else {
          httpClientResponsePromise.tryFail(response.statusMessage());
        }
      }))
    ;
  }

  @Override
  public WriteStream<Buffer> exceptionHandler(Handler<Throwable> handler) {
    httpClientRequest.exceptionHandler(handler);
    return this;
  }

  @Override
  public WriteStream<Buffer> write(Buffer data) {
    httpClientRequest.write(data);
    return this;
  }

  @Override
  public WriteStream<Buffer> write(Buffer data, Handler<AsyncResult<Void>> handler) {
    httpClientRequest.write(data, handler);
    return this;
  }

  @Override
  public void end() {
    httpClientRequest.end();
  }

  //You should be care with default void end(T data, Handler<AsyncResult<Void>> handler)
  //because that method will hang in such circumstances
  @Override
  public void end(Handler<AsyncResult<Void>> handler) {
    if (wasConnected && !connectionAlreadyClosed) {
      httpClientRequest.end(handler);
    } else {
      handler.handle(Future.failedFuture(connectionAlreadyClosed ? "Http connection is already closed" : "Http connection was not opened"));
    }
  }

  @Override
  public WriteStream<Buffer> setWriteQueueMaxSize(int maxSize) {
    httpClientRequest.setWriteQueueMaxSize(maxSize);
    return this;
  }

  @Override
  public boolean writeQueueFull() {
    return httpClientRequest.writeQueueFull();
  }

  @Override
  public WriteStream<Buffer> drainHandler(@Nullable Handler<Void> handler) {
    httpClientRequest.drainHandler(handler);
    return this;
  }
}
