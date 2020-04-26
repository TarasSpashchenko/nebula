package com.ts.nebula.srm.processingtools.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.impl.MessageProducerImpl;

public class ResponsibleMessageProducerImpl<T> extends MessageProducerImpl<T> {

  public ResponsibleMessageProducerImpl(Vertx vertx, String address, boolean send, DeliveryOptions options) {
    super(vertx, address, send, options);
  }

  @Override
  public void end(Handler<AsyncResult<Void>> handler) {
    close(handler);
  }
}
