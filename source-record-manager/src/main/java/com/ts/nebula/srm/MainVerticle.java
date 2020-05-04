package com.ts.nebula.srm;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Promise;

public class MainVerticle extends AbstractVerticle {

  @Override
  public void start(Promise<Void> promise) {
    Promise<String> httpVerticleDeployment = Promise.promise();
    vertx.deployVerticle("com.ts.nebula.srm.http.HttpServerVerticle", new DeploymentOptions().setInstances(1), httpVerticleDeployment);
    httpVerticleDeployment
      .future()
      .onSuccess(
        s -> promise.complete()
      )
      .onFailure(
        f -> promise.future()
      );
  }
}
