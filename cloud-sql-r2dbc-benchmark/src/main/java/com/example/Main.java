/*
 * Copyright 2018 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.example;


import io.opencensus.common.Scope;
import io.opencensus.exporter.trace.stackdriver.StackdriverTraceConfiguration;
import io.opencensus.exporter.trace.stackdriver.StackdriverTraceExporter;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;
import io.opencensus.trace.config.TraceConfig;
import io.opencensus.trace.samplers.Samplers;
import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import reactor.core.publisher.Mono;

public class Main {

  private static final String PROJECT_ID = System.getenv("PROJECT_ID");
  private static final String CONNECTION_NAME = System.getenv("CLOUD_SQL_CONNECTION_NAME");
  private static final String DB_NAME = System.getenv("DB_NAME");
  private static final String DB_USER = System.getenv("DB_USER");
  private static final String DB_PASSWORD = System.getenv("DB_PASS");

  private static final Logger LOGGER = Logger.getLogger(Main.class.getName());
  private static final Tracer TRACER = Tracing.getTracer();

  public static void main(String[] args) throws IOException {
    // Set up StackDriver
    StackdriverTraceExporter.createAndRegister(
        StackdriverTraceConfiguration.builder()
            .setProjectId(PROJECT_ID)
            .build());

    // Trace every request
    TraceConfig traceConfig = Tracing.getTraceConfig();
    traceConfig.updateActiveTraceParams(
        traceConfig.getActiveTraceParams().toBuilder().setSampler(Samplers.alwaysSample()).build());

    // Set up URL parameters
    String r2dbcURL = String
        .format("r2dbc:gcp:postgres://%s:%s@%s/%s", DB_USER, DB_PASSWORD, CONNECTION_NAME,
            DB_NAME);

    LOGGER.log(Level.INFO, "Started logging.");
    // Create thread pool
    ScheduledExecutorService execService = Executors.newScheduledThreadPool(2);

    // Start tests for regular connections
    execService.scheduleAtFixedRate(() -> {
      connectRegular(r2dbcURL);
    }, 0, 500L, TimeUnit.MILLISECONDS);

    // Start tests for pooled connections
    ConnectionPool pool = createPool(r2dbcURL);
    execService.scheduleAtFixedRate(() -> {
      connectWithPool(pool);
    }, 0, 500L, TimeUnit.MILLISECONDS);
  }

  private static void connectRegular(String r2dbcURL) {
    try (Scope rootSpan = TRACER.spanBuilder("r2dbc-regular-connection").startScopedSpan()) {
      // Get connection

      ConnectionFactory connectionFactory = ConnectionFactories.get(r2dbcURL);

      Mono.from(connectionFactory.create())
          .flatMap(conn -> Mono.from(conn.createStatement("SELECT 1").execute())
              .doOnTerminate(() -> Mono.from(conn.close()).subscribe()))
          .block();

    }
    LOGGER.log(Level.INFO, "[regular-connection] complete.");
  }

  private static void connectWithPool(ConnectionPool pool) {
    try (Scope rootSpan = TRACER.spanBuilder("r2dbc-pool-connection").startScopedSpan()) {
      // Get connection
      Mono<Connection> connectionMono = pool.create();
      connectionMono
          .flatMap(conn -> Mono.from(conn.createStatement("SELECT 1").execute())
              .doOnTerminate(() -> Mono.from(conn.close()).subscribe()))
          .block();
    }
    LOGGER.log(Level.INFO, "[pool-connection] complete.");
  }


  private static ConnectionPool createPool(String r2dbcURL) {
    ConnectionFactory connectionFactory = ConnectionFactories.get(r2dbcURL);

    ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration
        .builder(connectionFactory)
        .maxIdleTime(Duration.ofMillis(1000))
        .maxSize(5)
        .build();

    return new ConnectionPool(configuration);
  }

}
