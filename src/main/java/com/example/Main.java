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

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.opencensus.common.Scope;
import io.opencensus.exporter.trace.stackdriver.StackdriverTraceConfiguration;
import io.opencensus.exporter.trace.stackdriver.StackdriverTraceExporter;
import io.opencensus.trace.Span;
import io.opencensus.trace.Status;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;
import io.opencensus.trace.config.TraceConfig;
import io.opencensus.trace.samplers.Samplers;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.sql.DataSource;

public class Main {

  private static final String PROJECT_ID = "";
  private static final String CONNECTION_NAME = "";
  private static final String DB_NAME = "";
  private static final String DB_USER = "";
  private static final String DB_PASSWORD = "";

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
    String jdbcURL = String.format("jdbc:postgresql:///%s", DB_NAME);
    Properties connProps = new Properties();
    connProps.setProperty("user", DB_USER);
    connProps.setProperty("password", DB_PASSWORD);
    connProps.setProperty("socketFactory", "com.google.cloud.sql.postgres.SocketFactory");
    connProps.setProperty("cloudSqlInstance", CONNECTION_NAME);

    LOGGER.log(Level.INFO, "Started logging.");
    // Create thread pool
    ScheduledExecutorService execService = Executors.newScheduledThreadPool(2);

    // Start tests for regular connections
    execService.scheduleAtFixedRate(() -> {
      connectRegular(jdbcURL, connProps);
    }, 0, 500L, TimeUnit.MILLISECONDS);

    // Start tests for pooled connections
    DataSource pool = createPool(jdbcURL, connProps);
    execService.scheduleAtFixedRate(() -> {
      connectWithPool(pool);
    }, 0, 500L, TimeUnit.MILLISECONDS);
  }

  private static void connectRegular(String jdbcURL, Properties connProps) {
    try (Scope rootSpan = TRACER.spanBuilder("regular-connection").startScopedSpan()) {
      Connection conn;

      // Get connection
      try (Scope getConnSpan = TRACER.spanBuilder("getConnection").startScopedSpan()) {
        conn = DriverManager.getConnection(jdbcURL, connProps);
      } catch (Exception ex) {
        LOGGER.log(Level.WARNING, "[regular-connection] Error occurred during connection.", ex);
        return;
      }

      // Test connection
      executeStatement(conn);

      // Close connection
      closeConnection(conn);
    }
    LOGGER.log(Level.INFO, "[regular-connection] complete.");
  }

  private static void connectWithPool(DataSource pool) {
    try (Scope rootSpan = TRACER.spanBuilder("pool-connection").startScopedSpan()) {
      Connection conn;

      // Get connection
      try (Scope getConnSpan = TRACER.spanBuilder("getConnection").startScopedSpan()) {
        conn = pool.getConnection();
      } catch (Exception ex) {
        LOGGER.log(Level.WARNING, "[pool-connection] Error occurred during connection.", ex);
        return;
      }

      // Test connection
      executeStatement(conn);

      // Close connection
      closeConnection(conn);
    }
    LOGGER.log(Level.INFO, "[pool-connection] complete.");
  }

  // Create a connection to the database
  private static void closeConnection(Connection conn) {
    try (Scope closeConnSpan = TRACER.spanBuilder("closeConnection").startScopedSpan()) {
      conn.close();
    } catch (Exception ex) {
      LOGGER.log(Level.WARNING, "Error occurred during connection close.", ex);
    }
  }

  // Execute a simple statement to verify the connection works.
  private static void executeStatement(Connection conn) {
    try (Scope ss = TRACER.spanBuilder("executeStatement").startScopedSpan()) {
      conn.prepareStatement("SELECT true;").execute();
    } catch (Exception ex) {
      Span span = TRACER.getCurrentSpan();
      span.setStatus(Status.INTERNAL.withDescription(ex.toString()));
    }
  }

  private static DataSource createPool(String jdbcURL, Properties props) {
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl(jdbcURL);
    config.setDataSourceProperties(props);

    config.setMaximumPoolSize(5);
    config.setMinimumIdle(5);
    config.setConnectionTimeout(TimeUnit.MILLISECONDS.convert(10, TimeUnit.SECONDS)); // 10 seconds
    config.setIdleTimeout(TimeUnit.MILLISECONDS.convert(10, TimeUnit.MINUTES)); // 10 minutes
    config.setMaxLifetime(TimeUnit.MILLISECONDS.convert(60, TimeUnit.MINUTES));

    return new HikariDataSource(config);
  }

}
