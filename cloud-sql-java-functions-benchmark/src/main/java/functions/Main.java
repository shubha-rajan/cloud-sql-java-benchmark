package functions;

import com.google.cloud.functions.HttpFunction;
import com.google.cloud.functions.HttpRequest;
import com.google.cloud.functions.HttpResponse;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.opencensus.common.Scope;
import io.opencensus.exporter.trace.stackdriver.StackdriverTraceConfiguration;
import io.opencensus.exporter.trace.stackdriver.StackdriverTraceExporter;
import io.opencensus.trace.AttributeValue;
import io.opencensus.trace.Span;
import io.opencensus.trace.Status;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;
import io.opencensus.trace.config.TraceConfig;
import io.opencensus.trace.samplers.Samplers;
import java.io.BufferedWriter;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.sql.DataSource;
import org.apache.commons.lang3.exception.ExceptionUtils;


public class Main implements HttpFunction {

  private static final String PROJECT_ID = System.getenv("PROJECT_ID");
  private static final String CONNECTION_NAME = System.getenv("CLOUD_SQL_CONNECTION_NAME");
  private static final String DB_NAME = System.getenv("DB_NAME");
  private static final String DB_USER = System.getenv("DB_USER");
  private static final String DB_PASSWORD = System.getenv("DB_PASS");
  private static final Logger LOGGER = Logger.getLogger(Main.class.getName());

  // Create thread pool
  private static ScheduledExecutorService execService = Executors.newScheduledThreadPool(2);

  private static class SingletonTracer {

    private static final Tracer TRACER = InitTracer();

    private SingletonTracer() {
    }

    private static Tracer InitTracer() {
      Tracer tracer = Tracing.getTracer();

      try {
        // Set up StackDriver
        StackdriverTraceExporter.createAndRegister(
            StackdriverTraceConfiguration.builder()
                .setProjectId(PROJECT_ID)
                .build());

      } catch (java.io.IOException e) {
        LOGGER.log(Level.WARNING, "Warning: Could not set up Stackdriver Trace", e);
      }

      // Trace every request
      TraceConfig traceConfig = Tracing.getTraceConfig();
      traceConfig.updateActiveTraceParams(
          traceConfig.getActiveTraceParams().toBuilder().setSampler(Samplers.alwaysSample())
              .build());

      return tracer;
    }


    private static Tracer getTracer() {
      return SingletonTracer.TRACER;
    }
  }


  @Override
  public void service(HttpRequest request, HttpResponse response)
      throws IOException {

    Integer duration = Integer.parseInt(request.getFirstQueryParameter("duration").orElse("5000"));
    Integer interval = Integer.parseInt(request.getFirstQueryParameter("interval").orElse("500"));
    String connType = request.getFirstQueryParameter("conn_type").orElse("pool");

    String requestId = UUID.randomUUID().toString();

    // Set up URL parameters
    String jdbcURL = String.format("jdbc:postgresql:///%s", DB_NAME);
    Properties connProps = new Properties();
    connProps.setProperty("user", DB_USER);
    connProps.setProperty("password", DB_PASSWORD);
    connProps.setProperty("socketFactory", "com.google.cloud.sql.postgres.SocketFactory");
    connProps.setProperty("cloudSqlInstance", CONNECTION_NAME);

    LOGGER.log(Level.INFO, "Started logging.");

    Runnable connectFunc;
    switch (connType) {
      case "pool":
        connectFunc = () -> connectWithPool(createPool(jdbcURL, connProps), requestId);
        break;
      case "regular":
        connectFunc = () -> connectRegular(jdbcURL, connProps, requestId);
        break;
      default:
        response.setStatusCode(HttpURLConnection.HTTP_BAD_REQUEST);
        BufferedWriter writer = response.getWriter();
        writer.write("Valid options for conn_type are: pool, regular");
        return;
    }

    ScheduledFuture<?> task = execService
        .scheduleAtFixedRate(connectFunc, 0, interval, TimeUnit.MILLISECONDS);

    try {
      TimeUnit.MILLISECONDS.sleep(duration);
    } catch (InterruptedException e) {
      response.setStatusCode(HttpURLConnection.HTTP_INTERNAL_ERROR);
      BufferedWriter writer = response.getWriter();
      writer.write(ExceptionUtils.getStackTrace(e));
      return;
    } finally {
      task.cancel(true);
    }
    
    BufferedWriter writer = response.getWriter();
    writer.write(
        String.format("Logged connection attempts to Stackdriver Trace for %d ms", duration));
  }

  private static void connectRegular(String jdbcURL, Properties connProps, String requestId) {
    Tracer tracer = SingletonTracer.getTracer();
    try (Scope rootSpan = tracer.spanBuilder("regular-connection").startScopedSpan()) {
      Span span = tracer.getCurrentSpan();
      span.putAttribute("requestId", AttributeValue.stringAttributeValue(requestId));
      Connection conn;

      // Get connection
      try (Scope getConnSpan = tracer.spanBuilder("getConnection").startScopedSpan()) {
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

  private static void connectWithPool(DataSource pool, String requestId) {
    Tracer tracer = SingletonTracer.getTracer();
    try (Scope rootSpan = tracer.spanBuilder("pool-connection").startScopedSpan()) {
      Span span = tracer.getCurrentSpan();
      span.putAttribute("requestId", AttributeValue.stringAttributeValue(requestId));
      Connection conn;

      // Get connection
      try (Scope getConnSpan = tracer.spanBuilder("getConnection").startScopedSpan()) {
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

  private static DataSource createPool(String jdbcURL, Properties props) {
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl(jdbcURL);
    config.setDataSourceProperties(props);

    config.setMaximumPoolSize(5);
    config.setMinimumIdle(5);
    config.setConnectionTimeout(TimeUnit.MILLISECONDS.convert(30, TimeUnit.SECONDS)); // 10 seconds
    config.setIdleTimeout(TimeUnit.MILLISECONDS.convert(10, TimeUnit.MINUTES)); // 10 minutes
    config.setMaxLifetime(TimeUnit.MILLISECONDS.convert(60, TimeUnit.MINUTES));

    return new HikariDataSource(config);
  }

  // Create a connection to the database
  private static void closeConnection(Connection conn) {
    Tracer tracer = SingletonTracer.getTracer();
    try (Scope closeConnSpan = tracer.spanBuilder("closeConnection").startScopedSpan()) {
      conn.close();
    } catch (Exception ex) {
      LOGGER.log(Level.WARNING, "Error occurred during connection close.", ex);
    }
  }

  // Execute a simple statement to verify the connection works.
  private static void executeStatement(Connection conn) {
    Tracer tracer = SingletonTracer.getTracer();
    try (Scope ss = tracer.spanBuilder("executeStatement").startScopedSpan()) {
      conn.prepareStatement("SELECT true;").execute();
    } catch (Exception ex) {
      Span span = tracer.getCurrentSpan();
      span.setStatus(Status.INTERNAL.withDescription(ex.toString()));
    }
  }
}
