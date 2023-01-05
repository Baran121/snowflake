package customSnowFlake.internal;

import static org.mule.runtime.extension.api.annotation.param.MediaType.ANY;

import org.mule.runtime.extension.api.annotation.param.MediaType;
import org.mule.runtime.extension.api.annotation.param.Config;
import org.mule.runtime.extension.api.annotation.param.Connection;

import java.sql.*;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.json.*;

import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpRequest.BodyPublisher;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.logging.LogManager;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.HashMap;

/**
 * This class is a container for operations, every public method in this class
 * will be taken as an extension operation.
 */
public class CustomSnowFlakeOperations {

  /**
   * Example of an operation that uses the configuration and a connection instance
   * to perform some action.
   */
  // private static final Logger LOGGER =
  // LogManager.getLogger(CustomSnowFlakeOperations.class);

  @MediaType(value = ANY, strict = false)
  public byte[] executeQuery(@Connection CustomSnowFlakeConnection connection, String sqlText) {
    JSONArray result = null;
    HttpClient httpClient;
    try {
      oauthClient oClient = new oauthClient("");

      Map<String, String> params = new HashMap<String, String>();
      params.put("client_id", connection.getConfig().getClientId());
      params.put("client_secret", connection.getConfig().getClientSecret());
      params.put("grant_type", "client_credentials");
      params.put("scope", connection.getConfig().getScope());

      String form = params.entrySet()
          .stream()
          .map(e -> e.getKey() + "=" + URLEncoder.encode(e.getValue(), StandardCharsets.UTF_8))
          .collect(Collectors.joining("&"));

      String sToken = oClient.doPost(
          "https://login.microsoftonline.com/1c46ed43-151a-44b6-8aa6-bdfbc619aeae/oauth2/v2.0/token",
          form);
      JSONObject jsonObj = new JSONObject(sToken);
      String token = jsonObj.getString("access_token");
      java.sql.Connection dbConnection = getConnection(connection, token);
      Statement statement = dbConnection.createStatement();
      // statement.executeUpdate(sqlTexts[0]);
      statement.executeUpdate("use database " + connection.getConfig().getDatabase());
      statement.executeUpdate("use schema " + connection.getConfig().getSchema());

      // statement.executeUpdate("create or replace table demo(C1 STRING)");

      ResultSet resultSet = statement.executeQuery(sqlText);
      ResultSetMetaData md = resultSet.getMetaData();
      int numCols = md.getColumnCount();
      List<String> colNames = IntStream.range(0, numCols)
          .mapToObj(i -> {
            try {
              return md.getColumnName(i + 1);
            } catch (SQLException e) {
              e.printStackTrace();
              return "?";
            }
          })
          .collect(Collectors.toList());

      List<Map<String, String>> listOfMaps = new ArrayList<Map<String, String>>();

      result = new JSONArray();
      while (resultSet.next()) {
        JSONObject row = new JSONObject();
        colNames.forEach(cn -> {
          try {
            row.put(cn, resultSet.getObject(cn));
          } catch (JSONException | SQLException e) {
            e.printStackTrace();
          }
        });
        result.put(row);
      }
      statement.close();
      dbConnection.close();
      return result.toByteArray();
    } catch (Exception ex) {
      // LOGGER.debug("Exception is : " + ex.toString());
      return null;
    }

  }

  private static String getOAuthToken(CustomSnowFlakeConnection connection) {

    return "";
  }

  private static java.sql.Connection getConnection(CustomSnowFlakeConnection connection, String sToken)
      throws SQLException {

    // build connection properties
    Properties properties = new Properties();
    properties.put("user", connection.getConfig().getUser());

    // properties.put("role", "session:role:sysadmin");
    properties.put("authenticator", "oauth");
    properties.put("token", sToken);
    properties.put("account", connection.getConfig().getAccount()); // Pass Snowflake account name
    properties.put("warehouse", connection.getConfig().getWarehouse()); // Target warehouse name
    properties.put("db", connection.getConfig().getDatabase()); // Target database name
    properties.put("schema", connection.getConfig().getSchema()); // Target schema name
    properties.put("tracing", "ALL");
    // properties.put("role", "SYSADMIN");

    // create a new connection
    String connectStr = System.getenv("SF_JDBC_CONNECT_STRING");
    // use the default connection string if it is not set in environment
    if (connectStr == null) {
      connectStr = "jdbc:snowflake://" + connection.getConfig().getAccount(); // replace accountName
                                                                              // with your account
      // name
    }
    return java.sql.DriverManager.getConnection(connectStr, properties);
  }

}
