package com.example;

import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpRequest.BodyPublisher;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
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

import org.json.*;

/**
 * Hello world!
 */
public final class App {
    private App() {
    }

    /**
     * Says hello to the world.
     * 
     * @param args The arguments of the program.
     */

    public static void main(String[] args) throws Exception {

        HttpClient httpClient;
        oauthClient oClient = new oauthClient("");

        Map<String, String> params = new HashMap<String, String>();
        params.put("client_id", "a379a818-abf7-4a0b-b6af-21ea1709c1e7");
        params.put("client_secret", "ejr8Q~a7Rsw_iwjzdgo2iqV2_VmMIPc2zPRYYbz_");
        params.put("grant_type", "client_credentials");
        params.put("scope", "api://a379a818-abf7-4a0b-b6af-21ea1709c1e7/.default");

        String form = params.entrySet()
                .stream()
                .map(e -> e.getKey() + "=" + URLEncoder.encode(e.getValue(), StandardCharsets.UTF_8))
                .collect(Collectors.joining("&"));

        String sToken = oClient.doPost(
                "https://login.microsoftonline.com/1c46ed43-151a-44b6-8aa6-bdfbc619aeae/oauth2/v2.0/token",
                form);

        JSONObject jsonObj = new JSONObject(sToken);
        String token = jsonObj.getString("access_token");

        Connection dbConnection = getConnection(token);
        System.out.println("Done creating JDBC connectionn");
        // create statement
        System.out.println("Create JDBC statement");
        Statement statement = dbConnection.createStatement();
        System.out.println("Done creating JDBC statementn");
        statement.executeUpdate("use role sysadmin");
        statement.executeUpdate("use warehouse COMPUTE_WH;");
        // statement.executeUpdate("create or replace database apidemo1");
        statement.executeUpdate("use database apidemo1");
        // statement.executeUpdate("create or replace schema apitesting");
        statement.executeUpdate("use schema apitesting");
        // statement.executeUpdate("create or replace table demo(C1 STRING)");

        statement.executeUpdate("insert into demo values ('Here, I am')");
        ResultSet resultSet = statement.executeQuery("select  * from demo");
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

        JSONArray result = new JSONArray();
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
        System.out.println(result.toString());
        statement.close();
        dbConnection.close();

    }

    private static Connection getConnection(String token)
            throws SQLException {

        // build connection properties
        Properties properties = new Properties();
        properties.put("user", "cc87dca4-e174-4dc4-a329-19a992fa29f4");

        // properties.put("role", "session:role:sysadmin");
        properties.put("authenticator", "oauth");
        properties.put("token",
                token); // Pass
        properties.put("account", "anjsjzf-iy21662.snowflakecomputing.com"); // Pass Snowflake account name
        properties.put("warehouse", "COMPUTE_WH"); // Target warehouse name
        properties.put("db", "myown"); // Target database name
        properties.put("schema", "mysch"); // Target schema name
        properties.put("tracing", "ALL");
        // properties.put("role", "SYSADMIN");

        // create a new connection
        String connectStr = System.getenv("SF_JDBC_CONNECT_STRING");
        // use the default connection string if it is not set in environment
        if (connectStr == null) {
            connectStr = "jdbc:snowflake://anjsjzf-iy21662.snowflakecomputing.com"; // replace accountName
                                                                                    // with your account
            // name
        }
        return DriverManager.getConnection(connectStr, properties);
    }
}
