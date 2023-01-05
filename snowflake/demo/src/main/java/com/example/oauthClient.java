package com.example;

import java.io.IOException;

import static java.net.http.HttpRequest.BodyPublishers.noBody;
import static java.net.http.HttpRequest.BodyPublishers.ofString;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpRequest.BodyPublisher;
import java.nio.charset.StandardCharsets;
import java.security.*;
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

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.net.ssl.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.net.http.*;

public class oauthClient {

    private static final Logger LOGGER = LogManager.getLogger(oauthClient.class);

    private final long DEFAULT_TIMEOUT_MS = 60000;

    private HttpClient httpClient;
    protected final String server;
    private String token;

    private int retryTimes;
    private int backoffTimesMs;

    public oauthClient(String server) throws IOException {
        this.server = server;
    }

    public String doPost(String url, String body) throws IOException, URISyntaxException {

        HttpRequest request = postRequest(url, body, DEFAULT_TIMEOUT_MS);

        return doRequest(request);
    }

    private HttpRequest postRequest(String url, String body, long timeoutMs) {
        return setupARequest(url, timeoutMs).POST(ofString(body)).build();
    }

    private HttpRequest.Builder setupARequest(String url, long timeoutMs) {
        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(server + url))
                .timeout(Duration.ofMillis(timeoutMs))
                .header("accept", " application/json")
                .headers("Content-Type", "application/x-www-form-urlencoded");

        return builder;
    }

    private String doRequest(HttpRequest request) throws IOException {
        // LOGGER.debug("method: " + request.method() + " request.uri: " +
        // request.uri());
        String result = "";
        try {
            var handler = HttpResponse.BodyHandlers.ofString();
            HttpResponse<String> response = sendAsync(request, handler).get();
            // LOGGER.debug("method: " + request.method() + " response: " + response);
            int statusCode = response.statusCode();
            if (statusCode < 200 || statusCode > 299) {
                String body = response.body() != null ? response.body() : "";
                throw new IOException(
                        "Something happened with the connection, response status code: "
                                + statusCode
                                + " body: "
                                + body);
            }

            if (response.body() != null) {
                result = response.body();
            }
        } catch (Exception ex) {
            throw new IOException(ex);
        }
        return result;
    }

    private CompletableFuture<HttpResponse<String>> sendAsync(
            HttpRequest request, HttpResponse.BodyHandler<String> handler) {
        httpClient = HttpClient.newBuilder().build();
        return httpClient
                .sendAsync(request, handler)
                .handleAsync((response, throwable) -> tryResend(request, handler, 1, response, throwable))
                .thenCompose(Function.identity());
    }

    private CompletableFuture<HttpResponse<String>> tryResend(
            HttpRequest request,
            HttpResponse.BodyHandler<String> handler,
            int count,
            HttpResponse<String> response,
            Throwable throwable) {

        if (shouldRetry(response, throwable, count)) {
            System.out.println("shouldRetry: count=" + count);
            return httpClient
                    .sendAsync(request, handler)
                    .handleAsync((r, t) -> tryResend(request, handler, count + 1, r, t))
                    .thenCompose(Function.identity());
        } else if (throwable != null) {
            return CompletableFuture.failedFuture(throwable);
        } else {
            return CompletableFuture.completedFuture(response);
        }
    }

    private boolean shouldRetry(HttpResponse<String> response, Throwable throwable, int count) {
        if (response != null && !isRetrievableStatusCode(response) || count >= retryTimes)
            return false;
        var backoffTime = backoff(count);
        // LOGGER.debug("Sleeping before retry on " + backoffTime + " ms");
        return true;
    }

    private HttpRequest.BodyPublisher getParamsUrlEncoded(Map<String, String> parameters) {
        String urlEncoded = parameters.entrySet()
                .stream()
                .map(e -> e.getKey() + "=" + URLEncoder.encode(e.getValue(), StandardCharsets.UTF_8))
                .collect(Collectors.joining("&"));
        return HttpRequest.BodyPublishers.ofString(urlEncoded);
    }

    private <T> boolean isRetrievableStatusCode(HttpResponse<T> response) {
        return response.statusCode() == 429 || response.statusCode() == 503;
    }

    private int backoff(int count) {
        int backoff = 0;
        try {
            backoff = this.backoffTimesMs + (10 * count);
            Thread.sleep(backoff);
        } catch (Exception ex) {
            LOGGER.error(ex);
        }
        return backoff;
    }

    public String baseUrl() {
        return server;
    }
}
