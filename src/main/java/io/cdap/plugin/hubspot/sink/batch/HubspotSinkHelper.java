/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES O R CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.cdap.plugin.hubspot.sink.batch;

import org.apache.http.client.utils.URIBuilder;
import org.awaitility.Awaitility;
import org.awaitility.Duration;
import org.awaitility.core.ConditionTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

import static org.awaitility.pollinterval.IterativePollInterval.iterative;

/**
 * Helper class to incorporate Hubspot sink api interaction
 */
public class HubspotSinkHelper {

  private static final Logger LOG = LoggerFactory.getLogger(HubspotSinkHelper.class);

  public static final int MAX_RETRY_DURATION = 10;

  public void executeHTTPService(String input, SinkHubspotConfig config) throws Exception {
    HttpService httpService = new HttpService(input, config);
    try {
      Awaitility
        .await().with()
        .pollInterval(iterative(duration -> duration.multiply(2)).startDuration(Duration.ONE_SECOND))
        .pollDelay(0L, TimeUnit.MILLISECONDS)
        .timeout(MAX_RETRY_DURATION, TimeUnit.SECONDS)
        .until(httpService::postInput);
    } catch (ConditionTimeoutException ex) {
      throw httpService.getException();
    }
  }

  @Nullable
  public String getSinkEndpoint(SinkHubspotConfig sinkHubspotConfig) {
    String apiServerUrl = sinkHubspotConfig.getApiServerUrl();
    switch (sinkHubspotConfig.getObjectType()) {
      case CONTACT_LISTS:
        return String.format("%s/contacts/v1/lists", apiServerUrl);
      case CONTACTS:
        return String.format("%s/contacts/v1/contact", apiServerUrl);
      case COMPANIES:
        return String.format("%s/companies/v2/companies", apiServerUrl);
      case DEALS:
        return String.format("%s/deals/v1/deal", apiServerUrl);
      case DEAL_PIPELINES:
        return String.format("%s/crm-pipelines/v1/pipelines/deals", apiServerUrl);
      case MARKETING_EMAIL:
        return String.format("%s/marketing-emails/v1/emails", apiServerUrl);
      case PRODUCTS:
        return String.format("%s/crm-objects/v1/objects/products", apiServerUrl);
      case TICKETS:
        return String.format("%s/crm-objects/v1/objects/tickets", apiServerUrl);
      case ANALYTICS:
      case EMAIL_EVENTS:
      case EMAIL_SUBSCRIPTION:
      case RECENT_COMPANIES:
      default:
        return null;
    }
  }

  private class HttpService {

    private Exception exception = null;

    private String input;
    private SinkHubspotConfig config;

    private HttpService(String input, SinkHubspotConfig config) {
      this.input = input;
      this.config = config;
    }

    public Exception getException() {
      return exception;
    }

    public boolean postInput() {
      exception = null;
      HttpURLConnection conn = null;
      Map<String, String> headers = new HashMap<>();
      try {
        URIBuilder b = new URIBuilder(getSinkEndpoint(config));
        if (config.apiKey != null) {
          b.addParameter("hapikey", config.apiKey);
        }
        URL url = new URL(b.build().toString());
        conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        for (Map.Entry<String, String> propertyEntry : headers.entrySet()) {
          conn.addRequestProperty(propertyEntry.getKey(), propertyEntry.getValue());
        }
        if (!headers.containsKey("Content-Type")) {
          conn.addRequestProperty("Content-Type", "application/json");
        }

        if (input.length() > 0) {
          conn.setDoOutput(true);
          try (OutputStream outputStream = conn.getOutputStream()) {
            outputStream.write(input.getBytes());
          }
        }
        int responseCode = conn.getResponseCode();
        if (responseCode >= 300) {
          BufferedReader br = new BufferedReader(new InputStreamReader((conn.getErrorStream())));
          StringBuilder sb = new StringBuilder();
          String output;
          while ((output = br.readLine()) != null) {
            sb.append(output);
          }
          exception = new IllegalStateException(String.format("Received error response. Response code: %s %s",
                                                              responseCode, sb.toString()));
          return false;
        }
      } catch (MalformedURLException | ProtocolException e) {
        throw new IllegalStateException("Error opening url connection. Reason: " + e.getMessage(), e);
      } catch (Exception e) {
        LOG.warn("Error making {} request to url {}",
                 config.getObjectType(), getSinkEndpoint(config));
        exception = e;
        return false;
      } finally {
        if (conn != null) {
          conn.disconnect();
        }
      }
      return true;
    }
  }
}
