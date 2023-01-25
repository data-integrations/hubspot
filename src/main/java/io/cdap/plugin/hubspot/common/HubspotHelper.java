/*
 * Copyright © 2020 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.cdap.plugin.hubspot.common;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.cdap.cdap.api.data.format.StructuredRecord;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Helper class to incorporate Hubspot api interaction.
 */
public class HubspotHelper {

  private static final String AUTHORIZATION_HEADER_NAME = "Authorization";
  private static final String AUTHORIZATION_TOKEN_PREFIX = "Bearer ";
  private static final String HUBSPOT_API_KEY_PARAMETER = "hapikey";

  private static final int MAX_RETRIES_DEFAULT = 3;

  /**
   * Number of objects in one page to pull.
   */
  public static final String PAGE_SIZE = "100";

  /**
   * Return the instance of HubspotPage.
   * @param config the source hubspot config
   * @param offset the offset is string type
   * @return the instance of HubspotPage
   * @throws IOException on issues with data reading
   */
  @Nullable
  public HubspotPage getHubspotPage(SourceHubspotConfig config, String offset) throws IOException {
    CloseableHttpResponse response = executeRequestWithRetries(getRequest(config, offset));
    HttpEntity entity = response.getEntity();
    String result;
    if (entity != null) {
      result = EntityUtils.toString(entity);
      return parseJson(config, result);
    }
    return null;
  }

  /** Executes the given request until it's successful
   * or the maximum attempts number is exceeded (then {@link IOException} is thrown). */
  public static CloseableHttpResponse executeRequestWithRetries(HttpRequestBase request) throws IOException {
    return executeRequestWithRetries(request, MAX_RETRIES_DEFAULT);
  }

  /** Executes the given request until it's successful
   * or maximum retries attempts is exceeded (then {@link IOException} is thrown). */
  public static CloseableHttpResponse executeRequestWithRetries(HttpRequestBase request, int maxRetries)
          throws IOException {
    HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();
    CloseableHttpClient client = httpClientBuilder.build();

    int count = 0;
    StatusLine statusLine = null;
    while (count <= maxRetries) {
      ++count;
      CloseableHttpResponse response = client.execute(request);
      statusLine = response.getStatusLine();
      int statusCode = statusLine.getStatusCode();
      if (200 <= statusCode && statusCode < 300) {
        return response;
      }
      if (400 <= statusCode && statusCode < 500) {
        if (statusCode == 403) {
          throw new IOException("Hubspot authorization failed: " + statusLine.getReasonPhrase());
        }
        throw new IOException("The Hubspot API endpoint is not accessible: " + statusLine.getReasonPhrase());
      }
    }
    throw new IOException(String.format("Request execution max attempts (%d) exceeded, reason: '%s'",
            maxRetries + 1, statusLine == null ? "" : statusLine.getReasonPhrase()));
  }

  public static HttpRequestBase addCredentialsToRequest(HttpRequestBase request, BaseHubspotConfig config) {
    return addCredentialsToRequest(request, config.getApiKey(), config.getAccessToken());
  }

  private static HttpRequestBase addCredentialsToRequest(
          HttpRequestBase request, String apiKey, String accessToken) {
    if (accessToken != null && !accessToken.isEmpty()) {
      request.addHeader(getAuthHeader(accessToken));
      return request;
    }
    try {
      request.setURI(addApiKey(request.getURI(), apiKey));
      return request;
    } catch (URISyntaxException e) {
      throw new RuntimeException(String.format("A failure occurred while adding API key '%s' to URI '%s'.", apiKey,
                                               request.getURI()));
    }
  }

  private static URI addApiKey(URI uri, String apiKey) throws URISyntaxException {
    if (apiKey == null || apiKey.isEmpty()) {
      return uri;
    }
    return new URIBuilder(uri).setParameter(HUBSPOT_API_KEY_PARAMETER, apiKey).build();
  }

  private static Header getAuthHeader(String accessToken) {
    return new BasicHeader(AUTHORIZATION_HEADER_NAME, AUTHORIZATION_TOKEN_PREFIX + accessToken);
  }

  private HttpRequestBase getRequest(SourceHubspotConfig config, String offset) {
    URI uri;
    try {
      URIBuilder b = new URIBuilder(getEndpoint(config));
      if (config.startDate != null) {
        b.addParameter("start", config.startDate);
      }
      if (config.endDate != null) {
        b.addParameter("end", config.endDate);
      }
      for (String filter : config.getFilters()) {
        b.addParameter("f", filter);
      }
      if (getLimitPropertyName(config) != null) {
        b.addParameter(getLimitPropertyName(config), PAGE_SIZE);
      }
      if (offset != null && getOffsetPropertyName(config) != null) {
        b.addParameter(getOffsetPropertyName(config), offset);
      }
      uri = b.build();
      return addCredentialsToRequest(new HttpGet(uri), config);
    } catch (URISyntaxException e) {
      throw new RuntimeException("Can't build valid uri", e);
    }
  }

  private HubspotPage parseJson(SourceHubspotConfig sourceHubspotConfig, String json) throws IOException {
    JsonElement root = new JsonParser().parse(json);
    JsonObject jsonObject = root.getAsJsonObject();
    List<JsonElement> hubspotObjects = new ArrayList<>();
    String objectApiName = getObjectApiName(sourceHubspotConfig);
    if (objectApiName != null) {
      JsonElement jsonObjects = jsonObject.get(objectApiName);
      if (jsonObjects != null && jsonObjects.isJsonArray()) {
        JsonArray jsonObjectsArray = jsonObjects.getAsJsonArray();
        for (JsonElement jsonElement : jsonObjectsArray) {
          hubspotObjects.add(jsonElement);
        }
      } else {
        throw new IOException(
          String.format("Not expected JSON response format, '%s' element not found or wrong type",
                        objectApiName));
      }
    } else {
      hubspotObjects.add(root);
    }
    Boolean hasNext = null;
    String moreApiName = getMoreApiName(sourceHubspotConfig);
    if (moreApiName != null) {
      JsonElement hasNextElement = jsonObject.get(moreApiName);
      if (hasNextElement != null) {
          hasNext = hasNextElement.getAsBoolean();
      } else {
        throw new IOException(
          String.format("Not expected JSON response format, '%s' element not found or wrong type",
                        moreApiName));
      }
    }
    String offset = null;
    String offsetApiName = getOffsetApiName(sourceHubspotConfig);
    if (offsetApiName != null) {
      JsonElement offsetElement = jsonObject.get(offsetApiName);
      if (offsetElement != null) {
        offset = offsetElement.getAsString();
        JsonElement totalElement = jsonObject.get("total");
        if (hasNext == null && totalElement != null) {
          hasNext = !offset.equals(totalElement.getAsString()) && !offset.equals("0");
        }
      } else {
        throw new IOException(
          String.format("Not expected JSON response format, '%s' element not found or wrong type",
                        offsetApiName));
      }
    }
    return new HubspotPage(hubspotObjects, sourceHubspotConfig, offset, hasNext);
  }

  @Nullable
  private String getLimitPropertyName(SourceHubspotConfig sourceHubspotConfig) {
    switch (sourceHubspotConfig.getObjectType()) {
      case CONTACT_LISTS :
      case RECENT_COMPANIES:
      case COMPANIES :
      case CONTACTS :
        return "count";
      case EMAIL_EVENTS :
      case EMAIL_SUBSCRIPTION :
      case DEALS :
      case MARKETING_EMAIL :
      case ANALYTICS :
        return "limit";
      case DEAL_PIPELINES :
      case PRODUCTS :
      case TICKETS :
      default :
        return null;
    }
  }

  @Nullable
  private String getOffsetPropertyName(SourceHubspotConfig sourceHubspotConfig) {
    switch (sourceHubspotConfig.getObjectType()) {
      case CONTACT_LISTS :
      case EMAIL_EVENTS :
      case RECENT_COMPANIES:
      case COMPANIES :
      case DEALS :
      case MARKETING_EMAIL :
      case PRODUCTS :
      case TICKETS :
      case ANALYTICS :
      case EMAIL_SUBSCRIPTION :
        return "offset";
      case CONTACTS :
        return "vidOffset";
      case DEAL_PIPELINES :
      default :
        return null;
    }
  }

  @Nullable
  private String getOffsetApiName(SourceHubspotConfig sourceHubspotConfig) {
    switch (sourceHubspotConfig.getObjectType()) {
      case CONTACT_LISTS :
      case EMAIL_EVENTS :
      case RECENT_COMPANIES:
      case COMPANIES :
      case DEALS :
      case EMAIL_SUBSCRIPTION :
      case MARKETING_EMAIL :
      case PRODUCTS :
      case TICKETS :
        return "offset";
      case ANALYTICS :
        if (sourceHubspotConfig.getTimePeriod() != null &&
          sourceHubspotConfig.getTimePeriod().equals(TimePeriod.TOTAL)) {
          return "offset";
        }
        return null;
      case CONTACTS :
        return "vid-offset";
      case DEAL_PIPELINES :
      default :
        return null;
    }
  }

  @Nullable
  private String getMoreApiName(SourceHubspotConfig sourceHubspotConfig) {
    switch (sourceHubspotConfig.getObjectType()) {
      case CONTACT_LISTS :
      case CONTACTS :
      case COMPANIES :
        return "has-more";
      case EMAIL_EVENTS :
      case RECENT_COMPANIES:
      case DEALS :
      case EMAIL_SUBSCRIPTION :
      case PRODUCTS :
      case TICKETS :
        return "hasMore";
      case DEAL_PIPELINES :
      case MARKETING_EMAIL :
      case ANALYTICS :
      default :
        return null;
    }
  }

  @Nullable
  private String getObjectApiName(SourceHubspotConfig sourceHubspotConfig) {
    switch (sourceHubspotConfig.getObjectType()) {
      case CONTACT_LISTS :
        return "lists";
      case CONTACTS :
        return "contacts";
      case EMAIL_EVENTS :
        return "events";
      case EMAIL_SUBSCRIPTION :
        return "timeline";
      case RECENT_COMPANIES:
      case DEAL_PIPELINES :
        return "results";
      case COMPANIES :
        return "companies";
      case DEALS :
        return "deals";
      case MARKETING_EMAIL :
      case PRODUCTS :
      case TICKETS :
        return "objects";
      case ANALYTICS :
        if (sourceHubspotConfig.getTimePeriod() != null &&
          sourceHubspotConfig.getTimePeriod().equals(TimePeriod.TOTAL)) {
          return "breakdowns";
        }
        return null;
      default :
        return null;
    }
  }

  /**
   * Reurns the complete url as string.
   * @param sourceHubspotConfig the source hubspot config
   * @return the complete url as string
   */
  @Nullable
  public String getEndpoint(SourceHubspotConfig sourceHubspotConfig) {
    String apiServerUrl = sourceHubspotConfig.getApiServerUrl();
    switch (sourceHubspotConfig.getObjectType()) {
      case CONTACT_LISTS :
        return String.format("%s/contacts/v1/lists", apiServerUrl);
      case CONTACTS :
        return String.format("%s/contacts/v1/lists/all/contacts/all", apiServerUrl);
      case EMAIL_EVENTS :
        return String.format("%s/email/public/v1/events", apiServerUrl);
      case EMAIL_SUBSCRIPTION :
        return String.format("%s/email/public/v1/subscriptions/timeline", apiServerUrl);
      case RECENT_COMPANIES:
        return String.format("%s/companies/v2/companies/recent/modified", apiServerUrl);
      case COMPANIES :
        return String.format("%s/companies/v2/companies/paged", apiServerUrl);
      case DEALS :
        return String.format("%s/deals/v1/deal/paged", apiServerUrl);
      case DEAL_PIPELINES :
        return String.format("%s/crm-pipelines/v1/pipelines/deals", apiServerUrl);
      case MARKETING_EMAIL :
        return String.format("%s/marketing-emails/v1/emails", apiServerUrl);
      case PRODUCTS :
        return String.format("%s/crm-objects/v1/objects/products/paged", apiServerUrl);
      case TICKETS :
        return String.format("%s/crm-objects/v1/objects/tickets/paged", apiServerUrl);
      case ANALYTICS :
        return String.format("%s/analytics/v2/reports/%s/%s", apiServerUrl,
                             sourceHubspotConfig.getReportEndpoint().getStringValue(),
                             sourceHubspotConfig.getTimePeriod().getStringValue());
      default :
        return null;
    }
  }

  /**
   * Returns the StructuredRecord.
   * @param value the value is string type
   * @param config the source hubspot config
   * @return the StructuredRecord
   */
  public static StructuredRecord transform(String value, SourceHubspotConfig config) {
    StructuredRecord.Builder builder = StructuredRecord.builder(config.getSchema());
    builder.set("objectType", config.objectType);
    builder.set("object", value);
    return builder.build();
  }
}
