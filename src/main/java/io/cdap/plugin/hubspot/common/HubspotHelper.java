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

/**
 * Helper class to incorporate Hubspot api interaction
 */
public class HubspotHelper {

  public HubspotPage getHupspotPage(BaseHubspotConfig baseHubspotConfig, String offset) throws IOException {
    HttpGet request = getRequest(baseHubspotConfig, offset);

    CloseableHttpResponse response = downloadPage(baseHubspotConfig, request);
    HttpEntity entity = response.getEntity();
    String result;
    if (entity != null) {
      result = EntityUtils.toString(entity);
      return parseJson(baseHubspotConfig, result);
    }
    return null;
  }

  private CloseableHttpResponse downloadPage(BaseHubspotConfig baseHubspotConfig, HttpGet request) throws IOException {
    HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();
    ArrayList<Header> clientHeaders = new ArrayList<>();
    String accessToken = baseHubspotConfig.apiKey;
    clientHeaders.add(new BasicHeader("Authorization", "Bearer " + accessToken));
    httpClientBuilder.setDefaultHeaders(clientHeaders);
    CloseableHttpClient client = httpClientBuilder.build();

    int maxTries = 3;
    int count = 0;
    while (true) {
      CloseableHttpResponse response = client.execute(request);
      StatusLine statusLine = response.getStatusLine();
      if (statusLine.getStatusCode() != 200) {
        if (500 > statusLine.getStatusCode() &&
          statusLine.getStatusCode() >= 400) {
          throw new IOException(statusLine.getReasonPhrase());
        }
        if (++count == maxTries) {
          throw new IOException(statusLine.getReasonPhrase());
        }
      }
      return response;
    }
  }

  private HttpGet getRequest(BaseHubspotConfig baseHubspotConfig, String offset) {
    URI uri = null;
    try {
      URIBuilder b = new URIBuilder(getEndpoint(baseHubspotConfig));
      if (baseHubspotConfig.startDate != null) {
        b.addParameter("start", baseHubspotConfig.startDate);
      }
      if (baseHubspotConfig.endDate != null) {
        b.addParameter("end", baseHubspotConfig.endDate);
      }
      for (String filter : baseHubspotConfig.getFilters()) {
        b.addParameter("f", filter);
      }
      if (getLimitPropertyName(baseHubspotConfig) != null) {
        b.addParameter(getLimitPropertyName(baseHubspotConfig), "100");
      }
      if (offset != null && getOffsetPropertyName(baseHubspotConfig) != null) {
        b.addParameter(getOffsetPropertyName(baseHubspotConfig), offset);
      }
      uri = b.build();
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
    return new HttpGet(uri);
  }

  private HubspotPage parseJson(BaseHubspotConfig baseHubspotConfig, String json) {
    JsonElement root = new JsonParser().parse(json);
    JsonObject jsonObject = root.getAsJsonObject();
    List<JsonElement> hubspotObjects = new ArrayList<>();
    if (getObjectApiName(baseHubspotConfig) != null) {
      JsonArray jsonObjects = jsonObject.get(getObjectApiName(baseHubspotConfig)).getAsJsonArray();
      for (JsonElement jsonElement : jsonObjects) {
        hubspotObjects.add(jsonElement);
      }
    } else {
      hubspotObjects.add(root);
    }
    Boolean hasNext = null;
    if (getMoreApiName(baseHubspotConfig) != null) {
      hasNext = jsonObject.get(getMoreApiName(baseHubspotConfig)).getAsBoolean();
    }
    String offset = null;
    if (getOffsetApiName(baseHubspotConfig) != null) {
      offset = jsonObject.get(getOffsetApiName(baseHubspotConfig)).getAsString();
      if (hasNext == null) {
        hasNext = !offset.equals("0");
      }
    }

    return new HubspotPage(hubspotObjects, baseHubspotConfig, offset, hasNext);
  }

  private String getLimitPropertyName(BaseHubspotConfig baseHubspotConfig) {
    switch (baseHubspotConfig.getObjectType()) {
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

  private String getOffsetPropertyName(BaseHubspotConfig baseHubspotConfig) {
    switch (baseHubspotConfig.getObjectType()) {
      case CONTACT_LISTS :
      case EMAIL_EVENTS :
      case RECENT_COMPANIES:
      case COMPANIES :
      case DEALS :
      case MARKETING_EMAIL :
      case PRODUCTS :
      case TICKETS :
      case ANALYTICS :
        return "offset";
      case CONTACTS :
        return "vidOffset";
      case EMAIL_SUBSCRIPTION :
      case DEAL_PIPELINES :
      default :
        return null;
    }
  }

  private String getOffsetApiName(BaseHubspotConfig baseHubspotConfig) {
    switch (baseHubspotConfig.getObjectType()) {
      case CONTACT_LISTS :
      case EMAIL_EVENTS :
      case RECENT_COMPANIES:
      case COMPANIES :
      case DEALS :
      case EMAIL_SUBSCRIPTION :
      case MARKETING_EMAIL :
      case PRODUCTS :
      case TICKETS :
      case ANALYTICS :
        return "offset";
      case CONTACTS :
        return "vid-offset";
      case DEAL_PIPELINES :
      default :
        return null;
    }
  }

  private String getMoreApiName(BaseHubspotConfig baseHubspotConfig) {
    switch (baseHubspotConfig.getObjectType()) {
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

  private String getObjectApiName(BaseHubspotConfig baseHubspotConfig) {
    switch (baseHubspotConfig.getObjectType()) {
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
        return "breakdowns";
      default :
        return null;
    }
  }

  public String getEndpoint(BaseHubspotConfig baseHubspotConfig) {
    switch (baseHubspotConfig.getObjectType()) {
      case CONTACT_LISTS :
        return "https://api.hubapi.com/contacts/v1/lists";
      case CONTACTS :
        return "https://api.hubapi.com/contacts/v1/lists/all/contacts/all";
      case EMAIL_EVENTS :
        return "https://api.hubapi.com/email/public/v1/events";
      case EMAIL_SUBSCRIPTION :
        return "https://api.hubapi.com/email/public/v1/subscriptions/timeline";
      case RECENT_COMPANIES:
        return "https://api.hubapi.com/companies/v2/companies/recent/modified";
      case COMPANIES :
        return "https://api.hubapi.com/companies/v2/companies/paged";
      case DEALS :
        return "https://api.hubapi.com/deals/v1/deal/paged";
      case DEAL_PIPELINES :
        return "https://api.hubapi.com/crm-pipelines/v1/pipelines/deals";
      case MARKETING_EMAIL :
        return "https://api.hubapi.com/marketing-emails/v1/emails";
      case PRODUCTS :
        return "https://api.hubapi.com/crm-objects/v1/objects/products/paged";
      case TICKETS :
        return "https://api.hubapi.com/crm-objects/v1/objects/tickets/paged";
      case ANALYTICS :
        return String.format("https://api.hubapi.com/analytics/v2/reports/%s/%s",
                             baseHubspotConfig.getReportType().toString(),
                             baseHubspotConfig.getTimePeriod().toString());
      default :
        return null;
    }
  }

  public static StructuredRecord transform(JsonElement value, BaseHubspotConfig config) {
    StructuredRecord.Builder builder = StructuredRecord.builder(config.getSchema());
    builder.set("objectType", config.objectType);
    builder.set("object", value.toString());
    return builder.build();
  }
}
