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

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import io.cdap.cdap.api.data.format.StructuredRecord;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.junit.Assert;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

/**
 * Helper class to incorporate Hubspot api interaction
 */
public class TestingHelper {

  public static void checkAndDelete(SourceHubspotConfig config, boolean assertation)
    throws IOException, URISyntaxException, InterruptedException {
    if (assertation) {
      Thread.sleep(20000);
    }
    boolean exist = false;
    HubspotPagesIterator hubspotPagesIterator = new HubspotPagesIterator(config);
    while (hubspotPagesIterator.hasNext()) {
      JsonElement record = hubspotPagesIterator.next();
      String id = getId(config, record);
      if (record.toString().contains("testName") || getDetiles(config, id).contains("testName")) {
        exist = true;
        deleteObject(config, id);
      }

    }
    if (assertation) {
      Assert.assertEquals(true, exist);
    }
  }

  private static String getDetiles(SourceHubspotConfig config, String id) throws URISyntaxException, IOException {
    HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();

    ArrayList<Header> clientHeaders = new ArrayList<>();

    httpClientBuilder.setDefaultHeaders(clientHeaders);

    CloseableHttpClient client = httpClientBuilder.build();
    URIBuilder builder = null;
    switch (config.getObjectType()) {
      case CONTACT_LISTS:
        builder = new URIBuilder("https://api.hubapi.com/contacts/v1/lists/" + id);
        break;
      case CONTACTS:
        builder = new URIBuilder("https://api.hubapi.com/contacts/v1/contact/vid/" + id);
        break;
      case COMPANIES:
        builder = new URIBuilder("https://api.hubapi.com/companies/v2/companies/" + id);
        break;
      case DEALS:
        builder = new URIBuilder("https://api.hubapi.com/deals/v1/deal/" + id);
        break;
      case DEAL_PIPELINES:
        builder = new URIBuilder("https://api.hubapi.com/crm-pipelines/v1/pipelines/deal/" + id);
        break;
      case MARKETING_EMAIL:
        builder = new URIBuilder("https://api.hubapi.com/marketing-emails/v1/emails/" + id);
        break;
      case PRODUCTS:
        builder = new URIBuilder("https://api.hubapi.com/crm-objects/v1/objects/products/" + id);
        builder.setParameter("properties", "name");
        break;
      case TICKETS:
        builder = new URIBuilder("https://api.hubapi.com/crm-objects/v1/objects/tickets/" + id);
        builder.setParameter("properties", "subject");
        break;
    }
    builder.setParameter("hapikey", config.apiKey);

    HttpGet request = new HttpGet(builder.build());

    CloseableHttpResponse response = client.execute(request);

    HttpEntity entity = response.getEntity();
    if (entity != null) {
      String result = EntityUtils.toString(entity);
      Assert.assertTrue(result, response.getStatusLine().getStatusCode() < 300);
      return result;
    }
    Assert.assertTrue(response.getStatusLine().toString(), response.getStatusLine().getStatusCode() < 300);
    return null;
  }

  private static void deleteObject(SourceHubspotConfig config, String id) throws URISyntaxException, IOException {
    HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();

    ArrayList<Header> clientHeaders = new ArrayList<>();

    httpClientBuilder.setDefaultHeaders(clientHeaders);

    CloseableHttpClient client = httpClientBuilder.build();
    URIBuilder builder = null;
    switch (config.getObjectType()) {
      case CONTACT_LISTS:
        builder = new URIBuilder("https://api.hubapi.com/contacts/v1/lists/" + id);
        break;
      case CONTACTS:
        builder = new URIBuilder("https://api.hubapi.com/contacts/v1/contact/vid/" + id);
        break;
      case COMPANIES:
        builder = new URIBuilder("https://api.hubapi.com/companies/v2/companies/" + id);
        break;
      case DEALS:
        builder = new URIBuilder("https://api.hubapi.com/deals/v1/deal/" + id);
        break;
      case DEAL_PIPELINES:
        builder = new URIBuilder("https://api.hubapi.com/crm-pipelines/v1/pipelines/deal/" + id);
        break;
      case MARKETING_EMAIL:
        builder = new URIBuilder("https://api.hubapi.com/marketing-emails/v1/emails/" + id);
        break;
      case PRODUCTS:
        builder = new URIBuilder("https://api.hubapi.com/crm-objects/v1/objects/products/" + id);
        break;
      case TICKETS:
        builder = new URIBuilder("https://api.hubapi.com/crm-objects/v1/objects/tickets/" + id);
        break;
    }
    builder.setParameter("hapikey", config.apiKey);

    HttpDelete request = new HttpDelete(builder.build());

    CloseableHttpResponse response = client.execute(request);
    HttpEntity entity = response.getEntity();
    if (entity != null) {
      String result = EntityUtils.toString(entity);
      Assert.assertTrue(result, response.getStatusLine().getStatusCode() < 300);
    }
    Assert.assertTrue(response.getStatusLine().toString(), response.getStatusLine().getStatusCode() < 300);
  }

  private static String getId(SourceHubspotConfig config, JsonElement record) {
    switch (config.getObjectType()) {
      case CONTACT_LISTS:
        return record.getAsJsonObject().get("listId").getAsString();
      case CONTACTS:
        return record.getAsJsonObject().get("vid").getAsString();
      case COMPANIES:
        return record.getAsJsonObject().get("companyId").getAsString();
      case DEALS:
        return record.getAsJsonObject().get("dealId").getAsString();
      case DEAL_PIPELINES:
        return record.getAsJsonObject().get("pipelineId").getAsString();
      case MARKETING_EMAIL:
        return record.getAsJsonObject().get("id").getAsString();
      case PRODUCTS:
        return record.getAsJsonObject().get("objectId").getAsString();
      case TICKETS:
        return record.getAsJsonObject().get("objectId").getAsString();
    }
    return null;
  }


  public static void checkExist(SourceHubspotConfig config, List<StructuredRecord> records, boolean expected)
    throws IOException, URISyntaxException {
    boolean exist = false;
    for (StructuredRecord record : records) {
      String id = getId(config, new JsonParser().parse(record.get("object").toString()));
      if (record.get("object").toString().contains("testName") || getDetiles(config, id).contains("testName")) {
        exist = true;
      }
    }
    Assert.assertEquals(expected, exist);
  }

  public static void createTestObject(SourceHubspotConfig config, String object)
    throws URISyntaxException, IOException, InterruptedException {
    checkAndDelete(config, false);
    HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();
    ArrayList<Header> clientHeaders = new ArrayList<>();
    clientHeaders.add(new BasicHeader("Content-Type", "application/json"));
    httpClientBuilder.setDefaultHeaders(clientHeaders);

    CloseableHttpClient client = httpClientBuilder.build();
    URIBuilder builder = null;
    switch (config.getObjectType()) {
      case CONTACT_LISTS:
        builder = new URIBuilder("https://api.hubapi.com/contacts/v1/lists/");
        break;
      case CONTACTS:
        builder = new URIBuilder("https://api.hubapi.com/contacts/v1/contact/");
        break;
      case COMPANIES:
        builder = new URIBuilder("https://api.hubapi.com/companies/v2/companies/");
        break;
      case DEALS:
        builder = new URIBuilder("https://api.hubapi.com/deals/v1/deal/");
        break;
      case DEAL_PIPELINES:
        builder = new URIBuilder("https://api.hubapi.com/crm-pipelines/v1/pipelines/deal/");
        break;
      case MARKETING_EMAIL:
        builder = new URIBuilder("https://api.hubapi.com/marketing-emails/v1/emails/");
        break;
      case PRODUCTS:
        builder = new URIBuilder("https://api.hubapi.com/crm-objects/v1/objects/products/");
        break;
      case TICKETS:
        builder = new URIBuilder("https://api.hubapi.com/crm-objects/v1/objects/tickets/");
        break;
    }
    builder.setParameter("hapikey", config.apiKey);
    HttpPost request = new HttpPost(builder.build());

    request.setEntity(new StringEntity(object));

    CloseableHttpResponse response = client.execute(request);
    HttpEntity entity = response.getEntity();
    if (entity != null) {
      String result = EntityUtils.toString(entity);
      Assert.assertTrue(result, response.getStatusLine().getStatusCode() < 300);
    }
    Assert.assertTrue(response.getStatusLine().toString(), response.getStatusLine().getStatusCode() < 300);
    Thread.sleep(20000);
  }

}
