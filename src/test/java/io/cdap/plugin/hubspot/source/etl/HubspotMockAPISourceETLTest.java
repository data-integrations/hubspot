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
package io.cdap.plugin.hubspot.source.etl;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.datapipeline.SmartWorkflow;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.mock.batch.MockSink;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;

import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.WorkflowManager;
import io.cdap.plugin.hubspot.common.SourceHubspotConfig;
import io.cdap.plugin.hubspot.source.batch.HubspotBatchSource;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public abstract class HubspotMockAPISourceETLTest extends BaseHubspotETLTest {
  @Rule
  public WireMockRule wireMockRule = new WireMockRule(9854);

  @Test
  public void testContactLists() throws Exception {
    SourceHubspotConfig properties = new SourceHubspotConfig(testName.getMethodName(),
                                                             getServerAddress(),
                                                             "Contact Lists",
                                                             "some-api-key",
                                                             null,
                                                             null,
                                                             null,
                                                             null,
                                                             null,
                                                             null,
                                                             null,
                                                             null,
                                                             null);
    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/contacts/v1/lists?count=100&hapikey=some-api-key"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testContactListsP1.json"))));
    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/contacts/v1/lists?count=100&offset=2&hapikey=some-api-key"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testContactListsP2.json"))));
    List<StructuredRecord> records = getPipelineResults(properties, 4);
    Assert.assertEquals(4, records.size());
    for (int i = 0; i < records.size(); i++) {
      Assert.assertEquals("Contact Lists", records.get(i).get("objectType"));
      Assert.assertEquals(String.format("{\"testobj\":%s}", i), records.get(i).get("object"));
    }
  }

  @Test
  public void testContacts() throws Exception {
    SourceHubspotConfig properties = new SourceHubspotConfig(testName.getMethodName(),
                                                             getServerAddress(),
                                                             "Contacts",
                                                             "some-api-key",
                                                             null,
                                                             null,
                                                             null,
                                                             null,
                                                             null,
                                                             null,
                                                             null,
                                                             null,
                                                             null);
    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/contacts/v1/lists/all/contacts/all?count=100&hapikey=some-api-key"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testContactsP1.json"))));
    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/contacts/v1/lists/all/contacts/all?count=100&vidOffset=2&hapikey=some-api-key"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testContactsP2.json"))));
    List<StructuredRecord> records = getPipelineResults(properties, 4);
    Assert.assertEquals(4, records.size());
    for (int i = 0; i < records.size(); i++) {
      Assert.assertEquals("Contacts", records.get(i).get("objectType"));
      Assert.assertEquals(String.format("{\"testobj\":%s}", i), records.get(i).get("object"));
    }
  }

  @Test
  public void testEmailEvents() throws Exception {
    SourceHubspotConfig properties = new SourceHubspotConfig(testName.getMethodName(),
                                                             getServerAddress(),
                                                             "Email Events",
                                                             "some-api-key",
                                                             null,
                                                             null,
                                                             null,
                                                             null,
                                                             null,
                                                             null,
                                                             null,
                                                             null,
                                                             null);

    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/email/public/v1/events?limit=100&hapikey=some-api-key"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testEmailEventsP1.json"))));
    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/email/public/v1/events?limit=100&offset=CgoY__________9_&hapikey=some-api-key"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testEmailEventsP2.json"))));
    List<StructuredRecord> records = getPipelineResults(properties, 4);
    Assert.assertEquals(4, records.size());
    for (int i = 0; i < records.size(); i++) {
      Assert.assertEquals("Email Events", records.get(i).get("objectType"));
      Assert.assertEquals(String.format("{\"testobj\":%s}", i), records.get(i).get("object"));
    }
  }

  @Test
  public void testEmailSubscription() throws Exception {
    SourceHubspotConfig properties = new SourceHubspotConfig(testName.getMethodName(),
                                                             getServerAddress(),
                                                             "Email Subscription",
                                                             "some-api-key",
                                                             null,
                                                             null,
                                                             null,
                                                             null,
                                                             null,
                                                             null,
                                                             null,
                                                             null,
                                                             null);

    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/email/public/v1/subscriptions/timeline?limit=100&hapikey=some-api-key"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testEmailSubscriptionP1.json"))));
    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/email/public/v1/subscriptions/timeline" +
                            "?limit=100&offset=CP__________fw&hapikey=some-api-key"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testEmailSubscriptionP2.json"))));
    List<StructuredRecord> records = getPipelineResults(properties, 4);
    Assert.assertEquals(4, records.size());
    for (int i = 0; i < records.size(); i++) {
      Assert.assertEquals("Email Subscription", records.get(i).get("objectType"));
      Assert.assertEquals(String.format("{\"testobj\":%s}", i), records.get(i).get("object"));
    }
  }

  @Test
  public void testRecentCompanies() throws Exception {
    SourceHubspotConfig properties = new SourceHubspotConfig(testName.getMethodName(),
                                                             getServerAddress(),
                                                             "Recent Companies",
                                                             "some-api-key",
                                                             null,
                                                             null,
                                                             null,
                                                             null,
                                                             null,
                                                             null,
                                                             null,
                                                             null,
                                                             null);

    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/companies/v2/companies/recent/modified?count=100&hapikey=some-api-key"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testRecentCompaniesP1.json"))));
    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/companies/v2/companies/recent/modified?count=100&offset=2&hapikey=some-api-key"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testRecentCompaniesP2.json"))));
    List<StructuredRecord> records = getPipelineResults(properties, 4);
    Assert.assertEquals(4, records.size());
    for (int i = 0; i < records.size(); i++) {
      Assert.assertEquals("Recent Companies", records.get(i).get("objectType"));
      Assert.assertEquals(String.format("{\"testobj\":%s}", i), records.get(i).get("object"));
    }
  }

  @Test
  public void testCompanies() throws Exception {
    SourceHubspotConfig properties = new SourceHubspotConfig(testName.getMethodName(),
                                                             getServerAddress(),
                                                             "Companies",
                                                             "some-api-key",
                                                             null,
                                                             null,
                                                             null,
                                                             null,
                                                             null,
                                                             null,
                                                             null,
                                                             null,
                                                             null);

    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/companies/v2/companies/paged?count=100&hapikey=some-api-key"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testCompaniesP1.json"))));
    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/companies/v2/companies/paged?count=100&offset=2&hapikey=some-api-key"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testCompaniesP2.json"))));
    List<StructuredRecord> records = getPipelineResults(properties, 4);
    Assert.assertEquals(4, records.size());
    for (int i = 0; i < records.size(); i++) {
      Assert.assertEquals("Companies", records.get(i).get("objectType"));
      Assert.assertEquals(String.format("{\"testobj\":%s}", i), records.get(i).get("object"));
    }
  }

  @Test
  public void testDeals() throws Exception {
    SourceHubspotConfig properties = new SourceHubspotConfig(testName.getMethodName(),
                                                             getServerAddress(),
                                                             "Deals",
                                                             "some-api-key",
                                                             null,
                                                             null,
                                                             null,
                                                             null,
                                                             null,
                                                             null,
                                                             null,
                                                             null,
                                                             null);

    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/deals/v1/deal/paged?limit=100&hapikey=some-api-key"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testDealsP1.json"))));
    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/deals/v1/deal/paged?limit=100&offset=2&hapikey=some-api-key"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testDealsP2.json"))));
    List<StructuredRecord> records = getPipelineResults(properties, 4);
    Assert.assertEquals(4, records.size());
    for (int i = 0; i < records.size(); i++) {
      Assert.assertEquals("Deals", records.get(i).get("objectType"));
      Assert.assertEquals(String.format("{\"testobj\":%s}", i), records.get(i).get("object"));
    }
  }

  @Test
  public void testDealPipelines() throws Exception {
    SourceHubspotConfig properties = new SourceHubspotConfig(testName.getMethodName(),
                                                             getServerAddress(),
                                                             "Deal Pipelines",
                                                             "some-api-key",
                                                             null,
                                                             null,
                                                             null,
                                                             null,
                                                             null,
                                                             null,
                                                             null,
                                                             null,
                                                             null);

    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/crm-pipelines/v1/pipelines/deals?hapikey=some-api-key"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testDealPipelinesP1.json"))));
    List<StructuredRecord> records = getPipelineResults(properties, 4);
    Assert.assertEquals(4, records.size());
    for (int i = 0; i < records.size(); i++) {
      Assert.assertEquals("Deal Pipelines", records.get(i).get("objectType"));
      Assert.assertEquals(String.format("{\"testobj\":%s}", i), records.get(i).get("object"));
    }
  }

  @Test
  public void testMarketingEmail() throws Exception {
    SourceHubspotConfig properties = new SourceHubspotConfig(testName.getMethodName(),
                                                             getServerAddress(),
                                                             "Marketing Email",
                                                             "some-api-key",
                                                             null,
                                                             null,
                                                             null,
                                                             null,
                                                             null,
                                                             null,
                                                             null,
                                                             null,
                                                             null);
    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/marketing-emails/v1/emails?limit=100&hapikey=some-api-key"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testMarketingEmailP1.json"))));
    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/marketing-emails/v1/emails?limit=100&offset=2&hapikey=some-api-key"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testMarketingEmailP2.json"))));
    List<StructuredRecord> records = getPipelineResults(properties, 4);
    Assert.assertEquals(4, records.size());
    for (int i = 0; i < records.size(); i++) {
      Assert.assertEquals("Marketing Email", records.get(i).get("objectType"));
      Assert.assertEquals(String.format("{\"testobj\":%s}", i), records.get(i).get("object"));
    }
  }

  @Test
  public void testProducts() throws Exception {
    SourceHubspotConfig properties = new SourceHubspotConfig(testName.getMethodName(),
                                                             getServerAddress(),
                                                             "Products",
                                                             "some-api-key",
                                                             null,
                                                             null,
                                                             null,
                                                             null,
                                                             null,
                                                             null,
                                                             null,
                                                             null,
                                                             null);

    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/crm-objects/v1/objects/products/paged?hapikey=some-api-key"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testProductsP1.json"))));
    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/crm-objects/v1/objects/products/paged?offset=2&hapikey=some-api-key"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testProductsP2.json"))));
    List<StructuredRecord> records = getPipelineResults(properties, 4);
    Assert.assertEquals(4, records.size());
    for (int i = 0; i < records.size(); i++) {
      Assert.assertEquals("Products", records.get(i).get("objectType"));
      Assert.assertEquals(String.format("{\"testobj\":%s}", i), records.get(i).get("object"));
    }
  }

  @Test
  public void testTickets() throws Exception {
    SourceHubspotConfig properties = new SourceHubspotConfig(testName.getMethodName(),
                                                             getServerAddress(),
                                                             "Tickets",
                                                             "some-api-key",
                                                             null,
                                                             null,
                                                             null,
                                                             null,
                                                             null,
                                                             null,
                                                             null,
                                                             null,
                                                             null);

    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/crm-objects/v1/objects/tickets/paged?hapikey=some-api-key"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testTicketsP1.json"))));
    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/crm-objects/v1/objects/tickets/paged?offset=2&hapikey=some-api-key"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testTicketsP2.json"))));
    List<StructuredRecord> records = getPipelineResults(properties, 4);
    Assert.assertEquals(4, records.size());
    for (int i = 0; i < records.size(); i++) {
      Assert.assertEquals("Tickets", records.get(i).get("objectType"));
      Assert.assertEquals(String.format("{\"testobj\":%s}", i), records.get(i).get("object"));
    }
  }

  @Test
  public void testAnalyticsCategory() throws Exception {
    SourceHubspotConfig properties = new SourceHubspotConfig(testName.getMethodName(),
                                                             getServerAddress(),
                                                             "Analytics",
                                                             "some-api-key",
                                                             null,
                                                             null,
                                                             "20190101",
                                                             "20191111",
                                                             "Category",
                                                             null,
                                                             "totals",
                                                             null,
                                                             "total");

    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/analytics/v2/reports/totals/total" +
                            "?start=20190101&end=20191111&limit=100&hapikey=some-api-key"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testAnalyticsP1.json"))));
    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/analytics/v2/reports/totals/total" +
                            "?start=20190101&end=20191111&limit=100&offset=2&hapikey=some-api-key"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testAnalyticsP2.json"))));
    List<StructuredRecord> records = getPipelineResults(properties, 4);
    Assert.assertEquals(4, records.size());
    for (int i = 0; i < records.size(); i++) {
      Assert.assertEquals("Analytics", records.get(i).get("objectType"));
      Assert.assertEquals(String.format("{\"testobj\":%s}", i), records.get(i).get("object"));
    }
  }

  @Test
  public void testAnalyticsContent() throws Exception {
    SourceHubspotConfig properties = new SourceHubspotConfig(testName.getMethodName(),
                                                             getServerAddress(),
                                                             "Analytics",
                                                             "some-api-key",
                                                             null,
                                                             null,
                                                             "20190101",
                                                             "20191111",
                                                             "Content",
                                                             "standard-pages",
                                                             null,
                                                             null,
                                                             "total");

    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/analytics/v2/reports/standard-pages/total" +
                            "?start=20190101&end=20191111&limit=100&hapikey=some-api-key"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testAnalyticsP1.json"))));
    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/analytics/v2/reports/standard-pages/total" +
                            "?start=20190101&end=20191111&limit=100&offset=2&hapikey=some-api-key"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testAnalyticsP2.json"))));
    List<StructuredRecord> records = getPipelineResults(properties, 4);
    Assert.assertEquals(4, records.size());
    for (int i = 0; i < records.size(); i++) {
      Assert.assertEquals("Analytics", records.get(i).get("objectType"));
      Assert.assertEquals(String.format("{\"testobj\":%s}", i), records.get(i).get("object"));
    }
  }

  @Test
  public void testAnalyticsObject() throws Exception {
    SourceHubspotConfig properties = new SourceHubspotConfig(testName.getMethodName(),
                                                             getServerAddress(),
                                                             "Analytics",
                                                             "some-api-key",
                                                             null,
                                                             null,
                                                             "20190101",
                                                             "20191111",
                                                             "Object",
                                                             null,
                                                             null,
                                                             "pages",
                                                             "total");

    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/analytics/v2/reports/pages/total" +
                            "?start=20190101&end=20191111&limit=100&hapikey=some-api-key"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testAnalyticsP1.json"))));
    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/analytics/v2/reports/pages/total" +
                            "?start=20190101&end=20191111&limit=100&offset=2&hapikey=some-api-key"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testAnalyticsP2.json"))));
    List<StructuredRecord> records = getPipelineResults(properties, 4);
    Assert.assertEquals(4, records.size());
    for (int i = 0; i < records.size(); i++) {
      Assert.assertEquals("Analytics", records.get(i).get("objectType"));
      Assert.assertEquals(String.format("{\"testobj\":%s}", i), records.get(i).get("object"));
    }
  }

  @Test
  public void testAnalyticsCategorySummarizeDaily() throws Exception {
    SourceHubspotConfig properties = new SourceHubspotConfig(testName.getMethodName(),
                                                             getServerAddress(),
                                                             "Analytics",
                                                             "some-api-key",
                                                             null,
                                                             null,
                                                             "20190101",
                                                             "20191111",
                                                             "Category",
                                                             null,
                                                             "totals",
                                                             null,
                                                             "summarize/daily");

    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/analytics/v2/reports/totals/summarize/daily" +
                            "?start=20190101&end=20191111&limit=100&hapikey=some-api-key"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testAnalyticsDailyP1.json"))));

    List<StructuredRecord> records = getPipelineResults(properties, 1);
    Assert.assertEquals(1, records.size());
    for (int i = 0; i < records.size(); i++) {
      Assert.assertEquals("Analytics", records.get(i).get("objectType"));
      Assert.assertEquals(String.format("{\"testobj\":%s}", i), records.get(i).get("object"));
    }
  }

  @Test
  public void testAnalyticsContentDaily() throws Exception {
    SourceHubspotConfig properties = new SourceHubspotConfig(testName.getMethodName(),
                                                             getServerAddress(),
                                                             "Analytics",
                                                             "some-api-key",
                                                             null,
                                                             "client",
                                                             "20190101",
                                                             "20191111",
                                                             "Content",
                                                             "standard-pages",
                                                             null,
                                                             null,
                                                             "daily");

    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/analytics/v2/reports/standard-pages/daily" +
                            "?start=20190101&end=20191111&f=client&limit=100&hapikey=some-api-key"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testAnalyticsDailyP1.json"))));
    List<StructuredRecord> records = getPipelineResults(properties, 1);
    Assert.assertEquals(1, records.size());
    for (int i = 0; i < records.size(); i++) {
      Assert.assertEquals("Analytics", records.get(i).get("objectType"));
      Assert.assertEquals(String.format("{\"testobj\":%s}", i), records.get(i).get("object"));
    }
  }

  @Test
  public void testAnalyticsObjectMonthly() throws Exception {
    SourceHubspotConfig properties = new SourceHubspotConfig(testName.getMethodName(),
                                                             getServerAddress(),
                                                             "Analytics",
                                                             "some-api-key",
                                                             null,
                                                             "client",
                                                             "20190101",
                                                             "20191111",
                                                             "Object",
                                                             null,
                                                             null,
                                                             "pages",
                                                             "monthly");

    wireMockRule.stubFor(WireMock.get(
      WireMock.urlEqualTo("/analytics/v2/reports/pages/monthly" +
                            "?start=20190101&end=20191111&f=client&limit=100&hapikey=some-api-key"))
                           .willReturn(WireMock.aResponse()
                                         .withBody(readResourceFile("testAnalyticsDailyP1.json"))));
    List<StructuredRecord> records = getPipelineResults(properties, 1);
    Assert.assertEquals(1, records.size());
    for (int i = 0; i < records.size(); i++) {
      Assert.assertEquals("Analytics", records.get(i).get("objectType"));
      Assert.assertEquals(String.format("{\"testobj\":%s}", i), records.get(i).get("object"));
    }
  }

  public List<StructuredRecord> getPipelineResults(SourceHubspotConfig sourceHubspotConfig) throws Exception {
    ImmutableMap.Builder<String, String> builder = new ImmutableMap.Builder<String, String>();
    builder.put("referenceName", sourceHubspotConfig.referenceName);
    builder.put(SourceHubspotConfig.API_KEY, sourceHubspotConfig.getApiKey());
    builder.put(SourceHubspotConfig.ACCESS_TOKEN, sourceHubspotConfig.getAccessToken());
    builder.put(SourceHubspotConfig.OBJECT_TYPE, sourceHubspotConfig.objectType);

    if (sourceHubspotConfig.apiServerUrl != null) {
      builder.put(SourceHubspotConfig.API_SERVER_URL, sourceHubspotConfig.apiServerUrl);
    }
    if (sourceHubspotConfig.filters != null) {
      builder.put(SourceHubspotConfig.FILTERS, sourceHubspotConfig.filters);
    }
    if (sourceHubspotConfig.startDate != null) {
      builder.put(SourceHubspotConfig.START_DATE, sourceHubspotConfig.startDate);
    }
    if (sourceHubspotConfig.endDate != null) {
      builder.put(SourceHubspotConfig.END_DATE, sourceHubspotConfig.endDate);
    }
    if (sourceHubspotConfig.reportType != null) {
      builder.put(SourceHubspotConfig.REPORT_TYPE, sourceHubspotConfig.reportType);
    }
    if (sourceHubspotConfig.reportContent != null) {
      builder.put(SourceHubspotConfig.REPORT_CONTENT, sourceHubspotConfig.reportContent);
    }
    if (sourceHubspotConfig.reportCategory != null) {
      builder.put(SourceHubspotConfig.REPORT_CATEGORY, sourceHubspotConfig.reportCategory);
    }
    if (sourceHubspotConfig.reportObject != null) {
      builder.put(SourceHubspotConfig.REPORT_OBJECT, sourceHubspotConfig.reportObject);
    }
    if (sourceHubspotConfig.timePeriod != null) {
      builder.put(SourceHubspotConfig.TIME_PERIOD, sourceHubspotConfig.timePeriod);
    }
    Map<String, String> sourceProperties = builder.build();

    ETLStage source = new ETLStage(HubspotBatchSource.NAME,
                                   new ETLPlugin(HubspotBatchSource.NAME, BatchSource.PLUGIN_TYPE,
                                                 sourceProperties, null));

    String outputDatasetName = "output-batchsourcetest_" + testName.getMethodName();
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(outputDatasetName));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    ApplicationId pipelineId = NamespaceId.DEFAULT.app("HubspotBatch_" + testName.getMethodName());
    ApplicationManager appManager = deployApplication(pipelineId, new AppRequest<>(BatchInitializer.APP_ARTIFACT,
                                                                                   etlConfig));

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.startAndWaitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(outputDatasetName);
    List<StructuredRecord> outputRecords = MockSink.readOutput(outputManager);

    return outputRecords;
  }

  protected String readResourceFile(String filename) throws URISyntaxException, IOException {
    return new String(Files.readAllBytes(
      Paths.get(getClass().getClassLoader().getResource(filename).toURI())));
  }

  protected String getServerAddress() {
    return "http://localhost:" + wireMockRule.port();
  }
}
