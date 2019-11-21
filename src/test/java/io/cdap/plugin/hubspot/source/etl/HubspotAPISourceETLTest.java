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
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.cdap.plugin.hubspot.source.etl;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.datapipeline.DataPipelineApp;
import io.cdap.cdap.datapipeline.SmartWorkflow;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.mock.batch.MockSink;
import io.cdap.cdap.etl.mock.test.HydratorTestBase;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.TestConfiguration;
import io.cdap.cdap.test.WorkflowManager;
import io.cdap.plugin.hubspot.common.SourceHubspotConfig;
import io.cdap.plugin.hubspot.source.batch.HubspotBatchSource;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class HubspotAPISourceETLTest extends HydratorTestBase {
  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);
  private static final ArtifactSummary APP_ARTIFACT = new ArtifactSummary("data-pipeline", "3.2.0");
  @Rule
  public TestName testName = new TestName();

  private static String apiKey;

  @BeforeClass
  public static void setupTestClass() throws Exception {
    apiKey = System.getProperty("hubspot.api.key");
    if (apiKey == null || apiKey.isEmpty()) {
      throw new IllegalArgumentException("hubspot.api.key system property must not be empty.");
    }

    ArtifactId parentArtifact = NamespaceId.DEFAULT.artifact(APP_ARTIFACT.getName(), APP_ARTIFACT.getVersion());

    // add the artifact and mock plugins
    setupBatchArtifacts(parentArtifact, DataPipelineApp.class);

    // add our plugins artifact with the artifact as its parent.
    // this will make our plugins available.
    addPluginArtifact(NamespaceId.DEFAULT.artifact("example-plugins", "1.0.0"),
                      parentArtifact,
                      HubspotBatchSource.class);
  }

  @Test
  public void testContactLists() throws Exception {

    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put("referenceName", testName.getMethodName())
      .put(SourceHubspotConfig.API_KEY, apiKey)
      .put(SourceHubspotConfig.OBJECT_TYPE, "Contact Lists")
      .build();

    List<StructuredRecord> records = getPipelineResults(properties);
  }

  @Test
  public void testContacts() throws Exception {

    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put("referenceName", testName.getMethodName())
      .put(SourceHubspotConfig.API_KEY, apiKey)
      .put(SourceHubspotConfig.OBJECT_TYPE, "Contacts")
      .build();

    List<StructuredRecord> records = getPipelineResults(properties);
  }

  @Test
  public void testEmailEvents() throws Exception {

    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put("referenceName", testName.getMethodName())
      .put(SourceHubspotConfig.API_KEY, apiKey)
      .put(SourceHubspotConfig.OBJECT_TYPE, "Email Events")
      .build();

    List<StructuredRecord> records = getPipelineResults(properties);
  }

  @Test
  public void testEmailSubscription() throws Exception {

    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put("referenceName", testName.getMethodName())
      .put(SourceHubspotConfig.API_KEY, apiKey)
      .put(SourceHubspotConfig.OBJECT_TYPE, "Email Subscription")
      .build();

    List<StructuredRecord> records = getPipelineResults(properties);
  }

  @Test
  public void testRecentCompanies() throws Exception {

    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put("referenceName", testName.getMethodName())
      .put(SourceHubspotConfig.API_KEY, apiKey)
      .put(SourceHubspotConfig.OBJECT_TYPE, "Recent Companies")
      .build();

    List<StructuredRecord> records = getPipelineResults(properties);
  }

  @Test
  public void testCompanies() throws Exception {

    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put("referenceName", testName.getMethodName())
      .put(SourceHubspotConfig.API_KEY, apiKey)
      .put(SourceHubspotConfig.OBJECT_TYPE, "Companies")
      .build();

    List<StructuredRecord> records = getPipelineResults(properties);
  }

  @Test
  public void testDeals() throws Exception {

    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put("referenceName", testName.getMethodName())
      .put(SourceHubspotConfig.API_KEY, apiKey)
      .put(SourceHubspotConfig.OBJECT_TYPE, "Deals")
      .build();

    List<StructuredRecord> records = getPipelineResults(properties);
  }

  @Test
  public void testDealPipelines() throws Exception {

    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put("referenceName", testName.getMethodName())
      .put(SourceHubspotConfig.API_KEY, apiKey)
      .put(SourceHubspotConfig.OBJECT_TYPE, "Deal Pipelines")
      .build();

    List<StructuredRecord> records = getPipelineResults(properties);
  }

  @Test
  public void testMarketingEmail() throws Exception {

    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put("referenceName", testName.getMethodName())
      .put(SourceHubspotConfig.API_KEY, apiKey)
      .put(SourceHubspotConfig.OBJECT_TYPE, "Marketing Email")
      .build();

    List<StructuredRecord> records = getPipelineResults(properties);
  }

  @Test
  public void testProducts() throws Exception {

    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put("referenceName", testName.getMethodName())
      .put(SourceHubspotConfig.API_KEY, apiKey)
      .put(SourceHubspotConfig.OBJECT_TYPE, "Products")
      .build();

    List<StructuredRecord> records = getPipelineResults(properties);
  }

  @Test
  public void testTickets() throws Exception {

    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put("referenceName", testName.getMethodName())
      .put(SourceHubspotConfig.API_KEY, apiKey)
      .put(SourceHubspotConfig.OBJECT_TYPE, "Tickets")
      .build();

    List<StructuredRecord> records = getPipelineResults(properties);
  }

  @Test
  public void testAnalyticsCategory() throws Exception {

    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put("referenceName", testName.getMethodName())
      .put(SourceHubspotConfig.API_KEY, apiKey)
      .put(SourceHubspotConfig.OBJECT_TYPE, "Analytics")
      .put(SourceHubspotConfig.TIME_PERIOD, "total")
      .put(SourceHubspotConfig.REPORT_TYPE, "Category")
      .put(SourceHubspotConfig.REPORT_CATEGORY, "totals")
      .put(SourceHubspotConfig.START_DATE, "20190101")
      .put(SourceHubspotConfig.END_DATE, "20191111")
      .build();

    List<StructuredRecord> records = getPipelineResults(properties);
  }

  @Test
  public void testAnalyticsContent() throws Exception {

    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put("referenceName", testName.getMethodName())
      .put(SourceHubspotConfig.API_KEY, apiKey)
      .put(SourceHubspotConfig.OBJECT_TYPE, "Analytics")
      .put(SourceHubspotConfig.TIME_PERIOD, "total")
      .put(SourceHubspotConfig.REPORT_TYPE, "Content")
      .put(SourceHubspotConfig.REPORT_CONTENT, "standard-pages")
      .put(SourceHubspotConfig.START_DATE, "20190101")
      .put(SourceHubspotConfig.END_DATE, "20191111")
      .build();

    List<StructuredRecord> records = getPipelineResults(properties);
  }

  @Test
  public void testAnalyticsObject() throws Exception {

    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put("referenceName", testName.getMethodName())
      .put(SourceHubspotConfig.API_KEY, apiKey)
      .put(SourceHubspotConfig.OBJECT_TYPE, "Analytics")
      .put(SourceHubspotConfig.TIME_PERIOD, "total")
      .put(SourceHubspotConfig.REPORT_TYPE, "Object")
      .put(SourceHubspotConfig.REPORT_OBJECT, "pages")
      .put(SourceHubspotConfig.START_DATE, "20190101")
      .put(SourceHubspotConfig.END_DATE, "20191111")
      .build();

    List<StructuredRecord> records = getPipelineResults(properties);
  }

  @Test
  public void testAnalyticsCategorySummarizeDaily() throws Exception {

    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put("referenceName", testName.getMethodName())
      .put(SourceHubspotConfig.API_KEY, apiKey)
      .put(SourceHubspotConfig.OBJECT_TYPE, "Analytics")
      .put(SourceHubspotConfig.TIME_PERIOD, "summarize/daily")
      .put(SourceHubspotConfig.REPORT_TYPE, "Category")
      .put(SourceHubspotConfig.REPORT_CATEGORY, "totals")
      .put(SourceHubspotConfig.START_DATE, "20190101")
      .put(SourceHubspotConfig.END_DATE, "20191111")
      .build();

    List<StructuredRecord> records = getPipelineResults(properties);
  }

  @Test
  public void testAnalyticsContentDaily() throws Exception {

    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put("referenceName", testName.getMethodName())
      .put(SourceHubspotConfig.API_KEY, apiKey)
      .put(SourceHubspotConfig.OBJECT_TYPE, "Analytics")
      .put(SourceHubspotConfig.TIME_PERIOD, "daily")
      .put(SourceHubspotConfig.REPORT_TYPE, "Content")
      .put(SourceHubspotConfig.REPORT_CONTENT, "standard-pages")
      .put(SourceHubspotConfig.START_DATE, "20190101")
      .put(SourceHubspotConfig.END_DATE, "20191111")
      .put(SourceHubspotConfig.FILTERS, "client")
      .build();

    List<StructuredRecord> records = getPipelineResults(properties);
  }

  @Test
  public void testAnalyticsObjectMonthly() throws Exception {

    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put("referenceName", testName.getMethodName())
      .put(SourceHubspotConfig.API_KEY, apiKey)
      .put(SourceHubspotConfig.OBJECT_TYPE, "Analytics")
      .put(SourceHubspotConfig.TIME_PERIOD, "monthly")
      .put(SourceHubspotConfig.REPORT_TYPE, "Object")
      .put(SourceHubspotConfig.REPORT_OBJECT, "pages")
      .put(SourceHubspotConfig.START_DATE, "20190101")
      .put(SourceHubspotConfig.END_DATE, "20191111")
      .put(SourceHubspotConfig.FILTERS, "client")
      .build();


    List<StructuredRecord> records = getPipelineResults(properties);
  }

  public List<StructuredRecord> getPipelineResults(Map<String, String> sourceProperties) throws Exception {

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
    ApplicationManager appManager = deployApplication(pipelineId, new AppRequest<>(APP_ARTIFACT, etlConfig));

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.startAndWaitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(outputDatasetName);
    List<StructuredRecord> outputRecords = MockSink.readOutput(outputManager);

    return outputRecords;
  }
}
