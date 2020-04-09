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
import io.cdap.plugin.hubspot.common.TestingHelper;
import io.cdap.plugin.hubspot.source.batch.HubspotBatchSource;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
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
    SourceHubspotConfig sourceHubspotConfig = new SourceHubspotConfig(testName.getMethodName(),
                                                                      null,
                                                                      "Contact Lists",
                                                                      apiKey,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null);
    try {
      TestingHelper.createTestObject(sourceHubspotConfig, readResourceFile("testContactlist.json"));
      List<StructuredRecord> records = getPipelineResults(sourceHubspotConfig);
      TestingHelper.checkExist(sourceHubspotConfig, records, true);
    } finally {
      TestingHelper.checkAndDelete(sourceHubspotConfig, false);
    }
  }

  @Test
  public void testContacts() throws Exception {
    SourceHubspotConfig sourceHubspotConfig = new SourceHubspotConfig(testName.getMethodName(),
                                                                      null,
                                                                      "Contacts",
                                                                      apiKey,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null);
    try {
      TestingHelper.createTestObject(sourceHubspotConfig, readResourceFile("testContact.json"));
      List<StructuredRecord> records = getPipelineResults(sourceHubspotConfig);
      TestingHelper.checkExist(sourceHubspotConfig, records, true);
    } finally {
      TestingHelper.checkAndDelete(sourceHubspotConfig, false);
    }
  }

  @Test
  public void testEmailEvents() throws Exception {
    SourceHubspotConfig sourceHubspotConfig = new SourceHubspotConfig(testName.getMethodName(),
                                                                      null,
                                                                      "Email Events",
                                                                      apiKey,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null);
    List<StructuredRecord> records = getPipelineResults(sourceHubspotConfig);
  }

  @Test
  public void testEmailSubscription() throws Exception {
    SourceHubspotConfig sourceHubspotConfig = new SourceHubspotConfig(testName.getMethodName(),
                                                                      null,
                                                                      "Email Subscription",
                                                                      apiKey,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null);
    List<StructuredRecord> records = getPipelineResults(sourceHubspotConfig);
  }

  @Test
  public void testRecentCompanies() throws Exception {
    SourceHubspotConfig sourceHubspotConfig = new SourceHubspotConfig(testName.getMethodName(),
                                                                      null,
                                                                      "Recent Companies",
                                                                      apiKey,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null);
    List<StructuredRecord> records = getPipelineResults(sourceHubspotConfig);
  }

  @Test
  public void testCompanies() throws Exception {
    SourceHubspotConfig sourceHubspotConfig = new SourceHubspotConfig(testName.getMethodName(),
                                                                      null,
                                                                      "Companies",
                                                                      apiKey,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null);
    try {
      TestingHelper.createTestObject(sourceHubspotConfig, readResourceFile("testCompany.json"));
      List<StructuredRecord> records = getPipelineResults(sourceHubspotConfig);
      TestingHelper.checkExist(sourceHubspotConfig, records, true);
    } finally {
      TestingHelper.checkAndDelete(sourceHubspotConfig, false);
    }
  }

  @Test
  public void testDeals() throws Exception {
    SourceHubspotConfig sourceHubspotConfig = new SourceHubspotConfig(testName.getMethodName(),
                                                                      null,
                                                                      "Deals",
                                                                      apiKey,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null);
    try {
      TestingHelper.createTestObject(sourceHubspotConfig, readResourceFile("testDeal.json"));
      List<StructuredRecord> records = getPipelineResults(sourceHubspotConfig);
      TestingHelper.checkExist(sourceHubspotConfig, records, true);
    } finally {
      TestingHelper.checkAndDelete(sourceHubspotConfig, false);
    }
  }

  @Test
  public void testDealPipelines() throws Exception {
    SourceHubspotConfig sourceHubspotConfig = new SourceHubspotConfig(testName.getMethodName(),
                                                                      null,
                                                                      "Deal Pipelines",
                                                                      apiKey,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null);
    try {
      // randomize to avoid soft delete ERROR
      TestingHelper.createTestObject(sourceHubspotConfig,
                                     "{\"pipelineId\":\"testName" +
                                       RandomStringUtils.random(20, false, true) +
                                       "\",\"label\":\"test Name\",\"displayOrder\":2," +
                                       "\"active\":true,\"stages\":[{\"stageId\":\"testStage" +
                                       RandomStringUtils.random(20, false, true) +
                                       "\",\"label\":\"test Stage\"," +
                                       "\"displayOrder\":1,\"metadata\":{\"probability\":0.5}}" +
                                       ",{\"stageId\":\"testStage2" +
                                       RandomStringUtils.random(20, false, true) +
                                       "\",\"label\":\"test Stage2\",\"displayOrder\":2," +
                                       "\"metadata\":{\"probability\":1.0}}]}");
      List<StructuredRecord> records = getPipelineResults(sourceHubspotConfig);
      TestingHelper.checkExist(sourceHubspotConfig, records, true);
    } finally {
      TestingHelper.checkAndDelete(sourceHubspotConfig, false);
    }
  }

  @Test
  public void testMarketingEmail() throws Exception {
    SourceHubspotConfig sourceHubspotConfig = new SourceHubspotConfig(testName.getMethodName(),
                                                                      null,
                                                                      "Marketing Email",
                                                                      apiKey,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null);
    try {
      TestingHelper.createTestObject(sourceHubspotConfig, readResourceFile("testMarketingEmail.json"));
      List<StructuredRecord> records = getPipelineResults(sourceHubspotConfig);
      TestingHelper.checkExist(sourceHubspotConfig, records, true);
    } finally {
      TestingHelper.checkAndDelete(sourceHubspotConfig, false);
    }
  }

  @Test
  public void testProducts() throws Exception {
    SourceHubspotConfig sourceHubspotConfig = new SourceHubspotConfig(testName.getMethodName(),
                                                                      null,
                                                                      "Products",
                                                                      apiKey,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null);
    try {
      TestingHelper.createTestObject(sourceHubspotConfig, readResourceFile("testProduct.json"));
      List<StructuredRecord> records = getPipelineResults(sourceHubspotConfig);
      TestingHelper.checkExist(sourceHubspotConfig, records, true);
    } finally {
      TestingHelper.checkAndDelete(sourceHubspotConfig, false);
    }
  }

  @Test
  public void testTickets() throws Exception {
    SourceHubspotConfig sourceHubspotConfig = new SourceHubspotConfig(testName.getMethodName(),
                                                                      null,
                                                                      "Tickets",
                                                                      apiKey,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null);
    try {
      TestingHelper.createTestObject(sourceHubspotConfig, readResourceFile("testTicket.json"));
      List<StructuredRecord> records = getPipelineResults(sourceHubspotConfig);
      TestingHelper.checkExist(sourceHubspotConfig, records, true);
    } finally {
      TestingHelper.checkAndDelete(sourceHubspotConfig, false);
    }
  }

  @Test
  public void testAnalyticsCategory() throws Exception {
    SourceHubspotConfig sourceHubspotConfig = new SourceHubspotConfig(testName.getMethodName(),
                                                                      null,
                                                                      "Analytics",
                                                                      apiKey,
                                                                      null,
                                                                      "20190101",
                                                                      "20191111",
                                                                      "Category",
                                                                      null,
                                                                      "totals",
                                                                      null,
                                                                      "total");
    List<StructuredRecord> records = getPipelineResults(sourceHubspotConfig);
  }

  @Test
  public void testAnalyticsContent() throws Exception {
    SourceHubspotConfig sourceHubspotConfig = new SourceHubspotConfig(testName.getMethodName(),
                                                                      null,
                                                                      "Analytics",
                                                                      apiKey,
                                                                      null,
                                                                      "20190101",
                                                                      "20191111",
                                                                      "Content",
                                                                      "standard-pages",
                                                                      null,
                                                                      null,
                                                                      "total");
    List<StructuredRecord> records = getPipelineResults(sourceHubspotConfig);
  }

  @Test
  public void testAnalyticsObject() throws Exception {
    SourceHubspotConfig sourceHubspotConfig = new SourceHubspotConfig(testName.getMethodName(),
                                                                      null,
                                                                      "Analytics",
                                                                      apiKey,
                                                                      null,
                                                                      "20190101",
                                                                      "20191111",
                                                                      "Object",
                                                                      null,
                                                                      null,
                                                                      "pages",
                                                                      "total");
    List<StructuredRecord> records = getPipelineResults(sourceHubspotConfig);
  }

  @Test
  public void testAnalyticsCategorySummarizeDaily() throws Exception {
    SourceHubspotConfig sourceHubspotConfig = new SourceHubspotConfig(testName.getMethodName(),
                                                                      null,
                                                                      "Analytics",
                                                                      apiKey,
                                                                      null,
                                                                      "20190101",
                                                                      "20191111",
                                                                      "Category",
                                                                      null,
                                                                      "totals",
                                                                      null,
                                                                      "summarize/daily");
    List<StructuredRecord> records = getPipelineResults(sourceHubspotConfig);
  }

  @Test
  public void testAnalyticsContentDaily() throws Exception {
    SourceHubspotConfig sourceHubspotConfig = new SourceHubspotConfig(testName.getMethodName(),
                                                                      null,
                                                                      "Analytics",
                                                                      apiKey,
                                                                      "client",
                                                                      "20190101",
                                                                      "20191111",
                                                                      "Content",
                                                                      "standard-pages",
                                                                      null,
                                                                      null,
                                                                      "daily");
    List<StructuredRecord> records = getPipelineResults(sourceHubspotConfig);
  }

  @Test
  public void testAnalyticsObjectMonthly() throws Exception {
    SourceHubspotConfig sourceHubspotConfig = new SourceHubspotConfig(testName.getMethodName(),
                                                                      null,
                                                                      "Analytics",
                                                                      apiKey,
                                                                      "client",
                                                                      "20190101",
                                                                      "20191111",
                                                                      "Object",
                                                                      null,
                                                                      null,
                                                                      "pages",
                                                                      "monthly");

    List<StructuredRecord> records = getPipelineResults(sourceHubspotConfig);
  }

  public List<StructuredRecord> getPipelineResults(SourceHubspotConfig sourceHubspotConfig) throws Exception {
    ImmutableMap.Builder<String, String> builder = new ImmutableMap.Builder<String, String>();
    builder.put("referenceName", sourceHubspotConfig.referenceName);
    builder.put(SourceHubspotConfig.API_KEY, sourceHubspotConfig.apiKey);
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
    ApplicationManager appManager = deployApplication(pipelineId, new AppRequest<>(APP_ARTIFACT, etlConfig));

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
}
