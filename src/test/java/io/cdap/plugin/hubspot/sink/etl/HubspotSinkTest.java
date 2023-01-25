/*
 * Copyright © 2017 Cask Data, Inc.
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

package io.cdap.plugin.hubspot.sink.etl;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.datapipeline.DataPipelineApp;
import io.cdap.cdap.datapipeline.SmartWorkflow;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.mock.batch.MockSource;
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
import io.cdap.plugin.hubspot.common.BaseETLTest;
import io.cdap.plugin.hubspot.common.SourceHubspotConfig;
import io.cdap.plugin.hubspot.common.TestingHelper;
import io.cdap.plugin.hubspot.sink.batch.HubspotBatchSink;
import io.cdap.plugin.hubspot.sink.batch.SinkHubspotConfig;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
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

/**
 * A collection of tests for {@link HubspotBatchSink}.
 * This test is ignored by default because it requires Hubspot credentials.
 */
@Ignore
public class HubspotSinkTest extends BaseETLTest {
  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);


  @Rule
  public TestName testName = new TestName();

  protected static final ArtifactId BATCH_ARTIFACT_ID = NamespaceId.DEFAULT.artifact("data-pipeline", "4.0.0");
  protected static final ArtifactSummary BATCH_ARTIFACT = new ArtifactSummary("data-pipeline", "4.0.0");
  private static final Schema inputSchema = Schema.recordOf(
    "input-record",
    Schema.Field.of("body", Schema.of(Schema.Type.STRING)));

  @BeforeClass
  public static void setupTestClass() throws Exception {
    getCredentials();

    setupBatchArtifacts(BATCH_ARTIFACT_ID, DataPipelineApp.class);
    addPluginArtifact(NamespaceId.DEFAULT.artifact("example-plugins", "1.0.0"), BATCH_ARTIFACT_ID,
                      HubspotBatchSink.class);
  }

  @Test
  public void testContactHubspotSink() throws Exception {
    SourceHubspotConfig sourceHubspotConfig = new SourceHubspotConfig(testName.getMethodName(),
                                                                      null,
                                                                      "Contacts",
                                                                      apiKey,
                                                                      accessToken,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null);
    try {
      TestingHelper.checkAndDelete(sourceHubspotConfig, false);
      executeSink(sourceHubspotConfig, readResourceFile("testContact.json"));
    } finally {
      TestingHelper.checkAndDelete(sourceHubspotConfig, true);
    }
  }

  @Test
  public void testCompanyHubspotSink() throws Exception {
    SourceHubspotConfig sourceHubspotConfig = new SourceHubspotConfig(testName.getMethodName(),
                                                                      null,
                                                                      "Companies",
                                                                      apiKey,
                                                                      accessToken,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null);

    try {
      TestingHelper.checkAndDelete(sourceHubspotConfig, false);
      executeSink(sourceHubspotConfig, readResourceFile("testCompany.json"));
    } finally {
      TestingHelper.checkAndDelete(sourceHubspotConfig, true);
    }
  }

  @Test
  public void testContactlistHubspotSink() throws Exception {
    SourceHubspotConfig sourceHubspotConfig = new SourceHubspotConfig(testName.getMethodName(),
                                                                      null,
                                                                      "Contact Lists",
                                                                      apiKey,
                                                                      accessToken,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null);

    try {
      TestingHelper.checkAndDelete(sourceHubspotConfig, false);
      executeSink(sourceHubspotConfig, readResourceFile("testContactlist.json"));
    } finally {
      TestingHelper.checkAndDelete(sourceHubspotConfig, true);
    }
  }

  @Test
  public void testDealHubspotSink() throws Exception {
    SourceHubspotConfig sourceHubspotConfig = new SourceHubspotConfig(testName.getMethodName(),
                                                                      null,
                                                                      "Deals",
                                                                      apiKey,
                                                                      accessToken,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null);

    try {
      TestingHelper.checkAndDelete(sourceHubspotConfig, false);
      executeSink(sourceHubspotConfig, readResourceFile("testDeal.json"));
    } finally {
      TestingHelper.checkAndDelete(sourceHubspotConfig, true);
    }
  }

  @Test
  public void testDealPipelineHubspotSink() throws Exception {
    SourceHubspotConfig sourceHubspotConfig = new SourceHubspotConfig(testName.getMethodName(),
                                                                      null,
                                                                      "Deal Pipelines",
                                                                      apiKey,
                                                                      accessToken,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null);

    try {
      TestingHelper.checkAndDelete(sourceHubspotConfig, false);
      executeSink(sourceHubspotConfig,
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
    } finally {
      TestingHelper.checkAndDelete(sourceHubspotConfig, true);
    }
  }

  @Test
  public void testMarketingEmailHubspotSink() throws Exception {
    SourceHubspotConfig sourceHubspotConfig = new SourceHubspotConfig(testName.getMethodName(),
                                                                      null,
                                                                      "Marketing Email",
                                                                      apiKey,
                                                                      accessToken,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null);

    try {
      TestingHelper.checkAndDelete(sourceHubspotConfig, false);
      executeSink(sourceHubspotConfig, readResourceFile("testMarketingEmail.json"));
    } finally {
      TestingHelper.checkAndDelete(sourceHubspotConfig, true);
    }
  }

  @Test
  public void testProductHubspotSink() throws Exception {
    SourceHubspotConfig sourceHubspotConfig = new SourceHubspotConfig(testName.getMethodName(),
                                                                      null,
                                                                      "Products",
                                                                      apiKey,
                                                                      accessToken,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null);

    try {
      TestingHelper.checkAndDelete(sourceHubspotConfig, false);
      executeSink(sourceHubspotConfig, readResourceFile("testProduct.json"));
    } finally {
      TestingHelper.checkAndDelete(sourceHubspotConfig, true);
    }
  }

  @Test
  public void testTicketHubspotSink() throws Exception {
    SourceHubspotConfig sourceHubspotConfig = new SourceHubspotConfig(testName.getMethodName(),
                                                                      null,
                                                                      "Tickets",
                                                                      apiKey,
                                                                      accessToken,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null);

    try {
      TestingHelper.checkAndDelete(sourceHubspotConfig, false);
      executeSink(sourceHubspotConfig, readResourceFile("testTicket.json"));
    } finally {
      TestingHelper.checkAndDelete(sourceHubspotConfig, true);
    }
  }

  private void executeSink(SourceHubspotConfig sourceHubspotConfig, String object) throws Exception {

    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
            .put("referenceName", sourceHubspotConfig.referenceName)
            .put(SinkHubspotConfig.API_KEY, sourceHubspotConfig.getApiKey())
            .put(SinkHubspotConfig.ACCESS_TOKEN, sourceHubspotConfig.getAccessToken())
            .put(SinkHubspotConfig.OBJECT_TYPE, sourceHubspotConfig.objectType)
            .put(SinkHubspotConfig.OBJECT_FIELD, "body")
            .build();

    List<StructuredRecord> input = ImmutableList.of(
      StructuredRecord.builder(inputSchema).set("body", object).build()
    );

    String inputDatasetName = "input-hubspot-sink" + testName.getMethodName();
    ETLStage source = new ETLStage("source", MockSource.getPlugin(inputDatasetName));

    ETLStage sink = new ETLStage("Hubspot", new ETLPlugin("Hubspot", BatchSink.PLUGIN_TYPE, properties, null));
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(BATCH_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("hubspotsinktest");
    ApplicationManager appManager = deployApplication(appId, appRequest);
    DataSetManager<Table> inputManager = getDataset(inputDatasetName);
    MockSource.writeInput(inputManager, input);
    WorkflowManager manager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    manager.start();
    manager.waitForRun(ProgramRunStatus.COMPLETED, 1, TimeUnit.MINUTES);
  }

  protected String readResourceFile(String filename) throws URISyntaxException, IOException {
    return new String(Files.readAllBytes(
      Paths.get(getClass().getClassLoader().getResource(filename).toURI())));
  }
}
