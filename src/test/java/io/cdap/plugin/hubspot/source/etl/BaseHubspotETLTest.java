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

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.datapipeline.DataPipelineApp;
import io.cdap.cdap.datapipeline.SmartWorkflow;
import io.cdap.cdap.datastreams.DataStreamsApp;
import io.cdap.cdap.datastreams.DataStreamsSparkLauncher;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.streaming.StreamingSource;
import io.cdap.cdap.etl.mock.batch.MockSink;
import io.cdap.cdap.etl.mock.test.HydratorTestBase;
import io.cdap.cdap.etl.proto.v2.DataStreamsConfig;
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
import io.cdap.cdap.test.ProgramManager;
import io.cdap.cdap.test.SparkManager;
import io.cdap.cdap.test.TestConfiguration;
import io.cdap.cdap.test.WorkflowManager;
import io.cdap.plugin.hubspot.common.SourceHubspotConfig;
import io.cdap.plugin.hubspot.source.batch.HubspotBatchSource;
import io.cdap.plugin.hubspot.source.streaming.HubspotStreamingSource;
import io.cdap.plugin.hubspot.source.streaming.HubspotStreamingSourceConfig;
import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TestName;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public abstract class BaseHubspotETLTest extends HydratorTestBase {
  @Rule
  public TestName testName = new TestName();
  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  public abstract TestsRunner getTestRunner();

  protected List<StructuredRecord> getPipelineResults(SourceHubspotConfig sourceHubspotConfig,
                                                      int expectedRecordsCount) throws Exception {
    return getTestRunner().getPipelineResults(sourceHubspotConfig, expectedRecordsCount);
  }

  protected String readResourceFile(String filename) throws URISyntaxException, IOException {
    return new String(Files.readAllBytes(
      Paths.get(getClass().getClassLoader().getResource(filename).toURI())));
  }

  private Map<String, String> getPropertiesFromConfig(SourceHubspotConfig sourceHubspotConfig) {
    ImmutableMap.Builder<String, String> builder = new ImmutableMap.Builder<>();
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
    return builder.build();
  }

  public interface TestsRunner {
    List<StructuredRecord> getPipelineResults(SourceHubspotConfig sourceHubspotConfig,
                                                     int expectedRecordsCount) throws Exception;
  }

  public static class BatchInitializer {
    static final ArtifactSummary APP_ARTIFACT = new ArtifactSummary("data-pipeline", "3.2.0");

    public static void setupTestClass() throws Exception {
      ArtifactId parentArtifact = NamespaceId.DEFAULT.artifact(APP_ARTIFACT.getName(), APP_ARTIFACT.getVersion());

      // add the artifact and mock plugins
      setupBatchArtifacts(parentArtifact, DataPipelineApp.class);

      // add our plugins artifact with the artifact as its parent.
      // this will make our plugins available.
      addPluginArtifact(NamespaceId.DEFAULT.artifact("example-plugins", "1.0.0"),
                        parentArtifact,
                        HubspotBatchSource.class);
    }
  }

  public class BatchTestRunner implements TestsRunner {
    private final ArtifactSummary appArtifact = new ArtifactSummary("data-pipeline", "3.2.0");

    @Override
    public List<StructuredRecord> getPipelineResults(SourceHubspotConfig config,
                                                     int expectedRecordsCount) throws Exception {
      ETLStage source = new ETLStage(HubspotBatchSource.NAME,
                                     new ETLPlugin(HubspotBatchSource.NAME, BatchSource.PLUGIN_TYPE,
                                                   getPropertiesFromConfig(config), null));

      String outputDatasetName = "output-batchsourcetest_" + testName.getMethodName();
      ETLStage sink = new ETLStage("sink", MockSink.getPlugin(outputDatasetName));

      ETLBatchConfig etlConfig = ETLBatchConfig.builder()
        .addStage(source)
        .addStage(sink)
        .addConnection(source.getName(), sink.getName())
        .build();

      ApplicationId pipelineId = NamespaceId.DEFAULT.app("HubspotBatch_" + testName.getMethodName());
      ApplicationManager appManager = deployApplication(pipelineId, new AppRequest<>(appArtifact, etlConfig));

      WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
      workflowManager.startAndWaitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

      DataSetManager<Table> outputManager = getDataset(outputDatasetName);
      List<StructuredRecord> outputRecords = MockSink.readOutput(outputManager);

      return outputRecords;
    }
  }

  public static class StreamingInitializer {
    private static final ArtifactId APP_ARTIFACT_ID = NamespaceId.DEFAULT.artifact("data-streams", "1.0.0");

    public static void setupTestClass() throws Exception {
      setupStreamingArtifacts(APP_ARTIFACT_ID, DataStreamsApp.class);

      addPluginArtifact(NamespaceId.DEFAULT.artifact("hubspot-plugins", "1.0.0"),
                        APP_ARTIFACT_ID,
                        HubspotStreamingSource.class
      );
    }
  }

  public class StreamingTestRunner implements TestsRunner {
    private final ArtifactSummary appArtifact = new ArtifactSummary("data-streams", "1.0.0");
    private final int waitForRecordsTimeoutSeconds = 60;
    private final long waitForRecordsPollingIntervalMs = 100;

    public List<StructuredRecord> getPipelineResults(SourceHubspotConfig config,
                                              int expectedRecordsCount) throws Exception {
      ProgramManager programManager = startPipeline(getPropertiesFromConfig(config));
      return waitForRecords(programManager, expectedRecordsCount);
    }

    private SparkManager deployETL(ETLPlugin sourcePlugin, ETLPlugin sinkPlugin, String appName) throws Exception {
      ETLStage source = new ETLStage("source", sourcePlugin);
      ETLStage sink = new ETLStage("sink", sinkPlugin);
      DataStreamsConfig etlConfig = DataStreamsConfig.builder()
        .addStage(source)
        .addStage(sink)
        .addConnection(source.getName(), sink.getName())
        .setBatchInterval("1s")
        .build();

      AppRequest<DataStreamsConfig> appRequest = new AppRequest<>(appArtifact, etlConfig);
      ApplicationId appId = NamespaceId.DEFAULT.app(appName);
      ApplicationManager applicationManager = deployApplication(appId, appRequest);

      return applicationManager.getSparkManager(DataStreamsSparkLauncher.NAME);
    }

    private ProgramManager startPipeline(Map<String, String> properties) throws Exception {
      Map<String, String> sourceProperties = new ImmutableMap.Builder<String, String>()
        .putAll(properties)
        .put(HubspotStreamingSourceConfig.PULL_FREQUENCY, "15 min")
        .build();

      ETLPlugin sourceConfig = new ETLPlugin("Hubspot", StreamingSource.PLUGIN_TYPE, sourceProperties);
      ETLPlugin sinkConfig = MockSink.getPlugin(getOutputDatasetName());

      ProgramManager programManager =
        deployETL(sourceConfig, sinkConfig, "HubspotStreaming_" + testName.getMethodName());
      programManager.startAndWaitForRun(ProgramRunStatus.RUNNING, 30, TimeUnit.SECONDS);

      return programManager;
    }

    private List<StructuredRecord> waitForRecords(ProgramManager programManager,
                                                  int expectedRecordsCount) throws Exception {
      DataSetManager<Table> outputManager = getDataset(getOutputDatasetName());

      Awaitility.await()
        .atMost(waitForRecordsTimeoutSeconds, TimeUnit.SECONDS)
        .pollInterval(waitForRecordsPollingIntervalMs, TimeUnit.MILLISECONDS)
        .untilAsserted((() -> {
          int recordsCount = MockSink.readOutput(outputManager).size();
          Assert.assertTrue(
            String.format("At least %d records expected, but %d found", expectedRecordsCount, recordsCount),
            recordsCount >= expectedRecordsCount);
        }));

      programManager.stop();
      programManager.waitForStopped(10, TimeUnit.SECONDS);
      programManager.waitForRun(ProgramRunStatus.KILLED, 10, TimeUnit.SECONDS);

      return MockSink.readOutput(outputManager);
    }

    private String getOutputDatasetName() {
      return "output-realtimesourcetest_" + testName.getMethodName();
    }
  }
}
