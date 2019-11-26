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

package io.cdap.plugin.hubspot.sink.batch;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Sink plugin to  put data to HubSpot CRM.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name(HubspotBatchSink.NAME)
@Description("Sink plugin to  put data to HubSpot CRM.")
public class HubspotBatchSink extends BatchSink<StructuredRecord, Void, Void> {

  private static final Logger LOG = LoggerFactory.getLogger(HubspotBatchSink.class);

  private SinkHubspotConfig config;

  public static final String NAME = "Hubspot";

  public HubspotBatchSink(SinkHubspotConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    FailureCollector collector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    config.validate(collector);
    collector.getOrThrowException();
  }

  @Override
  public void prepareRun(BatchSinkContext context) {
    FailureCollector collector = context.getFailureCollector();
    config.validate(collector);
    collector.getOrThrowException();

    context.addOutput(Output.of(config.referenceName, new HubspotOutputFormatProvider()));
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<Void, Void>> emitter) throws Exception {
    HubspotSinkHelper hubspotSinkHelper = new HubspotSinkHelper();
    hubspotSinkHelper.executeHTTPService(input.get(config.objectField), config);
  }

  /**
   * Output format provider for Hubspot Sink.
   */
  private static class HubspotOutputFormatProvider implements OutputFormatProvider {
    private Map<String, String> conf = new HashMap<>();

    @Override
    public String getOutputFormatClassName() {
      return NullOutputFormat.class.getName();
    }

    @Override
    public Map<String, String> getOutputFormatConfiguration() {
      return conf;
    }
  }

}
