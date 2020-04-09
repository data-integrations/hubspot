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
package io.cdap.plugin.hubspot.sink.batch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Submit {@link String} records to Hubspot.
 */
public class HubspotRecordWriter extends RecordWriter<NullWritable, String> {
  private HubspotSinkHelper hubspotSinkHelper;
  private final SinkHubspotConfig sinkHubspotConfig;

  public HubspotRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException {
    Configuration conf = taskAttemptContext.getConfiguration();
    String configJson = conf.get(HubspotOutputFormatProvider.PROPERTY_CONFIG_JSON);
    sinkHubspotConfig = HubspotOutputFormatProvider.GSON.fromJson(configJson, SinkHubspotConfig.class);
  }

  @Override
  public void write(NullWritable nullWritable, String input) throws IOException {
    hubspotSinkHelper = new HubspotSinkHelper();
    try {
      hubspotSinkHelper.executeHTTPService(input, sinkHubspotConfig);
    } catch (Exception e) {
      throw new RuntimeException("Submit record to Hubspot failed with:", e);
    }
  }

  @Override
  public void close(TaskAttemptContext taskAttemptContext) {
    //no-op
  }
}
