/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.plugin.hubspot.source.batch;

import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.batch.InputFormatProvider;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.plugin.hubspot.common.Hacks;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Test source
 */
@Plugin(type = "batchsource")
@Name("OAuthRefreshTest")
public class TestSource extends BatchSource<NullWritable, StructuredRecord, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(TestSource.class);
  private static final String PROVIDER_KEY = "provider";
  private final Conf conf;

  public TestSource(Conf conf) {
    this.conf = conf;
  }

  @Override
  public void prepareRun(BatchSourceContext batchSourceContext) {
    batchSourceContext.setInput(Input.of("abc", new InputFormatProvider() {
      @Override
      public String getInputFormatClassName() {
        return TestInputFormat.class.getName();
      }

      @Override
      public Map<String, String> getInputFormatConfiguration() {
        return Collections.singletonMap(PROVIDER_KEY, conf.provider);
      }
    }));
  }

  @Override
  public void transform(KeyValue<NullWritable, StructuredRecord> input, Emitter<StructuredRecord> emitter) {
    emitter.emit(input.getValue());
  }

  /**
   * Test format
   */
  public static class TestInputFormat extends InputFormat<NullWritable, StructuredRecord> {

    @Override
    public List<InputSplit> getSplits(JobContext jobContext) {
      List<InputSplit> splits = new ArrayList<>(2);
      splits.add(new DummySplit());
      splits.add(new DummySplit());
      return splits;
    }

    @Override
    public RecordReader<NullWritable, StructuredRecord> createRecordReader(
      InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException {
      Configuration conf = taskAttemptContext.getConfiguration();
      String provider = conf.get(PROVIDER_KEY);

      try {
        Hacks.OAuthTokenRefresher oAuthTokenRefresher = Hacks.getTokenRefresher(provider, "dummy");
        LOG.info("Test calling authurl endpoint -- response = {}", oAuthTokenRefresher.getAuthURL());
      } catch (Exception e) {
        throw new IOException(e);
      }

      return new RecordReader<NullWritable, StructuredRecord>() {
        @Override
        public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext){

        }

        @Override
        public boolean nextKeyValue() {
          return false;
        }

        @Override
        public NullWritable getCurrentKey() {
          return null;
        }

        @Override
        public StructuredRecord getCurrentValue() {
          return null;
        }

        @Override
        public float getProgress() {
          return 0;
        }

        @Override
        public void close() {

        }
      };
    }
  }

  /**
   * dummy split
   */
  public static class DummySplit extends InputSplit implements Writable {

    @Override
    public void write(DataOutput dataOutput) {

    }

    @Override
    public void readFields(DataInput dataInput) {

    }

    @Override
    public long getLength() {
      return 1;
    }

    @Override
    public String[] getLocations() {
      return new String[0];
    }
  }

  /**
   * Plugin Config class.
   */
  public static class Conf extends PluginConfig {
    private String provider;
  }
}
