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
package io.cdap.plugin.hubspot.batch.source;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import io.cdap.plugin.hubspot.common.BaseHubspotConfig;
import io.cdap.plugin.hubspot.common.HubspotHelper;
import io.cdap.plugin.hubspot.common.HubspotPage;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.Iterator;

/**
 * RecordReader implementation, which reads object instances from Hubspot.
 */
public class HubspotRecordReader extends RecordReader<NullWritable, JsonElement> {

  protected static final Gson GSON = new GsonBuilder().create();

  private HubspotPage currentPage;
  private Iterator<JsonElement> currentPageIterator;
  private JsonElement currentObject;

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException {
    Configuration conf = taskAttemptContext.getConfiguration();
    String configJson = conf.get(HubspotInputFormatProvider.PROPERTY_CONFIG_JSON);
    BaseHubspotConfig baseHubspotConfig = GSON.fromJson(configJson, BaseHubspotConfig.class);
    currentPage = new HubspotHelper().getHupspotPage(baseHubspotConfig, null);
    currentPageIterator = currentPage.getIterator();
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (!currentPageIterator.hasNext()) {
      // switch page
      HubspotPage nextPage = currentPage.nextPage();
      if (nextPage != null) {
        currentPage = nextPage;
        currentPageIterator = currentPage.getIterator();
        return nextKeyValue();
      }
      return false;
    } else {
      currentObject = currentPageIterator.next();
      return true;
    }
  }

  @Override
  public NullWritable getCurrentKey() throws IOException, InterruptedException {
    return null;
  }

  @Override
  public JsonElement getCurrentValue() throws IOException, InterruptedException {
    return currentObject;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return 0;
  }

  @Override
  public void close() throws IOException {

  }
}
