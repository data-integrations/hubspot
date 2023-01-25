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
package io.cdap.plugin.hubspot.sink.batch;

import io.cdap.plugin.hubspot.common.HubspotHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.http.Header;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHeader;

/**
 * Submit {@link String} records to Hubspot.
 */
public class HubspotRecordWriter extends RecordWriter<NullWritable, String> {
  private final SinkHubspotConfig config;

  private static final Header POST_REQUEST_HEADER = new BasicHeader("Content-Type", "application/json");

  /**
   * Constructor for HubspotRecordWriter object.
   * @param taskAttemptContext the task attempt context
   */
  public HubspotRecordWriter(TaskAttemptContext taskAttemptContext) {
    Configuration configuration = taskAttemptContext.getConfiguration();
    String configJson = configuration.get(HubspotOutputFormatProvider.PROPERTY_CONFIG_JSON);
    config = HubspotOutputFormatProvider.GSON.fromJson(configJson, SinkHubspotConfig.class);
  }

  @Override
  public void write(NullWritable nullWritable, String input) {
    try {
      HttpPost request = (HttpPost) HubspotHelper.addCredentialsToRequest(
              new HttpPost(getSinkEndpoint(config)), config);
      request.addHeader(POST_REQUEST_HEADER);
      request.setEntity(new StringEntity(input));
      HubspotHelper.executeRequestWithRetries(request);

    } catch (Exception e) {
      throw new RuntimeException("Submit record to Hubspot failed with:", e);
    }
  }

  @Override
  public void close(TaskAttemptContext taskAttemptContext) {
    //no-op
  }

  private static String getSinkEndpoint(SinkHubspotConfig sinkHubspotConfig) {
    String apiServerUrl = sinkHubspotConfig.getApiServerUrl();
    switch (sinkHubspotConfig.getObjectType()) {
      case CONTACT_LISTS:
        return String.format("%s/contacts/v1/lists", apiServerUrl);
      case CONTACTS:
        return String.format("%s/contacts/v1/contact", apiServerUrl);
      case COMPANIES:
        return String.format("%s/companies/v2/companies", apiServerUrl);
      case DEALS:
        return String.format("%s/deals/v1/deal", apiServerUrl);
      case DEAL_PIPELINES:
        return String.format("%s/crm-pipelines/v1/pipelines/deals", apiServerUrl);
      case MARKETING_EMAIL:
        return String.format("%s/marketing-emails/v1/emails", apiServerUrl);
      case PRODUCTS:
        return String.format("%s/crm-objects/v1/objects/products", apiServerUrl);
      case TICKETS:
        return String.format("%s/crm-objects/v1/objects/tickets", apiServerUrl);
      case ANALYTICS:
      case EMAIL_EVENTS:
      case EMAIL_SUBSCRIPTION:
      case RECENT_COMPANIES:
      default:
        return null;
    }
  }
}
