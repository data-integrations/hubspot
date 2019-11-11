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
package io.cdap.plugin.hubspot.common;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.common.ReferencePluginConfig;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Provides base configuration for accessing Hubspot API
 */
public class BaseHubspotConfig extends ReferencePluginConfig {

  public static final String APP_ID = "appId";
  public static final String API_KEY = "apiKey";
  public static final String OBJECT_TYPE = "objectType";
  public static final String TIME_PERIOD = "timePeriod";
  public static final String REPORT_TYPE = "reportType";
  public static final String CALLS_PER_DAY = "callsPerDay";
  public static final String START_DATE = "startDate";
  public static final String END_DATE = "endDate";
  public static final String FILTERS = "filters";
  @Name(FILTERS)
  @Description(" Filter the analytics report data to include only the specified breakdowns.")
  @Macro
  @Nullable
  public String filters;
  @Name(START_DATE)
  @Description("Start date for the analytics report data. YYYYMMDD format.")
  @Macro
  @Nullable
  public String startDate;
  @Name(END_DATE)
  @Description("End date for the analytics report data. YYYYMMDD format.")
  @Macro
  @Nullable
  public String endDate;
  @Name(REPORT_TYPE)
  @Description("The analytics report type of content that you want to get data for. Must be one of:\n" +
    "\n" +
    "landing-pages - Pull data for landing pages. \n" +
    "standard-pages - Pull data for website pages. \n" +
    "blog-posts - Pull data for individual blog posts. \n" +
    "listing-pages - Pull data for blog listing pages.\n" +
    "knowledge-articles - Pull data for knowledge base articles.\n" +
    "\n" +
    "The analytics report category used to break down the analytics data. Must be one of:\n" +
    "\n" +
    "totals - Data will be the totals rolled up from \n" +
    "sessions - Data broken down by session details \n" +
    "sources - Data broken down by traffic source\n" +
    "geolocation - Data broken down by geographic location\n" +
    "utm-:utm_type - Data broken down by the standard UTM parameters. " +
    ":utm_type must be one of campaigns, contents, mediums, sources, or terms (i.e. utm-campaigns).\n" +
    "\n" +
    "The analytics report type of object that you want the analytics data for. Must be one of:\n" +
    "\n" +
    "event-completions - Get data for analytics events. The results are broken down by the event ID." +
    " You can get the details for the events using this endpoint.\n" +
    "forms - Get data for your HubSpot forms. The results are broken down by form guids." +
    " You can get the details for the forms through the Forms API. \n" +
    "pages - Get data for all URLs with data collected by HubSpot tracking code." +
    " The results are broken down by URL. \n" +
    "social-assists - Get data for messages published through the social publishing tools." +
    " The results are broken down by the broadcastGuid of the messages." +
    " You can get the details of those messages through the Social Media API.")
  @Macro
  @Nullable
  public String reportType;
  @Name(TIME_PERIOD)
  @Description("The time period used to group the data. Must be one of:\n" +
    "\n" +
    "total - Data is rolled up to totals covering the specified time.\n" +
    "daily - Grouped by day\n" +
    "weekly - Grouped by week\n" +
    "monthly - Grouped by month\n" +
    "summarize/daily - Grouped by day, data is rolled up across all breakdowns \n" +
    "summarize/weekly - Grouped by week, data is rolled up across all breakdowns \n" +
    "summarize/monthly - Grouped by month, data is rolled up across all breakdowns")
  @Macro
  @Nullable
  public String timePeriod;
  @Name(APP_ID)
  @Description("OAuth2 App ID")
  @Macro
  public String appId;
  @Name(API_KEY)
  @Description("OAuth2 API Key")
  @Macro
  public String apiKey;
  @Name(OBJECT_TYPE)
  @Description("Select from Contact Lists, Contacts, Email Events, Email Subscription," +
    " Recent Campaigns, Analytics, Companies, Deals, Deal Pipelines, Marketing Email, Products, Tickets")
  @Macro
  public String objectType;
  @Name(CALLS_PER_DAY)
  @Description("The number of API calls to make per day, to avoid hitting HubSpot's rate limits")
  @Macro
  public int callsPerDay;

  public BaseHubspotConfig(String referenceName) {
    super(referenceName);
  }

  public void validate(FailureCollector failureCollector) {
    validateAuthorization(failureCollector);
    validateObjectType(failureCollector);
    if (!containsMacro(OBJECT_TYPE) && getObjectType().equals(ObjectType.ANALYTICS)) {
      validateReportType(failureCollector);
      validateTimePeriod(failureCollector);
      validateFilters(failureCollector);
      validateDateRange(failureCollector);
    }
  }

  private void validateTimePeriod(FailureCollector failureCollector) {
    if (containsMacro(TIME_PERIOD)) {
      return;
    }
    try {
      getTimePeriod();
    } catch (IllegalArgumentException e) {
      failureCollector.addFailure(String.format("Time Period '%s' is not a valid Time Period", timePeriod),
                                  null).withConfigProperty(TIME_PERIOD);
    }
  }

  private void validateFilters(FailureCollector failureCollector) {
    if (containsMacro(FILTERS) &&
      containsMacro(TIME_PERIOD)) {
      return;
    }
    List<String> filters = getFilters();
    for (String filter : filters) {
      if (filter.isEmpty()) {
        failureCollector.addFailure(String.format("Filter '%s' is not a valid filter", filter),
                                    null).withConfigProperty(FILTERS);
      }
    }
    switch (getTimePeriod()) {
      case DAILY:
      case WEEKLY:
      case MONTHLY:
        if (filters.isEmpty()) {
          failureCollector.addFailure("When using daily, weekly, or monthly for the time_period," +
                                       " you must include at least one filter",
                                     null).withConfigProperty(FILTERS);
        }
    }
  }

  private void validateReportType(FailureCollector failureCollector) {
    if (containsMacro(REPORT_TYPE)) {
      return;
    }
    try {
      getReportType();
    } catch (IllegalArgumentException e) {
      failureCollector.addFailure(String.format("reportFormat '%s' is not a valid Report Type", reportType),
                                  null).withConfigProperty(REPORT_TYPE);
    }
  }

  protected void validateObjectType(FailureCollector failureCollector) {
    if (containsMacro(OBJECT_TYPE)) {
      return;
    }
    try {
      getObjectType();
    } catch (IllegalArgumentException e) {
      failureCollector.addFailure(String.format("reportFormat '%s' is not a valid Object Type", objectType),
                                  null).withConfigProperty(OBJECT_TYPE);
    }
  }

  protected void validateAuthorization(FailureCollector failureCollector) {
    if (containsMacro(API_KEY)
      || containsMacro(APP_ID)) {
      return;
    }
    //todo validate
  }

  public Schema getSchema() {
    Set<Schema.Field> schemaFields = new HashSet<>();
    schemaFields.add(Schema.Field.of("objectType", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    schemaFields.add(Schema.Field.of("object", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    return Schema.recordOf("HubspotObject", schemaFields);
  }

  public ObjectType getObjectType() {
    return ObjectType.fromString(objectType);
  }

  protected void validateDateRange(FailureCollector failureCollector) {
    if (containsMacro(START_DATE)
      || containsMacro(END_DATE)) {
      return;
    }
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyDDmm");
    Date startDate = null;
    Date endDate = null;
    try {
      startDate = simpleDateFormat.parse(this.startDate);
    } catch (ParseException e) {
      failureCollector.addFailure("Invalid startDate format.", "Use YYYYMMDD date format.")
        .withConfigProperty(START_DATE);
    }
    try {
      endDate = simpleDateFormat.parse(this.endDate);
    } catch (ParseException e) {
      failureCollector.addFailure("Invalid endDate format.", "Use YYYYMMDD date format.")
        .withConfigProperty(END_DATE);
    }
    if (startDate != null &&
      endDate != null &&
      startDate.after(endDate)) {
      failureCollector.addFailure("startDate must be earlier than endDate.", "Enter valid date.");
    }
  }

  @Nullable
  public ReportType getReportType() {
    return ReportType.fromString(reportType);
  }

  @Nullable
  public TimePeriod getTimePeriod() {
    return TimePeriod.fromString(timePeriod);
  }

  @Nullable
  public List<String> getFilters() {
    List<String> list = new ArrayList();
    if (filters != null && !filters.isEmpty()) {
      list.addAll(Arrays.asList(filters.split(",")));
    }
    return list;
  }
}
