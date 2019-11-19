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
 * WARRANTIES O R CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.cdap.plugin.hubspot.common;

import io.cdap.cdap.etl.api.FailureCollector;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * Helper class to incorporate Hubspot Config Validation
 */
public class ConfigValidator {
  public static void validateTimePeriod(BaseHubspotConfig baseHubspotConfig, FailureCollector failureCollector) {
    if (baseHubspotConfig.containsMacro(BaseHubspotConfig.TIME_PERIOD)) {
      return;
    }
    try {
      TimePeriod period = baseHubspotConfig.getTimePeriod();
      if (!baseHubspotConfig.containsMacro(BaseHubspotConfig.REPORT_TYPE) &&
        baseHubspotConfig.getReportEndpoint().equals(ReportEndpoint.TOTALS)) {
        switch (period) {
          case MONTHLY:
          case WEEKLY:
          case DAILY:
            failureCollector.addFailure(String.format("Time period '%s' is not valid for '%s'.",
                                                      baseHubspotConfig.timePeriod,
                                                      baseHubspotConfig.reportType),
                                        "Use summarized Time Periods for totals.")
              .withConfigProperty(BaseHubspotConfig.TIME_PERIOD);
        }
      }
    } catch (IllegalArgumentException e) {
      failureCollector.addFailure(String.format("Time period '%s' is not valid.", baseHubspotConfig.timePeriod),
                                  "Select one of: total, daily, weekly, monthly, summarize/daily, " +
                                    "summarize/weekly, summarize/monthly")
        .withConfigProperty(BaseHubspotConfig.TIME_PERIOD);
    }
  }

  static void validateFilters(BaseHubspotConfig baseHubspotConfig, FailureCollector failureCollector) {
    if (baseHubspotConfig.containsMacro(BaseHubspotConfig.FILTERS) ||
      baseHubspotConfig.containsMacro(BaseHubspotConfig.TIME_PERIOD)) {
      return;
    }
    List<String> filters = baseHubspotConfig.getFilters();
    switch (baseHubspotConfig.getTimePeriod()) {
      case DAILY:
      case WEEKLY:
      case MONTHLY:
        if (filters == null || filters.isEmpty()) {
          failureCollector.addFailure("NO filters defined.",
                                      "When using daily, weekly, or monthly for the time_period," +
                                        " you must include at least one filter.")
            .withConfigProperty(BaseHubspotConfig.FILTERS);
        }
        for (String filter : filters) {
          if (filters == null || filters.isEmpty()) {
            failureCollector.addFailure("Filter must not be empty.",
                                        null).withConfigProperty(BaseHubspotConfig.FILTERS);
          } else {
            if (!filter.matches("\\w+")) {
              failureCollector.addFailure(String.format("Filter '%s' is not a valid filter", filter),
                                          "Filter must one word without special symbols")
                .withConfigProperty(BaseHubspotConfig.FILTERS);

            }
          }
        }
    }
  }

  static void validateReportType(BaseHubspotConfig baseHubspotConfig, FailureCollector failureCollector) {
    if (baseHubspotConfig.containsMacro(BaseHubspotConfig.REPORT_TYPE)) {
      return;
    }
    try {
      switch (baseHubspotConfig.getReportType()) {
        case REPORT_CATEGORY:
          if (baseHubspotConfig.containsMacro(BaseHubspotConfig.REPORT_CATEGORY)) {
            return;
          }
          try {
            baseHubspotConfig.getReportEndpoint(baseHubspotConfig.reportCategory);
          } catch (IllegalArgumentException e) {
            failureCollector.addFailure(String.format("Report Category '%s' is not valid.",
                                                      baseHubspotConfig.reportCategory),
                                        null).withConfigProperty(BaseHubspotConfig.REPORT_CATEGORY);
          }
          break;
        case REPORT_OBJECT:
          if (baseHubspotConfig.containsMacro(BaseHubspotConfig.REPORT_OBJECT)) {
            return;
          }
          try {
            baseHubspotConfig.getReportEndpoint(baseHubspotConfig.reportObject);
          } catch (IllegalArgumentException e) {
            failureCollector.addFailure(String.format("Report Object '%s' is not valid.",
                                                      baseHubspotConfig.reportObject),
                                        null).withConfigProperty(BaseHubspotConfig.REPORT_OBJECT);
          }
          break;
        case REPORT_CONTENT:
          if (baseHubspotConfig.containsMacro(BaseHubspotConfig.REPORT_CONTENT)) {
            return;
          }
          try {
            baseHubspotConfig.getReportEndpoint(baseHubspotConfig.reportContent);
          } catch (IllegalArgumentException e) {
            failureCollector.addFailure(String.format("Report Content '%s' is not valid.",
                                                      baseHubspotConfig.reportContent),
                                        null).withConfigProperty(BaseHubspotConfig.REPORT_CONTENT);
          }
          break;
      }
    } catch (IllegalArgumentException e) {
      failureCollector.addFailure(String.format("Report Type '%s' is not valid.", baseHubspotConfig.reportType),
                                  null).withConfigProperty(BaseHubspotConfig.REPORT_TYPE);
    }
  }

  protected static void validateObjectType(BaseHubspotConfig baseHubspotConfig, FailureCollector failureCollector) {
    if (baseHubspotConfig.containsMacro(BaseHubspotConfig.OBJECT_TYPE)) {
      return;
    }
    try {
      baseHubspotConfig.getObjectType();
    } catch (IllegalArgumentException e) {
      failureCollector.addFailure(String.format("Object Type '%s' is not valid.", baseHubspotConfig.objectType),
                                  null).withConfigProperty(BaseHubspotConfig.OBJECT_TYPE);
    }
  }

  protected static void validateAuthorization(BaseHubspotConfig baseHubspotConfig, FailureCollector failureCollector) {
    if (baseHubspotConfig.containsMacro(BaseHubspotConfig.TIME_PERIOD) ||
      baseHubspotConfig.containsMacro(BaseHubspotConfig.FILTERS) ||
      baseHubspotConfig.containsMacro(BaseHubspotConfig.REPORT_TYPE) ||
      baseHubspotConfig.containsMacro(BaseHubspotConfig.OBJECT_TYPE) ||
      baseHubspotConfig.containsMacro(BaseHubspotConfig.API_KEY) ||
      baseHubspotConfig.containsMacro(BaseHubspotConfig.START_DATE) ||
      baseHubspotConfig.containsMacro(BaseHubspotConfig.END_DATE)) {
      return;
    }
    try {
      new HubspotHelper().getHupspotPage(baseHubspotConfig, null);
    } catch (IOException e) {
      if (e.getMessage().toLowerCase().contains("forbidden")) {
        failureCollector.addFailure("Api endpoint not accessible with provided Api Key.", null)
          .withConfigProperty(BaseHubspotConfig.API_KEY);
      } else {
        failureCollector.addFailure("Api endpoint not accessible with provided configuration.", null);
      }
    }
  }

  protected static void validateDateRange(BaseHubspotConfig baseHubspotConfig, FailureCollector failureCollector) {
    if (baseHubspotConfig.containsMacro(BaseHubspotConfig.START_DATE)
      || baseHubspotConfig.containsMacro(BaseHubspotConfig.END_DATE)) {
      return;
    }
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyDDmm");
    Date startDate = null;
    Date endDate = null;
    if (baseHubspotConfig.startDate == null || baseHubspotConfig.startDate.isEmpty()) {
      failureCollector.addFailure("Start Date not defined for ANALYTICS object selected.",
                                  "Use YYYYMMDD date format.")
        .withConfigProperty(BaseHubspotConfig.START_DATE);
    }
    if (baseHubspotConfig.endDate == null || baseHubspotConfig.endDate.isEmpty()) {
      failureCollector.addFailure("End Date not defined for ANALYTICS object selected.",
                                  "Use YYYYMMDD date format.")
        .withConfigProperty(BaseHubspotConfig.END_DATE);
    }
    if (baseHubspotConfig.startDate != null && baseHubspotConfig.endDate != null) {
      try {
        startDate = simpleDateFormat.parse(baseHubspotConfig.startDate);
      } catch (ParseException e) {
        failureCollector.addFailure("Invalid startDate format.", "Use YYYYMMDD date format.")
          .withConfigProperty(BaseHubspotConfig.START_DATE);
      }
      try {
        endDate = simpleDateFormat.parse(baseHubspotConfig.endDate);
      } catch (ParseException e) {
        failureCollector.addFailure("Invalid endDate format.", "Use YYYYMMDD date format.")
          .withConfigProperty(BaseHubspotConfig.END_DATE);
      }
      if (startDate != null &&
        endDate != null &&
        startDate.after(endDate)) {
        failureCollector.addFailure("startDate must be earlier than endDate.", "Enter valid date.");
      }
    }
  }
}
