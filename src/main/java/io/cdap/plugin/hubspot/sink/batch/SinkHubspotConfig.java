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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.hubspot.common.BaseHubspotConfig;
import io.cdap.plugin.hubspot.common.ConfigValidator;

/**
 * Provides Sink configuration for accessing Hubspot API.
 */
public class SinkHubspotConfig extends BaseHubspotConfig {

  public static final String OBJECT_FIELD = "objectField";

  @Name(OBJECT_FIELD)
  @Description("Name of Field with object description json.")
  @Macro
  public String objectField;

  public SinkHubspotConfig(String referenceName) {
    super(referenceName);
  }

  public void validate(FailureCollector failureCollector) {
    ConfigValidator.validateSinkObjectType(this, failureCollector);
  }
}
