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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.common.ReferencePluginConfig;
import io.cdap.plugin.hubspot.common.ConfigValidator;
import io.cdap.plugin.hubspot.common.ObjectType;

import javax.annotation.Nullable;

/**
 * Provides Sink configuration for accessing Hubspot API
 */
public class SinkHubspotConfig extends ReferencePluginConfig {

  public static final String API_KEY = "apiKey";
  public static final String OBJECT_TYPE = "objectType";
  public static final String API_SERVER_URL = "apiServerUrl";
  public static final String OBJECT_FIELD = "objectField";

  @Name(API_SERVER_URL)
  @Description("Api Server Url. Not visible, by default null, can be redefined")
  @Macro
  @Nullable
  public String apiServerUrl;
  @Name(API_KEY)
  @Description("OAuth2 API Key")
  @Macro
  public String apiKey;
  @Name(OBJECT_TYPE)
  @Description("Name of Object(s) to put to Hubspot.")
  @Macro
  public String objectType;
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

  public ObjectType getObjectType() {
    return ObjectType.fromString(objectType);
  }

}
