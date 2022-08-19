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
package io.cdap.plugin.hubspot.common;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.plugin.common.ReferencePluginConfig;

import javax.annotation.Nullable;

/**
 * Provides base configuration for accessing Hubspot API.
 */
public class BaseHubspotConfig extends ReferencePluginConfig {

  public static final String API_SERVER_URL = "apiServerUrl";
  public static final String OBJECT_TYPE = "objectType";
  public static final String AUTHORIZATION_METHOD = "authorizationMethod";
  public static final String API_KEY = "apiKey";
  public static final String ACCESS_TOKEN = "accessToken";
  public static final String OAUTH_INFO = "oAuthInfo";
  public static final String DEFAULT_API_SERVER_URL = "https://api.hubapi.com";

  @Name(API_SERVER_URL)
  @Description("Api Server Url. Not visible, by default null, can be redefined")
  @Macro
  @Nullable
  public String apiServerUrl;
  @Name(OBJECT_TYPE)
  @Description("Name of object to pull from Hubspot.")
  @Macro
  public String objectType;
  @Name(AUTHORIZATION_METHOD)
  @Description("Authorization method.")
  @Macro
  @Nullable
  public String authorizationMethod;
  @Name(API_KEY)
  @Description("OAuth2 API Key")
  @Macro
  @Nullable
  public String apiKey;
  @Name(ACCESS_TOKEN)
  @Description("Private app access token")
  @Macro
  @Nullable
  public String accessToken;
  @Name(OAUTH_INFO)
  @Description("OAuth information for connecting to Hubspot. " +
          "It is expected to be a json string containing two properties, \"accessToken\" and \"instanceURL\", " +
          "which carry the OAuth access token and the URL to connect to respectively. " +
          "Use the ${oauth(provider, credentialId)} macro function for acquiring OAuth information dynamically. ")
  @Macro
  @Nullable
  public OAuthInfo oAuthInfo;

  public BaseHubspotConfig(String referenceName) {
    super(referenceName);
  }

  /**
   * Constructor for BaseHubspotConfig object.
   * @param referenceName the reference name
   * @param apiServerUrl the api server url of hub spot
   * @param objectType the object type
   * @param apiKey the api key of hub spot
   */
  public BaseHubspotConfig(String referenceName,
                           String apiServerUrl,
                           String objectType,
                           String apiKey,
                           String accessToken) {
    super(referenceName);
    this.apiServerUrl = apiServerUrl;
    this.objectType = objectType;
    this.apiKey = apiKey;
    this.accessToken = accessToken;
  }

  public ObjectType getObjectType() {
    return ObjectType.fromString(objectType);
  }

  /**
   * Returns the string as an api server url.
   * @return the string as an api server url
   */
  public String getApiServerUrl() {
    String apiServerUrl = BaseHubspotConfig.DEFAULT_API_SERVER_URL;
    if (this.apiServerUrl != null &&
      !this.apiServerUrl.isEmpty()) {
      apiServerUrl = this.apiServerUrl;
    }
    return apiServerUrl;
  }

  public String getApiKey() {
    return apiKey == null ? "" : apiKey;
  }

  public String getAccessToken() {
    if (!this.containsMacro(OAUTH_INFO) && oAuthInfo != null) {
      String oauthAccessToken = oAuthInfo.getAccessToken();
      if (oauthAccessToken != null && !oauthAccessToken.isEmpty()) {
        accessToken = oauthAccessToken;
      }
    }
    return accessToken == null ? "" : accessToken;
  }
}
