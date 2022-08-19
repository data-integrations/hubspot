/*
 * Copyright © 2022 Cask Data, Inc.
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

import io.cdap.cdap.etl.mock.test.HydratorTestBase;

import javax.annotation.Nullable;

public abstract class BaseETLTest extends HydratorTestBase {
    @Nullable
    protected static String apiKey = null;
    @Nullable
    protected static String accessToken;

    protected static void getCredentials() {
        accessToken = System.getProperty("hubspot.access.token");
        if (accessToken == null || accessToken.isEmpty()) {
            accessToken = null;
            apiKey = System.getProperty("hubspot.api.key");
            if (apiKey == null || apiKey.isEmpty()) {
                throw new IllegalArgumentException("Either hubspot.access.token or " +
                        "hubspot.api.key system property must be present and not empty.");
            }
        }
    }
}
