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
package io.cdap.plugin.hubspot.source.etl;

import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.etl.spark.Compat;
import io.cdap.cdap.test.TestConfiguration;
import org.junit.BeforeClass;
import org.junit.ClassRule;

public class HubspotStreamingAPISourceETLTest extends HubspotAPISourceETLTest {
  @ClassRule
  public static final TestConfiguration CONFIG_STREAMING =
    new TestConfiguration(Constants.Explore.EXPLORE_ENABLED, false,
                          Constants.AppFabric.SPARK_COMPAT, Compat.SPARK_COMPAT);

  @Override
  public TestsRunner getTestRunner() {
    return new BaseHubspotETLTest.StreamingTestRunner();
  }

  @BeforeClass
  public static void setupTestClass() throws Exception {
    getCredentials();
    StreamingInitializer.setupTestClass();
  }
}
