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

import com.google.gson.JsonElement;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * Representing page in Hubspot API
 */
public class HubspotPage {

  private List<JsonElement> hubspotObjects;
  private BaseHubspotConfig hubspotConfig;
  private String offset;
  private Boolean hasNext;

  public HubspotPage(List<JsonElement> hubspotObjects,
                     BaseHubspotConfig hubspotConfig,
                     String offset, boolean hasNext) {
    this.hubspotObjects = hubspotObjects;
    this.hubspotConfig = hubspotConfig;
    this.offset = offset;
    this.hasNext = hasNext;
  }

  public Iterator<JsonElement> getIterator() {
    return hubspotObjects.iterator();
  }

  public HubspotPage nextPage() throws IOException {
    if (hasNext) {
      return new HubspotHelper().getHupspotPage(hubspotConfig, offset);
    } else {
      return null;
    }
  }
}
