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

import com.google.gson.JsonElement;

import java.io.IOException;
import java.util.Iterator;

/**
 * Iterates over all records in all pages.
 */
public class HubspotPagesIterator implements Iterator<JsonElement> {
  private HubspotPage currentPage;
  private Iterator<JsonElement> currentPageIterator;

  public HubspotPagesIterator(BaseHubspotConfig config) throws IOException {
    this.currentPage = new HubspotHelper().getHubspotPage(config, null);
    this.currentPageIterator = currentPage.getIterator();
  }

  public void switchPageIfNeeded() throws IOException {
    if (!currentPageIterator.hasNext()) {
      // switch page
      HubspotPage nextPage = currentPage.nextPage();

      if (nextPage != null) {
        currentPage = nextPage;
        currentPageIterator = currentPage.getIterator();
      } else {
        currentPageIterator = null;
      }
    }
  }

  @Override
  public boolean hasNext() {
    try {
      switchPageIfNeeded();
    } catch (IOException e) {
      throw new RuntimeException("Failed to switch to next page", e);
    }
    return (currentPageIterator != null);
  }

  @Override
  public JsonElement next() {
    return currentPageIterator.next();
  }
}
