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

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.data.schema.Schema.LogicalType;
import java.util.Arrays;

/**
 * Convenience enum to map ObjectType UI selections to meaningful values.
 *
 * Select from Contact Lists, Contacts, Email Events, Email Subscription,
 * Recent Campaigns, Analytics, Companies, Deals, Deal Pipelines, Marketing Email, Products, Tickets
 */
public enum ObjectType {

  CONTACT_LISTS("Contact Lists", null),
  CONTACTS("Contacts", Schema.recordOf("contact",
      Schema.Field.of("email", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("firstname", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("lastname", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("phone", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("company", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("website", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("lifecyclestage", Schema.nullableOf(Schema.of(Schema.Type.STRING))))),
  EMAIL_EVENTS("Email Events", Schema.recordOf("email_event",
      Schema.Field.of("type", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("recipient", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("portalId", Schema.nullableOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("appId", Schema.nullableOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("appName", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("emailCampaignId", Schema.nullableOf(Schema.of(Schema.Type.LONG))))),
  EMAIL_SUBSCRIPTION("Email Subscription", Schema.recordOf("email_subscription",
      Schema.Field.of("type", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("recipient", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("portalId", Schema.nullableOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("appId", Schema.nullableOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("appName", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("emailCampaignId", Schema.nullableOf(Schema.of(Schema.Type.LONG))))),
  RECENT_COMPANIES("Recent Companies", null),
  ANALYTICS("Analytics", null),
  COMPANIES("Companies", Schema.recordOf("company",
      Schema.Field.of("name", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("domain", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("city", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("industry", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("phone", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("state", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("lifecyclestage", Schema.nullableOf(Schema.of(Schema.Type.STRING))))),
  DEALS("Deals", Schema.recordOf("deal",
      Schema.Field.of("amount", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
      Schema.Field.of("closedate", Schema.nullableOf(Schema.of(LogicalType.TIMESTAMP_MICROS))),
      Schema.Field.of("dealname", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("pipeline", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("dealstage", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("hubspot_owner_id", Schema.nullableOf(Schema.of(Schema.Type.LONG))))),
  DEAL_PIPELINES("Deal Pipelines", null),
  MARKETING_EMAIL("Marketing Email", null),
  PRODUCTS("Products", null),
  TICKETS("Tickets", Schema.recordOf("deal",
      Schema.Field.of("hs_pipeline", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("hs_pipeline_stage", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("hs_ticket_priority", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("subject", Schema.nullableOf(Schema.of(Schema.Type.STRING)))));

  private final String stringValue;
  private final Schema schema;

  ObjectType(String stringValue, Schema schema) {
    this.stringValue = stringValue;
    this.schema = schema;
  }

  /**
   * Returns the ObjectType.
   * @param value the value is string type
   * @return the ObjectType
   */
  public static ObjectType fromString(String value) {
    return Arrays.stream(ObjectType.values())
      .filter(type -> type.stringValue.equals(value))
      .findFirst()
      .orElseThrow(() -> new IllegalArgumentException(String.format("'%s' is invalid ObjectType.", value)));
  }

  public Schema getSchema() {
    return schema;
  }
}
