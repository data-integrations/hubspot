# Hubspot Streaming Source

Description
-----------
The plugin allows users to stream data from HubSpot CRM. After the initial pull, which fetches
all the data, updates will periodically be pulled.

Properties
----------

### Authorization

**Authorization method:** Select either Hubspot account API key or private app access token option.

**API Key:** Hubspot account API Key (Deprecated. See this [guide](https://developers.hubspot.com/docs/api/migrate-an-api-key-integration-to-a-private-app) for migrating to using a private app access token.)

**Private app access token:** Private app access token. The app should be allowed to read the respective type (selected below) of Hubspot objects.

### Common properties

**Reference Name:** Name used to uniquely identify this source for lineage, annotating metadata, etc.

**Object type:** Type of the objects to pull from Hubspot.

Should be one of (follow the links for Hubspot documentation):
- [Analytics](https://legacydocs.hubspot.com/docs/methods/analytics/analytics-overview)
- [Contact Lists](https://developers.hubspot.com/docs/methods/lists/create_list)
- [Contacts](https://developers.hubspot.com/docs/api/crm/contacts)
- [Companies](https://developers.hubspot.com/docs/api/crm/companies)
- [Deals](https://developers.hubspot.com/docs/api/crm/deals)
- [Deal Pipelines](https://developers.hubspot.com/docs/api/crm/pipelines)
- [Email events](https://legacydocs.hubspot.com/docs/methods/email/email_events_overview)
- [Email Subscription](https://legacydocs.hubspot.com/docs/methods/email/email_subscriptions_overview)
- [Marketing Email](https://legacydocs.hubspot.com/docs/methods/lists/marketing-email-overview)
- [Products](https://developers.hubspot.com/docs/api/crm/products)
- [Recently Modified Companies](https://legacydocs.hubspot.com/docs/methods/companies/get_companies_modified)
- [Tickets](https://developers.hubspot.com/docs/api/crm/tickets)

**Pull frequency:** Delay interval in between Hubspot API polling for updates.

### Analytics properties

**Time Period:** Time period used to group the data
(to get examples and more details [documentation](https://developers.hubspot.com/docs/methods/analytics/get-analytics-data-breakdowns)).
Must be one of:
- total - Data is rolled up to totals covering the specified time.
- daily - Grouped by day.
- weekly - Grouped by week.
- monthly - Grouped by month.
- summarize/daily - Grouped by day, data is rolled up across all breakdowns.
- summarize/weekly - Grouped by week, data is rolled up across all breakdowns.
- summarize/monthly - Grouped by month, data is rolled up across all breakdowns.

NOTE: When using daily, weekly, or monthly as a Time Period, at least one filter must be present.

**Filters:** Keywords to filter the analytics report data to include only the specified breakdowns.

**Report Type:** Analytics report target to get data for.
Must be one of (see below the details for each option):
- Content
- Category
- Object

**Report Content:** The type of content that you want to get data for
(to get examples and more details see [documentation](https://legacydocs.hubspot.com/docs/methods/analytics/get-data-for-hubspot-content))
Must be one of:
- landing-pages - Pull data for landing pages.
- standard-pages - Pull data for website pages.
- blog-posts - Pull data for individual blog posts.
- listing-pages - Pull data for blog listing pages.
- knowledge-articles - Pull data for knowledge base articles.


**Report Category:** Analytics report category used to break down the analytics data
(to get examples and more details see [documentation](https://legacydocs.hubspot.com/docs/methods/analytics/get-analytics-data-breakdowns)). Must be one of:
- totals - Data will be the totals rolled up from.
- sessions - Data broken down by session details.
- sources - Data broken down by traffic source.
- geolocation - Data broken down by geographic location.
- utm-:utm_type - Data broken down by the standard UTM parameters. :utm_type must be one of campaigns, contents, mediums, sources, or terms (i.e. utm-campaigns).

**Report Object:** Analytics report type of object that you want the analytics data for
(to get examples and more details see [documentation](https://developers.hubspot.com/docs/methods/analytics/get-analytics-data-by-object)). Must be one of:
- event-completions - Get data for analytics events. The results are broken down by the event ID. You can get the details for the events using this endpoint.
- forms - Get data for your HubSpot forms. The results are broken down by form guids. You can get the details for the forms through the Forms API.
- pages - Get data for all URLs with data collected by HubSpot tracking code. The results are broken down by URL.
- social-assists - Get data for messages published through the social publishing tools. The results are broken down by the broadcastGuid of the messages. You can get the details of those messages through the Social Media API.

**Start Date:** A start date for the report data. YYYYMMDD format.

**End Date:** An end date for the report data. YYYYMMDD format.