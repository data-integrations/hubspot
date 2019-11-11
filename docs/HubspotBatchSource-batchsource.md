# Google Ads - batch source

Description
-----------
HubSpot is a developer and marketer of software products for inbound marketing and sales. HubSpot is an inbound marketing and sales platform that helps companies attract visitors, convert leads, and close customers. Its products and services aim to provide tools for social media marketing, content management, web analytics, CRM and search engine optimization. They expose an API that allows you to build an integration to their CRM software. The plugins will allow users to pull data from HubSpot CRM, and use it in CDAP pipelines to enrich and synthesize with other data.

Properties
----------
### Basic

**App Id:** OAuth2 App ID.

**API Key:** OAuth2 API Key.

**Object(s) to pull:** Select from Contact Lists, Contacts, Email Events, Email Subscription, Recent Campaigns, Analytics, Companies, Deals, Deal Pipelines, Marketing Email, Products, Tickets

**Number of API calls per day:** The number of API calls to make per day, to avoid hitting HubSpot's rate limits

### Analytics

**Time Period:** The time period used to group the data. Must be one of:
* total - Data is rolled up to totals covering the specified time.
* daily - Grouped by day
* weekly - Grouped by week
* monthly - Grouped by month
* summarize/daily - Grouped by day, data is rolled up across all breakdowns
* summarize/weekly - Grouped by week, data is rolled up across all breakdowns
* summarize/monthly - Grouped by month, data is rolled up across all breakdowns

NOTE: When using daily, weekly, or monthly for the time_period, you must include at least one filter 

**Report Type:** The analytics report type of content that you want to get data for. Must be one of:
* landing-pages - Pull data for landing pages.
* standard-pages - Pull data for website pages.
* blog-posts - Pull data for individual blog posts.
* listing-pages - Pull data for blog listing pages.
* knowledge-articles - Pull data for knowledge base articles.

The analytics report category used to break down the analytics data. Must be one of:
* totals - Data will be the totals rolled up from
* sessions - Data broken down by session details
* sources - Data broken down by traffic source
* geolocation - Data broken down by geographic location
* utm-:utm_type - Data broken down by the standard UTM parameters. :utm_type must be one of campaigns, contents, mediums, sources, or terms (i.e. utm-campaigns).

The analytics report type of object that you want the analytics data for. Must be one of:
* event-completions - Get data for analytics events. The results are broken down by the event ID. You can get the details for the events using this endpoint.
* forms - Get data for your HubSpot forms. The results are broken down by form guids. You can get the details for the forms through the Forms API.
* pages - Get data for all URLs with data collected by HubSpot tracking code. The results are broken down by URL.
* social-assists - Get data for messages published through the social publishing tools. The results are broken down by the broadcastGuid of the messages. You can get the details of those messages through the Social Media API.

**Start Date:** Start date for the report data. YYYYMMDD format.

**End Date:** End date for the report data. YYYYMMDD format.

**Filters:** Filter the analytics report data to include only the specified breakdowns.
