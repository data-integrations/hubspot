# Hubspot Batch Sink
Description
-----------
A batch sink that inserts objects into Hubspot.

### Authorization

**Authorization method:** Select either Hubspot account API key or private app access token option.

**API Key:** Hubspot account API Key (Deprecated. See this [guide](https://developers.hubspot.com/docs/api/migrate-an-api-key-integration-to-a-private-app) for migrating to using a private app access token).

**Private app access token:** Private app access token. The app should be allowed to write the respective type (selected below) of Hubspot objects.

### Properties

**Reference Name:** Name used to uniquely identify this source for lineage, annotating metadata, etc.

**Object type:** The type of Hubspot objects to be inserted.

The currently available options are below (follow the links for Hubspot documentation):
  [Contact Lists](https://developers.hubspot.com/docs/methods/lists/create_list)
  [Contacts](https://developers.hubspot.com/docs/methods/contacts/create_contact)
  [Companies](https://developers.hubspot.com/docs/methods/companies/create_company)
  [Deals](https://developers.hubspot.com/docs/methods/deals/create_deal)
  [Deal Pipelines](https://developers.hubspot.com/docs/methods/pipelines/create_new_pipeline)
  [Marketing Email](https://developers.hubspot.com/docs/methods/cms_email/create-a-new-marketing-email)
  [Products](https://developers.hubspot.com/docs/methods/products/create-product)
  [Tickets](https://developers.hubspot.com/docs/methods/tickets/create-ticket)
  
  
  **Input Field Name:** Name of field with object description json.