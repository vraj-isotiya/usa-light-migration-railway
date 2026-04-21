const { createAdminApiClient } = require("@shopify/admin-api-client");

const shopifyClient = createAdminApiClient({
  storeDomain: process.env.SHOPIFY_STORE,
  apiVersion: process.env.SHOPIFY_API_VERSION,
  accessToken: process.env.SHOPIFY_ACCESS_TOKEN,
});

module.exports = shopifyClient;
