const shopifyClient = require("../config/shopify");

// Only the variants that need fixing: wrong → canonical
// Correct names (ENVisionLED, Satco, Halco, NaturaLED) are intentionally excluded
const VENDOR_CORRECTIONS = [{ from: "ENVisionLED", to: "EnVisionLED" }];

const PRODUCTS_BY_VENDOR_QUERY = `
  query productsByVendor($first: Int!, $after: String, $query: String!) {
    products(first: $first, after: $after, query: $query) {
      pageInfo {
        hasNextPage
        endCursor
      }
      nodes {
        id
        title
        vendor
      }
    }
  }
`;

const PRODUCT_UPDATE_MUTATION = `
  mutation productUpdate($input: ProductInput!) {
    productUpdate(input: $input) {
      product {
        id
        title
        vendor
      }
      userErrors {
        field
        message
      }
    }
  }
`;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * Queries Shopify only for products whose vendor matches a wrong variant,
 * then updates each one to its canonical vendor name.
 *
 * @param {object}   [options]
 * @param {number}   [options.batchSize=100]  Products per page (max 250)
 * @param {number}   [options.delayMs=300]    Delay between mutation calls (ms)
 * @param {boolean}  [options.dryRun=false]   Preview changes without writing
 * @param {Function} [options.onProgress]     Called after each product is processed
 * @returns {Promise<{updated, failed, total, errors}>}
 */
async function updateProductVendors({
  batchSize = 100,
  delayMs = 200,
  dryRun = false,
  onProgress,
} = {}) {
  const first = Math.min(batchSize, 250);
  let total = 0;
  let updated = 0;
  let failed = 0;
  const errors = [];

  for (const { from, to } of VENDOR_CORRECTIONS) {
    let after = null;

    do {
      const response = await shopifyClient.request(PRODUCTS_BY_VENDOR_QUERY, {
        variables: {
          first,
          after,
          query: `vendor:"${from}"`,
        },
      });

      const topErrors = response?.errors || [];
      if (topErrors.length > 0) {
        throw new Error(topErrors.map((e) => e.message).join("; "));
      }

      const connection = response?.data?.products;
      const nodes = connection?.nodes || [];

      for (const product of nodes) {
        total++;

        if (dryRun) {
          updated++;
          console.log(
            `[DRY RUN] "${product.title}" vendor: "${from}" → "${to}"`,
          );
          onProgress?.({
            index: total,
            status: "dry-run",
            product,
            currentVendor: from,
            targetVendor: to,
          });
          continue;
        }

        const mutResult = await shopifyClient.request(PRODUCT_UPDATE_MUTATION, {
          variables: { input: { id: product.id, vendor: to } },
        });

        const userErrors = mutResult?.data?.productUpdate?.userErrors || [];

        if (userErrors.length > 0) {
          failed++;
          errors.push({
            id: product.id,
            title: product.title,
            currentVendor: from,
            targetVendor: to,
            errors: userErrors,
          });
          onProgress?.({
            index: total,
            status: "failed",
            product,
            currentVendor: from,
            targetVendor: to,
            errors: userErrors,
          });
        } else {
          updated++;
          onProgress?.({
            index: total,
            status: "updated",
            product,
            currentVendor: from,
            targetVendor: to,
          });
        }

        if (delayMs > 0) await sleep(delayMs);
      }

      after = connection?.pageInfo?.endCursor || null;
      if (!connection?.pageInfo?.hasNextPage) break;
    } while (true);
  }

  return { updated, failed, total, errors };
}

module.exports = { updateProductVendors, VENDOR_CORRECTIONS };
