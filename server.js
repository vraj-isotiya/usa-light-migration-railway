require("dotenv").config();
const express = require("express");
const fs = require("fs");
const path = require("path");
const shopifyClient = require("./config/shopify");
const { bulkImport } = require("./lib/import-service");
const { exportProductsToFile } = require("./lib/export-products-service");
const {
  migrateCategoriesToCollections,
  syncCategoryCollectionUrlMap,
} = require("./lib/migrate-categories-service");
const { migrateCustomers } = require("./lib/migrate-customers-service");
const { migrateOrders } = require("./lib/migrate-orders-service");
const { updateProductVendors } = require("./lib/update-vendor-service");

const app = express();

app.use(express.json());

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

app.get("/products", async (req, res) => {
  try {
    const response = await shopifyClient.request(`
        query {
          products(first: 5) {
            edges {
              node {
                id
                title
                handle
              }
            }
          }
        }
      `);

    res.json(response.data.products.edges);
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Shopify query failed" });
  }
});

app.get("/publishlocations", async (req, res) => {
  try {
    const response = await shopifyClient.request(`
  query {
    publications(first: 10) {
      edges {
        node {
          id
          name
        }
      }
    }
  }
`);
    res.json(response.data.publications.edges);
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Shopify query failed" });
  }
});

//////////////////////////////////////////////////////
// LOCATIONS
//////////////////////////////////////////////////////
app.get("/locations", async (req, res) => {
  try {
    const first = Math.min(parseInt(req.query.first, 10) || 50, 100);
    const response = await shopifyClient.request(
      `query {
  locations(first: 10) {
    edges {
      node {
        id
        name
        address {
          formatted
        }
        fulfillsOnlineOrders
        isActive
      }
    }
  }
}
`,
    );
    res.json(response.data.locations.edges);
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Failed to fetch locations" });
  }
});

//////////////////////////////////////////////////////
// BULK IMPORT (productSet mutation - new Shopify API)
//////////////////////////////////////////////////////
app.get("/import-json", async (req, res) => {
  try {
    const file = req.query.file || "products.json";
    const raw = fs.readFileSync(`./${file}`, "utf8");
    const products = JSON.parse(raw);

    const startParam = parseInt(req.query.start, 10);
    const endParam = parseInt(req.query.end, 10);
    const limit = parseInt(req.query.limit, 10);

    const start = !Number.isNaN(startParam) && startParam >= 0 ? startParam : 0;
    const end =
      !Number.isNaN(endParam) && endParam >= start ? endParam + 1 : undefined;
    let items = products.slice(start, end);
    if (!Number.isNaN(limit) && limit > 0) {
      items = items.slice(0, limit);
    }

    const results = await bulkImport(items, {
      locationId: process.env.SHOPIFY_LOCATION_ID,
      delayMs: 400,
      allProducts: products,
      onProgress: ({ index, total, product, status }) => {
        const title = product?.title || product?.productname || `#${index}`;
        if (status === "skipped") {
          console.log(`[SKIPPED] [${index}/${total}] ${title}`);
          return;
        }
        const icon = status === "created" ? "✅" : "❌";
        console.log(`${icon} [${index}/${total}] ${title}`);
      },
    });

    res.json({
      message: "Import completed",
      created: results.created,
      failed: results.failed,
      skipped: results.skipped || 0,
      total: items.length,
      errors: results.errors.slice(0, 10),
      mappingReportFile: results.mappingReportFile,
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Import failed", message: err.message });
  }
});

//////////////////////////////////////////////////////
// EXPORT PRODUCTS (Admin GraphQL -> JSON file)
//////////////////////////////////////////////////////
app.get("/export-products", async (req, res) => {
  try {
    const { outputPath, total } = await exportProductsToFile(100);
    res.json({
      message: "Products exported successfully",
      total,
      file: outputPath,
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({
      error: "Export failed",
      message: err.message || String(err),
    });
  }
});

//////////////////////////////////////////////////////
// MIGRATE CATEGORIES (Admin GraphQL -> Collections)
//////////////////////////////////////////////////////
app.get("/migrate-categories", async (req, res) => {
  try {
    const file = req.query.file || "categories.json";
    const limit = Number.parseInt(req.query.limit, 10);
    const startIndex = Number.parseInt(req.query.start, 10);
    const endIndex = Number.parseInt(req.query.end, 10);
    const delayMs = Number.parseInt(req.query.delayMs, 10);
    const parentIds = req.query.parentIds || "";

    const result = await migrateCategoriesToCollections({
      file,
      limit: Number.isFinite(limit) ? limit : undefined,
      startIndex: Number.isFinite(startIndex) ? startIndex : undefined,
      endIndex: Number.isFinite(endIndex) ? endIndex : undefined,
      delayMs: Number.isFinite(delayMs) ? delayMs : undefined,
      parentIds,
      onProgress: ({ index, total, status, category }) => {
        const title =
          category?.categoryname || category?.categoryid || `#${index}`;
        const icon = status === "created" ? "✅" : "❌";
        console.log(`${icon} [${index}/${total}] ${title}`);
      },
    });

    res.json({
      message: "Import completed",
      created: result.created,
      failed: result.failed,
      total: result.total,
      errors: result.errors.slice(0, 10),
      mappingFile: result.mappingFile,
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({
      error: "Category migration failed",
      message: err.message || String(err),
    });
  }
});

//////////////////////////////////////////////////////
// SYNC CATEGORY-COLLECTION URL MAP
// Validates `shopifyCollectionId`; creates missing collections and updates map.
//////////////////////////////////////////////////////
app.get("/sync-category-collection-map", async (req, res) => {
  try {
    const mappingFile = req.query.file || "category-collection-url-map.json";
    const categoriesFile = req.query.categoriesFile || "categories.json";
    const delayMs = Number.parseInt(req.query.delayMs, 10);

    const result = await syncCategoryCollectionUrlMap({
      mappingFile,
      categoriesFile,
      delayMs: Number.isFinite(delayMs) ? delayMs : undefined,
      onProgress: ({ index, total, status, category }) => {
        const label =
          category?.categoryname || category?.categoryid || `#${index}`;
        const icon =
          status === "created" ? "✅" : status === "existing" ? "ℹ️" : "❌";
        console.log(`${icon} [${index}/${total}] ${label}`);
      },
    });

    res.json({
      message: "Category-collection map sync completed",
      checked: result.checked,
      existing: result.existing,
      created: result.created,
      updated: result.updated || 0,
      failed: result.failed,
      errors: result.errors.slice(0, 20),
      mappingFile: result.mappingFile,
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({
      error: "Category-collection map sync failed",
      message: err.message || String(err),
    });
  }
});

//////////////////////////////////////////////////////
// MIGRATE CUSTOMERS (Admin GraphQL -> Customers)
//////////////////////////////////////////////////////
app.get("/migrate-customers", async (req, res) => {
  try {
    const file = req.query.file || "customers.json";
    const limit = Number.parseInt(req.query.limit, 10);
    const start = Number.parseInt(req.query.start, 10);
    const end = Number.parseInt(req.query.end, 10);
    const delayMs = Number.parseInt(req.query.delayMs, 10);

    const result = await migrateCustomers({
      file,
      limit: Number.isFinite(limit) ? limit : undefined,
      start: Number.isFinite(start) ? start : undefined,
      end: Number.isFinite(end) ? end : undefined,
      delayMs: Number.isFinite(delayMs) ? delayMs : undefined,
      onProgress: ({ index, total, status, customer }) => {
        const label =
          customer?.emailaddress ||
          [customer?.firstname, customer?.lastname]
            .filter(Boolean)
            .join(" ")
            .trim() ||
          customer?.customerid ||
          `#${index}`;
        console.log(`[${status}] [${index}/${total}] ${label}`);
      },
    });

    const created = result.created || 0;
    const failed = result.failed || 0;
    const total = result.total || created + failed;

    res.json({
      message: "Import completed",
      created,
      failed,
      total,
      failedCustomers: result.errors,
      errors: result.errors,
      mappingFile: result.mappingFile,
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({
      error: "Customer migration failed",
      message: err.message || String(err),
    });
  }
});

//////////////////////////////////////////////////////
// MIGRATE ORDERS (Admin GraphQL -> Orders)
//////////////////////////////////////////////////////
app.get("/migrate-orders", async (req, res) => {
  try {
    const file = req.query.file || "orders_merged.json";
    const start = Number.parseInt(req.query.start, 10);
    const end = Number.parseInt(req.query.end, 10);
    const limit = Number.parseInt(req.query.limit, 10);
    const startAt = Number.parseInt(req.query.startAt, 10);
    const delayMs = Number.parseInt(req.query.delayMs, 10);
    const flushEvery = Number.parseInt(req.query.flushEvery, 10);

    // start/end take precedence over startAt/limit when both are valid.
    const effectiveStartAt = Number.isFinite(start) && start >= 0
      ? start
      : Number.isFinite(startAt) && startAt >= 0
        ? startAt
        : 0;
    const effectiveLimit =
      Number.isFinite(start) &&
      start >= 0 &&
      Number.isFinite(end) &&
      end >= start
        ? end - start + 1
        : Number.isFinite(limit) && limit > 0
          ? limit
          : undefined;

    const result = await migrateOrders({
      file,
      limit: effectiveLimit,
      startAt: effectiveStartAt,
      delayMs: Number.isFinite(delayMs) ? delayMs : undefined,
      flushEvery: Number.isFinite(flushEvery) ? flushEvery : undefined,
      onProgress: ({ index, status, sourceOrder }) => {
        const label = sourceOrder?.orderid || `#${index}`;
        const currentIndex = effectiveStartAt + Math.max(0, index - 1);
        const totalLabel = Number.isFinite(effectiveLimit)
          ? effectiveStartAt + effectiveLimit - 1
          : "?";
        const icon = status === "created" ? "✅" : status === "existing" ? "ℹ️" : "❌";
        console.log(`${icon} [${currentIndex}/${totalLabel}] ${label}`);
      },
    });

    res.json({
      message: "Order migration completed",
      start: effectiveStartAt,
      end: Number.isFinite(effectiveLimit)
        ? effectiveStartAt + effectiveLimit - 1
        : null,
      created: result.created,
      existing: result.existing || 0,
      failed: result.failed,
      cancelled: result.cancelled,
      total: result.total,
      errors: result.errors.slice(0, 20),
      mappingFile: result.mappingFile,
      customerMappingFile: result.customerMappingFile,
      productMappingFile: result.productMappingFile,
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({
      error: "Order migration failed",
      message: err.message || String(err),
    });
  }
});

//////////////////////////////////////////////////////
// UPDATE PRODUCT VENDORS (normalise vendor names)
//////////////////////////////////////////////////////
app.get("/update-vendor", async (req, res) => {
  try {
    const batchSize = Number.parseInt(req.query.batchSize, 10);
    const delayMs = Number.parseInt(req.query.delayMs, 10);
    const dryRun =
      String(req.query.dryRun || "").toLowerCase() === "true" ||
      String(req.query.dryRun || "") === "1";

    const result = await updateProductVendors({
      batchSize: Number.isFinite(batchSize) ? batchSize : 100,
      delayMs: Number.isFinite(delayMs) ? delayMs : 300,
      dryRun,
      onProgress: ({ index, status, product, currentVendor, targetVendor }) => {
        if (status === "skipped") return;
        const icon =
          status === "updated" ? "✅" : status === "dry-run" ? "🔍" : "❌";
        console.log(
          `${icon} [${index}] "${product.title}" vendor: "${currentVendor}" → "${targetVendor}"`,
        );
      },
    });

    res.json({
      message: dryRun
        ? "Dry run completed — no changes made"
        : "Vendor update completed",
      total: result.total,
      updated: result.updated,
      skipped: result.skipped,
      failed: result.failed,
      errors: result.errors.slice(0, 20),
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({
      error: "Vendor update failed",
      message: err.message || String(err),
    });
  }
});

//////////////////////////////////////////////////////
// DELETE COLLECTIONS (all except keep list)
//////////////////////////////////////////////////////
const COLLECTIONS_QUERY = `
  query collections($first: Int!, $after: String) {
    collections(first: $first, after: $after) {
      pageInfo {
        hasNextPage
        endCursor
      }
      nodes {
        id
        title
        handle
      }
    }
  }
`;

const COLLECTION_DELETE_MUTATION = `
  mutation collectionDelete($input: CollectionDeleteInput!) {
    collectionDelete(input: $input) {
      deletedCollectionId
      userErrors {
        field
        message
      }
    }
  }
`;

const KEEP_COLLECTION_IDS = new Set([
  "291030401127",
  "290861875303",
  "290235220071",
  "290235875431",
  "290470068327",
  "290492678247",
  "290469937255",
]);

function extractCollectionNumericId(gid) {
  const match = String(gid || "").match(/Collection\/(\d+)$/);
  return match ? match[1] : "";
}

app.get("/delete-collections-except", async (req, res) => {
  try {
    const first = Math.min(Number.parseInt(req.query.first, 10) || 100, 250);
    const delayMs = Number.parseInt(req.query.delayMs, 10);
    const sleepMs = Number.isFinite(delayMs) ? delayMs : 150;
    const dryRun =
      String(req.query.dryRun || "").toLowerCase() === "true" ||
      String(req.query.dryRun || "") === "1";

    let after = null;
    let total = 0;
    let kept = 0;
    let deleted = 0;
    let failed = 0;
    const errors = [];

    do {
      const response = await shopifyClient.request(COLLECTIONS_QUERY, {
        variables: { first, after },
      });
      const topErrors = response?.errors || [];
      if (topErrors.length > 0) {
        throw new Error(topErrors.map((e) => e.message).join("; "));
      }

      const connection = response?.data?.collections;
      const nodes = connection?.nodes || [];

      for (const collection of nodes) {
        total++;
        const numericId = extractCollectionNumericId(collection.id);
        if (numericId && KEEP_COLLECTION_IDS.has(numericId)) {
          kept++;
          continue;
        }

        if (dryRun) {
          console.log(
            `[DRY RUN] Would delete collection ${collection.title} (${collection.id})`,
          );
          continue;
        }

        console.log(
          `Deleting collection ${collection.title} (${collection.id})...`,
        );
        const deleteRes = await shopifyClient.request(
          COLLECTION_DELETE_MUTATION,
          { variables: { input: { id: collection.id } } },
        );
        const deleteErrors =
          deleteRes?.data?.collectionDelete?.userErrors || [];
        if (deleteErrors.length > 0) {
          failed++;
          errors.push({
            id: collection.id,
            title: collection.title,
            errors: deleteErrors,
          });
        } else {
          deleted++;
        }

        if (sleepMs > 0) await sleep(sleepMs);
      }

      after = connection?.pageInfo?.endCursor || null;
      if (!connection?.pageInfo?.hasNextPage) break;
    } while (true);

    res.json({
      message: dryRun
        ? "Dry run completed (no deletions performed)"
        : "Delete completed",
      total,
      kept,
      deleted,
      failed,
      errors: errors.slice(0, 10),
      keepIds: [...KEEP_COLLECTION_IDS],
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({
      error: "Collection delete failed",
      message: err.message || String(err),
    });
  }
});

//////////////////////////////////////////////////////
// DOWNLOAD ORDER IMPORT MAPPING (streams large JSON)
//////////////////////////////////////////////////////
app.get("/download-order-import-mapping", (req, res) => {
  const fileName = "order-import-mapping.json";
  const filePath = path.join(__dirname, fileName);

  fs.stat(filePath, (statErr, stats) => {
    if (statErr || !stats.isFile()) {
      return res
        .status(404)
        .json({ error: "File not found", file: fileName });
    }

    res.setHeader("Content-Type", "application/json");
    res.setHeader("Content-Length", stats.size);
    res.setHeader(
      "Content-Disposition",
      `attachment; filename="${fileName}"`,
    );

    const stream = fs.createReadStream(filePath);
    stream.on("error", (err) => {
      console.error(err);
      if (!res.headersSent) {
        res
          .status(500)
          .json({ error: "Download failed", message: err.message });
      } else {
        res.destroy(err);
      }
    });
    stream.pipe(res);
  });
});

app.listen(process.env.PORT, () => {
  console.log("Server running on port " + process.env.PORT);
});
