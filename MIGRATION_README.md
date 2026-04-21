# USA Light — Volusion → Shopify Migration

One-time migration tool for moving **USA Light** from Volusion to Shopify via the Shopify Admin GraphQL API.

Covers: products (including option/child-driven variants), variants, inventory, images (WebP), PDFs in descriptions, metafields, categories → smart collections, manufacturer logos, SEO, historical orders, and audit mapping files.

---

## Quick Start

```bash
# 1. Install dependencies
npm install

# 2. Configure environment
cp .env.example .env   # then fill in SHOPIFY_ACCESS_TOKEN and other values

# 3. Start the server
npm run dev
# → http://localhost:<PORT>   (PORT must be set in .env)
```

**Recommended order:** **categories → products → customers → orders.** Smart collections and product tags must exist before products are imported; order migration expects `product-import-mapping.json` and usually `customer-import-mapping.json`.

```
GET /migrate-categories?file=categories.json
GET /import-json?file=products.json
GET /migrate-customers?file=customers.json
GET /migrate-orders?file=orders_merged.json
```

---

## Prerequisites

| Requirement | Detail |
| ----------- | ------ |
| Node.js | v18+ |
| sharp | Installed automatically via `npm install` (used for WebP image conversion) |
| Shopify Custom App | Scopes typically include: `write_products`, `read_products`, `write_inventory`, `read_inventory`, `write_publications`, `read_publications`, `write_files`, `read_files`, `write_collections`, `read_collections`, `write_customers`, `read_customers`, `write_orders`, `read_orders` (exact set depends on which endpoints you use) |
| Volusion data exports | `categories.json`, `products.json`, `customers.json`, `product_option.json`, `product_option_categories.json` in project root |
| Orders (optional) | `orders_merged.json` — top-level JSON array of order objects with `order_details` (see [Order migration](#order-migration)) |
| Combined orders (optional) | `combined_orders.json` — PascalCase order records keyed by `OrderID`, used to enrich fulfillments with carrier tracking data (see [Order migration](#order-migration)) |

---

## Table of Contents

1. [Environment Variables](#environment-variables)
2. [Project Structure](#project-structure)
3. [Architecture](#architecture)
4. [API Endpoints](#api-endpoints)
5. [Migration Workflow](#migration-workflow)
6. [Module Reference](#module-reference)
7. [Data Files](#data-files)
8. [Field Mappings](#field-mappings)
9. [Metafields Reference](#metafields-reference)
10. [Business Logic](#business-logic)
11. [Operational Notes](#operational-notes)
12. [Audit Outputs](#audit-outputs)
13. [Customer Migration](#customer-migration)
14. [Order Migration](#order-migration)

---

## Environment Variables

```env
SHOPIFY_STORE=your-store.myshopify.com
SHOPIFY_ACCESS_TOKEN=shpat_xxxxxxxxxxxxxxxxxxxx
SHOPIFY_API_VERSION=2026-01
SHOPIFY_LOCATION_ID=gid://shopify/Location/XXXXXXXXXXXXXXX
PORT=3000

# Publications (sales channels) — required for publishing products/collections
# Comma-separated and/or indexed variables (both patterns are merged):
SHOPIFY_PUBLICATION_IDS=gid://shopify/Publication/AAA,gid://shopify/Publication/BBB
SHOPIFY_PUBLICATION_ID_1=gid://shopify/Publication/XXXXXXXXXXXXXXX

# Optional — defaults to https://www.usalight.com
SOURCE_FILE_BASE_URL=https://www.usalight.com
```

**`SHOPIFY_LOCATION_ID`:** `GET /locations` (response lists locations; the server query currently requests up to 10 locations).

**Publication IDs:** `GET /publishlocations` — copy the relevant channel publication GID(s) into `SHOPIFY_PUBLICATION_IDS` or `SHOPIFY_PUBLICATION_ID_1`, etc.

> **Never commit `.env`.** Rotate `SHOPIFY_ACCESS_TOKEN` once migration is complete.

---

## Project Structure

```
.
├── server.js                         # Express HTTP server — route definitions
├── config/
│   └── shopify.js                  # Shopify Admin API client (singleton)
├── lib/
│   ├── migration-mapper.js         # Volusion → productSet / variant / metafield input
│   ├── import-service.js           # Product import (images, PDFs, productSet, metafields)
│   ├── migrate-categories-service.js
│   ├── migrate-customers-service.js
│   ├── migrate-orders-service.js   # Historical orders via orderCreate
│   ├── update-vendor-service.js    # Post-migration vendor name normalisation
│   └── export-products-service.js  # Sample export to export-products.json
│
├── categories.json                 # [INPUT] Volusion category tree
├── products.json                   # [INPUT] Volusion products
├── customers.json                  # [INPUT] Volusion customers
├── product_option.json
├── product_option_categories.json
├── category.json                   # volusion_google_category → Shopify Taxonomy GID
├── product-manufacturer-logo.json  # Manufacturer name → logo URL
├── orders_merged.json              # [INPUT] Volusion orders (optional; large)
├── combined_orders.json            # [INPUT] PascalCase order records — provides carrier
│                                   #         tracking (TrackingNumbers, Shipment_Cost, Gateway)
│
├── product-import-mapping.json     # [OUTPUT] Product / variant audit rows
├── customer-import-mapping.json    # [OUTPUT] Customer ID → Shopify GID
├── category-collection-url-map.json# [OUTPUT] Category → collection mapping
├── order-import-mapping.json       # [OUTPUT] Order migration audit
├── missing-product-identity.json   # [OUTPUT] Products that could not be resolved
├── export-products.json            # [OUTPUT] Last /export-products snapshot
└── output2.json                    # Legacy / manual full export (if present; often huge)
```

`package.json` also defines `npm run prepare:pdf-links` pointing at `scripts/prepare-pdf-links.js` — add that script locally if you use it; it is not required for the HTTP server.

`npm run dev` uses `nodemon` and ignores mapping output files to prevent unnecessary restarts.

---

## Architecture

### Why HTTP endpoints instead of CLI scripts?

Long-running bulk operations need observable execution. Query parameters (`limit`, `delayMs`, `start`, `end`, etc.) adjust runs without editing code, and JSON responses return counts and errors.

### Product import pipeline

```
Read products.json (+ full array passed as allProducts for child lookups)
  → For each top-level product row (default 400 ms between items):
      1. Skip child rows that belong to a variant-controlled parent (recorded in mapping as skipped_child_row)
      2. Prepare PDF links and description images; prepare main/gallery images (WebP via sharp) → Shopify Files
      3. migration-mapper.js → productSet input (options from cartesian product and/or child rows)
      4. productSet (synchronous)
      5. metafieldsSet in batches (25) for definitions-heavy fields
      6. manufacturer_logo (file_reference) when applicable
      7. publishablePublish for each configured publication ID
      8. Append rows to product-import-mapping.json (see Audit outputs)
  → Merge-write mapping file once at end of the request
```

### Category migration pipeline

```
Read categories.json
  → Resolve hierarchy (cycle-safe), order roots → dependent nodes
  → For each category: collectionCreate (smart collection rules), images, metafields, publish
  → Merge-write category-collection-url-map.json once at end of the request
```

### Variant generation

- **Option cartesian model:** From `product_option.json` + `product_option_categories.json` when the product uses option-driven variants.
- **Child-row model:** When `enableoptions_inventorycontrol` is `"Y"` and child product rows exist, variants can be built from child SKUs (`ischildofproductcode`), with Volusion option category parent IDs mapped to typed `variant` metafield keys in `migration-mapper.js` (`ATTRIBUTE_PARENT_TO_KEY` — dozens of parent IDs, many unique keys such as `wattage`, `color`, `voltage`, etc.).

### Order migration pipeline (summary)

```
Streaming-read orders_merged.json (top-level array)
  → Load customer-import-mapping.json, product-import-mapping.json,
     customers.json, combined_orders.json
  → Fetch shop currency code via Admin API
  → For each order:
      1. Build orderCreate input (line items, addresses, transactions, taxes,
         discounts, metafields, fulfillment with carrier tracking)
      2. Resolve products by SKU/code from mapping; optionally create placeholder
         products for unmapped lines
      3. orderCreate with inventory bypass (sendReceipt: false)
      4. If order has ship date + fulfillment: fulfillmentEventCreate (DELIVERED)
      5. If cancel date is set: orderCancel (reason mapped from Volusion value)
      6. Upsert customer mapping if a new customer was created inline
  → Flush order/customer/product mapping files every N dirty records (default 100)
```

---

## API Endpoints

| Endpoint | Query parameters | Description |
| -------- | ---------------- | ----------- |
| `GET /products` | — | Smoke test: first 5 products |
| `GET /publishlocations` | — | List publications (name + id) |
| `GET /locations` | `first` *(parsed in code; GraphQL currently requests 10 locations)* | Inventory locations |
| `GET /import-json` | `file` (default `products.json`), `start`, `end`, `limit` | Product import |
| `GET /export-products` | — | Writes **`export-products.json`** — **at most one GraphQL page (max 100 products)** — not a full-catalog export |
| `GET /migrate-categories` | `file`, `limit`, `start`, `end`, `delayMs`, `parentIds` | Categories → smart collections |
| `GET /sync-category-collection-map` | `file` (default `category-collection-url-map.json`), `categoriesFile` (default `categories.json`), `delayMs` | Validates existing `shopifyCollectionId` entries; creates any missing collections and updates the map file |
| `GET /migrate-customers` | `file`, `limit`, `start`, `end`, `delayMs` | Customers |
| `GET /migrate-orders` | `file` (default `orders_merged.json`), `limit`, `startAt`, `delayMs`, `flushEvery` | Historical orders |
| `GET /update-vendor` | `batchSize` (default `100`), `delayMs` (default `200`), `dryRun` | Normalises vendor names — queries only affected products and updates them in place |
| `GET /delete-collections-except` | `first`, `delayMs`, `dryRun` | Deletes collections not in the server allowlist |

**Always test with `limit` first:**

```
GET /migrate-categories?file=categories.json&limit=5
GET /import-json?file=products.json&limit=3
GET /migrate-customers?file=customers.json&limit=5
GET /migrate-orders?file=orders_merged.json&limit=2
```

**Product import slice:** `start` and `end` are **inclusive, 0-based** indices on the JSON array (`end` is treated inclusively — `endParam + 1` in code). `limit` caps the slice length after `start`/`end`.

**Customer import slice:** `start` and `end` are **1-based** (i.e. `start=1` is the first record). This differs from the product endpoint.

---

## Migration Workflow

### Step 1 — Migrate categories

```
GET /migrate-categories?file=categories.json
```

Collections should exist before products receive collection tags. Duplicate handles are handled with fallbacks (e.g. `handle-{categoryid}` on conflict).

### Step 1b — Sync category-collection map (optional recovery)

```
GET /sync-category-collection-map
GET /sync-category-collection-map?categoriesFile=categories.json&file=category-collection-url-map.json
```

Run this if you suspect `category-collection-url-map.json` has stale or missing `shopifyCollectionId` values. The endpoint queries each stored ID, creates any missing collections, and updates the map. Returns counts: `checked`, `existing`, `created`, `updated`, `failed`.

### Step 2 — Import products

```
GET /import-json?file=products.json
```

`product-import-mapping.json` is **merged and written once when the HTTP request finishes** (not after each product). A crash mid-run can lose the in-memory rows from that batch — use smaller batches via `start`/`end`/`limit` for safer restarts.

### Step 3 — Validate (limited export)

```
GET /export-products
```

This writes `export-products.json` with **up to 100 products** from a single query. It is useful for spot-checking shape and metafields, **not** for proving full-catalog completeness. For a full store dump, use Shopify's own export tools or extend `export-products-service.js` with pagination.

### Step 3b — Normalise vendor names (post-import cleanup)

```
GET /update-vendor?dryRun=true
GET /update-vendor
```

Run after product import to correct any brand names that were recorded in Shopify with an incorrect capitalisation or spelling. Preview first with `dryRun=true`, then run without it to apply the changes. See [Vendor name normalisation](#vendor-name-normalisation).

### Step 4 — Migrate customers

```
GET /migrate-customers?file=customers.json
```

Run after products if you rely on operational ordering; the service itself needs `customers.json` and writes `customer-import-mapping.json` (periodic flush every 200 successes by default).

### Step 5 — Migrate orders (optional)

```
GET /migrate-orders?file=orders_merged.json
```

Requires `orders_merged.json` plus mappings produced by product and customer steps for best line-item and customer association behavior. Place `combined_orders.json` in the project root before running if carrier tracking data is available. See [Order migration](#order-migration).

### Step 6 — Cleanup (destructive)

```
GET /delete-collections-except?dryRun=true
GET /delete-collections-except
```

Whitelisted Shopify **numeric** collection IDs are hard-coded in `server.js` (`KEEP_COLLECTION_IDS`):

```
291030401127, 290861875303, 290235220071, 290235875431,
290470068327, 290492678247, 290469937255
```

Verify this list before running. The `dryRun=true` flag logs what would be deleted without performing any mutations.

---

## Module Reference

### `config/shopify.js`

Creates `@shopify/admin-api-client` from `SHOPIFY_STORE`, `SHOPIFY_API_VERSION`, `SHOPIFY_ACCESS_TOKEN`.

### `lib/migration-mapper.js`

Pure transformation for product payloads: prices, status, inventory policy, `google_product_category` → Shopify Taxonomy GID via `category.json`, option/child variant construction, `variant` metafields from option categories, category-derived tags and `variant.collection_names`.

### `lib/import-service.js`

Orchestrates product import: WebP staging (via `sharp`), PDF/description handling, `productSet`, batched `metafieldsSet`, manufacturer logo file reference, publishing, and merged `product-import-mapping.json` output.

### `lib/migrate-categories-service.js`

Creates smart collections, uploads category imagery, sets collection metafields, publishes, merges `category-collection-url-map.json`. Also exports `syncCategoryCollectionUrlMap` used by the `/sync-category-collection-map` endpoint.

### `lib/migrate-customers-service.js`

Creates/updates customers (dedupe by email/phone), metafields, retries on some validation errors, flushes `customer-import-mapping.json` in chunks. Handles sales representative circular references safely.

### `lib/migrate-orders-service.js`

Streams large `orders_merged.json` arrays, calls `orderCreate` / `orderCancel`, creates fulfillment events for shipped orders, reads carrier tracking from `combined_orders.json`, updates mapping files periodically, can create placeholder products for unmapped SKUs and append rows to `product-import-mapping.json`.

### `lib/update-vendor-service.js`

Post-migration vendor normalisation. Queries Shopify **only for products whose `vendor` matches a known incorrect variant**, then updates each product to its canonical brand name. The correction table (`VENDOR_CORRECTIONS`) is defined inline and exported so it can be inspected without running the server. Supports `dryRun` mode (preview without writes), configurable `batchSize` (max 250 per page), and `delayMs` between mutations.

Returns `{ updated, failed, total, errors }`.

### `lib/export-products-service.js`

Single-page product query (max **100** per request) → `export-products.json` for validation samples.

---

## Data Files

### Input

| File | Contents |
| ---- | -------- |
| `products.json` | Volusion product records |
| `categories.json` | Category tree (`categoryid`, `parentid`, `rootid`, names, SEO) |
| `customers.json` | Customer records |
| `product_option.json` / `product_option_categories.json` | Option definitions |
| `category.json` | `volusion_google_category` → `shopify_category` (Taxonomy GID) |
| `product-manufacturer-logo.json` | Manufacturer name → logo URL |
| `orders_merged.json` | Orders with nested `order_details` (optional) |
| `combined_orders.json` | PascalCase order records keyed by `OrderID`; provides `TrackingNumbers` (array or object with `TrackingNumber`, `Gateway`, `Shipment_Cost`) used to enrich fulfillments and set the `carrier_shipment_cost` order metafield |

### Output

| File | Contents |
| ---- | -------- |
| `product-import-mapping.json` | Array of audit rows keyed merge by `productcode` |
| `customer-import-mapping.json` | Array of `{ customerid, …, shopifyCustomerId }` |
| `category-collection-url-map.json` | Array of `{ categoryid, shopifyCollectionId, collectionUrl, … }` |
| `order-import-mapping.json` | Per-order migration status and Shopify order ids |
| `missing-product-identity.json` | Products that could not be resolved during import |
| `export-products.json` | Sample export (≤ 100 products) |

---

## Field Mappings

### Product / variant (high level)

| Volusion | Shopify | Notes |
| -------- | ------- | ----- |
| `vendor_partno` (and variants) | variant `sku` | Base variant uses `vendor_partno`; option/child paths may fall back to `productcode` |
| `productname` | `title` | |
| `hideproduct` | `status` | `"Y"` → `DRAFT` |
| `stockstatus` | inventory quantity | At `SHOPIFY_LOCATION_ID` when set |
| `donotallowbackorders` | `inventoryPolicy` | `"Y"` → `DENY` |
| `upc_code` | `barcode` | |
| `productweight` | weight | `POUNDS` |
| `productprice` / `saleprice` | price / compare_at | See [Business logic](#business-logic) |
| `taxableproduct` | `taxable` | |
| `productmanufacturer` | `vendor` | |
| `vendor_price` | `inventoryItem.cost` | |
| `productdescription` | `descriptionHtml` | PDF links rewritten where supported |
| `photourl` (+ sequenced / additional) | product media | WebP upload via `sharp` |
| `metatag_title` / `metatag_description` | SEO | |
| `google_product_category` | `category` (Taxonomy GID) | From `category.json`; not duplicated as a product metafield in code |

---

## Metafields Reference

### `product` namespace (from `buildMetafields` + logo handling)

Written via `productSet` and/or `metafieldsSet`. Keys include: `availability`, `custom_field_1`–`custom_field_5`, `product_popularity`, `search_keywords`, `short_name`, `free_shipping`, `minimum_qty`, `stock_low_qty_alarm`, `price_subtext`, `short_description`, `meta_keywords`, `meta_override`, `product_features`, `tech_specs`, `mpn`, `description_above_price`. The **`download_file`** push is **commented out** in code (PDFs are handled in description HTML / file flow instead).

`manufacturer_logo` is a **`file_reference`** set from `product-manufacturer-logo.json` after upload (see `import-service.js`).

### `variant` namespace

Includes `collection_names` (list), `about_option`, `display_type`, `allowed_qty`, plus dynamic keys from `ATTRIBUTE_PARENT_TO_KEY` (e.g. `wattage`, `color`, `voltage`, …).

### `customer` namespace

See [Customer migration](#customer-migration).

### `collection` namespace

Includes navigation and SEO-related fields such as `parent_collection`, `root_collection`, `breadcrumb`, `hidden_category`, etc. (see `migrate-categories-service.js`).

### `order` namespace (order migration)

Examples: `shipping_fax_number`, `billing_fax_number`, `payment_transaction_id`, `customer_ip_address`, `card_last4`, `order_source`, `affiliate_commissionable_value`, `sales_rep`, `is_residential_shipping`, `gift_card_used`, `carrier_shipment_cost` (money, from `combined_orders.json`), `original_cancelled_at` (date_time) — see `buildOrderMetafields` in `migrate-orders-service.js`.

---

## Business Logic

### Price

| Condition | `price` | `compare_at_price` |
| --------- | ------- | ------------------- |
| `saleprice` present and > 0 | `saleprice` | `productprice` |
| No sale price | `productprice` | `null` |

### Inventory policy

| `donotallowbackorders` | `inventoryPolicy` |
| ----------------------- | ----------------- |
| `"Y"` | `DENY` |
| Otherwise | `CONTINUE` |

### Vendor name normalisation

`update-vendor-service.js` maintains a `VENDOR_CORRECTIONS` array of `{ from, to }` pairs. For each pair, the service pages through Shopify products filtered by `vendor:"<from>"` and runs `productUpdate` to set the correct canonical brand name. Products that already use the canonical name are never queried or touched.

### Order cancel reason mapping

| Volusion `cancelreason` | Shopify reason |
| ----------------------- | -------------- |
| `BuyerCanceled` / `BuyerCancelled` | `CUSTOMER` |
| `DuplicateInvalid` | `OTHER` |
| `FraudFake` | `FRAUD` |
| `MerchantCancelled` | `OTHER` |
| Contains "declin" or "paypal" | `DECLINED` |
| Contains "inventory" or "stock" | `INVENTORY` |
| All other / unrecognised | `OTHER` |

When the mapped reason is `OTHER`, the original Volusion cancel reason is embedded in both the order's **staff note** (Timeline) and the order **note** (Notes section) so it remains visible in the Shopify admin.

### Order discount code priority

Discount line items are identified by the presence of `discounttype` / `discountvalue` / `couponcode` fields. The code (merged from product name + coupon code) is applied with the following priority:

1. **Percentage** — `discounttype` 2 or 4, when `totalprice` matches the computed percentage within $0.01.
2. **Fixed amount** — `totalprice` is used as the authoritative amount when a percentage mismatch is detected, or for other non-zero, non-percentage, non-free-shipping types.
3. **Free shipping** — `discounttype` 0 or product name contains "free shipping", or any line item has `freeshippingitem = Y`.

---

## Operational Notes

**Rate limiting** — Default **400 ms** between products in `server.js` (not overridden by query string). Categories default to **150 ms** between collections unless `delayMs` is passed. Orders default to **250 ms** unless `delayMs` is passed. Customer migration defaults to **150 ms**. Vendor update defaults to **200 ms** between mutations (pass `delayMs` to override).

**Restartability** — Order migration flushes `order-import-mapping.json` (and optionally customer/product mappings) every time the combined dirty-record count reaches `flushEvery` (default **100**). Product and category mapping files are written **at the end of each HTTP request**; for long product runs, prefer slicing with `start`/`end`/`limit`.

**Idempotency** — Image/PDF uploads use caches (e.g. hashed URLs) where implemented; re-runs may still create **new** Shopify products if SKUs differ or products already exist — there is no global upsert-by-SKU guard in `productSet`.

**Large files** — `categories.json` and `orders_merged.json` can be very large. Order import uses a **streaming JSON parser** (character-by-character, no full-file load) for the top-level array. `combined_orders.json` is fully loaded into memory at startup. Avoid loading multi-gigabyte JSON in editors.

**Console output** — Product import logs `[CREATED]`, `[FAILED]`, `[SKIPPED]` per item; category migration logs ✅/❌; order migration logs `[created]`, `[failed]` per order.

**Order `sourceName`** — All migrated orders are tagged with `sourceName: "volusion_migrated_order"` and `sourceIdentifier: <orderid>` for traceability.

---

## Audit Outputs

### `product-import-mapping.json`

A **JSON array**. Each element includes `productcode` and, for successful parent imports, Shopify product fields such as `shopify_product_id`, `shopify_product_name`, `shopify_product_url`, variant audit fields (`mapping_type`, `variant_child_count`, `variant_missing_mappings`, …). Child variant rows include `parent_productcode`, `is_variant`, `shopify_variant_id`, etc. Skipped standalone child rows include `import_status: "skipped_child_row"`.

New runs **merge** with the existing file by `productcode` (last write wins per code).

### `category-collection-url-map.json`

A **JSON array** of objects with at least `categoryid`, `shopifyCollectionId`, and `collectionUrl` (older object-shaped files are merged with backward compatibility).

### `customer-import-mapping.json`

Array of objects with `customerid`, name, `emailaddress`, `shopifyCustomerId`, etc.

### `order-import-mapping.json`

Array of per-order records with `orderid`, `shopify_order_id`, `shopify_order_name`, `status`, `cancel_status`, `missing_product_codes`, `customerid`, `shopify_customer_id`, `orderdate`, `canceldate`, `error`.

---

## Customer Migration

### Field mapping

| Volusion | Shopify | Notes |
| -------- | ------- | ----- |
| `emailaddress` | `email` | Dedup key |
| `firstname` / `lastname` | `firstName` / `lastName` | |
| `phonenumber` | `phone` | E.164 normalization; retry may drop |
| `billingaddress*` / `city` / `state` / `country` / `postalcode` | default address | Retry may strip |
| `emailsubscriber` | marketing consent | Separate mutation on update |
| `faxnumber`, `websiteaddress`, `salesrep_customerid`, `custom_field_custom1`, `taxentityusecode` | `customer` metafields | See earlier sections |

### Retry strategy

Retries apply mainly to **phone/address** validation errors. On duplicate conflicts, the service updates the existing customer and applies email consent separately.

Up to 4 attempt configurations are tried per customer (with phone + address → without phone → without address → without either), stopping as soon as one succeeds.

### Sales representative linking

When a customer has `salesrep_customerid`, the service recursively ensures the referenced sales rep is migrated first, then sets a `customer_reference` metafield pointing to the rep's Shopify ID. Self-referencing sales rep IDs (customer is their own rep) are handled correctly. Circular reference chains beyond self are detected and raise an error.

---

## Order Migration

### Prerequisites

- **`orders_merged.json`:** Must be a **top-level JSON array** of order objects (streaming parser). Each order should include `orderid`, addresses, payment fields, and **`order_details`** (line items). Discount rows are detected via `discounttype` / coupon fields.
- **`product-import-mapping.json`:** Rows with `shopify_product_id` (and product codes) so line items can reference real products. Missing SKUs can trigger **on-the-fly placeholder product** creation and append rows to the product mapping file.
- **`customer-import-mapping.json`** and **`customers.json`:** Used to associate `customerid` to Shopify customers; orders can also upsert minimal customer data when needed.
- **`combined_orders.json`** *(optional)*: PascalCase records keyed by `OrderID`. Provides `TrackingNumbers[0].TrackingNumber`, `TrackingNumbers[0].Gateway`, and `TrackingNumbers[0].Shipment_Cost` used to populate fulfillment tracking fields and the `carrier_shipment_cost` order metafield. If the file is absent, tracking fields are omitted.
- **`SHOPIFY_LOCATION_ID`:** Used for transactions and fulfillments where applicable.

### Behaviour summary

- **Financial / fulfillment:** Maps Volusion payment and ship flags into Shopify fields; cancelled Volusion orders (cancel date set) result in **`orderCreate` followed by `orderCancel`** when possible.
- **Fulfillment events:** After `orderCreate`, if the order has a `shipdate` and a fulfillment was included, a `fulfillmentEventCreate` mutation is called with `status: DELIVERED` at the ship date timestamp.
- **Inventory:** `orderCreate` uses `inventoryBehaviour: "BYPASS"` in options. `sendReceipt` and `sendFulfillmentReceipt` are both `false`.
- **Line items:** Resolves `productId` / `variantId` from mapping; `optionids` may carry a full variant GID; otherwise the default variant is queried and cached.
- **Order note:** Combines `ordernotes`, `order_comments`, and `giftwrapnote` (prefixed with "Gift Wrap Note:") into the Shopify order `note` field. When cancel reason maps to `OTHER`, the original Volusion `cancelreason` is also appended.
- **Date conversion:** Order dates in `M/D/YYYY H:MM:SS AM/PM` format are converted from US Pacific time (DST-aware) to UTC before storing as ISO 8601.
- **Output:** Appends to `order-import-mapping.json` and periodically refreshes customer/product mapping files when those are updated.

### Endpoint

```
GET /migrate-orders?file=orders_merged.json&startAt=0&limit=100&delayMs=250&flushEvery=100
```

`startAt` skips the first N orders in the array (after parsing). Use `limit` to page through large archives.
