const fs = require("fs");
const path = require("path");
const shopifyClient = require("../config/shopify");

const EXPORT_PRODUCTS_QUERY = `
  query ExportProducts($first: Int!) {
    products(first: $first) {
      nodes {
        id
        title
        status
        vendor
        descriptionHtml
        category {
          id
          fullName
        }
        featuredMedia {
          ... on MediaImage {
            image {
              url
            }
          }
        }
        seo {
          title
          description
        }
        availability: metafield(namespace: "product", key: "availability") { value }
        custom_field_1: metafield(namespace: "product", key: "custom_field_1") { value }
        custom_field_2: metafield(namespace: "product", key: "custom_field_2") { value }
        custom_field_3: metafield(namespace: "product", key: "custom_field_3") { value }
        custom_field_4: metafield(namespace: "product", key: "custom_field_4") { value }
        custom_field_5: metafield(namespace: "product", key: "custom_field_5") { value }
        product_popularity: metafield(namespace: "product", key: "product_popularity") { value }
        search_keywords: metafield(namespace: "product", key: "search_keywords") { value }
        short_name: metafield(namespace: "product", key: "short_name") { value }
        product_features: metafield(namespace: "product", key: "product_features") { value }
        tech_specs: metafield(namespace: "product", key: "tech_specs") { value }
        free_shipping: metafield(namespace: "product", key: "free_shipping") { value }
        minimum_qty: metafield(namespace: "product", key: "minimum_qty") { value }
        mpn: metafield(namespace: "product", key: "mpn") { value }
        download_file: metafield(namespace: "product", key: "download_file") { value }
        price_subtext: metafield(namespace: "product", key: "price_subtext") { value }
        short_description: metafield(namespace: "product", key: "short_description") { value }
        meta_keywords: metafield(namespace: "product", key: "meta_keywords") { value }
        description_above_price: metafield(namespace: "product", key: "description_above_price") { value }
        meta_override: metafield(namespace: "product", key: "meta_override") { value }
        variants(first: 250) {
          nodes {
            id
            sku
            price
            compareAtPrice
            barcode
            inventoryQuantity
            inventoryPolicy
            taxable
            inventoryItem {
              unitCost {
                amount
              }
              measurement {
                weight {
                  value
                  unit
                }
              }
            }
            allowed_qty: metafield(namespace: "variant", key: "allowed_qty") { value }
            about_option: metafield(namespace: "variant", key: "about_option") { value }
            display_type: metafield(namespace: "variant", key: "display_type") { value }
          }
        }
      }
    }
  }
`;

function toText(value) {
  if (value === undefined || value === null) return "";
  return String(value);
}

function toNumberValue(value) {
  const n = Number.parseFloat(String(value ?? "").trim());
  return Number.isFinite(n) ? n : 0;
}

function toIntValue(value) {
  const n = Number.parseInt(String(value ?? "").trim(), 10);
  return Number.isFinite(n) ? n : 0;
}

function toBoolValue(value) {
  if (typeof value === "boolean") return value;
  const v = String(value ?? "")
    .trim()
    .toLowerCase();
  if (!v) return false;
  return v === "true" || v === "1" || v === "yes";
}

function toIntList(value) {
  if (Array.isArray(value)) {
    return value.map((x) => toIntValue(x)).filter((n) => Number.isInteger(n));
  }

  if (typeof value !== "string") return [];

  const raw = value.trim();
  if (!raw) return [];

  try {
    const parsed = JSON.parse(raw);
    if (Array.isArray(parsed)) {
      return parsed
        .map((x) => toIntValue(x))
        .filter((n) => Number.isInteger(n));
    }
  } catch {
    // parse fallback below
  }

  return raw
    .split(/[,\s]+/)
    .map((x) => toIntValue(x))
    .filter((n) => Number.isInteger(n));
}

function weightToLb(weightObj) {
  const value = toNumberValue(weightObj?.value);
  const unit = toText(weightObj?.unit).toUpperCase();

  if (!value || !unit) return 0;
  if (unit === "POUNDS") return value;
  if (unit === "KILOGRAMS") return value * 2.2046226218;
  if (unit === "OUNCES") return value / 16;
  if (unit === "GRAMS") return value / 453.59237;
  return value;
}

function mapVariant(variantNode) {
  const allowedQtyRaw = variantNode?.allowed_qty?.value;
  return {
    id: toText(variantNode?.id),
    sku: toText(variantNode?.sku),
    price: toNumberValue(variantNode?.price),
    compareAtPrice: toNumberValue(variantNode?.compareAtPrice),
    barcode: toText(variantNode?.barcode),
    weightLb: weightToLb(variantNode?.inventoryItem?.measurement?.weight),
    inventoryQuantity: toIntValue(variantNode?.inventoryQuantity),
    inventoryPolicy: toText(variantNode?.inventoryPolicy),
    continueSelling: toText(variantNode?.inventoryPolicy) === "CONTINUE",
    taxable: Boolean(variantNode?.taxable),
    cost: toNumberValue(variantNode?.inventoryItem?.unitCost?.amount),
    metafields: {
      allowed_qty: toIntList(allowedQtyRaw),
      about_option: toText(variantNode?.about_option?.value),
      display_type: toText(variantNode?.display_type?.value),
    },
  };
}

function mapProduct(productNode) {
  return {
    id: toText(productNode?.id),

    title: toText(productNode?.title),
    status: toText(productNode?.status),
    vendor: toText(productNode?.vendor),
    descriptionHtml: toText(productNode?.descriptionHtml),
    productCategory: toText(productNode?.category?.fullName),
    featuredImageSrc: toText(productNode?.featuredMedia?.image?.url),
    seoTitle: toText(productNode?.seo?.title),
    seoDescription: toText(productNode?.seo?.description),

    availability: toText(productNode?.availability?.value),
    custom_field_1: toText(productNode?.custom_field_1?.value),
    custom_field_2: toText(productNode?.custom_field_2?.value),
    custom_field_3: toText(productNode?.custom_field_3?.value),
    custom_field_4: toText(productNode?.custom_field_4?.value),
    custom_field_5: toText(productNode?.custom_field_5?.value),
    product_popularity: toIntValue(productNode?.product_popularity?.value),
    search_keywords: toText(productNode?.search_keywords?.value),
    short_name: toText(productNode?.short_name?.value),
    product_features: toText(productNode?.product_features?.value),
    tech_specs: toText(productNode?.tech_specs?.value),
    free_shipping: toBoolValue(productNode?.free_shipping?.value),
    minimum_qty: toIntValue(productNode?.minimum_qty?.value),
    mpn: toText(productNode?.mpn?.value),
    download_file: toText(productNode?.download_file?.value),
    price_subtext: toText(productNode?.price_subtext?.value),
    short_description: toText(productNode?.short_description?.value),
    meta_keywords: toText(productNode?.meta_keywords?.value),
    description_above_price: toText(
      productNode?.description_above_price?.value,
    ),
    meta_override: toText(productNode?.meta_override?.value),

    variants: Array.isArray(productNode?.variants?.nodes)
      ? productNode.variants.nodes.map(mapVariant)
      : [],
  };
}

async function exportProductsToFile(first = 100) {
  const response = await shopifyClient.request(EXPORT_PRODUCTS_QUERY, {
    variables: { first: Math.min(Math.max(Number(first) || 100, 1), 100) },
  });

  if (response?.errors?.length) {
    throw new Error(
      `GraphQL errors: ${response.errors.map((e) => e.message).join("; ")}`,
    );
  }

  const products = response?.data?.products?.nodes || [];
  const transformed = products.map(mapProduct);

  const payload = {
    exportedAt: new Date().toISOString(),
    total: transformed.length,
    products: transformed,
  };

  const outputPath = path.join(process.cwd(), "export-products.json");
  fs.writeFileSync(outputPath, JSON.stringify(payload, null, 2), "utf8");

  return { outputPath, total: transformed.length };
}

module.exports = {
  exportProductsToFile,
  EXPORT_PRODUCTS_QUERY,
};
