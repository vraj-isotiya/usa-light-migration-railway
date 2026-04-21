const fs = require("fs");
const path = require("path");
const shopifyClient = require("../config/shopify");
const { importProduct } = require("./import-service");

const ORDER_CREATE_MUTATION = `
  mutation orderCreate($order: OrderCreateOrderInput!, $options: OrderCreateOptionsInput) {
    orderCreate(order: $order, options: $options) {
      userErrors {
        field
        message
        code
      }
      order {
        id
        name
        fulfillments(first: 1) {
          id
        }
        customer {
          id
        }
      }
    }
  }
`;

const FULFILLMENT_EVENT_CREATE_MUTATION = `
  mutation fulfillmentEventCreate($fulfillmentEvent: FulfillmentEventInput!) {
    fulfillmentEventCreate(fulfillmentEvent: $fulfillmentEvent) {
      fulfillmentEvent {
        id
        happenedAt
        status
      }
      userErrors {
        field
        message
      }
    }
  }
`;

const ORDER_CANCEL_MUTATION = `
  mutation OrderCancel($orderId: ID!, $reason: OrderCancelReason!, $restock: Boolean!, $refundMethod: OrderCancelRefundMethodInput, $staffNote: String, $notifyCustomer: Boolean) {
    orderCancel(orderId: $orderId, reason: $reason, restock: $restock, refundMethod: $refundMethod, staffNote: $staffNote, notifyCustomer: $notifyCustomer) {
      job {
        id
        done
      }
      orderCancelUserErrors {
        field
        message
        code
      }
    }
  }
`;

const PRODUCT_DEFAULT_VARIANT_QUERY = `
  query ProductVariantLookup($id: ID!) {
    product(id: $id) {
      id
      variants(first: 1) {
        nodes {
          id
        }
      }
    }
  }
`;

const SHOP_CURRENCY_QUERY = `
  query ShopCurrency {
    shop {
      currencyCode
    }
  }
`;

const ORDER_BY_NAME_QUERY = `
  query FindOrderByName($query: String!) {
    orders(first: 5, query: $query, sortKey: PROCESSED_AT, reverse: true) {
      nodes {
        id
        name
        customer {
          id
        }
      }
    }
  }
`;

const COUNTRY_NAME_TO_CODE = {
  "UNITED STATES": "US",
  "UNITED STATES OF AMERICA": "US",
  USA: "US",
  "U.S.A.": "US",
  CANADA: "CA",
  "PUERTO RICO": "US",
  "US VIRGIN ISLANDS": "US",
  "U.S. VIRGIN ISLANDS": "US",
  "VIRGIN ISLANDS, U.S.": "US",
  "ST. JOHN": "US",
  "ST JOHN": "US",
  "SAINT JOHN": "US",
  "ST. THOMAS": "US",
  "ST THOMAS": "US",
  "SAINT THOMAS": "US",
  BARBADOS: "BB",
  BAHAMAS: "BS",
  "ANTIGUA & BARBUDA": "AG",
  "ANTIGUA AND BARBUDA": "AG",
  SPAIN: "ES",
  "UNITED KINGDOM": "GB",
  UK: "GB",
  "SOUTH AFRICA": "ZA",
  AUSTRIA: "AT",
  CHINA: "CN",
  "SAUDI ARABIA": "SA",
  BERMUDA: "BM",
  MEXICO: "MX",
  ITALY: "IT",
  RUSSIA: "RU",
  BELGIUM: "BE",
  ARMENIA: "AM",
  "SOUTH KOREA": "KR",
  "CAYMAN ISLANDS": "KY",
  ISRAEL: "IL",
  BELIZE: "BZ",
};

const SHOPIFY_COUNTRY_CODES = new Set(
  `
AC AD AE AF AG AI AL AM AN AO AR AT AU AW AX AZ BA BB BD BE BF BG BH BI BJ BL
BM BN BO BQ BR BS BT BV BW BY BZ CA CC CD CF CG CH CI CK CL CM CN CO CR CU CV
CW CX CY CZ DE DJ DK DM DO DZ EC EE EG EH ER ES ET FI FJ FK FO FR GA GB GD GE
GF GG GH GI GL GM GN GP GQ GR GS GT GW GY HK HM HN HR HT HU ID IE IL IM IN IO
IQ IR IS IT JE JM JO JP KE KG KH KI KM KN KP KR KW KY KZ LA LB LC LI LK LR LS
LT LU LV LY MA MC MD ME MF MG MK ML MM MN MO MQ MR MS MT MU MV MW MX MY MZ NA
NC NE NF NG NI NL NO NP NR NU NZ OM PA PE PF PG PH PK PL PM PN PS PT PY QA RE
RO RS RU RW SA SB SC SD SE SG SH SI SJ SK SL SM SN SO SR SS ST SV SX SY SZ TA
TC TD TF TG TH TJ TK TL TM TN TO TR TT TV TW TZ UA UG UM US UY UZ VA VC VE VG
VN VU WF WS XK YE YT ZA ZM ZW ZZ
`
    .split(/\s+/)
    .filter(Boolean),
);

const US_TERRITORY_TO_PROVINCE_CODE = {
  "PUERTO RICO": "PR",
  PR: "PR",
  "US VIRGIN ISLANDS": "VI",
  "U S VIRGIN ISLANDS": "VI",
  "VIRGIN ISLANDS U S": "VI",
  "ST JOHN": "VI",
  "SAINT JOHN": "VI",
  "ST THOMAS": "VI",
  "SAINT THOMAS": "VI",
  VI: "VI",
};

const SHIPPING_METHOD_MAP = {
  101: "UPS : Free Shipping - 7 Day Ground",
  102: "Free Shipping - 7 Day UPS Ground",
  103: "no shipping method",
  104: "no shipping method",
  105: "no shipping method",
  501: "FEDEX : FedEx Ground",
  504: "FEDEX : FedEx 2Day",
  505: "FEDEX : FedEx Standard Overnight",
  701: "UPS : UPS Ground",
  702: "UPS : UPS Standard",
  703: "UPS : UPS 3Day Select",
  704: "UPS : UPS 2nd Day Air",
  707: "UPS : UPS Next Day Air",
  710: "UPS : UPS Worldwide Saver (1-3 Business Days)",
  711: "UPS : UPS Worldwide Express",
  919: "UPS : UPS Ground",
  921: "UPS : Free Shipping - 7 Day Ground",
  927: "FEDEX : Free Shipping - 7 Day Fedex Ground",
  928: "FEDEX : FedEx Ground",
  936: "FEDEX : Free Shipping - 7 Day Fedex Ground",
  937: "FEDEX : Free Shipping - 7 Day Fedex Ground",
  939: "FEDEX : FedEx Ground",
  942: "FEDEX : FedEx Ground",
  943: "FEDEX : FedEx Ground",
  944: "FEDEX : Free Shipping - 7 Day Fedex Ground",
};

const PAYMENT_METHOD_MAP = {
  2: "check by mail",
  5: "Credit Card: visa",
  6: "Credit Card: MasterCard",
  7: "Credit Card: American Express",
  8: "Credit Card: Discover",
  14: "Current Method (Wire Transfer)",
  17: "Current Method (Cash)",
  18: "Current Method (PayPal)",
  26: "Current Method (PayPal Express)",
  29: "Current Method (PayPal)",
};

function resolveShippingMethodTitle(methodId) {
  const id = Number.parseInt(toStringValue(methodId).trim(), 10);
  return SHIPPING_METHOD_MAP[id] || "Custom Shipping";
}

function resolvePaymentGateway(paymentMethodId) {
  const id = Number.parseInt(toStringValue(paymentMethodId).trim(), 10);
  return PAYMENT_METHOD_MAP[id] || "Pay to USA Light";
}

const CUSTOMER_IMPORT_MAPPING_FILE = "customer-import-mapping.json";
const PRODUCT_IMPORT_MAPPING_FILE = "product-import-mapping.json";
const ORDER_IMPORT_MAPPING_FILE = "order-import-mapping.json";
const CUSTOMERS_SOURCE_FILE = "customers.json";
const COMBINED_ORDERS_FILE = "combined_orders.json";

const ORDER_METAFIELD_NAMESPACE = "order";

const DEFAULT_DELAY_MS = 600;
const DEFAULT_FLUSH_EVERY = 10;
const GRAPHQL_MAX_RETRIES = 6;
const GRAPHQL_RETRY_BASE_MS = 1200;
const THROTTLE_RESUME_POINTS = 200;
const MAX_DISCOUNT_CODE_LENGTH = 255;

const fsp = fs.promises;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function toStringValue(value) {
  if (value === undefined || value === null) return "";
  return String(value);
}

function toNumber(value, fallback = 0) {
  const n = Number.parseFloat(toStringValue(value).trim());
  return Number.isFinite(n) ? n : fallback;
}

function toInteger(value, fallback = 0) {
  const n = Number.parseInt(toStringValue(value).trim(), 10);
  return Number.isFinite(n) ? n : fallback;
}

// Returns the UTC offset in milliseconds for US Pacific time on the given local date/hour.
// Post-2007 rules: spring forward 2nd Sunday of March at 2 AM (PDT = UTC-7),
//                  fall back 1st Sunday of November at 2 AM (PST = UTC-8).
function pacificUtcOffsetMs(year, month1, day, hour) {
  function nthSunday(yr, mo0, n) {
    const firstDay = new Date(Date.UTC(yr, mo0, 1));
    return 1 + ((7 - firstDay.getUTCDay()) % 7) + (n - 1) * 7;
  }
  const dstStart = nthSunday(year, 2, 2); // 2nd Sunday of March
  const dstEnd = nthSunday(year, 10, 1); // 1st Sunday of November

  const inDST =
    (month1 > 3 && month1 < 11) ||
    (month1 === 3 && (day > dstStart || (day === dstStart && hour >= 2))) ||
    (month1 === 11 && (day < dstEnd || (day === dstEnd && hour < 2)));

  return inDST ? -7 * 3600 * 1000 : -8 * 3600 * 1000;
}

function parseDateToIso(rawDate) {
  const value = toStringValue(rawDate).trim();
  if (!value) return "";

  // Already ISO — pass through as-is so timezone is preserved
  if (/^\d{4}-\d{2}-\d{2}T/.test(value)) {
    const d = new Date(value);
    return Number.isNaN(d.getTime()) ? "" : d.toISOString();
  }

  // "M/D/YYYY H:MM:SS AM/PM" — parse components then convert from Pacific
  // time to UTC so dates display correctly in a Pacific-timezone Shopify store
  const match = value.match(
    /^(\d{1,2})\/(\d{1,2})\/(\d{4})\s+(\d{1,2}):(\d{2})(?::(\d{2}))?\s*(AM|PM)?$/i,
  );
  if (match) {
    const [, mon, day, year, rawHr, min, sec = "0", ampm = ""] = match;
    let hour = Number(rawHr);
    const upper = ampm.toUpperCase();
    if (upper === "PM" && hour !== 12) hour += 12;
    if (upper === "AM" && hour === 12) hour = 0;

    // localMs is the number of ms treating the parsed components as if UTC;
    // subtracting the (negative) Pacific offset shifts it to true UTC.
    // e.g. 5:14 PM PDT (UTC-7): localMs – (−7 h) = localMs + 7 h → correct UTC
    const localMs = Date.UTC(
      Number(year),
      Number(mon) - 1,
      Number(day),
      hour,
      Number(min),
      Number(sec),
    );
    const offsetMs = pacificUtcOffsetMs(
      Number(year),
      Number(mon),
      Number(day),
      hour,
    );
    const d = new Date(localMs - offsetMs);
    return Number.isNaN(d.getTime()) ? "" : d.toISOString();
  }

  // Fallback for any other format
  const d = new Date(value);
  return Number.isNaN(d.getTime()) ? "" : d.toISOString();
}

function parseBooleanFromYesNo(value) {
  const raw = toStringValue(value).trim().toUpperCase();
  if (!raw) return null;
  if (raw === "Y" || raw === "YES" || raw === "TRUE" || raw === "1")
    return true;
  if (raw === "N" || raw === "NO" || raw === "FALSE" || raw === "0")
    return false;
  return null;
}

function isValidEmail(email) {
  const value = toStringValue(email).trim();
  if (!value) return false;
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(value);
}

function normalizeCountryCode(rawCountry) {
  const value = toStringValue(rawCountry).trim();
  if (!value) return "";
  if (/^[A-Za-z]{2}$/.test(value)) {
    const code = value.toUpperCase();
    return SHOPIFY_COUNTRY_CODES.has(code) ? code : "";
  }
  const mapped = COUNTRY_NAME_TO_CODE[value.toUpperCase()];
  if (!mapped) return "";
  return SHOPIFY_COUNTRY_CODES.has(mapped) ? mapped : "";
}

function normalizeCountryLikeKey(value) {
  return toStringValue(value)
    .trim()
    .toUpperCase()
    .replace(/\./g, "")
    .replace(/,/g, " ")
    .replace(/\s+/g, " ");
}

function resolveUsTerritoryProvinceCode(rawCountry, rawState) {
  const stateKey = normalizeCountryLikeKey(rawState);
  if (stateKey && US_TERRITORY_TO_PROVINCE_CODE[stateKey]) {
    return US_TERRITORY_TO_PROVINCE_CODE[stateKey];
  }

  const countryKey = normalizeCountryLikeKey(rawCountry);
  if (countryKey && US_TERRITORY_TO_PROVINCE_CODE[countryKey]) {
    return US_TERRITORY_TO_PROVINCE_CODE[countryKey];
  }

  return normalizeProvinceCode(rawState);
}

function shouldUseTerritoryLikeAddressFallback(
  rawCountry,
  normalizedCountryCode,
) {
  const countryKey = normalizeCountryLikeKey(rawCountry);
  if (!countryKey) return false;

  if (US_TERRITORY_TO_PROVINCE_CODE[countryKey]) {
    return true;
  }

  return !toStringValue(normalizedCountryCode).trim();
}

function normalizeProvinceCode(rawState) {
  const value = toStringValue(rawState).trim();
  if (!value) return "";
  return value.toUpperCase();
}

function resolveAddressGeo(rawCountry, rawState) {
  const normalizedCountryCode = normalizeCountryCode(rawCountry);
  const useTerritoryFallback = shouldUseTerritoryLikeAddressFallback(
    rawCountry,
    normalizedCountryCode,
  );

  return {
    countryCode: useTerritoryFallback ? "US" : normalizedCountryCode,
    provinceCode: useTerritoryFallback
      ? resolveUsTerritoryProvinceCode(rawCountry, rawState)
      : normalizeProvinceCode(rawState),
  };
}

function normalizePhone(rawPhone, countryCode) {
  const original = toStringValue(rawPhone).trim();
  if (!original) return "";

  const withoutExt = original.replace(/\s*(ext\.?|extension|x)\s*\d+\s*$/i, "");
  const plusPrefixedDigits = withoutExt.startsWith("+")
    ? withoutExt.replace(/[^\d]/g, "")
    : "";

  if (
    plusPrefixedDigits.length >= 8 &&
    plusPrefixedDigits.length <= 15 &&
    /^\+[\d\s().-]+$/.test(withoutExt)
  ) {
    return `+${plusPrefixedDigits}`;
  }

  const digits = withoutExt.replace(/[^\d]/g, "");
  if (!digits) return "";

  const usLikeCountry =
    !countryCode ||
    countryCode === "US" ||
    countryCode === "CA" ||
    countryCode === "PR" ||
    countryCode === "VI";

  if (usLikeCountry) {
    if (digits.length === 11 && digits.startsWith("1")) return `+${digits}`;
    if (digits.length === 10) return `+1${digits}`;
  }

  if (digits.length >= 8 && digits.length <= 15) return `+${digits}`;
  return "";
}

function compactObject(obj) {
  const output = {};
  for (const [key, value] of Object.entries(obj || {})) {
    if (value === undefined || value === null) continue;
    if (typeof value === "string" && !value.trim()) continue;
    if (Array.isArray(value)) {
      if (value.length === 0) continue;
      output[key] = value;
      continue;
    }
    if (typeof value === "object") {
      const nested = compactObject(value);
      if (Object.keys(nested).length === 0) continue;
      output[key] = nested;
      continue;
    }
    output[key] = value;
  }
  return output;
}

function normalizeProductCodeKey(value) {
  return toStringValue(value).trim().toUpperCase();
}

function normalizeCustomerId(value) {
  return toStringValue(value).trim();
}

function buildMoneyBagInput(amount, currencyCode) {
  const n = toNumber(amount, 0);
  return {
    shopMoney: {
      amount: Number(n.toFixed(2)),
      currencyCode,
    },
  };
}

function shopifyStoreBaseUrl() {
  const value = toStringValue(process.env.SHOPIFY_STORE).trim();
  if (!value) return "";
  if (/^https?:\/\//i.test(value)) return value.replace(/\/+$/, "");
  return `https://${value.replace(/\/+$/, "")}`;
}

function buildShopifyProductUrl(handle) {
  const base = shopifyStoreBaseUrl();
  const h = toStringValue(handle).trim();
  if (!base || !h) return "";
  return `${base}/products/${h}`;
}

function stringifyUserErrors(userErrors = []) {
  return userErrors
    .map((error) => {
      const field = Array.isArray(error?.field) ? error.field.join(".") : "";
      const message = toStringValue(error?.message).trim();
      if (!message) return "";
      return field ? `${field}: ${message}` : message;
    })
    .filter(Boolean)
    .join("; ");
}

function isRetryableGraphqlError(error) {
  const code = toStringValue(error?.extensions?.code || error?.code)
    .trim()
    .toUpperCase();
  const message = toStringValue(error?.message).trim();
  if (code === "THROTTLED") return true;
  return /too many attempts|try again later|throttl|rate limit/i.test(message);
}

function formatTopLevelErrors(errors = []) {
  return errors
    .map((e) => {
      const code = toStringValue(e?.extensions?.code || e?.code).trim();
      const message = toStringValue(e?.message).trim();
      if (!message) return "";
      return code ? `${code}: ${message}` : message;
    })
    .filter(Boolean)
    .join("; ");
}

function extractTopLevelGraphqlErrors(response) {
  const directErrors = Array.isArray(response?.errors)
    ? response.errors
    : Array.isArray(response?.errors?.graphQLErrors)
      ? response.errors.graphQLErrors
      : [];

  const bodyErrors = Array.isArray(response?.body?.errors)
    ? response.body.errors
    : Array.isArray(response?.body?.errors?.graphQLErrors)
      ? response.body.errors.graphQLErrors
      : [];

  return [...directErrors, ...bodyErrors].filter(Boolean);
}

function extractResponseData(response) {
  if (response?.data && typeof response.data === "object") return response.data;
  if (response?.body?.data && typeof response.body.data === "object")
    return response.body.data;
  return {};
}

function extractThrottleStatus(response) {
  return (
    response?.extensions?.cost?.throttleStatus ||
    response?.body?.extensions?.cost?.throttleStatus ||
    null
  );
}

function computeThrottleWaitMs(
  throttleStatus,
  targetPoints = THROTTLE_RESUME_POINTS,
) {
  const currentlyAvailable = Number(throttleStatus?.currentlyAvailable);
  const restoreRate = Number(throttleStatus?.restoreRate);
  if (
    !Number.isFinite(currentlyAvailable) ||
    !Number.isFinite(restoreRate) ||
    restoreRate <= 0
  ) {
    return 0;
  }
  if (currentlyAvailable >= targetPoints) return 0;
  const deficit = targetPoints - currentlyAvailable;
  return Math.ceil((deficit / restoreRate) * 1000);
}

function computeRetryDelayMs(attempt, throttleStatus) {
  const exponential = GRAPHQL_RETRY_BASE_MS * 2 ** attempt;
  const throttleWait = computeThrottleWaitMs(throttleStatus);
  const jitter = Math.floor(Math.random() * 350);
  return Math.max(exponential, throttleWait) + jitter;
}

async function requestGraphql(query, variables = {}) {
  let lastErrorMessage = "";

  for (let attempt = 0; attempt <= GRAPHQL_MAX_RETRIES; attempt++) {
    let response;
    try {
      response = await shopifyClient.request(query, { variables });
    } catch (err) {
      const message = toStringValue(err?.message).trim();
      lastErrorMessage = message || lastErrorMessage;
      const retryable =
        /too many attempts|try again later|throttl|rate limit/i.test(message);
      if (retryable && attempt < GRAPHQL_MAX_RETRIES) {
        await sleep(computeRetryDelayMs(attempt));
        continue;
      }
      throw err;
    }

    const topLevelErrors = extractTopLevelGraphqlErrors(response);
    const throttleStatus = extractThrottleStatus(response);

    if (topLevelErrors.length > 0) {
      const retryable = topLevelErrors.some(isRetryableGraphqlError);
      const formatted = formatTopLevelErrors(topLevelErrors);
      lastErrorMessage = formatted || "GraphQL request failed";

      if (retryable && attempt < GRAPHQL_MAX_RETRIES) {
        await sleep(computeRetryDelayMs(attempt, throttleStatus));
        continue;
      }

      throw new Error(lastErrorMessage);
    }

    const proactiveWaitMs = computeThrottleWaitMs(throttleStatus);
    if (proactiveWaitMs > 0) {
      await sleep(proactiveWaitMs);
    }

    return extractResponseData(response);
  }

  throw new Error(
    lastErrorMessage ||
    "GraphQL request exceeded retry attempts due to Shopify throttling",
  );
}

function normalizeOrderName(value) {
  const raw = toStringValue(value).trim();
  if (!raw) return "";
  return raw.replace(/^#/, "");
}

function matchesOrderName(sourceOrderId, candidateName) {
  const source = normalizeOrderName(sourceOrderId);
  const candidate = normalizeOrderName(candidateName);
  if (!source || !candidate) return false;
  return source === candidate;
}

async function findExistingOrderByName(sourceOrderId) {
  const normalizedId = normalizeOrderName(sourceOrderId);
  if (!normalizedId) return null;

  const queryVariants = [`name:${normalizedId}`, `name:#${normalizedId}`];

  for (const query of queryVariants) {
    const data = await requestGraphql(ORDER_BY_NAME_QUERY, { query });
    const nodes = data?.orders?.nodes || [];
    const exact = nodes.find((node) =>
      matchesOrderName(normalizedId, node?.name),
    );
    if (exact) return exact;
  }

  return null;
}

async function loadJsonArray(filePath) {
  try {
    const raw = await fsp.readFile(filePath, "utf8");
    try {
      const parsed = JSON.parse(raw);
      if (!Array.isArray(parsed)) return [];
      return parsed;
    } catch (parseErr) {
      const preview = raw.slice(0, 80).replace(/\s+/g, " ");
      throw new Error(
        `Failed to parse JSON from ${filePath} (starts with: ${preview}): ${parseErr.message}`,
      );
    }
  } catch (err) {
    if (err?.code === "ENOENT") return [];
    throw err;
  }
}

async function writeJsonArray(filePath, rows) {
  await fsp.writeFile(filePath, JSON.stringify(rows, null, 2), "utf8");
}

function buildCustomerMap(rows) {
  const map = new Map();
  for (const row of rows || []) {
    const customerid = normalizeCustomerId(row?.customerid);
    if (!customerid) continue;
    map.set(customerid, {
      customerid,
      firstname: toStringValue(row?.firstname).trim(),
      lastname: toStringValue(row?.lastname).trim(),
      emailaddress: toStringValue(row?.emailaddress).trim(),
      shopifyCustomerId: toStringValue(row?.shopifyCustomerId).trim(),
    });
  }
  return map;
}

function buildCustomersById(rows) {
  const map = new Map();
  for (const row of rows || []) {
    const id = normalizeCustomerId(row?.customerid);
    if (!id || map.has(id)) continue;
    map.set(id, row);
  }
  return map;
}

function buildProductMap(rows) {
  const map = new Map();
  for (const row of rows || []) {
    const key = normalizeProductCodeKey(row?.productcode);
    if (!key) continue;
    const shopifyProductId = toStringValue(row?.shopify_product_id).trim();
    if (!shopifyProductId) continue;
    map.set(key, shopifyProductId);
  }
  return map;
}

function normalizeProductMappingRow(row) {
  const productcode = toStringValue(row?.productcode).trim();
  const shopifyProductId = toStringValue(row?.shopify_product_id).trim();
  if (!productcode || !shopifyProductId) return null;

  return {
    productcode,
    productname: toStringValue(row?.productname).trim(),
    producturl: toStringValue(row?.producturl).trim(),
    shopify_product_id: shopifyProductId,
    shopify_product_name: toStringValue(row?.shopify_product_name).trim(),
    shopify_product_url: toStringValue(row?.shopify_product_url).trim(),
  };
}

function buildProductRowsByCode(rows) {
  const map = new Map();
  for (const row of rows || []) {
    const normalized = normalizeProductMappingRow(row);
    if (!normalized) continue;
    map.set(normalizeProductCodeKey(normalized.productcode), normalized);
  }
  return map;
}

function productRowsByCodeToRows(productRowsByCode) {
  return [...productRowsByCode.values()]
    .map((row) => normalizeProductMappingRow(row))
    .filter(Boolean);
}

function buildOrderReportMap(rows) {
  const map = new Map();
  for (const row of rows || []) {
    const key = toStringValue(row?.orderid).trim();
    if (!key) continue;
    map.set(key, row);
  }
  return map;
}

// Build a lookup map from combined_orders.json (PascalCase fields) keyed by OrderID.
function buildCombinedOrdersMap(rows) {
  const map = new Map();
  for (const row of rows || []) {
    const orderId = toStringValue(row?.OrderID).trim();
    if (!orderId) continue;
    map.set(orderId, row);
  }
  return map;
}

// Extract the first TrackingNumbers entry from a combined order record.
// TrackingNumbers may be a single object or an array — handles both.
function extractCombinedTracking(combinedOrder) {
  if (!combinedOrder) return null;
  let t = combinedOrder?.TrackingNumbers;
  if (!t) return null;
  if (Array.isArray(t)) {
    if (t.length === 0) return null;
    t = t[0];
  }
  return t;
}

function customerNameFromRow(row) {
  return [row?.firstname, row?.lastname]
    .map((x) => toStringValue(x).trim())
    .filter(Boolean)
    .join(" ")
    .trim();
}

function resolveSalesRepName(order, customersById, customerMap) {
  const salesRepCustomerId = normalizeCustomerId(order?.salesrep_customerid);
  if (!salesRepCustomerId) return "";

  const sourceCustomer = customersById.get(salesRepCustomerId);
  const sourceName = sourceCustomer
    ? [sourceCustomer?.firstname, sourceCustomer?.lastname]
      .map((x) => toStringValue(x).trim())
      .filter(Boolean)
      .join(" ")
      .trim()
    : "";
  if (sourceName) return sourceName;

  const mapped = customerMap.get(salesRepCustomerId);
  const mappedName = mapped ? customerNameFromRow(mapped) : "";
  return mappedName || "";
}

function buildOrderMetafields(
  order,
  customersById,
  customerMap,
  combinedOrder,
  currencyCode,
) {
  const metafields = [];

  const pushText = (key, value) => {
    const cleaned = toStringValue(value).trim();
    if (!cleaned) return;
    metafields.push({
      namespace: ORDER_METAFIELD_NAMESPACE,
      key,
      type: "single_line_text_field",
      value: cleaned,
    });
  };

  const pushDate = (key, value) => {
    const cleaned = toStringValue(value).trim();
    if (!cleaned) return;

    const date = new Date(cleaned);

    if (isNaN(date)) {
      console.error("Invalid date:", cleaned);
      return;
    }

    const iso = parseDateToIso(cleaned);

    metafields.push({
      namespace: ORDER_METAFIELD_NAMESPACE,
      key,
      type: "date_time",
      value: iso,
    });
  };

  const pushBoolean = (key, value) => {
    const parsed = parseBooleanFromYesNo(value);
    if (parsed === null) return;
    metafields.push({
      namespace: ORDER_METAFIELD_NAMESPACE,
      key,
      type: "boolean",
      value: parsed ? "true" : "false",
    });
  };

  const pushMoney = (key, value) => {
    const amount = toNumber(value, NaN);
    if (!Number.isFinite(amount) || amount < 0) return;
    const currency = toStringValue(currencyCode).trim() || "USD";
    metafields.push({
      namespace: ORDER_METAFIELD_NAMESPACE,
      key,
      type: "money",
      value: JSON.stringify({
        amount: amount.toFixed(2),
        currency_code: currency,
      }),
    });
  };

  pushText("shipping_fax_number", order?.shipfaxnumber);
  pushText("billing_fax_number", order?.billingfaxnumber);
  pushText("payment_transaction_id", order?.creditcardtransactionid);
  pushText("customer_ip_address", order?.customer_ipaddress);
  pushText("card_last4", order?.cc_last4);
  pushText("order_source", order?.order_entry_system);
  pushMoney(
    "affiliate_commissionable_value",
    order?.affiliate_commissionable_value,
  );

  if (order?.orderid_third_party) {
    pushText(
      "external_order_reference",
      ` PayPal Order ID ${order?.orderid_third_party}`,
    );
  }

  const salesRepName = resolveSalesRepName(order, customersById, customerMap);
  pushText("sales_rep", salesRepName);

  pushBoolean("is_residential_shipping", order?.shipresidential);
  pushBoolean("gift_card_used", order?.giftcardidused);

  // Carrier shipment cost from combined_orders.json
  const tracking = extractCombinedTracking(combinedOrder);
  pushMoney("carrier_shipment_cost", tracking?.Shipment_Cost);

  pushDate("original_cancelled_at", order?.canceldate);

  return metafields;
}

function buildOrderNote(order) {
  const notes = [];
  const orderNotes = toStringValue(order?.ordernotes).trim();
  const orderComments = toStringValue(order?.order_comments).trim();
  const giftWrapNote = toStringValue(order?.giftwrapnote).trim();

  if (orderNotes) notes.push(orderNotes);
  if (orderComments) notes.push(orderComments);
  if (giftWrapNote) notes.push(`Gift Wrap Note: ${giftWrapNote}`);

  // When reason maps to OTHER (DuplicateInvalid / MerchantCancelled), embed the
  // original Volusion cancel reason in the order note so it is visible in the
  // Shopify admin Notes section (staffNote only appears in the Timeline).
  if (shouldCancelOrder(order)) {
    const cancelNote = buildOrderCancelStaffNote(order);
    if (cancelNote) notes.push(cancelNote);
  }

  return notes.join("\n");
}

function mapOrderTransactionStatus(order) {
  return parseBooleanFromYesNo(order?.paymentdeclined) ? "FAILURE" : "SUCCESS";
}

function hasShippingEvidence(order, orderDetails = []) {
  if (parseBooleanFromYesNo(order?.shipped) === true) return true;
  if (toStringValue(order?.shipdate).trim()) return true;

  const detailShipped = (orderDetails || []).some((detail) => {
    if (parseBooleanFromYesNo(detail?.shipped) === true) return true;
    if (toStringValue(detail?.shipdate).trim()) return true;
    return toInteger(detail?.qtyshipped, 0) > 0;
  });
  if (detailShipped) return true;

  const status = toStringValue(order?.orderstatus).trim().toLowerCase();

  if (status === "shipped") return true;
  if (status === "partially shipped") return true;

  if (
    (status.includes("ship") && !status.includes("ready")) ||
    status.includes("fulfill") ||
    status.includes("deliver")
  )
    return true;

  return false;
}

function mapOrderFinancialStatus(order, orderDetails = []) {
  const paymentAmount = toNumber(order?.paymentamount, 0);
  const shippedOrFulfilled = hasShippingEvidence(order, orderDetails);

  const status = toStringValue(order?.orderstatus).trim().toLowerCase();
  if (status === "returned") return "REFUNDED";
  if (status === "partially returned") return "PARTIALLY_REFUNDED";
  if (status === "cancelled") return "VOIDED";

  // Legacy business rule: shipped orders should be imported as paid, even when
  // Volusion marks paymentdeclined = Y.
  if (parseBooleanFromYesNo(order?.paymentdeclined) === true) {
    if (shippedOrFulfilled) return "PAID";
    return "PENDING";
  }

  if (paymentAmount > 0) return "PAID";
  if (paymentAmount === 0 && shippedOrFulfilled) return "PAID";
  return "PENDING";
}

function mapOrderFulfillmentStatus(order, orderDetails = []) {
  const shippedFlag = parseBooleanFromYesNo(order?.shipped);
  if (shippedFlag === true) return "FULFILLED";

  const detailShipped = orderDetails.some(
    (detail) => parseBooleanFromYesNo(detail?.shipped) === true,
  );
  if (detailShipped) return "FULFILLED";

  const status = toStringValue(order?.orderstatus).trim().toLowerCase();
  switch (status) {
    case "shipped":
      return "FULFILLED";
    case "partially shipped":
      return "PARTIAL";
    case "returned":
      return "RESTOCKED";
    case "partially returned":
      return "PARTIAL";
    case "cancelled":
      return "RESTOCKED";
    case "ready to ship":
      return "";
    case "pending":
      return "";
    case "backordered":
      return "";
    case "processing":
      return "";
  }

  if (!status) return "";
  if (status.includes("restock")) return "RESTOCKED";
  if (status.includes("partial")) return "PARTIAL";
  if (status.includes("fulfill") || status.includes("shipped"))
    return "FULFILLED";
  return "";
}

function mapOrderShipmentStatus(order) {
  const status = toStringValue(order?.orderstatus).trim().toLowerCase();
  if (status.includes("deliver")) return "DELIVERED";
  if (status.includes("out for delivery")) return "OUT_FOR_DELIVERY";
  if (status === "partially shipped") return "CONFIRMED";
  return "DELIVERED";
}

function shouldCreateFulfillment(order, orderDetails = []) {
  if (parseBooleanFromYesNo(order?.shipped) === true) return true;
  if (toStringValue(order?.shipdate).trim()) return true;

  const detailShipped = orderDetails.some(
    (detail) => parseBooleanFromYesNo(detail?.shipped) === true,
  );
  if (detailShipped) return true;

  const status = toStringValue(order?.orderstatus).trim().toLowerCase();

  if (status === "shipped") return true;
  if (status === "partially shipped") return true;

  if (
    (status.includes("ship") && !status.includes("ready")) ||
    status.includes("fulfill") ||
    status.includes("deliver")
  )
    return true;

  return false;
}

// Volusion → Shopify cancel reason mapping:
//   BuyerCanceled / BuyerCancelled  → CUSTOMER
//   DuplicateInvalid                → OTHER  (original reason stored in staffNote)
//   FraudFake                       → FRAUD
//   MerchantCancelled               → OTHER  (original reason stored in staffNote)
//   Paypal transaction was declined → DECLINED
function buildOrderCancelReason(order) {
  const raw = toStringValue(order?.cancelreason).trim();
  const lower = raw.toLowerCase();

  if (/^buyercancell?ed$/i.test(raw)) return "CUSTOMER";
  if (/^duplicateinvalid$/i.test(raw)) return "OTHER";
  if (/^fraudfake$/i.test(raw)) return "FRAUD";
  if (/^merchantcancell?ed$/i.test(raw)) return "OTHER";
  if (/declin/i.test(raw) || lower.includes("paypal")) return "DECLINED";

  // Fallback heuristics for any unrecognised values
  if (lower.includes("customer") || lower.includes("buyer")) return "CUSTOMER";
  if (lower.includes("fraud") || lower.includes("fake")) return "FRAUD";
  if (lower.includes("declin") || lower.includes("payment")) return "DECLINED";
  if (lower.includes("inventory") || lower.includes("stock"))
    return "INVENTORY";
  return "OTHER";
}

// Only embed the original Volusion cancel reason in the staff note when the
// mapped Shopify reason is OTHER (DuplicateInvalid / MerchantCancelled).
// staffNote is not visible to the customer (max 255 characters).
function buildOrderCancelStaffNote(order) {
  const reason = buildOrderCancelReason(order);
  if (reason !== "OTHER") return "";
  const raw = toStringValue(order?.cancelreason).trim();
  if (!raw) return "";
  const note = `Volusion cancel reason: ${raw}`;
  return note.length > 255 ? note.slice(0, 252) + "..." : note;
}

function shouldCancelOrder(order) {
  if (toStringValue(order?.orderstatus).trim().toLowerCase() === "cancelled")
    return true;
  return Boolean(parseDateToIso(order?.canceldate));
}

function isDiscountDetail(detail) {
  return Boolean(toStringValue(detail?.discounttype).trim());
}

function normalizeVariantId(optionIdsValue) {
  const raw = toStringValue(optionIdsValue).trim();
  if (!raw) return "";
  if (/^gid:\/\/shopify\/ProductVariant\/\d+$/i.test(raw)) return raw;
  return "";
}

function deriveLineItemQuantity(detail) {
  let quantity = toInteger(detail?.quantity, 0);
  if (quantity > 0) return quantity;

  const totalPrice = toNumber(detail?.totalprice, 0);
  const unitPrice = toNumber(detail?.productprice, 0);
  if (totalPrice > 0 && unitPrice > 0) {
    const derived = Math.round(totalPrice / unitPrice);
    if (derived > 0) return derived;
  }

  quantity = toInteger(detail?.qtyshipped, 0);
  if (quantity > 0) return quantity;

  return 1;
}

function deriveLineItemUnitPrice(detail, quantity) {
  const productPrice = toNumber(detail?.productprice, NaN);
  if (Number.isFinite(productPrice) && productPrice >= 0) return productPrice;

  const totalPrice = toNumber(detail?.totalprice, NaN);
  if (Number.isFinite(totalPrice) && totalPrice >= 0 && quantity > 0) {
    return totalPrice / quantity;
  }

  return 0;
}

function buildLineItemProperties(detail) {
  const properties = [];

  const push = (name, value) => {
    const cleaned = toStringValue(value).trim();
    if (!cleaned) return;
    properties.push({
      name,
      value: cleaned,
    });
  };

  push("product_note", detail?.productnote);
  push("return_name", detail?.rma_number);
  push("rma_item_id", detail?.rmai_id);
  push("vendor_price", detail?.vendor_price);
  push("qty_on_backorder", detail?.qtyonbackorder);
  push("qty_on_hold", detail?.qtyonhold);
  push("qty_shipped", detail?.qtyshipped);
  push("freeshipping_item", detail?.freeshippingitem);
  push("custom_line_item", detail?.customlineitem);
  push("coupon_code", detail?.couponcode);
  push("discount_value", detail?.discountvalue);
  push("discount_type", detail?.discounttype);
  push("option_ids", detail?.optionids);

  return properties;
}

function mapLineItemFulfillmentService(detail) {
  const autoDropShip = parseBooleanFromYesNo(detail?.autodropship);
  if (autoDropShip === true) return "manual";
  return "";
}

function buildBillingAddress(order) {
  const geo = resolveAddressGeo(order?.billingcountry, order?.billingstate);
  const countryCode = geo.countryCode;
  const phone = normalizePhone(order?.billingphonenumber, countryCode);
  return compactObject({
    firstName: toStringValue(order?.billingfirstname).trim(),
    lastName: toStringValue(order?.billinglastname).trim(),
    company: toStringValue(order?.billingcompanyname).trim(),
    address1: toStringValue(order?.billingaddress1).trim(),
    address2: toStringValue(order?.billingaddress2).trim(),
    city: toStringValue(order?.billingcity).trim(),
    provinceCode: geo.provinceCode,
    zip: toStringValue(order?.billingpostalcode).trim(),
    countryCode,
    phone,
  });
}

function buildShippingAddress(order) {
  const geo = resolveAddressGeo(order?.shipcountry, order?.shipstate);
  const countryCode = geo.countryCode;
  const phone = normalizePhone(order?.shipphonenumber, countryCode);
  return compactObject({
    firstName: toStringValue(order?.shipfirstname).trim(),
    lastName: toStringValue(order?.shiplastname).trim(),
    company: toStringValue(order?.shipcompanyname).trim(),
    address1: toStringValue(order?.shipaddress1).trim(),
    address2: toStringValue(order?.shipaddress2).trim(),
    city: toStringValue(order?.shipcity).trim(),
    provinceCode: geo.provinceCode,
    zip: toStringValue(order?.shippostalcode).trim(),
    countryCode,
    phone,
  });
}

function sumFixedShippingCost(orderDetails) {
  return (orderDetails || []).reduce((sum, detail) => {
    const amount = toNumber(detail?.fixed_shippingcost, 0);
    return sum + (amount > 0 ? amount : 0);
  }, 0);
}

function buildShippingLines(order, orderDetails, currencyCode) {
  const methodId = toStringValue(order?.shippingmethodid).trim();
  const baseShipping = toNumber(order?.totalshippingcost, 0);
  const extraFixedShipping = sumFixedShippingCost(orderDetails);
  const totalShipping = Math.max(0, baseShipping + extraFixedShipping);

  if (!methodId && totalShipping <= 0) return [];

  const shippingTitle = resolveShippingMethodTitle(methodId);
  const line = compactObject({
    code: methodId || undefined,
    title: shippingTitle,
    priceSet: buildMoneyBagInput(totalShipping, currencyCode),
  });

  return [line];
}

function buildOrderTaxLines(order, currencyCode) {
  const lines = [];

  for (let i = 1; i <= 3; i++) {
    const title = toStringValue(order?.[`tax${i}_title`]).trim();
    const rate = toNumber(order?.[`salestaxrate${i}`], 0);
    const amount = toNumber(order?.[`salestax${i}`], 0);

    if (!title && rate <= 0 && amount <= 0) continue;

    lines.push({
      title: title || `Tax ${i}`,
      rate,
      priceSet: buildMoneyBagInput(Math.max(0, amount), currencyCode),
    });
  }

  return lines;
}

function buildOrderTransactions(order, currencyCode, locationId) {
  const amount = toNumber(order?.paymentamount, 0);
  if (amount <= 0) return [];

  const gatewayId = toStringValue(order?.paymentmethodid).trim();
  const processedAt = parseDateToIso(
    order?.creditcardauthorizationdate || order?.orderdate,
  );
  const authorizationCode = toStringValue(
    order?.creditcardauthorizationnumber,
  ).trim();

  const transaction = compactObject({
    amountSet: buildMoneyBagInput(amount, currencyCode),
    kind: "SALE",
    status: mapOrderTransactionStatus(order),
    gateway: resolvePaymentGateway(gatewayId),
    authorizationCode,
    processedAt,
    locationId: toStringValue(locationId).trim(),
  });

  return [transaction];
}

function buildOrderDiscountCode(orderDetails, currencyCode) {
  const allDetails = orderDetails || [];

  // Collect every detail that carries discount information
  const discountEntries = allDetails.filter((detail) => {
    const type = toStringValue(detail?.discounttype).trim();
    const value = toStringValue(detail?.discountvalue).trim();
    const code = toStringValue(detail?.couponcode).trim();
    return Boolean(type || value || code);
  });

  if (discountEntries.length === 0) return null;

  // Build each entry as "Title (CODE)" combining productname + couponcode/productcode.
  // Shopify's discount input only has a single `code` field — no separate title —
  // so both pieces of information are embedded in it.
  let mergedCode = discountEntries
    .map((d) => {
      const title = toStringValue(d?.productname).trim();
      const code =
        toStringValue(d?.couponcode).trim() ||
        toStringValue(d?.productcode).trim();
      if (title && code) return `${title} (${code})`;
      return title || code;
    })
    .filter(Boolean)
    .join(" + ");

  if (mergedCode.length > MAX_DISCOUNT_CODE_LENGTH) {
    mergedCode = `${mergedCode.slice(0, MAX_DISCOUNT_CODE_LENGTH - 3)}...`;
  }

  if (!mergedCode) return null;

  // Classify each entry
  const percentageEntries = discountEntries.filter((d) => {
    const t = toInteger(d?.discounttype, NaN);
    const v = toNumber(d?.discountvalue, 0);
    return (t === 2 || t === 4) && v > 0;
  });

  const fixedEntries = discountEntries.filter((d) => {
    const t = toInteger(d?.discounttype, NaN);
    const v = toNumber(d?.discountvalue, 0);
    return !Number.isNaN(t) && t !== 0 && t !== 2 && t !== 4 && v > 0;
  });

  const freeShippingEntries = discountEntries.filter((d) => {
    const t = toInteger(d?.discounttype, NaN);
    const pn = toStringValue(d?.productname).trim();
    return t === 0 || /free\s*shipping/i.test(pn);
  });

  const hasFreeShippingItem = allDetails.some(
    (d) => parseBooleanFromYesNo(d?.freeshippingitem) === true,
  );

  // Order subtotal from positive-priced, non-discount product lines
  const orderSubtotal = allDetails
    .filter(
      (d) =>
        !toStringValue(d?.discounttype).trim() &&
        toNumber(d?.productprice, 0) > 0,
    )
    .reduce((sum, d) => sum + Math.max(0, toNumber(d?.totalprice, 0)), 0);

  // Priority 1: percentage entries — verify against totalprice, fall back to fixed if mismatch
  if (percentageEntries.length > 0) {
    const totalPct = Math.min(
      100,
      percentageEntries.reduce(
        (sum, d) => sum + Math.max(0, toNumber(d?.discountvalue, 0)),
        0,
      ),
    );
    const pctComputedAmt = (totalPct / 100) * orderSubtotal;
    const actualAmt = percentageEntries.reduce(
      (sum, d) => sum + Math.abs(toNumber(d?.totalprice, 0)),
      0,
    );

    // Use percentage only when it matches totalprice within $0.01
    if (actualAmt > 0 && Math.abs(pctComputedAmt - actualAmt) < 0.01) {
      return {
        itemPercentageDiscountCode: { code: mergedCode, percentage: totalPct },
      };
    }

    // Mismatch → totalprice is authoritative; send as fixed discount
    const finalAmt = actualAmt > 0 ? actualAmt : pctComputedAmt;
    return {
      itemFixedDiscountCode: {
        code: mergedCode,
        amountSet: buildMoneyBagInput(
          Number(finalAmt.toFixed(2)),
          currencyCode,
        ),
      },
    };
  }

  // Priority 2: fixed entries — use totalprice as the actual amount
  if (fixedEntries.length > 0) {
    const totalAmt = fixedEntries.reduce(
      (sum, d) => sum + Math.abs(toNumber(d?.totalprice, 0)),
      0,
    );
    return {
      itemFixedDiscountCode: {
        code: mergedCode,
        amountSet: buildMoneyBagInput(totalAmt, currencyCode),
      },
    };
  }

  // Priority 3: free shipping
  if (freeShippingEntries.length > 0 || hasFreeShippingItem) {
    return { freeShippingDiscountCode: { code: mergedCode } };
  }

  // Fallback
  return {
    itemFixedDiscountCode: {
      code: mergedCode,
      amountSet: buildMoneyBagInput(0, currencyCode),
    },
  };
}

function resolveCustomerName(order, sourceCustomer, fieldName) {
  const byOrderBilling = toStringValue(
    fieldName === "firstName"
      ? order?.billingfirstname
      : order?.billinglastname,
  ).trim();
  if (byOrderBilling) return byOrderBilling;

  const byOrderShipping = toStringValue(
    fieldName === "firstName" ? order?.shipfirstname : order?.shiplastname,
  ).trim();
  if (byOrderShipping) return byOrderShipping;

  return toStringValue(
    fieldName === "firstName"
      ? sourceCustomer?.firstname
      : sourceCustomer?.lastname,
  ).trim();
}

function resolveCustomerEmail(sourceCustomer, mappedCustomer) {
  const candidates = [
    sourceCustomer?.emailaddress,
    mappedCustomer?.emailaddress,
  ];
  for (const email of candidates) {
    const normalized = toStringValue(email).trim();
    if (isValidEmail(normalized)) return normalized;
  }
  return "";
}

function resolveCustomerPhone(order, sourceCustomer) {
  const countryCode =
    normalizeCountryCode(sourceCustomer?.country) ||
    normalizeCountryCode(order?.billingcountry) ||
    normalizeCountryCode(order?.shipcountry);

  const candidates = [
    sourceCustomer?.phonenumber,
    order?.billingphonenumber,
    order?.shipphonenumber,
  ];

  for (const raw of candidates) {
    const normalized = normalizePhone(raw, countryCode);
    if (normalized) return normalized;
  }
  return "";
}

function buildCustomerUpsertAddress(order, sourceCustomer, names, phone) {
  const country = toStringValue(
    sourceCustomer?.country || order?.billingcountry,
  ).trim();
  const province = toStringValue(
    sourceCustomer?.state || order?.billingstate,
  ).trim();

  const address = compactObject({
    firstName: toStringValue(names?.firstName).trim(),
    lastName: toStringValue(names?.lastName).trim(),
    company: toStringValue(
      sourceCustomer?.companyname || order?.billingcompanyname,
    ).trim(),
    address1: toStringValue(
      sourceCustomer?.billingaddress1 || order?.billingaddress1,
    ).trim(),
    address2: toStringValue(
      sourceCustomer?.billingaddress2 || order?.billingaddress2,
    ).trim(),
    city: toStringValue(sourceCustomer?.city || order?.billingcity).trim(),
    province,
    zip: toStringValue(
      sourceCustomer?.postalcode || order?.billingpostalcode,
    ).trim(),
    country,
    phone: toStringValue(phone).trim(),
  });

  const hasAddress = [
    "address1",
    "address2",
    "city",
    "province",
    "zip",
    "country",
    "company",
  ].some((key) => Boolean(address[key]));

  if (!hasAddress) return null;
  return address;
}

function buildCustomerAssociation(order, customersById, customerMap) {
  const sourceCustomerId = normalizeCustomerId(order?.customerid);
  if (!sourceCustomerId) {
    return {
      customerInput: null,
      sourceCustomerId: "",
      customerEmail: "",
      customerFirstName: "",
      customerLastName: "",
      wasMapped: false,
    };
  }

  const mappedCustomer = customerMap.get(sourceCustomerId) || null;
  const sourceCustomer = customersById.get(sourceCustomerId) || null;

  const firstName = resolveCustomerName(order, sourceCustomer, "firstName");
  const lastName = resolveCustomerName(order, sourceCustomer, "lastName");
  const email = resolveCustomerEmail(sourceCustomer, mappedCustomer);
  const phone = resolveCustomerPhone(order, sourceCustomer);
  const normalizedEmail = isValidEmail(email) ? email : "";

  if (mappedCustomer?.shopifyCustomerId) {
    return {
      customerInput: {
        toAssociate: {
          id: mappedCustomer.shopifyCustomerId,
        },
      },
      sourceCustomerId,
      customerEmail: email,
      customerFirstName: firstName,
      customerLastName: lastName,
      wasMapped: true,
    };
  }

  // Shopify requires toUpsert to include at least `id` or `email`.
  // When source data has no mapped customer ID and no valid email,
  // create the order as a guest (without customer association).
  if (!normalizedEmail) {
    return {
      customerInput: null,
      sourceCustomerId,
      customerEmail: "",
      customerFirstName: firstName,
      customerLastName: lastName,
      wasMapped: false,
    };
  }

  const upsertAddress = buildCustomerUpsertAddress(
    order,
    sourceCustomer,
    { firstName, lastName },
    phone,
  );

  const upsert = compactObject({
    firstName,
    lastName,
    email: normalizedEmail,
    phone,
    addresses: upsertAddress ? [upsertAddress] : undefined,
  });

  if (Object.keys(upsert).length === 0) {
    return {
      customerInput: null,
      sourceCustomerId,
      customerEmail: "",
      customerFirstName: "",
      customerLastName: "",
      wasMapped: false,
    };
  }

  return {
    customerInput: {
      toUpsert: upsert,
    },
    sourceCustomerId,
    customerEmail: upsert.email || "",
    customerFirstName: upsert.firstName || "",
    customerLastName: upsert.lastName || "",
    wasMapped: false,
  };
}

async function resolveMappedVariantId(productId, variantCache) {
  if (!productId) return "";
  if (variantCache.has(productId)) return variantCache.get(productId) || "";

  const data = await requestGraphql(PRODUCT_DEFAULT_VARIANT_QUERY, {
    id: productId,
  });
  const variantId = toStringValue(
    data?.product?.variants?.nodes?.[0]?.id || "",
  ).trim();
  variantCache.set(productId, variantId || null);
  return variantId;
}

function buildOrderOnlyProductSource(detail, order) {
  const orderId = toStringValue(order?.orderid).trim() || "unknown";
  const orderDetailId = toStringValue(detail?.orderdetailid).trim() || "line";
  const rawProductCode = toStringValue(detail?.productcode).trim();
  const productCode = rawProductCode || `ORDER-${orderId}-${orderDetailId}`;

  const quantity = deriveLineItemQuantity(detail);
  const unitPrice = deriveLineItemUnitPrice(detail, quantity);
  const title = toStringValue(detail?.productname).trim() || productCode;

  return {
    productname: title,
    productcode: productCode,
    vendor_partno: productCode,
    productprice: toStringValue(Math.max(0, Number(unitPrice.toFixed(2)))),
    taxableproduct:
      parseBooleanFromYesNo(detail?.taxableproduct) === false ? "N" : "Y",
    productweight: toStringValue(detail?.productweight).trim(),
    vendor_price: toStringValue(detail?.vendor_price).trim(),
    stockstatus: "0",
    hideproduct: "Y",
    donotallowbackorders: "N",
    nonshippable:
      parseBooleanFromYesNo(detail?.nonshippable) === true ? "Y" : "N",
  };
}

async function ensureMappedProductForLineItem(detail, order, context) {
  const rawProductCode = toStringValue(detail?.productcode).trim();
  const source = buildOrderOnlyProductSource(detail, order);
  const productCode = source.productcode;
  const productCodeKey = normalizeProductCodeKey(productCode);

  const alreadyMappedId = context.productMap.get(productCodeKey);
  if (alreadyMappedId) {
    return {
      productId: alreadyMappedId,
      variantId: await resolveMappedVariantId(
        alreadyMappedId,
        context.variantCache,
      ),
      createdNow: false,
      productCode: productCode || rawProductCode,
    };
  }

  const created = await importProduct(
    source,
    context.locationId || undefined,
    {},
  );
  if (!created?.success || !created?.product?.id) {
    const errors = created?.errors || [
      { message: "Failed to create order product" },
    ];
    const error = new Error(
      stringifyUserErrors(errors) || "Order product create failed",
    );
    error.userErrors = errors;
    throw error;
  }

  const createdProduct = created.product;
  const productId = toStringValue(createdProduct?.id).trim();
  const variantId = toStringValue(
    createdProduct?.variants?.nodes?.[0]?.id,
  ).trim();
  if (!productId) {
    throw new Error(
      `Order product created without product ID for code ${productCode}`,
    );
  }

  context.productMap.set(productCodeKey, productId);
  if (variantId) context.variantCache.set(productId, variantId);

  const mappingRow = normalizeProductMappingRow({
    productcode: productCode,
    productname:
      toStringValue(createdProduct?.title).trim() || source.productname,
    producturl: "",
    shopify_product_id: productId,
    shopify_product_name:
      toStringValue(createdProduct?.title).trim() || source.productname,
    shopify_product_url: buildShopifyProductUrl(createdProduct?.handle),
  });

  if (mappingRow) {
    context.productRowsByCode.set(productCodeKey, mappingRow);
    context.markProductMappingDirty();
  }

  return {
    productId,
    variantId,
    createdNow: true,
    productCode: productCode || rawProductCode,
  };
}

async function buildLineItemInput(detail, order, context) {
  const quantity = deriveLineItemQuantity(detail);
  const unitPrice = deriveLineItemUnitPrice(detail, quantity);
  const productCode = toStringValue(detail?.productcode).trim();
  const productCodeKey = normalizeProductCodeKey(productCode);

  let title =
    toStringValue(detail?.productname).trim() || productCode || "Imported Item";

  const optionIdsRaw = toStringValue(detail?.optionids).trim();
  const optionsValue = toStringValue(detail?.options).trim();
  if (optionIdsRaw && optionsValue) {
    title = `${title} ${optionsValue}`.trim();
  }
  let mappedProductId = context.productMap.get(productCodeKey) || "";
  const preExistingInMap = Boolean(mappedProductId);
  let resolvedVariantId = "";
  let createdNow = false;

  if (!mappedProductId) {
    const ensured = await ensureMappedProductForLineItem(
      detail,
      order,
      context,
    );
    mappedProductId = toStringValue(ensured?.productId).trim();
    resolvedVariantId = toStringValue(ensured?.variantId).trim();
    createdNow = Boolean(ensured?.createdNow);
  }

  const shouldUseMappedReference = Boolean(mappedProductId);

  const lineItem = {
    quantity,
    title,
    sku: productCode,
    priceSet: buildMoneyBagInput(Math.max(0, unitPrice), context.currencyCode),
    taxable: parseBooleanFromYesNo(detail?.taxableproduct) !== false,
    requiresShipping: parseBooleanFromYesNo(detail?.nonshippable) !== true,
  };

  const weight = toNumber(detail?.productweight, 0);
  if (weight > 0) {
    lineItem.weight = {
      unit: "POUNDS",
      value: weight,
    };
  }

  const fulfillmentService = mapLineItemFulfillmentService(detail);
  if (fulfillmentService) lineItem.fulfillmentService = fulfillmentService;

  if (!preExistingInMap) {
    const properties = buildLineItemProperties(detail);
    if (properties.length > 0) lineItem.properties = properties;
  }

  const variantFromOptionIds = normalizeVariantId(detail?.optionids);
  if (shouldUseMappedReference) {
    lineItem.productId = mappedProductId;
    if (variantFromOptionIds) {
      lineItem.variantId = variantFromOptionIds;
    } else if (resolvedVariantId) {
      lineItem.variantId = resolvedVariantId;
    } else {
      const fallbackVariant = await resolveMappedVariantId(
        mappedProductId,
        context.variantCache,
      );
      if (fallbackVariant) lineItem.variantId = fallbackVariant;
    }
  }

  return {
    lineItemInput: compactObject(lineItem),
    usedMappedProduct: shouldUseMappedReference,
    createdProductNow: createdNow,
    productCode: productCode || toStringValue(detail?.productcode).trim(),
  };
}

function buildFallbackLineItem(order, currencyCode) {
  const orderId = toStringValue(order?.orderid).trim() || "unknown";
  const amount = Math.max(0, toNumber(order?.paymentamount, 0));
  return {
    quantity: 1,
    title: `Imported Order ${orderId}`,
    sku: `ORDER-${orderId}`,
    priceSet: buildMoneyBagInput(amount, currencyCode),
    taxable: false,
    requiresShipping: false,
  };
}

function buildOrderCustomAttributes(order) {
  const attributes = [];
  const push = (key, value) => {
    const cleaned = toStringValue(value).trim();
    if (!cleaned) return;
    attributes.push({ key, value: cleaned });
  };

  // push("volusion_ship_date", order?.shipdate);
  // push("volusion_cancel_date", order?.canceldate);

  return attributes;
}

async function buildOrderCreateInput(order, context) {
  const orderDetails = Array.isArray(order?.order_details)
    ? order.order_details
    : [];
  const productDetails = orderDetails.filter(
    (detail) => !isDiscountDetail(detail),
  );
  const customerAssociation = buildCustomerAssociation(
    order,
    context.customersById,
    context.customerMap,
  );

  const lineItems = [];
  const productCodesMissingInMap = new Set();
  for (const detail of productDetails) {
    const mapped = await buildLineItemInput(detail, order, context);
    lineItems.push(mapped.lineItemInput);
    if (!mapped.usedMappedProduct && mapped.productCode) {
      productCodesMissingInMap.add(mapped.productCode);
    }
  }

  if (lineItems.length === 0) {
    lineItems.push(buildFallbackLineItem(order, context.currencyCode));
  }

  const shippingAddress = buildShippingAddress(order);
  const billingAddress = buildBillingAddress(order);

  const shippingLines = buildShippingLines(
    order,
    orderDetails,
    context.currencyCode,
  );
  const taxLines = buildOrderTaxLines(order, context.currencyCode);
  const transactions = buildOrderTransactions(
    order,
    context.currencyCode,
    context.locationId,
  );
  const discountCode = buildOrderDiscountCode(
    orderDetails,
    context.currencyCode,
  );
  const orderNote = buildOrderNote(order);
  const sourceOrderId = toStringValue(order?.orderid).trim();
  const combinedOrder = context.combinedOrdersMap?.get(sourceOrderId) || null;
  const combinedTracking = extractCombinedTracking(combinedOrder);
  const metafields = buildOrderMetafields(
    order,
    context.customersById,
    context.customerMap,
    combinedOrder,
    context.currencyCode,
  );
  const customAttributes = buildOrderCustomAttributes(order);
  const willCancel = shouldCancelOrder(order);
  const fulfillmentStatus = mapOrderFulfillmentStatus(order, orderDetails);
  const needsFulfillment =
    !willCancel && shouldCreateFulfillment(order, orderDetails);
  const processedAt = parseDateToIso(order?.orderdate);

  const orderInput = compactObject({
    name: sourceOrderId || undefined,
    sourceIdentifier: toStringValue(order?.orderid).trim(),
    sourceName: "volusion_migrated_order",
    processedAt,
    financialStatus: mapOrderFinancialStatus(order, orderDetails),
    note: orderNote,
    customAttributes,
    customer: customerAssociation.customerInput,
    billingAddress,
    shippingAddress,
    lineItems,
    shippingLines,
    transactions,
    taxLines,
    discountCode,
    metafields,
    fulfillmentStatus: (!willCancel && fulfillmentStatus) || undefined,
    fulfillment:
      needsFulfillment && toStringValue(context.locationId).trim()
        ? {
          locationId: toStringValue(context.locationId).trim(),
          notifyCustomer: false,
          shipmentStatus: mapOrderShipmentStatus(order),
          trackingCompany:
            toStringValue(combinedTracking?.Gateway).trim() || undefined,
          trackingNumber:
            toStringValue(combinedTracking?.TrackingNumber).trim() ||
            undefined,
        }
        : undefined,
  });

  return {
    orderInput,
    customerAssociation,
    productCodesMissingInMap: [...productCodesMissingInMap],
  };
}

async function createOrder(orderInput) {
  let lastUserErrors = [];
  let lastResponseData = null;
  const sourceOrderName = toStringValue(
    orderInput?.name || orderInput?.sourceIdentifier,
  ).trim();

  for (let attempt = 0; attempt <= GRAPHQL_MAX_RETRIES; attempt++) {
    const data = await requestGraphql(ORDER_CREATE_MUTATION, {
      order: orderInput,
      options: {
        inventoryBehaviour: "BYPASS",
        sendReceipt: false,
        sendFulfillmentReceipt: false,
      },
    });

    lastResponseData = data;
    const payload = data?.orderCreate;
    if (!payload) {
      console.error(
        `[Order ${sourceOrderName}] orderCreate returned no payload (attempt ${attempt + 1}/${GRAPHQL_MAX_RETRIES + 1}). Response data:`,
        JSON.stringify(data, null, 2),
      );
      if (sourceOrderName) {
        const existingOrder = await findExistingOrderByName(sourceOrderName);
        if (existingOrder) return existingOrder;
      }
      if (attempt < GRAPHQL_MAX_RETRIES) {
        await sleep(computeRetryDelayMs(attempt));
        continue;
      }
      const responseDetail = JSON.stringify(lastResponseData || {});
      const error = new Error(
        `No payload returned from orderCreate for order ${sourceOrderName}. Response: ${responseDetail.slice(0, 500)}`,
      );
      error.userErrors = [{ message: error.message }];
      throw error;
    }

    const userErrors = payload?.userErrors || [];
    if (userErrors.length === 0 && payload?.order) {
      return payload.order;
    }

    if (userErrors.length === 0 && !payload?.order) {
      console.error(
        `[Order ${sourceOrderName}] orderCreate returned payload but no order (attempt ${attempt + 1}/${GRAPHQL_MAX_RETRIES + 1}). Payload:`,
        JSON.stringify(payload, null, 2),
      );
      if (sourceOrderName) {
        const existingOrder = await findExistingOrderByName(sourceOrderName);
        if (existingOrder) return existingOrder;
      }
      if (attempt < GRAPHQL_MAX_RETRIES) {
        await sleep(computeRetryDelayMs(attempt));
        continue;
      }
      const payloadDetail = JSON.stringify(payload || {});
      const error = new Error(
        `No order returned from orderCreate for order ${sourceOrderName}. Payload: ${payloadDetail.slice(0, 500)}`,
      );
      error.userErrors = [{ message: error.message }];
      throw error;
    }

    lastUserErrors = userErrors;
    const retryable = userErrors.some(isRetryableGraphqlError);
    if (retryable && attempt < GRAPHQL_MAX_RETRIES) {
      // console.warn(
      //   `[Order ${sourceOrderName}] orderCreate retryable error (attempt ${attempt + 1}): ${stringifyUserErrors(userErrors)}`,
      // );
      await sleep(computeRetryDelayMs(attempt));
      continue;
    }

    const error = new Error(
      stringifyUserErrors(userErrors) ||
      `orderCreate failed for order ${sourceOrderName}`,
    );
    error.userErrors = userErrors;
    throw error;
  }

  const error = new Error(
    stringifyUserErrors(lastUserErrors) ||
    `orderCreate failed after retries for order ${sourceOrderName}`,
  );
  error.userErrors = lastUserErrors;
  throw error;
}

async function cancelOrder(orderId, order) {
  const reason = buildOrderCancelReason(order);
  const staffNote = buildOrderCancelStaffNote(order);
  const data = await requestGraphql(ORDER_CANCEL_MUTATION, {
    orderId,
    reason,
    restock: false,
    refundMethod: { originalPaymentMethodsRefund: false },
    staffNote: staffNote || undefined,
    notifyCustomer: false,
  });

  const payload = data?.orderCancel;
  if (!payload) {
    return {
      success: false,
      errors: [{ message: "No payload returned from orderCancel" }],
    };
  }

  const userErrors = payload?.orderCancelUserErrors || [];
  if (userErrors.length > 0) {
    return { success: false, errors: userErrors };
  }

  return { success: true, job: payload?.job || null };
}

async function createFulfillmentEvent(fulfillmentId, happenedAt, order) {
  const data = await requestGraphql(FULFILLMENT_EVENT_CREATE_MUTATION, {
    fulfillmentEvent: {
      fulfillmentId,
      happenedAt,
      status: order ? mapOrderShipmentStatus(order) : "DELIVERED",
    },
  });

  const payload = data?.fulfillmentEventCreate;
  if (!payload) {
    return {
      success: false,
      errors: [{ message: "No payload from fulfillmentEventCreate" }],
    };
  }

  const userErrors = payload?.userErrors || [];
  if (userErrors.length > 0) return { success: false, errors: userErrors };

  return { success: true, fulfillmentEvent: payload.fulfillmentEvent };
}

async function fetchShopCurrencyCode() {
  try {
    const data = await requestGraphql(SHOP_CURRENCY_QUERY);
    const currencyCode = toStringValue(data?.shop?.currencyCode).trim();
    return currencyCode || "USD";
  } catch {
    return "USD";
  }
}

function normalizeCustomerMappingRow(row) {
  const customerid = normalizeCustomerId(row?.customerid);
  if (!customerid) return null;

  return {
    customerid,
    firstname: toStringValue(row?.firstname).trim(),
    lastname: toStringValue(row?.lastname).trim(),
    emailaddress: toStringValue(row?.emailaddress).trim(),
    shopifyCustomerId: toStringValue(row?.shopifyCustomerId).trim(),
  };
}

function upsertCustomerMapping(
  customerMap,
  sourceCustomerId,
  sourceOrder,
  createdCustomer,
  fallbackEmail = "",
) {
  const customerId = normalizeCustomerId(sourceCustomerId);
  const shopifyCustomerId = toStringValue(createdCustomer?.id).trim();
  if (!customerId || !shopifyCustomerId) return false;

  const existing = customerMap.get(customerId);
  if (existing?.shopifyCustomerId) return false;

  const row = normalizeCustomerMappingRow({
    customerid: customerId,
    firstname:
      toStringValue(createdCustomer?.firstName).trim() ||
      toStringValue(sourceOrder?.billingfirstname).trim() ||
      toStringValue(sourceOrder?.shipfirstname).trim(),
    lastname:
      toStringValue(createdCustomer?.lastName).trim() ||
      toStringValue(sourceOrder?.billinglastname).trim() ||
      toStringValue(sourceOrder?.shiplastname).trim(),
    emailaddress:
      toStringValue(
        createdCustomer?.defaultEmailAddress?.emailAddress,
      ).trim() || toStringValue(fallbackEmail).trim(),
    shopifyCustomerId,
  });

  if (!row) return false;
  customerMap.set(customerId, row);
  return true;
}

function customerMapToRows(customerMap) {
  return [...customerMap.values()]
    .map((row) => normalizeCustomerMappingRow(row))
    .filter(Boolean);
}

function orderReportMapToRows(orderReportMap) {
  return [...orderReportMap.values()];
}

async function resolveChunkFiles(baseFile) {
  const dir = process.cwd();
  const parsed = path.parse(baseFile);
  const baseName = parsed.name;
  const ext = parsed.ext;

  try {
    const allFiles = await fs.promises.readdir(dir);
    const chunkRegex = new RegExp(`^${baseName}(?:_part_\\d+)?\\${ext}$`, "i");
    const matches = allFiles.filter((f) => chunkRegex.test(f));

    if (matches.length === 1 && matches[0] === baseFile) {
      return [path.resolve(dir, baseFile)];
    }

    const parts = matches.filter((f) => f !== baseFile);
    if (parts.length > 0) {
      parts.sort((a, b) => {
        const numA = parseInt((a.match(/\d+/) || ["0"])[0], 10);
        const numB = parseInt((b.match(/\d+/) || ["0"])[0], 10);
        return numA - numB;
      });
      return parts.map((p) => path.resolve(dir, p));
    }
  } catch (e) {
    // ignore
  }
  return [path.resolve(dir, baseFile)];
}

async function iterateTopLevelJsonArray(filePaths, options, onItem) {
  const startAt =
    Number.isFinite(options?.startAt) && options.startAt > 0
      ? Math.trunc(options.startAt)
      : 0;
  const limit =
    Number.isFinite(options?.limit) && options.limit > 0
      ? Math.trunc(options.limit)
      : Number.POSITIVE_INFINITY;

  let seen = 0;
  let processed = 0;
  let done = false;

  const paths = Array.isArray(filePaths) ? filePaths : [filePaths];

  for (const fp of paths) {
    if (done) break;
    const stream = fs.createReadStream(fp, { encoding: "utf8" });
    let sawArrayStart = false;
    let depth = 0;
    let inString = false;
    let escaped = false;
    let objectBuffer = "";

    try {
      outer: for await (const chunk of stream) {
        for (let i = 0; i < chunk.length; i++) {
          const ch = chunk[i];

          if (!sawArrayStart) {
            if (/\s/.test(ch)) continue;
            if (ch === "[") {
              sawArrayStart = true;
              continue;
            }
            throw new Error(`orders file ${fp} must contain a top-level JSON array`);
          }

          if (depth === 0) {
            if (/\s|,/.test(ch)) continue;
            if (ch === "]") {
              break outer;
            }
            if (ch === "{") {
              depth = 1;
              objectBuffer = "{";
              continue;
            }
            throw new Error(`Invalid JSON token while parsing orders file ${fp}: ${ch}`);
          }

          objectBuffer += ch;

          if (inString) {
            if (escaped) escaped = false;
            else if (ch === "\\") escaped = true;
            else if (ch === '"') inString = false;
            continue;
          }

          if (ch === '"') {
            inString = true;
            continue;
          }

          if (ch === "{") {
            depth++;
            continue;
          }

          if (ch === "}") {
            depth--;
            if (depth !== 0) continue;

            seen++;
            const jsonText = objectBuffer;
            objectBuffer = "";

            let item;
            try {
              item = JSON.parse(jsonText);
            } catch (err) {
              throw new Error(
                `Failed to parse order object at array index ${seen} in ${fp}: ${err.message || String(err)}`,
              );
            }

            if (seen <= startAt) continue;

            processed++;
            await onItem(item, { seen, processed });

            if (processed >= limit) {
              done = true;
              break outer;
            }
          }
        }
      }
    } finally {
      try {
        stream.destroy();
      } catch {
        // no-op
      }
    }

    if (!sawArrayStart) {
      throw new Error(`orders file ${fp} must contain a top-level JSON array`);
    }
    if (depth !== 0 && !done) {
      throw new Error(
        `Malformed JSON while parsing orders file ${fp} (object depth mismatch)`,
      );
    }
  }

  return { seen, processed, completedArray: done };
}

async function migrateOrders(options = {}) {
  const {
    file = "orders_merged.json",
    limit,
    startAt,
    delayMs = DEFAULT_DELAY_MS,
    flushEvery = DEFAULT_FLUSH_EVERY,
    onProgress = () => { },
  } = options;

  const filePath = path.resolve(process.cwd(), file);
  const customerMappingPath = path.resolve(
    process.cwd(),
    CUSTOMER_IMPORT_MAPPING_FILE,
  );
  const productMappingPath = path.resolve(
    process.cwd(),
    PRODUCT_IMPORT_MAPPING_FILE,
  );
  const customerSourcePath = path.resolve(process.cwd(), CUSTOMERS_SOURCE_FILE);
  const orderMappingPath = path.resolve(
    process.cwd(),
    ORDER_IMPORT_MAPPING_FILE,
  );
  const combinedOrdersFiles = await resolveChunkFiles(COMBINED_ORDERS_FILE);
  const combinedOrdersMap = new Map();
  for (const fp of combinedOrdersFiles) {
    const rows = await loadJsonArray(fp);
    for (const row of rows || []) {
      const orderId = toStringValue(row?.OrderID).trim();
      if (orderId) combinedOrdersMap.set(orderId, row);
    }
  }

  const customerRows = await loadJsonArray(customerMappingPath);
  const productRows = await loadJsonArray(productMappingPath);
  const customerSourceRows = await loadJsonArray(customerSourcePath);
  const orderMappingRows = await loadJsonArray(orderMappingPath);

  const customerMap = buildCustomerMap(customerRows);
  const productMap = buildProductMap(productRows);
  const productRowsByCode = buildProductRowsByCode(productRows);
  const customersById = buildCustomersById(customerSourceRows);
  const orderReportMap = buildOrderReportMap(orderMappingRows);
  const variantCache = new Map();

  const currencyCode = await fetchShopCurrencyCode();
  const locationId = toStringValue(process.env.SHOPIFY_LOCATION_ID).trim();

  const results = {
    created: 0,
    existing: 0,
    failed: 0,
    cancelled: 0,
    total: 0,
    errors: [],
  };

  let dirtyOrders = 0;
  let dirtyCustomers = 0;
  let dirtyProducts = 0;
  const effectiveFlushEvery =
    Number.isFinite(flushEvery) && flushEvery > 0
      ? Math.trunc(flushEvery)
      : DEFAULT_FLUSH_EVERY;

  const flushFiles = async () => {
    if (dirtyOrders > 0) {
      await writeJsonArray(
        orderMappingPath,
        orderReportMapToRows(orderReportMap),
      );
      dirtyOrders = 0;
    }
    if (dirtyCustomers > 0) {
      await writeJsonArray(customerMappingPath, customerMapToRows(customerMap));
      dirtyCustomers = 0;
    }
    if (dirtyProducts > 0) {
      await writeJsonArray(
        productMappingPath,
        productRowsByCodeToRows(productRowsByCode),
      );
      dirtyProducts = 0;
    }
  };

  const orderFiles = await resolveChunkFiles(file);

  await iterateTopLevelJsonArray(
    orderFiles,
    { limit, startAt },
    async (sourceOrder, meta) => {
      const index = meta.processed;
      const sourceOrderId =
        toStringValue(sourceOrder?.orderid).trim() || `__index_${meta.seen}`;

      try {
        const existingOrder = await findExistingOrderByName(sourceOrderId);
        if (existingOrder) {
          results.existing++;
          results.total++;

          orderReportMap.set(sourceOrderId, {
            orderid: sourceOrderId,
            shopify_order_id: toStringValue(existingOrder?.id).trim(),
            shopify_order_name: toStringValue(existingOrder?.name).trim(),
            status: "existing",
            cancel_status: "not_requested",
            missing_product_codes: [],
            customerid: normalizeCustomerId(sourceOrder?.customerid),
            shopify_customer_id: toStringValue(
              existingOrder?.customer?.id,
            ).trim(),
            orderdate: toStringValue(sourceOrder?.orderdate).trim(),
            canceldate: toStringValue(sourceOrder?.canceldate).trim(),
            error: "",
          });
          dirtyOrders++;

          onProgress({
            index,
            status: "existing",
            sourceOrder,
            targetOrder: existingOrder,
          });

          return;
        }

        const buildContext = {
          currencyCode,
          locationId,
          productMap,
          productRowsByCode,
          customerMap,
          customersById,
          variantCache,
          combinedOrdersMap,
          markProductMappingDirty: () => {
            dirtyProducts++;
          },
        };

        const { orderInput, customerAssociation, productCodesMissingInMap } =
          await buildOrderCreateInput(sourceOrder, buildContext);

        const createdOrder = await createOrder(orderInput);

        results.created++;
        results.total++;

        const shipDate = parseDateToIso(sourceOrder?.shipdate);
        const fulfillmentId = toStringValue(
          createdOrder?.fulfillments?.[0]?.id,
        ).trim();
        if (shipDate && fulfillmentId && !shouldCancelOrder(sourceOrder)) {
          await createFulfillmentEvent(fulfillmentId, shipDate, sourceOrder);
        }

        const customerAdded = upsertCustomerMapping(
          customerMap,
          customerAssociation.sourceCustomerId,
          sourceOrder,
          createdOrder?.customer,
          customerAssociation.customerEmail,
        );
        if (customerAdded) dirtyCustomers++;

        let cancelStatus = "not_requested";
        let cancelErrors = [];
        if (shouldCancelOrder(sourceOrder)) {
          const cancelResult = await cancelOrder(createdOrder.id, sourceOrder);
          if (cancelResult.success) {
            cancelStatus = "cancelled";
            results.cancelled++;
          } else {
            cancelStatus = "cancel_failed";
            cancelErrors = cancelResult.errors || [];
            results.errors.push({
              orderId: sourceOrderId,
              phase: "cancel",
              errors: cancelErrors,
            });
          }
        }

        orderReportMap.set(sourceOrderId, {
          orderid: sourceOrderId,
          shopify_order_id: toStringValue(createdOrder?.id).trim(),
          shopify_order_name: toStringValue(createdOrder?.name).trim(),
          status:
            cancelStatus === "cancelled" ? "created_cancelled" : "created",
          cancel_status: cancelStatus,
          missing_product_codes: productCodesMissingInMap,
          customerid: normalizeCustomerId(sourceOrder?.customerid),
          shopify_customer_id: toStringValue(createdOrder?.customer?.id).trim(),
          orderdate: toStringValue(sourceOrder?.orderdate).trim(),
          canceldate: toStringValue(sourceOrder?.canceldate).trim(),
          error:
            cancelErrors.length > 0 ? stringifyUserErrors(cancelErrors) : "",
        });
        dirtyOrders++;

        onProgress({
          index,
          status: "created",
          sourceOrder,
          targetOrder: createdOrder,
          cancelStatus,
        });
      } catch (err) {
        results.failed++;
        results.total++;

        const wrappedErrors = Array.isArray(err?.userErrors)
          ? err.userErrors
          : [{ message: err?.message || String(err) }];

        results.errors.push({
          orderId: sourceOrderId,
          phase: "create",
          errors: wrappedErrors,
        });

        orderReportMap.set(sourceOrderId, {
          orderid: sourceOrderId,
          shopify_order_id: "",
          shopify_order_name: "",
          status: "failed",
          cancel_status: "not_requested",
          missing_product_codes: [],
          customerid: normalizeCustomerId(sourceOrder?.customerid),
          shopify_customer_id: "",
          orderdate: toStringValue(sourceOrder?.orderdate).trim(),
          canceldate: toStringValue(sourceOrder?.canceldate).trim(),
          error: stringifyUserErrors(wrappedErrors),
        });
        dirtyOrders++;

        onProgress({
          index,
          status: "failed",
          sourceOrder,
          errors: wrappedErrors,
        });
      }

      if (dirtyOrders + dirtyCustomers + dirtyProducts >= effectiveFlushEvery) {
        await flushFiles();
      }

      if (delayMs > 0) await sleep(delayMs);
    },
  );

  await flushFiles();

  return {
    ...results,
    mappingFile: orderMappingPath,
    customerMappingFile: customerMappingPath,
    productMappingFile: productMappingPath,
  };
}

module.exports = {
  migrateOrders,
  ORDER_CREATE_MUTATION,
  ORDER_CANCEL_MUTATION,
};
