const fs = require("fs");
const path = require("path");
const shopifyClient = require("../config/shopify");

const CUSTOMER_CREATE_MUTATION = `
  mutation customerCreate($input: CustomerInput!) {
    customerCreate(input: $input) {
      customer {
        id
        firstName
        lastName
        defaultEmailAddress {
          emailAddress
        }
        defaultPhoneNumber {
          phoneNumber
        }
      }
      userErrors {
        field
        message
      }
    }
  }
`;

const CUSTOMER_ADDRESS_CREATE_MUTATION = `
  mutation customerAddressCreate(
    $customerId: ID!
    $address: MailingAddressInput!
    $setAsDefault: Boolean
  ) {
    customerAddressCreate(
      customerId: $customerId
      address: $address
      setAsDefault: $setAsDefault
    ) {
      address {
        id
      }
      userErrors {
        field
        message
      }
    }
  }
`;

const METAFIELDS_SET_MUTATION = `
  mutation metafieldsSet($metafields: [MetafieldsSetInput!]!) {
    metafieldsSet(metafields: $metafields) {
      metafields {
        id
        key
      }
      userErrors {
        field
        message
      }
    }
  }
`;

const CUSTOMERS_BY_QUERY = `
  query customersByQuery($first: Int!, $query: String!) {
    customers(first: $first, query: $query) {
      nodes {
        id
        defaultEmailAddress {
          emailAddress
        }
      }
    }
  }
`;

const CUSTOMER_IMPORT_MAPPING_FILE = "customer-import-mapping.json";
const CUSTOMER_NAMESPACE = "customer";
const DEFAULT_DELAY_MS = 150;
const DEFAULT_FLUSH_EVERY = 200;
const fsp = fs.promises;

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

const TAX_ENTITY_USE_CODE_LABELS = {
  G: "RESALE",
  L: "OTHER/CUSTOM",
  N: "LOCAL GOVERNMENT",
};

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function toStringValue(value) {
  if (value === undefined || value === null) return "";
  return String(value);
}

function normalizeCustomerId(item) {
  return toStringValue(item?.customerid).trim();
}

function isValidEmail(email) {
  const value = toStringValue(email).trim();
  return value !== "";
}

function normalizeCustomerEmail(item) {
  const raw = toStringValue(item?.emailaddress).trim();
  if (!isValidEmail(raw)) return "";
  return raw.toLowerCase();
}

function parseEmailSubscriber(value) {
  const raw = toStringValue(value).trim().toUpperCase();
  if (!raw) return null;
  if (raw === "Y" || raw === "TRUE" || raw === "1") return true;
  if (raw === "N" || raw === "FALSE" || raw === "0") return false;
  return null;
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

function normalizePhone(rawPhone, countryCode) {
  const original = toStringValue(rawPhone).trim();
  if (!original) return "";

  // Remove simple extension suffixes before normalization.
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
    if (digits.length === 11 && digits.startsWith("1")) {
      return `+${digits}`;
    }
    if (digits.length === 10) {
      return `+1${digits}`;
    }
  }

  if (digits.length >= 8 && digits.length <= 15) {
    return `+${digits}`;
  }

  return "";
}

function parseDateToIso(rawDate) {
  const value = toStringValue(rawDate).trim();
  if (!value) return "";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "";
  return date.toISOString();
}

function parseCustomersFile(filePath) {
  const raw = fs.readFileSync(filePath, "utf8");
  const parsed = JSON.parse(raw);
  if (!Array.isArray(parsed)) {
    throw new Error("customers file must contain an array");
  }
  return parsed;
}

function compactObject(obj) {
  const output = {};
  for (const [key, value] of Object.entries(obj || {})) {
    if (value === undefined || value === null) continue;
    if (typeof value === "string" && !value.trim()) continue;
    if (Array.isArray(value) && value.length === 0) continue;
    if (typeof value === "object" && !Array.isArray(value)) {
      const nested = compactObject(value);
      if (Object.keys(nested).length === 0) continue;
      output[key] = nested;
      continue;
    }
    output[key] = value;
  }
  return output;
}

function buildCustomerMetafields(item, options = {}) {
  const salesRepresentativeCustomerId = toStringValue(
    options?.salesRepresentativeCustomerId,
  ).trim();
  const metafields = [];

  const push = (key, value) => {
    const cleaned = toStringValue(value).trim();
    if (!cleaned) return;
    metafields.push({
      namespace: CUSTOMER_NAMESPACE,
      key,
      type: "single_line_text_field",
      value: cleaned,
    });
  };

  const mapTaxEntityUseCode = (rawValue) => {
    const code = toStringValue(rawValue).trim().toUpperCase();
    if (!code) return "";
    const label = TAX_ENTITY_USE_CODE_LABELS[code];
    return label ? `${code} - ${label}` : code;
  };

  push("fax_number", item?.faxnumber);
  push("website", item?.websiteaddress);
  if (salesRepresentativeCustomerId) {
    metafields.push({
      namespace: CUSTOMER_NAMESPACE,
      key: "sales_representative",
      type: "customer_reference",
      value: salesRepresentativeCustomerId,
    });
  }
  push("custom_field_1", item?.custom_field_custom1);
  push("tax_entity_use_code", mapTaxEntityUseCode(item?.taxentityusecode));

  return metafields;
}

function buildEmailMarketingConsent(item, email) {
  const isSubscribed = parseEmailSubscriber(item?.emailsubscriber);
  if (isSubscribed === null || !email) return null;

  const consent = {
    marketingState: isSubscribed ? "SUBSCRIBED" : "NOT_SUBSCRIBED",
  };

  if (isSubscribed) {
    consent.marketingOptInLevel = "SINGLE_OPT_IN";
  }

  const consentUpdatedAt = parseDateToIso(
    item?.lastmodified || item?.createddate,
  );
  if (consentUpdatedAt) {
    consent.consentUpdatedAt = consentUpdatedAt;
  }

  return consent;
}

function buildDefaultAddress({
  item,
  countryCode,
  phone,
  includeAddress = true,
}) {
  if (!includeAddress) return null;

  const useTerritoryLikeFallback = shouldUseTerritoryLikeAddressFallback(
    item?.country,
    countryCode,
  );
  const cityRaw = toStringValue(item?.city).trim();
  const stateRaw = toStringValue(item?.state).trim();
  const address2Raw = toStringValue(item?.billingaddress2).trim();
  const territoryProvinceCode = resolveUsTerritoryProvinceCode(
    item?.country,
    item?.state,
  );

  const address = compactObject({
    firstName: toStringValue(item?.firstname).trim(),
    lastName: toStringValue(item?.lastname).trim(),
    company: toStringValue(item?.companyname).trim(),
    address1: toStringValue(item?.billingaddress1).trim(),
    // Territory/invalid-country normalization for Shopify:
    // move state -> city, move city -> address2, and use US country code.
    address2: useTerritoryLikeFallback ? cityRaw || address2Raw : address2Raw,
    city: useTerritoryLikeFallback ? stateRaw || cityRaw : cityRaw,
    provinceCode: useTerritoryLikeFallback
      ? territoryProvinceCode
      : normalizeProvinceCode(item?.state),
    zip: toStringValue(item?.postalcode).trim(),
    countryCode: useTerritoryLikeFallback ? "US" : countryCode,
    phone,
  });

  const hasPhysicalAddress = [
    "company",
    "address1",
    "address2",
    "city",
    "provinceCode",
    "zip",
    "countryCode",
    "phone",
  ].some((key) => Boolean(address[key]));

  if (!hasPhysicalAddress) return null;
  return address;
}

function buildAddressInputForExistingCustomer(item, options = {}) {
  const includePhone = options.includePhone !== false;
  const countryCode = normalizeCountryCode(item?.country);
  const phone = includePhone
    ? normalizePhone(item?.phonenumber, countryCode)
    : "";

  return buildDefaultAddress({
    item,
    countryCode,
    phone,
    includeAddress: true,
  });
}

function buildCustomerInput(item, sourceById, options = {}) {
  const includePhone = options.includePhone !== false;
  const includeAddress = options.includeAddress !== false;
  const salesRepresentativeCustomerId = toStringValue(
    options.salesRepresentativeCustomerId,
  ).trim();

  const firstName = toStringValue(item?.firstname).trim();
  const lastName = toStringValue(item?.lastname).trim();
  const note = toStringValue(item?.customer_notes).trim();

  const rawEmail = toStringValue(item?.emailaddress).trim();
  const email = isValidEmail(rawEmail) ? rawEmail : "";

  const countryCode = normalizeCountryCode(item?.country);
  const phone = includePhone
    ? normalizePhone(item?.phonenumber, countryCode)
    : "";
  const defaultAddress = buildDefaultAddress({
    item,
    countryCode,
    phone,
    includeAddress,
  });

  const metafields = buildCustomerMetafields(item, {
    salesRepresentativeCustomerId,
  });
  const emailMarketingConsent = buildEmailMarketingConsent(item, email);

  const input = compactObject({
    firstName,
    lastName,
    email,
    phone,
    note,
    addresses: defaultAddress ? [defaultAddress] : [],
    emailMarketingConsent,
    metafields,
  });

  const hasIdentity = Boolean(
    input.firstName || input.lastName || input.email || input.phone,
  );
  if (!hasIdentity) {
    throw new Error(
      "Customer must include at least one of: first name, last name, valid email, valid phone",
    );
  }

  return {
    input,
    hasAddress: Boolean(defaultAddress),
    hasPhone: Boolean(phone),
  };
}

function stringifyUserErrors(userErrors) {
  return (userErrors || [])
    .map((e) => {
      const field = Array.isArray(e?.field) ? e.field.join(".") : "";
      return field ? `${field}: ${e?.message}` : toStringValue(e?.message);
    })
    .filter(Boolean)
    .join("; ");
}

function hasErrorKeyword(userErrors, keywords) {
  const list = Array.isArray(keywords) ? keywords : [keywords];
  const normalizedKeywords = list
    .map((k) => toStringValue(k).toLowerCase().trim())
    .filter(Boolean);
  if (normalizedKeywords.length === 0) return false;

  return (userErrors || []).some((err) => {
    const field = Array.isArray(err?.field)
      ? err.field.join(".").toLowerCase()
      : "";
    const message = toStringValue(err?.message).toLowerCase();
    return normalizedKeywords.some(
      (keyword) => field.includes(keyword) || message.includes(keyword),
    );
  });
}

function isAddressAlreadyExistsError(userErrors) {
  return (userErrors || []).some((err) => {
    const message = toStringValue(err?.message).toLowerCase();
    return (
      message.includes("address already exists") ||
      (message.includes("address") && message.includes("already exists"))
    );
  });
}

async function requestGraphql(query, variables = {}) {
  const response = await shopifyClient.request(query, { variables });
  const topErrors = response?.errors || [];
  if (topErrors.length > 0) {
    throw new Error(topErrors.map((e) => e.message).join("; "));
  }
  return response;
}

async function runCustomerCreate(input) {
  const response = await requestGraphql(CUSTOMER_CREATE_MUTATION, { input });
  return response?.data?.customerCreate || { customer: null, userErrors: [] };
}

async function runCustomerAddressCreate({ customerId, address, setAsDefault }) {
  const response = await requestGraphql(CUSTOMER_ADDRESS_CREATE_MUTATION, {
    customerId,
    address,
    setAsDefault,
  });
  return (
    response?.data?.customerAddressCreate || {
      address: null,
      userErrors: [],
    }
  );
}

async function setCustomerSalesRepresentativeReference({
  customerId,
  salesRepresentativeCustomerId,
}) {
  const ownerId = toStringValue(customerId).trim();
  const referenceId = toStringValue(salesRepresentativeCustomerId).trim();
  if (!ownerId || !referenceId) {
    return { status: "sales_rep_reference_skipped" };
  }

  const response = await requestGraphql(METAFIELDS_SET_MUTATION, {
    metafields: [
      {
        ownerId,
        namespace: CUSTOMER_NAMESPACE,
        key: "sales_representative",
        type: "customer_reference",
        value: referenceId,
      },
    ],
  });

  const payload = response?.data?.metafieldsSet || {
    metafields: [],
    userErrors: [],
  };
  const userErrors = payload?.userErrors || [];
  if (userErrors.length > 0) {
    const error = new Error(
      stringifyUserErrors(userErrors) ||
        "Failed to set sales representative customer reference",
    );
    error.userErrors = userErrors;
    throw error;
  }

  return { status: "sales_rep_reference_set", metafields: payload.metafields };
}

async function addAddressToExistingCustomer(source, shopifyCustomerId) {
  const customerId = toStringValue(shopifyCustomerId).trim();
  if (!customerId) {
    throw new Error("Missing Shopify customer id for address update");
  }

  const withPhone = buildAddressInputForExistingCustomer(source, {
    includePhone: true,
  });
  if (!withPhone) {
    return { status: "address_skipped_no_source_address" };
  }

  const attempts = [withPhone];
  if (withPhone.phone) {
    const withoutPhone = { ...withPhone };
    delete withoutPhone.phone;
    attempts.push(withoutPhone);
  }

  let lastUserErrors = [];
  let lastErrorMessage = "";

  for (let i = 0; i < attempts.length; i++) {
    const address = attempts[i];
    const payload = await runCustomerAddressCreate({
      customerId,
      address,
      setAsDefault: false,
    });
    const userErrors = payload?.userErrors || [];
    if (userErrors.length === 0) {
      return { status: "address_added", customerAddress: payload.address };
    }
    if (isAddressAlreadyExistsError(userErrors)) {
      return { status: "address_already_exists", customerAddress: null };
    }

    lastUserErrors = userErrors;
    lastErrorMessage = stringifyUserErrors(userErrors);

    const hasPhoneError = i === 0 && hasErrorKeyword(userErrors, ["phone"]);
    if (hasPhoneError) {
      continue;
    }

    break;
  }

  const error = new Error(lastErrorMessage || "Customer address create failed");
  error.userErrors =
    lastUserErrors.length > 0
      ? lastUserErrors
      : [
          {
            message:
              lastErrorMessage || "Unknown customer address create error",
          },
        ];
  throw error;
}

function quoteSearchValue(value) {
  const escaped = toStringValue(value)
    .replace(/\\/g, "\\\\")
    .replace(/"/g, '\\"');
  return `"${escaped}"`;
}

async function findExistingCustomerByEmail(email) {
  if (!email) return null;
  const query = `email:${quoteSearchValue(email)}`;
  const response = await requestGraphql(CUSTOMERS_BY_QUERY, {
    first: 10,
    query,
  });

  const nodes = response?.data?.customers?.nodes || [];
  const expected = email.toLowerCase();
  for (const node of nodes) {
    const candidate = toStringValue(
      node?.defaultEmailAddress?.emailAddress,
    ).toLowerCase();
    if (candidate === expected) return node;
  }
  return null;
}

function buildAttemptConfigs(base) {
  const configs = [{ includePhone: true, includeAddress: true }];

  if (base.hasPhone) {
    configs.push({ includePhone: false, includeAddress: true });
  }
  if (base.hasAddress) {
    configs.push({ includePhone: true, includeAddress: false });
  }
  if (base.hasPhone && base.hasAddress) {
    configs.push({ includePhone: false, includeAddress: false });
  }

  const seen = new Set();
  return configs.filter((config) => {
    const key = `${config.includePhone}-${config.includeAddress}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

async function createCustomerOnly(item, sourceById, options = {}) {
  const salesRepresentativeCustomerId = toStringValue(
    options.salesRepresentativeCustomerId,
  ).trim();

  const initial = buildCustomerInput(item, sourceById, {
    includePhone: true,
    includeAddress: true,
    salesRepresentativeCustomerId,
  });
  const attemptConfigs = buildAttemptConfigs(initial);

  let lastUserErrors = [];
  let lastErrorMessage = "";

  for (const config of attemptConfigs) {
    let built;
    try {
      built = buildCustomerInput(item, sourceById, {
        ...config,
        salesRepresentativeCustomerId,
      });
    } catch (err) {
      lastErrorMessage = err.message || String(err);
      continue;
    }

    const createPayload = await runCustomerCreate(built.input);
    const createErrors = createPayload?.userErrors || [];
    if (createErrors.length === 0 && createPayload?.customer?.id) {
      return { status: "created", customer: createPayload.customer };
    }

    lastUserErrors = createErrors;
    lastErrorMessage = stringifyUserErrors(createErrors);

    const hasPhoneError =
      config.includePhone && hasErrorKeyword(createErrors, ["phone"]);
    const hasAddressError =
      config.includeAddress &&
      hasErrorKeyword(createErrors, [
        "address",
        "country",
        "province",
        "zip",
        "postal",
      ]);

    // Retry with less strict payload when phone/address-related validation fails.
    // This path never updates any existing customer.
    if (hasPhoneError || hasAddressError) {
      continue;
    }

    const error = new Error(lastErrorMessage || "Customer create failed");
    error.userErrors = createErrors;
    throw error;
  }

  const error = new Error(lastErrorMessage || "Failed to create customer");
  error.userErrors =
    lastUserErrors.length > 0
      ? lastUserErrors
      : [{ message: lastErrorMessage || "Unknown customer migration error" }];
  throw error;
}

function normalizeMappingRow(row) {
  const customerid = toStringValue(row?.customerid).trim();
  if (!customerid) return null;

  return {
    customerid,
    firstname: toStringValue(row?.firstname).trim(),
    lastname: toStringValue(row?.lastname).trim(),
    emailaddress: toStringValue(row?.emailaddress).trim(),
    shopifyCustomerId: toStringValue(row?.shopifyCustomerId).trim(),
  };
}

function buildMappingRow(sourceCustomer, targetCustomer) {
  return normalizeMappingRow({
    customerid: normalizeCustomerId(sourceCustomer),
    firstname: toStringValue(sourceCustomer?.firstname).trim(),
    lastname: toStringValue(sourceCustomer?.lastname).trim(),
    emailaddress: toStringValue(sourceCustomer?.emailaddress).trim(),
    shopifyCustomerId: toStringValue(targetCustomer?.id).trim(),
  });
}

async function loadCustomerMappingMap(mappingPath) {
  const map = new Map();

  let raw = "";
  try {
    raw = await fsp.readFile(mappingPath, "utf8");
  } catch (err) {
    if (err.code === "ENOENT") {
      // DO NOT create file here
      return map;
    }
    throw err;
  }

  const trimmed = raw.trim();
  if (!trimmed) return map;

  let parsed;
  try {
    parsed = JSON.parse(trimmed);
  } catch (err) {
    // DO NOT overwrite file if JSON is bad
    throw new Error(
      `Mapping file is corrupted. Fix it manually. File: ${mappingPath}`,
    );
  }

  const rows = Array.isArray(parsed) ? parsed : [];

  for (const row of rows) {
    const normalized = normalizeMappingRow(row);
    if (!normalized) continue;
    map.set(normalized.customerid, normalized);
  }

  return map;
}
function formatMappingRowForJsonArrayAppend(row) {
  return JSON.stringify(row, null, 2)
    .split(/\r?\n/)
    .map((line) => `  ${line}`)
    .join("\n");
}

async function appendCustomerMappingRows(mappingPath, rows) {
  const normalizedRows = (rows || [])
    .map((row) => normalizeMappingRow(row))
    .filter(Boolean);

  if (normalizedRows.length === 0) return;

  let existing = [];

  try {
    const raw = await fsp.readFile(mappingPath, "utf8");
    existing = raw.trim() ? JSON.parse(raw) : [];
  } catch (err) {
    if (err.code !== "ENOENT") throw err;
  }

  // Merge safely
  const existingMap = new Map();
  for (const row of existing) {
    const norm = normalizeMappingRow(row);
    if (norm) existingMap.set(norm.customerid, norm);
  }

  for (const row of normalizedRows) {
    existingMap.set(row.customerid, row);
  }

  const finalData = Array.from(existingMap.values());

  const tempPath = mappingPath + ".tmp";

  await fsp.writeFile(
    tempPath,
    JSON.stringify(finalData, null, 2) + "\n",
    "utf8",
  );

  await fsp.rename(tempPath, mappingPath);
}
async function migrateCustomers(options = {}) {
  const {
    file = "customers.json",
    limit,
    start,
    end,
    delayMs = DEFAULT_DELAY_MS,
    onProgress = () => {},
  } = options;

  const filePath = path.resolve(process.cwd(), file);
  const allCustomers = parseCustomersFile(filePath);

  const startIndex = Number.isFinite(start) && start >= 1 ? start - 1 : 0;
  const endIndex = Number.isFinite(end) && end >= 1 ? end : allCustomers.length;
  let customers = allCustomers.slice(startIndex, endIndex);

  if (Number.isFinite(limit) && limit > 0) {
    customers = customers.slice(0, limit);
  }

  const sourceById = new Map();
  for (const item of allCustomers) {
    const id = normalizeCustomerId(item);
    if (!id || sourceById.has(id)) continue;
    sourceById.set(id, item);
  }

  const mappingPath = path.resolve(process.cwd(), CUSTOMER_IMPORT_MAPPING_FILE);
  const mappingMap = await loadCustomerMappingMap(mappingPath);
  const persistedSourceIds = new Set(mappingMap.keys());
  const pendingMappingRows = [];
  const statusBySourceId = new Map();
  const emailToShopifyId = new Map();
  const inProgress = new Set();

  for (const [mappedSourceId, mappedRow] of mappingMap.entries()) {
    statusBySourceId.set(mappedSourceId, "mapped_existing_file");
    const mappedEmail = toStringValue(mappedRow?.emailaddress)
      .trim()
      .toLowerCase();
    const mappedShopifyId = toStringValue(mappedRow?.shopifyCustomerId).trim();
    if (mappedEmail && mappedShopifyId) {
      emailToShopifyId.set(mappedEmail, mappedShopifyId);
    }
  }

  const results = {
    created: 0,
    failed: 0,
    errors: [],
    total: customers.length,
  };

  function buildLabel(source, sourceId, index) {
    return (
      toStringValue(source?.emailaddress).trim() ||
      [source?.firstname, source?.lastname]
        .map((x) => toStringValue(x).trim())
        .filter(Boolean)
        .join(" ")
        .trim() ||
      sourceId ||
      `#${index}`
    );
  }

  function trackSourceMapping({
    source,
    sourceId,
    shopifyCustomerId,
    status,
    customer,
  }) {
    const targetCustomer =
      customer && toStringValue(customer?.id).trim()
        ? customer
        : { id: shopifyCustomerId };
    const targetId = toStringValue(targetCustomer?.id).trim();
    if (!targetId) {
      throw new Error(
        `Missing Shopify customer id while mapping customerid=${sourceId}`,
      );
    }

    const mappedRow = buildMappingRow(source, targetCustomer);
    mappingMap.set(sourceId, mappedRow);
    statusBySourceId.set(sourceId, status);
    if (!persistedSourceIds.has(sourceId)) {
      pendingMappingRows.push(mappedRow);
      persistedSourceIds.add(sourceId);
    }

    const normalizedEmail = normalizeCustomerEmail(source);
    if (normalizedEmail) {
      emailToShopifyId.set(normalizedEmail, targetId);
    }

    return {
      status,
      customer: targetCustomer,
      mapping: mappedRow,
    };
  }

  async function ensureCustomerMappedBySourceId(sourceId) {
    const normalizedSourceId = toStringValue(sourceId).trim();
    if (!normalizedSourceId) {
      throw new Error("Missing customerid");
    }

    const existingMapping = mappingMap.get(normalizedSourceId);
    if (existingMapping) {
      return {
        status: statusBySourceId.get(normalizedSourceId) || "mapped_existing",
        customer: { id: existingMapping.shopifyCustomerId },
        mapping: existingMapping,
      };
    }

    const source = sourceById.get(normalizedSourceId);
    if (!source) {
      throw new Error(
        `salesrep_customerid ${normalizedSourceId} not found in source data`,
      );
    }

    if (inProgress.has(normalizedSourceId)) {
      throw new Error(
        `Circular sales representative reference detected for customerid=${normalizedSourceId}`,
      );
    }

    inProgress.add(normalizedSourceId);
    try {
      const normalizedEmail = normalizeCustomerEmail(source);
      if (normalizedEmail) {
        const cachedId = toStringValue(
          emailToShopifyId.get(normalizedEmail),
        ).trim();
        if (cachedId) {
          const addressOutcome = await addAddressToExistingCustomer(
            source,
            cachedId,
          );
          return trackSourceMapping({
            source,
            sourceId: normalizedSourceId,
            shopifyCustomerId: cachedId,
            status:
              addressOutcome.status === "address_added"
                ? "mapped_existing_email_address_added"
                : addressOutcome.status === "address_already_exists"
                  ? "mapped_existing_email_address_exists"
                  : "mapped_existing_email",
          });
        }

        const existingByEmail =
          await findExistingCustomerByEmail(normalizedEmail);
        const existingId = toStringValue(existingByEmail?.id).trim();
        if (existingId) {
          const addressOutcome = await addAddressToExistingCustomer(
            source,
            existingId,
          );
          return trackSourceMapping({
            source,
            sourceId: normalizedSourceId,
            shopifyCustomerId: existingId,
            status:
              addressOutcome.status === "address_added"
                ? "mapped_existing_email_address_added"
                : addressOutcome.status === "address_already_exists"
                  ? "mapped_existing_email_address_exists"
                  : "mapped_existing_email",
            customer: existingByEmail,
          });
        }
      }

      const salesRepSourceId = toStringValue(
        source?.salesrep_customerid,
      ).trim();
      let salesRepresentativeCustomerId = "";
      let hasSelfSalesRepReference = false;
      if (salesRepSourceId) {
        if (salesRepSourceId === normalizedSourceId) {
          hasSelfSalesRepReference = true;
        } else {
          const salesRepOutcome =
            await ensureCustomerMappedBySourceId(salesRepSourceId);
          salesRepresentativeCustomerId = toStringValue(
            salesRepOutcome?.customer?.id,
          ).trim();
          if (!salesRepresentativeCustomerId) {
            throw new Error(
              `Unable to resolve sales representative for customerid=${normalizedSourceId}`,
            );
          }
        }
      }

      const outcome = await createCustomerOnly(source, sourceById, {
        salesRepresentativeCustomerId,
      });
      const createdId = toStringValue(outcome?.customer?.id).trim();
      if (!createdId) {
        throw new Error(
          `Customer created without id for customerid=${normalizedSourceId}`,
        );
      }

      if (hasSelfSalesRepReference) {
        await setCustomerSalesRepresentativeReference({
          customerId: createdId,
          salesRepresentativeCustomerId: createdId,
        });
      }

      return trackSourceMapping({
        source,
        sourceId: normalizedSourceId,
        shopifyCustomerId: createdId,
        status: "created",
        customer: outcome.customer,
      });
    } finally {
      inProgress.delete(normalizedSourceId);
    }
  }

  for (let i = 0; i < customers.length; i++) {
    const source = customers[i];
    const index = i + 1;
    const sourceId = normalizeCustomerId(source);
    const label = buildLabel(source, sourceId, index);

    if (!sourceId) {
      results.failed++;
      const errors = [{ message: "Missing customerid" }];
      results.errors.push({
        customer: label,
        customerId: "",
        errors,
      });
      onProgress({
        index,
        total: customers.length,
        status: "failed",
        customer: source,
        errors,
      });
      continue;
    }

    try {
      const outcome = await ensureCustomerMappedBySourceId(sourceId);

      if (outcome.status === "created") {
        results.created++;
      }

      onProgress({
        index,
        total: customers.length,
        status: outcome.status || "created",
        customer: source,
        targetCustomer: outcome.customer,
      });

      if (pendingMappingRows.length >= DEFAULT_FLUSH_EVERY) {
        const rowsToAppend = pendingMappingRows.splice(
          0,
          pendingMappingRows.length,
        );
        await appendCustomerMappingRows(mappingPath, rowsToAppend);
      }
    } catch (err) {
      results.failed++;
      const wrappedErrors = Array.isArray(err?.userErrors)
        ? err.userErrors
        : [{ message: err?.message || String(err) }];

      results.errors.push({
        customer: label,
        customerId: sourceId,
        errors: wrappedErrors,
      });

      onProgress({
        index,
        total: customers.length,
        status: "failed",
        customer: source,
        errors: wrappedErrors,
      });
    }

    if (delayMs > 0) await sleep(delayMs);
  }

  if (pendingMappingRows.length > 0) {
    const rowsToAppend = pendingMappingRows.splice(
      0,
      pendingMappingRows.length,
    );
    await appendCustomerMappingRows(mappingPath, rowsToAppend);
  }

  return {
    created: results.created,
    failed: results.failed,
    errors: results.errors,
    total: results.total,
    mappingFile: mappingPath,
  };
}

module.exports = {
  migrateCustomers,
};
