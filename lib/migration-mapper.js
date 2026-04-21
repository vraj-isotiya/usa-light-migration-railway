/**
 * Volusion → Shopify Migration Mapper
 * Senior Shopify backend architecture - strict best practices
 * Uses productSet mutation for atomic product + variant + metafield + inventory
 */

const fs = require("fs");
const path = require("path");

let optionDataCache = null;
let categoryDataCache = null;
const ALLOWED_QTY_OPTION_CATEGORY_IDS = new Set(["93", "95", "98"]);

const ATTRIBUTE_PARENT_TO_KEY = {
  2172: "wattage",
  249103: "wattage",
  2457: "voltage",
  249104: "voltage",
  2529: "bulb_type",
  249107: "bulb_type",
  2505: "beam_spread",
  2540: "color",
  249105: "color",
  2593: "trim_inner_color",
  2602: "type",
  2743: "lens_type",
  7906: "trim_outer_color",
  11565: "color_temperature",
  19246: "base_type",
  20487: "systems",
  21374: "lumens",
  1881: "fluorescent_ballast_type",
  1883: "high_pressure_sodium_ballast",
  1925: "track_accessory_type",
  1927: "griplock_accessory",
  1954: "landscape_transformer_type",
  1955: "led_driver_type",
  2740: "led_color",
  26836: "dimmable",
  11765: "bulb_length",
  52342: "equivalent_wattage",
  13489: "led_driver_model",
  136801: "california_compliant",
  65686: "landscape_filter_type",
  92059: "extension_wand_length",
  99062: "led_compatible",
  110092: "commercial_wiring_product",
  111904: "clearance_item",
  119146: "bug_rating",
  127205: "clearance_category",
  141632: "battery_backup",
  141781: "battery_backup",
  142227: "fireproof",
  142376: "fireproof",
  163766: "commercial_use",
  249106: "glass_finish",
};

// ─────────────────────────────────────────────────────────────────────────────
// CATEGORY MAPPING (volusion_google_category → shopify_category GID)
// Load from category.json; pass categoryMap to buildProductSetInput
// ─────────────────────────────────────────────────────────────────────────────
function loadCategoryMap(categoryJsonPath) {
  try {
    const filePath =
      categoryJsonPath || path.join(process.cwd(), "category.json");
    const raw = fs.readFileSync(filePath, "utf8");
    const arr = JSON.parse(raw);
    const map = {};
    for (const item of arr) {
      const key = String(item.volusion_google_category || "").trim();
      const val = item.shopify_category
        ? String(item.shopify_category).trim()
        : null;
      if (key && val) map[key] = val;
    }
    return map;
  } catch {
    return {};
  }
}

function resolveShopifyCategory(googleCategory, categoryMap) {
  if (!googleCategory || !categoryMap || Object.keys(categoryMap).length === 0)
    return null;
  const key = String(googleCategory)
    .replace(/^["']|["']$/g, "")
    .trim();
  return categoryMap[key] || null;
}

// ─────────────────────────────────────────────────────────────────────────────
// PRICE LOGIC (Shopify best practice)
// If saleprice exists: price = saleprice, compare_at_price = productprice
// If no saleprice: price = productprice, compare_at_price = null
// ─────────────────────────────────────────────────────────────────────────────
function resolvePrice(source) {
  const hasSalePrice =
    source.saleprice != null &&
    source.saleprice !== "" &&
    parseFloat(source.saleprice) > 0;
  const regularPrice =
    source.productprice != null && source.productprice !== ""
      ? String(source.productprice)
      : null;

  if (hasSalePrice) {
    return {
      price: String(source.saleprice),
      compareAtPrice: regularPrice || null,
    };
  }
  return {
    price: regularPrice,
    compareAtPrice: null,
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// INVENTORY LOGIC
// stock = 0 AND backorders not allowed → inventory_policy = DENY
// backorders allowed (donotallowbackorders ≠ "Y") → inventory_policy = CONTINUE
// Per spec: donotallowbackorders "Y" → continue selling = true → DENY
// ─────────────────────────────────────────────────────────────────────────────
function resolveInventoryPolicy(source) {
  return source.donotallowbackorders === "Y" ? "DENY" : "CONTINUE";
}

// ─────────────────────────────────────────────────────────────────────────────
// STATUS LOGIC
// hideproduct = "Y" → DRAFT, else → ACTIVE
// Never use ARCHIVED unless discontinued
// ─────────────────────────────────────────────────────────────────────────────
function resolveStatus(source) {
  return source.hideproduct === "Y" ? "DRAFT" : "ACTIVE";
}

function toNumber(value, fallback = 0) {
  const n = Number.parseFloat(String(value ?? "").trim());
  return Number.isFinite(n) ? n : fallback;
}

function toInt(value) {
  const n = Number.parseInt(String(value ?? "").trim(), 10);
  return Number.isFinite(n) ? n : null;
}

function toStrictInt(value) {
  const raw = String(value ?? "").trim();
  if (!/^\d+$/.test(raw)) return null;
  const n = Number.parseInt(raw, 10);
  return Number.isFinite(n) ? n : null;
}

function money(value) {
  return toNumber(value, 0).toFixed(2);
}

function normalizeProductManufacturer(value) {
  const raw = String(value || "").trim();
  if (!raw) return "";
  const key = raw
    .toLowerCase()
    .replace(/&/g, "and")
    .replace(/[^a-z0-9]+/g, "");
  const aliases = {
    usalightelectric: "USA Light & Electric",
    usanamebrand: "USA Light & Electric",
    usalightandelectric: "USA Light & Electric",
    usalight: "USA Light & Electric",
    eikolighting: "EiKO Lighting",
    eiko: "EiKO Lighting",
    elitelighting: "Elite Lighting",
    elite: "Elite Lighting",
    elcolighting: "Elco Lighting",
    elco: "Elco Lighting",
    fulham: "Fulham",
    fullham: "Fulham",
    rablighting: "RAB Lighting",
    rab: "RAB Lighting",
  };
  return aliases[key] || raw;
}

function normalizeSourceProductName(value) {
  return String(value || "")
    .trim()
    .replace(/\s+/g, " ");
}

function normalizeSourceProductSku(source) {
  return String(source?.vendor_partno || "").trim();
}

function buildSourceProductIdentity(source) {
  return {
    productcode: String(source?.productcode || "").trim(),
    name: normalizeSourceProductName(source?.productname),
    sku: normalizeSourceProductSku(source),
  };
}

function loadOptionData(optionsPath, categoriesPath) {
  if (optionDataCache) return optionDataCache;

  const resolvedOptionsPath =
    optionsPath || path.join(process.cwd(), "product_option.json");
  const resolvedCategoriesPath =
    categoriesPath ||
    path.join(process.cwd(), "product_option_categories.json");

  let options = [];
  let categories = [];

  try {
    options = JSON.parse(fs.readFileSync(resolvedOptionsPath, "utf8"));
  } catch {
    options = [];
  }

  try {
    categories = JSON.parse(fs.readFileSync(resolvedCategoriesPath, "utf8"));
  } catch {
    categories = [];
  }

  const optionById = {};
  for (const o of options) {
    const id = String(o.id || "").trim();
    if (!id) continue;
    optionById[id] = o;
  }

  const categoryById = {};
  for (const c of categories) {
    const id = String(c.id || "").trim();
    if (!id) continue;
    categoryById[id] = c;
  }

  optionDataCache = { optionById, categoryById };
  return optionDataCache;
}

function loadCategoryData(categoriesPath) {
  if (categoryDataCache) return categoryDataCache;

  const resolvedPath =
    categoriesPath || path.join(process.cwd(), "categories.json");
  let categories = [];
  try {
    categories = JSON.parse(fs.readFileSync(resolvedPath, "utf8"));
  } catch {
    categories = [];
  }

  const byId = {};
  for (const c of categories) {
    const id = String(c?.id || c?.categoryid || "").trim();
    if (!id) continue;
    byId[id] = c;
  }

  categoryDataCache = { byId };
  return categoryDataCache;
}

function parseCategoryIds(source) {
  const raw = String(source?.categoryids || "").trim();
  if (!raw) return [];
  return [
    ...new Set(
      raw
        .split(",")
        .map((id) => id.trim())
        .filter(Boolean),
    ),
  ];
}

function safeParseJsonArray(value) {
  if (!value || typeof value !== "string") return [];
  try {
    const parsed = JSON.parse(value);
    if (Array.isArray(parsed))
      return parsed.map((x) => String(x).trim()).filter(Boolean);
  } catch {
    return [];
  }
  return [];
}

function sanitizeShopifyTag(value) {
  const raw = String(value || "").trim();
  if (!raw) return "";

  // Shopify treats commas as tag separators; remove numeric thousand separators
  // and normalize any remaining commas to spaces.
  return raw
    .replace(/(\d),(\d)/g, "$1$2")
    .replace(/,/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function mergeVariantMetafields(existing = [], incoming = []) {
  const map = new Map();
  for (const mf of existing) {
    if (!mf?.namespace || !mf?.key) continue;
    map.set(`${mf.namespace}:${mf.key}`, { ...mf });
  }

  for (const mf of incoming) {
    if (!mf?.namespace || !mf?.key) continue;
    const id = `${mf.namespace}:${mf.key}`;
    const current = map.get(id);
    if (!current) {
      map.set(id, { ...mf });
      continue;
    }

    if (
      mf.namespace === "variant" &&
      (mf.type === "list.single_line_text_field" ||
        current.type === "list.single_line_text_field")
    ) {
      const merged = [
        ...new Set([
          ...safeParseJsonArray(current.value),
          ...safeParseJsonArray(mf.value),
        ]),
      ];
      map.set(id, {
        ...current,
        type: "list.single_line_text_field",
        value: JSON.stringify(merged),
      });
    }
  }

  return [...map.values()];
}

function buildVariantCategoryMetafields(source) {
  const categoryIds = parseCategoryIds(source);
  if (categoryIds.length === 0) return [];

  const { byId } = loadCategoryData();
  const collectionNames = new Set();
  const attributeValues = new Map();
  const chainCache = new Map();

  function getCategoryChain(categoryId) {
    const key = String(categoryId || "").trim();
    if (!key) return [];
    if (chainCache.has(key)) return chainCache.get(key);

    const chain = [];
    const visited = new Set();
    let currentId = key;
    while (currentId && currentId !== "0") {
      if (visited.has(currentId)) {
        console.warn(
          `[CategoryHierarchyCycle] product=${source.productname || ""} code=${source.productcode || ""} categoryId=${key} cycleAt=${currentId}`,
        );
        break;
      }
      visited.add(currentId);
      const current = byId[currentId];
      if (!current) break;
      chain.push({ id: currentId, category: current });
      const parentId = String(current?.parentid || "").trim();
      if (!parentId || parentId === "0") break;
      currentId = parentId;
    }

    const rootId = String(byId[key]?.rootid || "").trim();
    if (rootId && !visited.has(rootId)) {
      const rootCategory = byId[rootId];
      if (rootCategory) chain.push({ id: rootId, category: rootCategory });
    }

    chainCache.set(key, chain);
    return chain;
  }

  for (const categoryId of categoryIds) {
    const category = byId[categoryId];
    if (!category) continue;

    const categoryName = String(category?.categoryname || "").trim();
    const parentId = String(category?.parentid || "").trim();

    const mappedKey = ATTRIBUTE_PARENT_TO_KEY[parentId];
    if (mappedKey && categoryName) {
      let values = attributeValues.get(mappedKey);
      if (!values) {
        values = new Set();
        attributeValues.set(mappedKey, values);
      }
      const beforeSize = values.size;
      values.add(categoryName);
      // if (beforeSize > 0 && values.size > beforeSize) {
      //   console.warn(
      //     `[CategoryMetafieldOverride] product=${source.productname || ""} code=${source.productcode || ""} key=variant.${mappedKey} added="${categoryName}" all="${[...values].join("|")}"`,
      //   );
      // }
    }

    // Include the full ancestor chain (including the leaf category) as collection tags.
    const chain = getCategoryChain(categoryId);
    chain.forEach((node) => {
      const name = String(node?.category?.categoryname || "").trim();
      if (!name) return;
      collectionNames.add(name);
    });
  }

  const metafields = [];
  if (collectionNames.size > 0) {
    metafields.push({
      namespace: "variant",
      key: "collection_names",
      type: "list.single_line_text_field",
      value: JSON.stringify([...collectionNames]),
    });
  }

  for (const [key, values] of attributeValues.entries()) {
    metafields.push({
      namespace: "variant",
      key,
      type: "list.single_line_text_field",
      value: JSON.stringify([...values]),
    });
  }

  return metafields;
}

function parseCsvIds(value) {
  const raw = String(value || "").trim();
  if (!raw) return [];
  return [
    ...new Set(
      raw
        .split(",")
        .map((id) => id.trim())
        .filter(Boolean),
    ),
  ];
}

function parseOptionIds(source) {
  return parseCsvIds(source?.optionids);
}

function parseSelectedOptionIds(source) {
  return [
    ...new Set([
      ...parseCsvIds(source?.selectedoptionids),
      ...parseCsvIds(source?.optionids),
    ]),
  ];
}

function normalizeSearchText(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim()
    .replace(/\s+/g, " ");
}

function escapeRegExp(value) {
  return String(value || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function textIncludesOptionValue(text, optionValue) {
  const source = String(text || "");
  const candidate = String(optionValue || "").trim();
  if (!source || !candidate) return false;

  const escaped = escapeRegExp(candidate).replace(/\s+/g, "\\s+");
  const strictPattern = new RegExp(
    `(^|[^A-Za-z0-9])${escaped}($|[^A-Za-z0-9])`,
    "i",
  );
  if (strictPattern.test(source)) return true;

  const normalizedSource = normalizeSearchText(source);
  const normalizedCandidate = normalizeSearchText(candidate);
  if (!normalizedSource || !normalizedCandidate) return false;

  // Prevent false positives for very short values such as "M" or "S".
  if (normalizedCandidate.length <= 2) {
    return normalizedSource.split(" ").includes(normalizedCandidate);
  }

  return normalizedSource.includes(normalizedCandidate);
}

function dedupeOptionRowsByValue(rows = []) {
  const seen = new Set();
  const unique = [];
  for (const row of rows) {
    const value = String(row?.option?.optionsdesc || "")
      .trim()
      .toLowerCase();
    if (!value || seen.has(value)) continue;
    seen.add(value);
    unique.push(row);
  }
  return unique;
}

function buildOptionCategoryGroups(optionIds, optionById, categoryById) {
  const selectedOptions = optionIds
    .map((id) => optionById[id])
    .filter(Boolean)
    .filter((o) => String(o.novalue || "N").trim() !== "Y");

  const grouped = new Map();
  for (const opt of selectedOptions) {
    if (String(opt.isproductquantity || "").trim() === "Y") continue;
    const isChooseQtyValue =
      isAllowedQtyOptionCategory(opt) &&
      Number.isInteger(toStrictInt(opt.optionsdesc));
    if (isChooseQtyValue) continue;

    const categoryId = String(opt.optioncatid || "").trim();
    if (!categoryId) continue;
    const category = categoryById[categoryId];
    if (!category) continue;

    const row = { option: opt, category, categoryId };
    if (!grouped.has(categoryId)) grouped.set(categoryId, []);
    grouped.get(categoryId).push(row);
  }

  return [...grouped.entries()]
    .map(([categoryId, rows]) => {
      const category = rows[0]?.category;
      return {
        categoryId,
        category,
        rows: dedupeOptionRowsByValue(
          rows.sort(
            (a, b) =>
              (toInt(a.option.arrangeoptionsby) ?? 9999) -
              (toInt(b.option.arrangeoptionsby) ?? 9999),
          ),
        ),
        categoryOrder: toInt(category?.arrangeoptioncategoriesby) ?? 9999,
      };
    })
    .filter((group) => group.rows.length > 0)
    .sort(
      (a, b) =>
        a.categoryOrder - b.categoryOrder ||
        a.categoryId.localeCompare(b.categoryId),
    );
}

function resolveVariantFileFromSource(source, fallbackAlt = "") {
  const file =
    source?.variantImageFile ||
    (Array.isArray(source?.productImageFiles)
      ? source.productImageFiles[0]
      : null);

  if (!file || (!file.id && !file.url)) return null;

  const alt =
    source?.photo_alttext || source?.productname || fallbackAlt || null;
  if (file.id) {
    return {
      id: file.id,
      alt,
    };
  }

  return {
    originalSource: String(file.url),
    contentType: "IMAGE",
    alt,
  };
}

function buildVariantFromSource(baseVariant, source, locationId) {
  const variant = JSON.parse(JSON.stringify(baseVariant));
  const { price, compareAtPrice } = resolvePrice(source);
  const inventoryPolicy = resolveInventoryPolicy(source);
  const taxable = source.taxableproduct === "Y";
  const qty = parseInt(source.stockstatus || 0, 10) || 0;
  const weightVal = source.productweight
    ? parseFloat(source.productweight)
    : null;

  // Child variants need a unique SKU per option combination.
  variant.sku = source.productcode || source.vendor_partno || null;
  variant.barcode = source.upc_code || null;
  variant.price = price || "0.00";
  variant.compareAtPrice = compareAtPrice || null;
  variant.taxable = taxable;
  variant.inventoryPolicy = inventoryPolicy;
  variant.inventoryItem = {
    tracked: true,
    cost: source.vendor_price ? String(source.vendor_price) : null,
    measurement:
      weightVal != null && weightVal > 0
        ? { weight: { value: weightVal, unit: "POUNDS" } }
        : null,
  };
  variant.inventoryQuantities =
    locationId && qty >= 0
      ? [{ locationId, name: "available", quantity: qty }]
      : undefined;

  if (!variant.sku) delete variant.sku;
  if (!variant.barcode) delete variant.barcode;
  if (!variant.compareAtPrice) delete variant.compareAtPrice;
  if (!variant.inventoryItem.cost) delete variant.inventoryItem.cost;
  if (!variant.inventoryItem.measurement)
    delete variant.inventoryItem.measurement;
  if (!variant.inventoryQuantities) delete variant.inventoryQuantities;

  return variant;
}

function optionNameFromCategory(category) {
  const heading = String(category?.headinggroup || "").trim();
  if (heading) return heading;
  return String(category?.optioncategoriesdesc || "").trim() || "Option";
}

function cartesianProduct(arrays) {
  if (!arrays.length) return [];
  return arrays.reduce(
    (acc, current) => acc.flatMap((a) => current.map((c) => a.concat([c]))),
    [[]],
  );
}

function buildVariantMetafieldsFromSelection(selection) {
  const metafields = [];

  const aboutValues = [
    ...new Set(
      selection
        .map((x) => String(x.category?.aboutoptioncategories || "").trim())
        .filter(Boolean),
    ),
  ];
  if (aboutValues.length > 0) {
    metafields.push({
      namespace: "variant",
      key: "about_option",
      type: "multi_line_text_field",
      value: aboutValues.join("\n"),
    });
  }

  const displayValues = [
    ...new Set(
      selection
        .map((x) => String(x.category?.displaytype || "").trim())
        .filter(Boolean),
    ),
  ];
  if (displayValues.length > 0) {
    metafields.push({
      namespace: "variant",
      key: "display_type",
      type: "single_line_text_field",
      value: displayValues.join("|"),
    });
  }

  return metafields;
}

function buildChooseQuantityRows(selectedOptions = []) {
  const numericQtyOptions = selectedOptions
    .filter((o) => isAllowedQtyOptionCategory(o))
    .filter((o) => Number.isInteger(toStrictInt(o.optionsdesc)))
    .sort((a, b) => {
      const orderDiff =
        (toInt(a.arrangeoptionsby) ?? 9999) -
        (toInt(b.arrangeoptionsby) ?? 9999);
      if (orderDiff !== 0) return orderDiff;
      return (toInt(a.optionsdesc) ?? 0) - (toInt(b.optionsdesc) ?? 0);
    });

  const byQty = new Map();
  for (const opt of numericQtyOptions) {
    const qty = toStrictInt(opt.optionsdesc);
    if (!Number.isInteger(qty)) continue;
    const key = String(qty);
    if (byQty.has(key)) continue;
    byQty.set(key, {
      option: {
        ...opt,
        optionsdesc: key,
      },
      category: {
        headinggroup: "Choose Quantity",
        optioncategoriesdesc: "Choose Quantity",
        displaytype: "DROPDOWN",
        aboutoptioncategories: "",
        arrangeoptioncategoriesby: "9999",
      },
      categoryId: "__choose_quantity",
    });
  }

  return [...byQty.values()];
}

function isAllowedQtyOptionCategory(option) {
  const categoryId = String(option?.optioncatid || "").trim();
  return ALLOWED_QTY_OPTION_CATEGORY_IDS.has(categoryId);
}

function resolveAllowedQtyFromChooseQuantityRows(rows = []) {
  const seen = new Set();
  const allowed = [];
  for (const row of rows) {
    const qty = toStrictInt(row?.option?.optionsdesc);
    if (!Number.isInteger(qty) || qty <= 0) continue;
    const key = String(qty);
    if (seen.has(key)) continue;
    seen.add(key);
    allowed.push(qty);
  }
  return allowed;
}

/** Variant line total when Choose Quantity is present: (base + non-qty pricediffs) × qty. */
function computeChooseQuantityVariantPrice(
  selection,
  basePrice,
  baseCompareAtPrice,
) {
  let qty = 1;
  let priceDiffNonQty = 0;
  for (const row of selection) {
    if (String(row?.categoryId || "").trim() === "__choose_quantity") {
      const q = toInt(row?.option?.optionsdesc);
      if (Number.isInteger(q) && q > 0) qty = q;
      continue;
    }
    priceDiffNonQty += toNumber(row?.option?.pricediff, 0);
  }
  const unitPrice = basePrice + priceDiffNonQty;
  const price = money(unitPrice * qty);
  let compareAtPrice = null;
  if (baseCompareAtPrice != null) {
    const compareUnit = baseCompareAtPrice + priceDiffNonQty;
    compareAtPrice = money(compareUnit * qty);
  }
  return { price, compareAtPrice };
}

function resolveOptionRowForChild(group, child, selectedOptionIdsSet) {
  const selectedRows = group.rows.filter((row) =>
    selectedOptionIdsSet.has(String(row?.option?.id || "").trim()),
  );
  if (selectedRows.length === 1) return selectedRows[0];
  if (selectedRows.length > 1) {
    return selectedRows.sort(
      (a, b) =>
        String(b?.option?.optionsdesc || "").trim().length -
          String(a?.option?.optionsdesc || "").trim().length ||
        (toInt(a?.option?.arrangeoptionsby) ?? 9999) -
          (toInt(b?.option?.arrangeoptionsby) ?? 9999),
    )[0];
  }

  const searchText = [
    child?.productname,
    child?.productnameshort,
    child?.productcode,
    child?.vendor_partno,
  ]
    .map((x) => String(x || "").trim())
    .filter(Boolean)
    .join(" | ");

  const textMatchedRows = group.rows.filter((row) =>
    textIncludesOptionValue(searchText, row?.option?.optionsdesc),
  );
  if (textMatchedRows.length === 1) return textMatchedRows[0];
  if (textMatchedRows.length > 1) {
    return textMatchedRows.sort(
      (a, b) =>
        String(b?.option?.optionsdesc || "").trim().length -
          String(a?.option?.optionsdesc || "").trim().length ||
        (toInt(a?.option?.arrangeoptionsby) ?? 9999) -
          (toInt(b?.option?.arrangeoptionsby) ?? 9999),
    )[0];
  }

  if (group.rows.length === 1) return group.rows[0];
  return null;
}

function selectionKey(selection = []) {
  return selection
    .map((row) => String(row?.option?.id || "").trim())
    .filter(Boolean)
    .join("|");
}

function selectionSummary(selection = [], productOptions = []) {
  return selection.map((row, idx) => ({
    option_id: String(row?.option?.id || "").trim(),
    option_name:
      String(productOptions[idx]?.name || "").trim() ||
      optionNameFromCategory(row?.category),
    option_value: String(row?.option?.optionsdesc || "").trim(),
  }));
}

function buildPlaceholderVariantFromSelection(
  baseVariant,
  selection,
  productOptions,
) {
  const variant = JSON.parse(JSON.stringify(baseVariant));
  variant.optionValues = selection.map((row, optionIdx) => ({
    optionName: productOptions[optionIdx].name,
    name: String(row.option.optionsdesc || "").trim() || "Option",
  }));

  // No source child row: keep structural variant but avoid assigning child-specific values.
  delete variant.sku;
  delete variant.barcode;
  delete variant.inventoryQuantities;
  if (variant.inventoryItem?.cost) delete variant.inventoryItem.cost;

  variant.metafields = mergeVariantMetafields(variant.metafields || [], [
    {
      namespace: "variant",
      key: "source_productcode_missing",
      type: "boolean",
      value: "true",
    },
  ]);

  return variant;
}

function buildChildOptionVariantModel(
  parentSource,
  childProducts,
  locationId,
  baseVariant,
) {
  const optionIds = parseOptionIds(parentSource);
  if (optionIds.length === 0) return null;

  const { optionById, categoryById } = loadOptionData();
  const categories = buildOptionCategoryGroups(
    optionIds,
    optionById,
    categoryById,
  );
  if (categories.length === 0) return null;

  if (categories.length > 3) {
    console.warn(
      `[VariantLimit] Product ${parentSource.productcode || parentSource.productname || "unknown"} has ${categories.length} option categories (>3). Skipping child-variant generation.`,
    );
    return {
      source: "child-products",
      productOptions: [],
      variants: [],
      overflow: true,
      skippedChildren: childProducts.map((child) => ({
        productcode: String(child?.productcode || "").trim(),
        reason: "too-many-option-categories",
      })),
    };
  }

  const productOptions = categories.map((group, idx) => ({
    name: optionNameFromCategory(group.category),
    position: idx + 1,
    values: group.rows.map((r) => ({
      name: String(r.option.optionsdesc || "").trim() || "Option",
    })),
  }));

  const parentCode = String(parentSource?.productcode || "").trim();
  const normalizedChildren = (
    Array.isArray(childProducts) ? childProducts : []
  ).filter(
    (child) => String(child?.ischildofproductcode || "").trim() === parentCode,
  );

  const combos = cartesianProduct(categories.map((c) => c.rows));
  if (combos.length === 0) {
    return {
      source: "child-products",
      productOptions,
      variants: [],
      overflow: false,
      skippedChildren: [],
      missingMappings: [],
      missingOptionIds: [],
    };
  }

  const variantBySelectionKey = new Map();
  const usedOptionIds = new Set();
  const skippedChildren = [];
  const missingMappings = [];

  for (const child of normalizedChildren) {
    const selectedOptionIdsSet = new Set(parseSelectedOptionIds(child));
    const selection = [];

    let unresolved = false;
    for (const group of categories) {
      const matchedRow = resolveOptionRowForChild(
        group,
        child,
        selectedOptionIdsSet,
      );
      if (!matchedRow) {
        unresolved = true;
        break;
      }
      selection.push(matchedRow);
    }

    if (unresolved || selection.length !== categories.length) {
      skippedChildren.push({
        productcode: String(child?.productcode || "").trim(),
        reason: "could-not-resolve-option-values-from-child",
      });
      continue;
    }

    const variant = buildVariantFromSource(baseVariant, child, locationId);
    variant.optionValues = selection.map((row, optionIdx) => ({
      optionName: productOptions[optionIdx].name,
      name: String(row.option.optionsdesc || "").trim() || "Option",
    }));

    const variantMetafields = mergeVariantMetafields(
      buildVariantMetafieldsFromSelection(selection),
      buildVariantCategoryMetafields(child),
    );
    const sourceProductCode = String(child?.productcode || "").trim();
    if (sourceProductCode) {
      variantMetafields.push({
        namespace: "variant",
        key: "source_productcode",
        type: "single_line_text_field",
        value: sourceProductCode,
      });
    }
    if (variantMetafields.length > 0) {
      variant.metafields = variantMetafields;
    }

    const variantFile = resolveVariantFileFromSource(
      child,
      parentSource?.productname,
    );
    if (variantFile) variant.file = variantFile;

    const key = selectionKey(selection);
    if (!key) {
      skippedChildren.push({
        productcode: String(child?.productcode || "").trim(),
        reason: "resolved-selection-key-empty",
      });
      continue;
    }

    if (variantBySelectionKey.has(key)) {
      skippedChildren.push({
        productcode: String(child?.productcode || "").trim(),
        reason: "duplicate-selection-for-combination",
      });
      continue;
    }

    for (const row of selection) {
      const id = String(row?.option?.id || "").trim();
      if (id) usedOptionIds.add(id);
    }
    variantBySelectionKey.set(key, variant);
  }

  const variants = [];
  for (const selection of combos) {
    const key = selectionKey(selection);
    const existing = key ? variantBySelectionKey.get(key) : null;
    if (existing) {
      variants.push(existing);
      continue;
    }

    const placeholder = buildPlaceholderVariantFromSelection(
      baseVariant,
      selection,
      productOptions,
    );
    variants.push(placeholder);
    missingMappings.push({
      parent_productcode: parentCode,
      combination_key: key,
      combination: selectionSummary(selection, productOptions),
      reason: "no-child-product-found-for-combination",
    });
  }

  variants.forEach((variant, idx) => {
    variant.position = idx + 1;
  });

  const relevantOptionIds = new Set(
    categories.flatMap((category) =>
      category.rows
        .map((row) => String(row?.option?.id || "").trim())
        .filter(Boolean),
    ),
  );
  const missingOptionIds = [...relevantOptionIds]
    .filter((id) => !usedOptionIds.has(id))
    .map((id) => ({
      option_id: id,
      option_name: optionNameFromCategory(
        categoryById[optionById[id]?.optioncatid],
      ),
      option_value: String(optionById[id]?.optionsdesc || "").trim(),
      reason: "present-in-parent-optionids-but-not-mapped-from-children",
    }))
    .filter((x) => x.option_id && x.option_value);

  if (normalizedChildren.length === 0) {
    missingMappings.push({
      parent_productcode: parentCode,
      reason: "no-child-products-found",
    });
  }

  return {
    source: "child-products",
    productOptions,
    variants,
    overflow: false,
    skippedChildren,
    missingMappings,
    missingOptionIds,
  };
}

function buildOptionVariantModel(source, basePrice, baseCompareAtPrice) {
  const optionIds = parseOptionIds(source);
  if (optionIds.length === 0) return null;

  const { optionById, categoryById } = loadOptionData();
  const selectedOptions = optionIds
    .map((id) => optionById[id])
    .filter(Boolean)
    .filter((o) => String(o.novalue || "N").trim() !== "Y");

  if (selectedOptions.length === 0) return null;

  const fallbackCategoryRows = selectedOptions
    .map((o) => {
      const categoryId = String(o.optioncatid || "").trim();
      if (!categoryId) return null;
      const category = categoryById[categoryId];
      if (!category) return null;
      return { option: o, category, categoryId };
    })
    .filter(Boolean);

  const fallbackAboutValues = [
    ...new Set(
      fallbackCategoryRows
        .map((x) => String(x.category?.aboutoptioncategories || "").trim())
        .filter(Boolean),
    ),
  ];

  const fallbackDisplayValues = [
    ...new Set(
      fallbackCategoryRows
        .map((x) => String(x.category?.displaytype || "").trim())
        .filter(Boolean),
    ),
  ];

  const chooseQuantityRows = buildChooseQuantityRows(
    selectedOptions.filter(isAllowedQtyOptionCategory),
  );
  const allowedQty = resolveAllowedQtyFromChooseQuantityRows(chooseQuantityRows);

  const variantEligibleOptions = selectedOptions.filter(
    (o) => {
      const isMarkedAsProductQty =
        String(o.isproductquantity || "").trim() === "Y";
      if (isMarkedAsProductQty) return false;
      const isChooseQtyCandidate =
        isAllowedQtyOptionCategory(o) &&
        Number.isInteger(toStrictInt(o.optionsdesc));
      return !isChooseQtyCandidate;
    },
  );

  if (variantEligibleOptions.length === 0 && chooseQuantityRows.length === 0) {
    return {
      productOptions: [],
      variants: [],
      quantityBreaks: [],
      allowedQty,
      fallbackAboutValues,
      fallbackDisplayValues,
      overflow: false,
      keepBaseSku: false,
    };
  }

  const grouped = new Map();
  for (const opt of variantEligibleOptions) {
    const categoryId = String(opt.optioncatid || "").trim();
    if (!categoryId) continue;
    const category = categoryById[categoryId];
    if (!category) continue;

    const row = { option: opt, category, categoryId };
    if (!grouped.has(categoryId)) grouped.set(categoryId, []);
    grouped.get(categoryId).push(row);
  }

  const categories = [...grouped.entries()]
    .map(([categoryId, rows]) => {
      const category = rows[0].category;
      return {
        categoryId,
        category,
        rows: rows.sort(
          (a, b) =>
            toInt(a.option.arrangeoptionsby) - toInt(b.option.arrangeoptionsby),
        ),
        categoryOrder: toInt(category.arrangeoptioncategoriesby) ?? 9999,
      };
    })
    .sort(
      (a, b) =>
        a.categoryOrder - b.categoryOrder ||
        a.categoryId.localeCompare(b.categoryId),
    );

  if (chooseQuantityRows.length > 0) {
    categories.push({
      categoryId: "__choose_quantity",
      category: chooseQuantityRows[0].category,
      rows: chooseQuantityRows,
      categoryOrder: 9999,
    });
  }

  if (categories.length === 0) {
    return {
      productOptions: [],
      variants: [],
      quantityBreaks: [],
      allowedQty,
      fallbackAboutValues,
      fallbackDisplayValues,
      overflow: false,
      keepBaseSku: false,
    };
  }

  if (categories.length > 3) {
    const detail = categories.map((c) => ({
      categoryId: c.categoryId,
      optionName: optionNameFromCategory(c.category),
      arrangeoptioncategoriesby: c.categoryOrder,
      values: c.rows.map((r) => ({
        id: r.option.id,
        optionsdesc: r.option.optionsdesc,
        arrangeoptionsby: r.option.arrangeoptionsby,
        pricediff: r.option.pricediff,
      })),
    }));
    console.warn(
      `[VariantLimit] Product ${source.productcode || source.productname || "unknown"} has ${categories.length} option categories (>3). Skipping variant generation.`,
    );
    console.warn(`[VariantLimitDetail] ${JSON.stringify(detail)}`);
    return {
      productOptions: [],
      variants: [],
      quantityBreaks: [],
      allowedQty,
      fallbackAboutValues,
      fallbackDisplayValues,
      overflow: true,
      keepBaseSku: chooseQuantityRows.length > 0,
    };
  }

  const productOptions = categories.map((group, idx) => ({
    name: optionNameFromCategory(group.category),
    position: idx + 1,
    values: group.rows.map((r) => ({
      name: String(r.option.optionsdesc || "").trim() || "Option",
    })),
  }));

  const combos = cartesianProduct(categories.map((c) => c.rows));
  if (combos.length === 0) return null;

  const sortedCombos = combos.sort((a, b) => {
    for (let i = 0; i < a.length; i++) {
      const x = toInt(a[i].option.arrangeoptionsby) ?? 9999;
      const y = toInt(b[i].option.arrangeoptionsby) ?? 9999;
      if (x !== y) return x - y;
    }
    return 0;
  });

  const hasChooseQuantityCategory = categories.some(
    (c) => String(c?.categoryId || "").trim() === "__choose_quantity",
  );

  const variants = sortedCombos.map((selection, idx) => {
    let price;
    let compareAtPrice;
    if (hasChooseQuantityCategory) {
      const computed = computeChooseQuantityVariantPrice(
        selection,
        basePrice,
        baseCompareAtPrice,
      );
      price = computed.price;
      compareAtPrice = computed.compareAtPrice;
    } else {
      const priceDiff = selection.reduce(
        (sum, row) => sum + toNumber(row.option.pricediff, 0),
        0,
      );
      price = money(basePrice + priceDiff);
      compareAtPrice =
        baseCompareAtPrice != null
          ? money(baseCompareAtPrice + priceDiff)
          : null;
    }

    return {
      position: idx + 1,
      optionValues: selection.map((row, optionIdx) => ({
        optionName: productOptions[optionIdx].name,
        name: String(row.option.optionsdesc || "").trim() || "Option",
      })),
      price,
      compareAtPrice,
      metafields: buildVariantMetafieldsFromSelection(selection),
    };
  });

  return {
    productOptions,
    variants,
    quantityBreaks: [],
    allowedQty,
    fallbackAboutValues,
    fallbackDisplayValues,
    overflow: false,
    keepBaseSku: chooseQuantityRows.length > 0,
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// METAFIELDS
// namespace: product
// All keys per spec; omit empty values
// metafieldsSet creates definitions if not present
// ─────────────────────────────────────────────────────────────────────────────
function buildMetafields(source) {
  const metafields = [];

  const push = (key, type, value) => {
    if (value !== undefined && value !== null && String(value).trim() !== "") {
      metafields.push({
        namespace: "product",
        key,
        type,
        value: String(value),
      });
    }
  };

  push("availability", "single_line_text_field", source.availability);
  push("custom_field_1", "single_line_text_field", source.customfield1);
  push("custom_field_2", "single_line_text_field", source.customfield2);
  push("custom_field_3", "single_line_text_field", source.customfield3);
  push("custom_field_4", "single_line_text_field", source.customfield4);
  push("custom_field_5", "single_line_text_field", source.customfield5);

  push("product_popularity", "number_integer", source.productpopularity);
  push("search_keywords", "multi_line_text_field", source.productkeywords);
  push("short_name", "single_line_text_field", source.productnameshort);

  if (
    source.freeshippingitem !== undefined &&
    source.freeshippingitem !== null
  ) {
    metafields.push({
      namespace: "product",
      key: "free_shipping",
      type: "boolean",
      value: source.freeshippingitem === "Y" ? "true" : "false",
    });
  }

  push("minimum_qty", "number_integer", source.minqty);
  push("stock_low_qty_alarm", "number_integer", source.stocklowqtyalarm);

  //push("download_file", "url", source.downloadfile);

  const allowedQty = (() => {
    const optionIds = parseOptionIds(source);
    if (optionIds.length === 0) return [];
    const { optionById } = loadOptionData();
    const selectedOptions = optionIds
      .map((id) => optionById[id])
      .filter(Boolean)
      .filter((o) => String(o.novalue || "N").trim() !== "Y");
    if (selectedOptions.length === 0) return [];
    const chooseQuantityRows = buildChooseQuantityRows(
      selectedOptions.filter(isAllowedQtyOptionCategory),
    );
    return resolveAllowedQtyFromChooseQuantityRows(chooseQuantityRows);
  })();
  if (allowedQty.length > 0) {
    metafields.push({
      namespace: "product",
      key: "allowed_qty",
      type: "list.number_integer",
      value: JSON.stringify(allowedQty),
    });
  }

  push("price_subtext", "single_line_text_field", source.price_subtext_short);
  push(
    "short_description",
    "multi_line_text_field",
    source.productdescriptionshort,
  );
  push("meta_keywords", "multi_line_text_field", source.metatag_keywords);
  push(
    "meta_override",
    "multi_line_text_field",
    source.custom_metatags_override,
  );

  // productfeatures → product.product_features
  push("product_features", "multi_line_text_field", source.productfeatures);

  // techspecs → product.tech_specs
  push("tech_specs", "multi_line_text_field", source.techspecs);

  // mpn → product.mpn (single_line_text_field)
  push("mpn", "single_line_text_field", source.mpn);

  // productdescription_abovepricing → product.description_above_price
  push(
    "description_above_price",
    "multi_line_text_field",
    source.productdescription_abovepricing,
  );

  return metafields;
}

// ─────────────────────────────────────────────────────────────────────────────
// BUILD PRODUCT SET INPUT (productSet mutation)
// Single atomic call: product + variant + metafields + images + inventory
// options: { categoryMap } - map from volusion_google_category to shopify_category GID
// ─────────────────────────────────────────────────────────────────────────────
function buildProductSetInput(source, locationId, options = {}) {
  const sourceIdentity = buildSourceProductIdentity(source);
  const categoryMap =
    options.categoryMap || loadCategoryMap(options.categoryJsonPath);
  const { price, compareAtPrice } = resolvePrice(source);
  const inventoryPolicy = resolveInventoryPolicy(source);
  const status = resolveStatus(source);
  const taxable = source.taxableproduct === "Y";
  const qty = parseInt(source.stockstatus || 0, 10) || 0;
  const weightVal = source.productweight
    ? parseFloat(source.productweight)
    : null;

  const baseVariant = {
    optionValues: [{ optionName: "Title", name: "Default Title" }],
    sku: sourceIdentity.sku || null,
    barcode: source.upc_code || null,
    price: price || "0.00",
    compareAtPrice: compareAtPrice || null,
    taxable,
    inventoryPolicy,
    inventoryItem: {
      tracked: true,
      cost: source.vendor_price ? String(source.vendor_price) : null,
      measurement:
        weightVal != null && weightVal > 0
          ? { weight: { value: weightVal, unit: "POUNDS" } }
          : null,
    },
    inventoryQuantities:
      locationId && qty >= 0
        ? [{ locationId, name: "available", quantity: qty }]
        : undefined,
  };

  // Remove null/undefined for GraphQL
  if (!baseVariant.sku) delete baseVariant.sku;
  if (!baseVariant.barcode) delete baseVariant.barcode;
  if (!baseVariant.compareAtPrice) delete baseVariant.compareAtPrice;
  if (!baseVariant.inventoryItem.cost) delete baseVariant.inventoryItem.cost;
  if (!baseVariant.inventoryItem.measurement)
    delete baseVariant.inventoryItem.measurement;
  if (!baseVariant.inventoryQuantities) delete baseVariant.inventoryQuantities;

  const childProducts = Array.isArray(options.childProducts)
    ? options.childProducts
    : [];
  const hasOptionInventoryControl =
    String(source.enableoptions_inventorycontrol || "")
      .trim()
      .toUpperCase() === "Y";

  let optionVariantModel = null;
  if (hasOptionInventoryControl && parseOptionIds(source).length > 0) {
    optionVariantModel = buildChildOptionVariantModel(
      source,
      childProducts,
      locationId,
      baseVariant,
    );
    if (optionVariantModel?.source === "child-products") {
      const skippedChildren = Array.isArray(optionVariantModel.skippedChildren)
        ? optionVariantModel.skippedChildren
        : [];
      if (skippedChildren.length > 0) {
        console.warn(
          `[ChildVariantSkipped] parent=${source.productcode || source.productname || "unknown"} skipped=${JSON.stringify(skippedChildren)}`,
        );
      }
    }
  }

  const attemptedChildVariantModel =
    optionVariantModel?.source === "child-products";
  if (
    (!optionVariantModel || optionVariantModel.variants.length === 0) &&
    !attemptedChildVariantModel
  ) {
    optionVariantModel = buildOptionVariantModel(
      source,
      toNumber(price, 0),
      compareAtPrice != null ? toNumber(compareAtPrice, 0) : null,
    );
  }
  const categoryVariantMetafields = buildVariantCategoryMetafields(source);
  const categoryCollectionNames = (() => {
    const mf = categoryVariantMetafields.find(
      (m) => m?.namespace === "variant" && m?.key === "collection_names",
    );
    if (!mf?.value) return [];
    return safeParseJsonArray(mf.value);
  })();

  let productOptionsInput = [
    { name: "Title", values: [{ name: "Default Title" }] },
  ];
  let variantsInput = [baseVariant];
  const hasGeneratedVariants =
    optionVariantModel && optionVariantModel.variants.length > 0;

  if (hasGeneratedVariants) {
    productOptionsInput = optionVariantModel.productOptions;
    if (optionVariantModel.source === "child-products") {
      variantsInput = optionVariantModel.variants.map((variant, idx) => {
        const normalized = JSON.parse(JSON.stringify(variant));
        normalized.position = idx + 1;
        if (!normalized.optionValues || normalized.optionValues.length === 0) {
          normalized.optionValues = [
            { optionName: "Title", name: "Default Title" },
          ];
        }
        if (categoryVariantMetafields.length > 0) {
          normalized.metafields = mergeVariantMetafields(
            normalized.metafields || [],
            categoryVariantMetafields,
          );
        }
        return normalized;
      });
    } else {
      variantsInput = optionVariantModel.variants.map((v) => {
        const variant = JSON.parse(JSON.stringify(baseVariant));
        variant.position = v.position;
        variant.optionValues = v.optionValues;
        if (!optionVariantModel.keepBaseSku) {
          // Default behavior: generated variants don't inherit identical base SKU.
          delete variant.sku;
        }
        variant.price = v.price;
        if (v.compareAtPrice) variant.compareAtPrice = v.compareAtPrice;
        else delete variant.compareAtPrice;
        const mergedVariantMetafields = mergeVariantMetafields(
          v.metafields || [],
          categoryVariantMetafields,
        );
        if (mergedVariantMetafields.length > 0) {
          variant.metafields = mergedVariantMetafields;
        }
        return variant;
      });
    }
  } else if (
    optionVariantModel &&
    ((Array.isArray(optionVariantModel.fallbackAboutValues) &&
      optionVariantModel.fallbackAboutValues.length > 0) ||
      (Array.isArray(optionVariantModel.fallbackDisplayValues) &&
        optionVariantModel.fallbackDisplayValues.length > 0))
  ) {
    baseVariant.metafields = [];
    if (
      Array.isArray(optionVariantModel.fallbackAboutValues) &&
      optionVariantModel.fallbackAboutValues.length > 0
    ) {
      baseVariant.metafields.push({
        namespace: "variant",
        key: "about_option",
        type: "multi_line_text_field",
        value: optionVariantModel.fallbackAboutValues.join("\n"),
      });
    }
    if (
      Array.isArray(optionVariantModel.fallbackDisplayValues) &&
      optionVariantModel.fallbackDisplayValues.length > 0
    ) {
      baseVariant.metafields.push({
        namespace: "variant",
        key: "display_type",
        type: "single_line_text_field",
        value: optionVariantModel.fallbackDisplayValues.join("|"),
      });
    }
  }

  if (categoryVariantMetafields.length > 0) {
    baseVariant.metafields = mergeVariantMetafields(
      baseVariant.metafields || [],
      categoryVariantMetafields,
    );
  }

  if (
    options &&
    options.variantBuildReport &&
    typeof options.variantBuildReport === "object"
  ) {
    options.variantBuildReport.source = optionVariantModel?.source || "default";
    options.variantBuildReport.variantCount = Array.isArray(variantsInput)
      ? variantsInput.length
      : 0;
    options.variantBuildReport.skippedChildren = Array.isArray(
      optionVariantModel?.skippedChildren,
    )
      ? optionVariantModel.skippedChildren
      : [];
    options.variantBuildReport.missingMappings = Array.isArray(
      optionVariantModel?.missingMappings,
    )
      ? optionVariantModel.missingMappings
      : [];
    options.variantBuildReport.missingOptionIds = Array.isArray(
      optionVariantModel?.missingOptionIds,
    )
      ? optionVariantModel.missingOptionIds
      : [];
  }

  // Resolve Shopify category from google_product_category via category.json mapping
  const shopifyCategory = resolveShopifyCategory(
    source.google_product_category,
    categoryMap,
  );

  const input = {
    title: sourceIdentity.name || "Untitled Product",
    descriptionHtml: source.productdescription || "",
    vendor: normalizeProductManufacturer(source.productmanufacturer),
    status,
    seo: {
      title: source.metatag_title || sourceIdentity.name || "",
      description: source.metatag_description || "",
    },
    productOptions: productOptionsInput,
    variants: variantsInput,
    metafields: buildMetafields(source),
  };

  if (shopifyCategory) input.category = shopifyCategory;
  if (categoryCollectionNames.length > 0) {
    input.tags = [
      ...new Set(
        categoryCollectionNames
          .map((tag) => sanitizeShopifyTag(tag))
          .filter(Boolean),
      ),
    ];
  }

  // Image from uploaded Shopify files or URL(s) (productSet uses files)
  const uploadedImageFiles = Array.isArray(source.productImageFiles)
    ? source.productImageFiles.filter((f) => f && (f.id || f.url))
    : [];
  const primaryPhotoUrl =
    source.photourl && String(source.photourl).trim()
      ? String(source.photourl).trim()
      : null;
  const additionalPhotoUrls = Array.isArray(source.additionalPhotoUrls)
    ? source.additionalPhotoUrls
        .map((url) => String(url || "").trim())
        .filter(Boolean)
    : [];
  const allPhotoUrls = [
    ...new Set([primaryPhotoUrl, ...additionalPhotoUrls].filter(Boolean)),
  ];
  const galleryPhotoUrls = allPhotoUrls.slice(1);
  const alt = source.photo_alttext || source.productname || "";
  const filesByKey = new Map();
  const addFileInput = (fileInput) => {
    if (!fileInput) return null;

    if (fileInput.id) {
      const key = `id:${String(fileInput.id)}`;
      if (!filesByKey.has(key)) {
        filesByKey.set(key, {
          id: fileInput.id,
          alt: fileInput.alt || alt || null,
        });
      }
      return filesByKey.get(key);
    }

    const originalSource = String(
      fileInput.originalSource || fileInput.url || "",
    ).trim();
    if (!originalSource) return null;
    const key = `url:${originalSource}`;
    if (!filesByKey.has(key)) {
      filesByKey.set(key, {
        originalSource,
        contentType: "IMAGE",
        alt: fileInput.alt || alt || null,
      });
    }
    return filesByKey.get(key);
  };

  if (uploadedImageFiles.length > 0) {
    uploadedImageFiles.forEach((f) =>
      addFileInput(
        f.id
          ? { id: f.id, alt: alt || null }
          : { originalSource: f.url, contentType: "IMAGE", alt: alt || null },
      ),
    );
  } else {
    galleryPhotoUrls.forEach((imgUrl) =>
      addFileInput({
        originalSource: imgUrl,
        contentType: "IMAGE",
        alt: alt || null,
      }),
    );
  }

  for (const variant of input.variants || []) {
    const normalizedVariantFile = addFileInput(variant?.file);
    if (normalizedVariantFile) {
      variant.file = normalizedVariantFile;
    }
  }

  if (
    !hasGeneratedVariants &&
    input.variants?.[0] &&
    !input.variants[0].file &&
    filesByKey.size > 0
  ) {
    input.variants[0].file = [...filesByKey.values()][0];
  }

  if (filesByKey.size > 0) {
    input.files = [...filesByKey.values()];
  }

  return input;
}

// ─────────────────────────────────────────────────────────────────────────────
// METAFIELDS PAYLOAD FOR metafieldsSet (create if not present)
// Use after product create when you need to ensure metafield definitions exist
// ─────────────────────────────────────────────────────────────────────────────
function buildMetafieldsSetPayload(productId, source) {
  const metafields = buildMetafields(source);
  return metafields.map((m) => ({
    ownerId: productId,
    namespace: m.namespace,
    key: m.key,
    type: m.type,
    value: m.value,
  }));
}

module.exports = {
  loadCategoryMap,
  resolveShopifyCategory,
  resolvePrice,
  resolveInventoryPolicy,
  resolveStatus,
  normalizeSourceProductName,
  normalizeSourceProductSku,
  buildSourceProductIdentity,
  buildMetafields,
  buildProductSetInput,
  buildMetafieldsSetPayload,
};
