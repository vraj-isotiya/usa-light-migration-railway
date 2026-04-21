const fs = require("fs");
const path = require("path");
const sharp = require("sharp");
const crypto = require("crypto");
const shopifyClient = require("../config/shopify");

const DEFAULT_SOURCE_BASE_URL = "https://www.usalight.com";
const CATEGORY_IMAGE_BASE_URL =
  "https://cdn4.volusion.store/sjrzx-uggtc/v/vspfiles/photos/categories";
const COLLECTION_NAMESPACE = "collection";
const COLLECTION_BATCH_DELAY_MS = 150;

const COLLECTION_CREATE_MUTATION = `
  mutation collectionCreate($input: CollectionInput!) {
    collectionCreate(input: $input) {
      collection {
        id
        title
        handle
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
        namespace
        key
      }
      userErrors {
        field
        message
      }
    }
  }
`;

const FILE_CREATE_MUTATION = `
  mutation fileCreate($files: [FileCreateInput!]!) {
    fileCreate(files: $files) {
      files {
        ... on File {
          id
          fileStatus
        }
        ... on MediaImage {
          image {
            url
          }
        }
      }
      userErrors {
        field
        message
      }
    }
  }
`;

const FILE_STATUS_QUERY = `
  query getFileStatus($id: ID!) {
    node(id: $id) {
      ... on File {
        id
        fileStatus
      }
      ... on MediaImage {
        image {
          url
        }
      }
    }
  }
`;

const FILES_BY_NAME_QUERY = `
  query filesByName($query: String!) {
    files(first: 50, query: $query) {
      nodes {
        ... on File {
          id
          fileStatus
        }
        ... on MediaImage {
          image {
            url
          }
        }
      }
    }
  }
`;

const STAGED_UPLOADS_CREATE_MUTATION = `
  mutation stagedUploadsCreate($input: [StagedUploadInput!]!) {
    stagedUploadsCreate(input: $input) {
      stagedTargets {
        url
        resourceUrl
        parameters {
          name
          value
        }
      }
      userErrors {
        field
        message
      }
    }
  }
`;

const PUBLISHABLE_PUBLISH_MUTATION = `
  mutation publishablePublish($id: ID!, $input: [PublicationInput!]!) {
    publishablePublish(id: $id, input: $input) {
      publishable {
        ... on Collection {
          id
        }
      }
      userErrors {
        field
        message
      }
    }
  }
`;

const COLLECTION_BY_ID_QUERY = `
  query collectionById($id: ID!) {
    collection(id: $id) {
      id
      title
      handle
    }
  }
`;

const uploadedDescriptionImageCache = new Map();
const fsp = fs.promises;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function toStringValue(value) {
  if (value === undefined || value === null) return "";
  return String(value);
}

function normalizeCategoryId(item) {
  return toStringValue(item?.categoryid || item?.id).trim();
}

function normalizeCategoryName(item) {
  return toStringValue(item?.categoryname).trim();
}

function sanitizeShopifyTagValue(value) {
  const raw = toStringValue(value).trim();
  if (!raw) return "";

  // Shopify splits tags by comma; keep numeric values readable and comma-safe.
  return raw
    .replace(/(\d),(\d)/g, "$1$2")
    .replace(/,/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizeParentId(item) {
  return toStringValue(item?.parentid).trim();
}

function normalizeRootId(item) {
  return toStringValue(item?.rootid).trim();
}

function parseParentIds(parentIds) {
  if (Array.isArray(parentIds)) {
    return new Set(
      parentIds.map((x) => toStringValue(x).trim()).filter(Boolean),
    );
  }
  return new Set(
    toStringValue(parentIds)
      .split(",")
      .map((x) => x.trim())
      .filter(Boolean),
  );
}

function normalizeSourceUrl(rawUrl, baseUrl = DEFAULT_SOURCE_BASE_URL) {
  const value = toStringValue(rawUrl).trim();
  if (!value) return "";

  const encodePathAndQuery = (urlStr) => {
    try {
      const u = new URL(urlStr);
      u.pathname = u.pathname
        .split("/")
        .map((seg) => encodeURIComponent(decodeURIComponent(seg)))
        .join("/");
      return u.toString();
    } catch {
      return urlStr;
    }
  };

  if (/^https?:\/\//i.test(value)) return encodePathAndQuery(value);
  if (value.startsWith("//")) return encodePathAndQuery(`https:${value}`);
  if (value.startsWith("/")) {
    return encodePathAndQuery(`${baseUrl.replace(/\/+$/, "")}${value}`);
  }
  return encodePathAndQuery(
    `${baseUrl.replace(/\/+$/, "")}/${value.replace(/^\/+/, "")}`,
  );
}

function extractFileNameFromUrl(url) {
  try {
    const pathname = new URL(url).pathname || "";
    return pathname.split("/").filter(Boolean).pop() || "image";
  } catch {
    return "image";
  }
}

function buildDeterministicWebpFileName(sourceUrl) {
  const sourceName = extractFileNameFromUrl(sourceUrl);
  const baseName = sourceName.replace(/\.[^./?#]+$/i, "");
  const hash = crypto
    .createHash("sha1")
    .update(String(sourceUrl))
    .digest("hex")
    .slice(0, 8);
  return `${baseName}-${hash}.webp`;
}

function fileUrlFromNode(node) {
  return node?.image?.url || null;
}

function mapSortOrder(defaultSortBy) {
  const value = toStringValue(defaultSortBy).trim();
  if (value === "PriceHigh") return "PRICE_DESC";
  if (value === "PriceLow") return "PRICE_ASC";
  if (value === "Title") return "ALPHA_ASC";
  return null;
}

function toBooleanString(value) {
  const raw = toStringValue(value).trim().toUpperCase();
  return raw === "Y" || raw === "TRUE" || raw === "1" ? "true" : "false";
}

function toIntegerString(value) {
  const n = Number.parseInt(toStringValue(value).trim(), 10);
  return Number.isFinite(n) ? String(n) : "0";
}

function toStoreBaseUrl() {
  const fromEnv = toStringValue(
    process.env.SHOPIFY_COLLECTION_URL_BASE || process.env.SHOPIFY_STORE,
  ).trim();
  if (!fromEnv) return "";
  if (/^https?:\/\//i.test(fromEnv)) return fromEnv.replace(/\/+$/, "");
  return `https://${fromEnv.replace(/\/+$/, "")}`;
}

function buildCollectionUrlFromHandle(handle) {
  const base = toStoreBaseUrl();
  const h = toStringValue(handle).trim();
  if (!base || !h) return "";
  return `${base}/collections/${h}`;
}

function buildCategoryImageUrl(categoryId) {
  const id = toStringValue(categoryId).trim();
  if (!id) return "";
  return `${CATEGORY_IMAGE_BASE_URL}/${encodeURIComponent(id)}.jpg`;
}

async function isRemoteImageAvailable(url) {
  if (!url) return false;

  const isImageResponse = (res) => {
    const contentType = (res.headers.get("content-type") || "").toLowerCase();
    return !contentType || contentType.startsWith("image/");
  };

  try {
    const headRes = await fetch(url, { method: "HEAD" });
    if (headRes.ok) return isImageResponse(headRes);
    if (headRes.status !== 403 && headRes.status !== 405) return false;
  } catch {
    // Fall through to GET with range.
  }

  try {
    const getRes = await fetch(url, {
      method: "GET",
      headers: { Range: "bytes=0-0" },
    });
    if (!getRes.ok && getRes.status !== 206) return false;
    const ok = isImageResponse(getRes);
    // Ensure the body is drained to avoid hanging connections.
    await getRes.arrayBuffer();
    return ok;
  } catch {
    return false;
  }
}

async function buildCollectionImageInput(item) {
  const categoryId = normalizeCategoryId(item);
  const categoryName = normalizeCategoryName(item);
  if (!categoryId || !categoryName) return null;

  const url = buildCategoryImageUrl(categoryId);
  const available = await isRemoteImageAvailable(url);
  if (!available) return null;

  return {
    src: url,
    altText: categoryName,
  };
}

function buildTagRulesForCategory(item, categoryById) {
  const rules = [];
  const seen = new Set();
  const visited = new Set();

  const addRule = (name) => {
    const value = sanitizeShopifyTagValue(name);
    if (!value || seen.has(value)) return;
    seen.add(value);
    rules.push({
      column: "TAG",
      relation: "EQUALS",
      condition: value,
    });
  };

  const currentId = normalizeCategoryId(item);
  const currentName = normalizeCategoryName(item);

  if (!currentId) {
    throw new Error("Category id/categoryid is missing");
  }
  if (!currentName) {
    throw new Error(`Category name missing for categoryid=${currentId}`);
  }

  // Always include the current category tag.
  addRule(currentName);

  let parentId = normalizeParentId(item);
  while (parentId && parentId !== "0") {
    if (visited.has(parentId)) {
      throw new Error(
        `Category hierarchy cycle detected at categoryid=${currentId}, parentid=${parentId}`,
      );
    }
    visited.add(parentId);

    const parentItem = categoryById.get(parentId);
    if (!parentItem) {
      throw new Error(`Parent category not found for parentid=${parentId}`);
    }

    const parentName = normalizeCategoryName(parentItem);
    if (!parentName) {
      throw new Error(`Parent category name missing for parentid=${parentId}`);
    }
    addRule(parentName);

    parentId = normalizeParentId(parentItem);
  }

  return rules;
}

function collectAncestorCollectionIds(item, categoryById, idMap) {
  const ids = [];
  const visited = new Set();
  let parentId = normalizeParentId(item);
  const rootId = normalizeRootId(item);

  while (parentId && parentId !== "0") {
    if (visited.has(parentId)) {
      throw new Error(
        `Category hierarchy cycle detected while collecting parents for categoryid=${normalizeCategoryId(item) || "unknown"}`,
      );
    }
    visited.add(parentId);

    const parentItem = categoryById.get(parentId);
    if (!parentItem) {
      throw new Error(`Parent category not found for parentid=${parentId}`);
    }

    const parentParentId = normalizeParentId(parentItem);
    const isRootParent =
      (rootId && parentId === rootId) || parentParentId === "0";
    if (!isRootParent) {
      const parentCollectionId = idMap.get(parentId);
      if (!parentCollectionId) {
        throw new Error(
          `Parent collection mapping not found for parentid=${parentId}`,
        );
      }
      ids.push(parentCollectionId);
    } else {
      // Root should not be included in parent_collection list.
      break;
    }

    parentId = parentParentId;
  }

  return ids;
}

function partitionCategories(categories) {
  const roots = [];
  const parents = [];
  const children = [];

  for (const item of categories) {
    const id = normalizeCategoryId(item);
    const rootId = normalizeRootId(item);
    const parentId = normalizeParentId(item);

    if (id && rootId && id === rootId) {
      roots.push(item);
      continue;
    }
    if (parentId === "0") {
      parents.push(item);
      continue;
    }
    if (Number(parentId) > 0) {
      children.push(item);
    }
  }

  return { roots, parents, children };
}

function parseCategoriesFile(filePath) {
  const raw = fs.readFileSync(filePath, "utf8");
  const parsed = JSON.parse(raw);
  if (!Array.isArray(parsed)) {
    throw new Error("categories file must contain an array");
  }
  return parsed;
}

async function upsertCategoryMappingFile(mappingPath, newEntries) {
  let existingRows = [];

  try {
    const raw = await fsp.readFile(mappingPath, "utf8");
    const parsed = JSON.parse(raw);
    if (Array.isArray(parsed)) {
      existingRows = parsed;
    } else if (parsed && typeof parsed === "object") {
      // Backward compatibility for old object format.
      existingRows = Object.values(parsed);
    }
  } catch (err) {
    // File might not exist yet; start from empty.
    if (err?.code !== "ENOENT") {
      throw err;
    }
  }

  const mergedById = new Map();
  for (const row of existingRows) {
    const id = toStringValue(row?.categoryid).trim();
    if (!id) continue;
    mergedById.set(id, row);
  }
  for (const [id, row] of Object.entries(newEntries || {})) {
    const key =
      toStringValue(id).trim() || toStringValue(row?.categoryid).trim();
    if (!key) continue;
    mergedById.set(key, row);
  }

  const mergedRows = [...mergedById.values()];

  await fsp.writeFile(mappingPath, JSON.stringify(mergedRows, null, 2), "utf8");
}

function buildBaseCollectionInput(item, descriptionHtml, rules, imageInput) {
  const sortOrder = mapSortOrder(item?.default_sortby);
  const categoryName = normalizeCategoryName(item);

  const input = {
    title: categoryName,
    descriptionHtml: toStringValue(descriptionHtml),
    handle: toStringValue(item?.link_title_tag).trim() || undefined,
    seo: {
      title: toStringValue(item?.metatag_title),
      description: toStringValue(item?.metatag_description),
    },
    metafields: [
      {
        namespace: COLLECTION_NAMESPACE,
        key: "meta_keywords",
        type: "multi_line_text_field",
        value: toStringValue(item?.metatag_keywords),
      },
      {
        namespace: COLLECTION_NAMESPACE,
        key: "short_description",
        type: "multi_line_text_field",
        value: toStringValue(item?.categorydescriptionshort),
      },
      {
        namespace: COLLECTION_NAMESPACE,
        key: "below_products_description",
        type: "multi_line_text_field",
        value: toStringValue(item?.categorydescription_belowproducts),
      },
      {
        namespace: COLLECTION_NAMESPACE,
        key: "hidden_category",
        type: "boolean",
        value: toBooleanString(item?.hidden),
      },
      {
        namespace: COLLECTION_NAMESPACE,
        key: "alternate_url",
        type: "single_line_text_field",
        value: toStringValue(item?.alternateurl),
      },
      {
        namespace: COLLECTION_NAMESPACE,
        key: "category_order",
        type: "number_integer",
        value: toIntegerString(item?.categoryorder),
      },
      {
        namespace: COLLECTION_NAMESPACE,
        key: "breadcrumb",
        type: "single_line_text_field",
        value: toStringValue(item?.breadcrumb),
      },
    ],
    ruleSet: {
      appliedDisjunctively: false,
      rules,
    },
  };

  if (imageInput) input.image = imageInput;
  if (sortOrder) input.sortOrder = sortOrder;
  if (!input.handle) delete input.handle;
  input.metafields = input.metafields.filter((mf) =>
    String(mf?.value ?? "").trim(),
  );
  return input;
}

function buildHandleWithCategorySuffix(handle, categoryId) {
  const base = toStringValue(handle).trim();
  const id = toStringValue(categoryId).trim();
  if (!base || !id) return base;
  if (base.endsWith(`-${id}`)) return base;
  return `${base}-${id}`.slice(0, 255);
}

async function requestGraphql(query, variables = {}) {
  const response = await shopifyClient.request(query, { variables });
  const topErrors = response?.errors || [];
  if (topErrors.length > 0) {
    throw new Error(topErrors.map((e) => e.message).join("; "));
  }
  return response;
}

async function waitForImageReady(fileId, timeoutMs = 120000) {
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeoutMs) {
    const response = await requestGraphql(FILE_STATUS_QUERY, { id: fileId });
    const fileNode = response?.data?.node;
    if (!fileNode) throw new Error(`File not found for id ${fileId}`);
    const status = fileNode?.fileStatus;
    const url = fileUrlFromNode(fileNode);
    if (status === "READY" && url) return url;
    if (status === "FAILED")
      throw new Error(`Image processing failed for ${fileId}`);
    await sleep(1000);
  }
  throw new Error(`Timed out waiting for image READY: ${fileId}`);
}

function fileQueryByFilename(fileName) {
  const escaped = toStringValue(fileName).replace(/"/g, '\\"');
  return `filename:"${escaped}"`;
}

async function findExistingImageByName(fileName) {
  const response = await requestGraphql(FILES_BY_NAME_QUERY, {
    query: fileQueryByFilename(fileName),
  });
  const nodes = response?.data?.files?.nodes || [];
  const expectedLower = fileName.toLowerCase();

  for (const node of nodes) {
    const url = fileUrlFromNode(node);
    if (!url) continue;
    let nodeName = "";
    try {
      nodeName = decodeURIComponent(
        new URL(url).pathname.split("/").pop() || "",
      ).toLowerCase();
    } catch {
      continue;
    }
    if (nodeName !== expectedLower) continue;
    if (node.fileStatus === "READY") return url;
    if (node.fileStatus === "PROCESSING" && node.id) {
      return waitForImageReady(node.id);
    }
  }
  return null;
}

async function uploadToStagedTarget(target, fileBuffer, filename, mimeType) {
  const form = new FormData();
  for (const p of target.parameters || []) form.append(p.name, p.value);
  const blob = new Blob([fileBuffer], { type: mimeType });
  form.append("file", blob, filename);

  const uploadRes = await fetch(target.url, { method: "POST", body: form });
  if (!uploadRes.ok) {
    const body = await uploadRes.text();
    throw new Error(
      `Staged upload failed (${uploadRes.status}): ${body.slice(0, 300)}`,
    );
  }
}

async function requestStagedTarget(filename, fileSize) {
  async function attempt(resource) {
    const response = await requestGraphql(STAGED_UPLOADS_CREATE_MUTATION, {
      input: [
        {
          filename,
          mimeType: "image/webp",
          resource,
          httpMethod: "POST",
          fileSize: String(fileSize),
        },
      ],
    });
    const userErrors = response?.data?.stagedUploadsCreate?.userErrors || [];
    if (userErrors.length > 0) {
      throw new Error(userErrors.map((e) => e.message).join("; "));
    }
    return response?.data?.stagedUploadsCreate?.stagedTargets?.[0] || null;
  }

  let target = await attempt("IMAGE");
  if (!target?.url || !target?.resourceUrl) target = await attempt("FILE");
  if (!target?.url || !target?.resourceUrl) {
    throw new Error("No staged upload target returned for webp image");
  }
  return target;
}

async function createShopifyWebpImageFromSourceUrl(sourceUrl) {
  if (uploadedDescriptionImageCache.has(sourceUrl)) {
    return uploadedDescriptionImageCache.get(sourceUrl);
  }

  const webpName = buildDeterministicWebpFileName(sourceUrl);
  const existing = await findExistingImageByName(webpName);
  if (existing) {
    uploadedDescriptionImageCache.set(sourceUrl, existing);
    return existing;
  }

  const sourceRes = await fetch(sourceUrl);
  if (!sourceRes.ok) {
    throw new Error(
      `Source image download failed (${sourceRes.status}) for ${sourceUrl}`,
    );
  }
  const sourceBuffer = Buffer.from(await sourceRes.arrayBuffer());
  const webpBuffer = await sharp(sourceBuffer)
    .webp({ quality: 100 })
    .toBuffer();

  const stagedTarget = await requestStagedTarget(webpName, webpBuffer.length);
  await uploadToStagedTarget(stagedTarget, webpBuffer, webpName, "image/webp");

  const response = await requestGraphql(FILE_CREATE_MUTATION, {
    files: [
      {
        contentType: "IMAGE",
        originalSource: stagedTarget.resourceUrl,
        filename: webpName,
        duplicateResolutionMode: "APPEND_UUID",
      },
    ],
  });
  const userErrors = response?.data?.fileCreate?.userErrors || [];
  if (userErrors.length > 0) {
    throw new Error(userErrors.map((e) => e.message).join("; "));
  }

  const file = response?.data?.fileCreate?.files?.[0];
  if (!file?.id) throw new Error(`No file id returned for ${sourceUrl}`);
  const url =
    file.fileStatus === "READY" && fileUrlFromNode(file)
      ? fileUrlFromNode(file)
      : await waitForImageReady(file.id);

  uploadedDescriptionImageCache.set(sourceUrl, url);
  return url;
}

async function rewriteDescriptionImageUrls(html) {
  const source = toStringValue(html);
  if (!source.trim()) return "";

  const srcRegex = /src\s*=\s*(["'])(.*?)\1/gi;
  const rawSrcs = [];
  let match = srcRegex.exec(source);
  while (match) {
    rawSrcs.push(match[2]);
    match = srcRegex.exec(source);
  }

  const normalizedSrcs = [...new Set(rawSrcs)]
    .map((rawUrl) => normalizeSourceUrl(rawUrl))
    .filter(Boolean);
  if (normalizedSrcs.length === 0) return source;

  const replacements = new Map();
  for (const src of normalizedSrcs) {
    try {
      const shopifyUrl = await createShopifyWebpImageFromSourceUrl(src);
      replacements.set(src, shopifyUrl);
    } catch (err) {
      console.warn(`Category description image upload failed: ${src}`);
      console.warn(err.message || String(err));
      replacements.set(src, src);
    }
  }

  return source.replace(/src\s*=\s*(["'])(.*?)\1/gi, (full, quote, rawUrl) => {
    const normalized = normalizeSourceUrl(rawUrl);
    const replacement = replacements.get(normalized);
    if (!replacement) return full;
    return `src=${quote}${replacement}${quote}`;
  });
}

async function createCollectionWithTagRules(item, rules) {
  if (!Array.isArray(rules) || rules.length === 0) {
    throw new Error(
      `No tag rules resolved for category ${normalizeCategoryName(item) || normalizeCategoryId(item)}`,
    );
  }

  const descriptionHtml = await rewriteDescriptionImageUrls(
    item?.categorydescription,
  );
  let imageInput = null;
  try {
    imageInput = await buildCollectionImageInput(item);
  } catch (err) {
    console.warn(
      `Collection image check failed for category ${normalizeCategoryId(item) || "unknown"}`,
    );
    console.warn(err?.message || String(err));
  }

  const input = buildBaseCollectionInput(
    item,
    descriptionHtml,
    rules,
    imageInput,
  );

  function isHandleConflict(userErrors = []) {
    return userErrors.some((e) => {
      const field = Array.isArray(e?.field) ? e.field.join(".") : "";
      const msg = toStringValue(e?.message).toLowerCase();
      return (
        field.includes("handle") ||
        msg.includes("handle") ||
        msg.includes("already been taken")
      );
    });
  }

  function hasImageRelatedUserErrors(userErrors = []) {
    return userErrors.some((e) => {
      const field = Array.isArray(e?.field)
        ? e.field.join(".").toLowerCase()
        : toStringValue(e?.field).toLowerCase();
      const msg = toStringValue(e?.message).toLowerCase();
      return (
        field.includes("image") ||
        msg.includes("image") ||
        msg.includes("media") ||
        msg.includes("download") ||
        msg.includes("fetch")
      );
    });
  }

  async function attemptCreate(overrideHandle, includeImage = true) {
    const payload = { ...input };
    if (!includeImage) delete payload.image;
    if (overrideHandle) payload.handle = overrideHandle;
    try {
      const response = await requestGraphql(COLLECTION_CREATE_MUTATION, {
        input: payload,
      });
      const userErrors = response?.data?.collectionCreate?.userErrors || [];
      return { response, userErrors, error: null, includeImage };
    } catch (error) {
      return { response: null, userErrors: [], error, includeImage };
    }
  }

  async function attemptCreateWithHandleFallback(includeImage = true) {
    let result = await attemptCreate(input.handle, includeImage);
    if (
      !result.error &&
      isHandleConflict(result.userErrors) &&
      input.handle
    ) {
      const fallbackHandle = buildHandleWithCategorySuffix(
        input.handle,
        item?.categoryid || item?.id,
      );
      result = await attemptCreate(fallbackHandle, includeImage);
    }
    return result;
  }

  let createResult = await attemptCreateWithHandleFallback(Boolean(imageInput));
  if (
    imageInput &&
    (createResult.error || hasImageRelatedUserErrors(createResult.userErrors))
  ) {
    console.warn(
      `Collection image failed for category ${normalizeCategoryId(item) || "unknown"}; retrying without image`,
    );
    if (createResult.error) {
      console.warn(createResult.error?.message || String(createResult.error));
    } else if (createResult.userErrors.length > 0) {
      console.warn(createResult.userErrors.map((e) => e.message).join("; "));
    }
    createResult = await attemptCreateWithHandleFallback(false);
  }

  if (createResult.error) {
    throw createResult.error;
  }

  if (createResult.userErrors.length > 0) {
    throw new Error(createResult.userErrors.map((e) => e.message).join("; "));
  }

  const collection = createResult.response?.data?.collectionCreate?.collection;
  if (!collection?.id) {
    throw new Error(
      `Collection id missing for category ${toStringValue(item?.categoryname)}`,
    );
  }
  return collection;
}

async function setCollectionMetafields(ownerId, metafields) {
  if (!Array.isArray(metafields) || metafields.length === 0) return;

  const response = await requestGraphql(METAFIELDS_SET_MUTATION, {
    metafields: metafields.map((mf) => ({
      ownerId,
      namespace: COLLECTION_NAMESPACE,
      key: mf.key,
      type: mf.type,
      value: mf.value,
    })),
  });
  const userErrors = response?.data?.metafieldsSet?.userErrors || [];
  if (userErrors.length > 0) {
    throw new Error(userErrors.map((e) => e.message).join("; "));
  }
}

function getPublicationIdsFromEnv() {
  const idsFromList = (process.env.SHOPIFY_PUBLICATION_IDS || "")
    .split(",")
    .map((id) => id.trim())
    .filter(Boolean);
  const idsFromIndexedVars = Object.keys(process.env)
    .filter((key) => /^SHOPIFY_PUBLICATION_ID_\d+$/.test(key))
    .sort()
    .map((key) => process.env[key]?.trim())
    .filter(Boolean);
  return [...new Set([...idsFromList, ...idsFromIndexedVars])];
}

async function publishCollectionToChannels(collectionId) {
  const publicationIds = getPublicationIdsFromEnv();
  if (publicationIds.length === 0) {
    throw new Error(
      "No publication IDs configured. Set SHOPIFY_PUBLICATION_ID_1 or SHOPIFY_PUBLICATION_IDS.",
    );
  }
  const response = await requestGraphql(PUBLISHABLE_PUBLISH_MUTATION, {
    id: collectionId,
    input: publicationIds.map((publicationId) => ({ publicationId })),
  });
  const userErrors = response?.data?.publishablePublish?.userErrors || [];
  if (userErrors.length > 0) {
    throw new Error(userErrors.map((e) => e.message).join("; "));
  }
}

async function migrateCategoriesToCollections(options = {}) {
  const {
    file = "categories.json",
    limit,
    startIndex,
    endIndex,
    delayMs = COLLECTION_BATCH_DELAY_MS,
    parentIds = [],
    onProgress = () => {},
  } = options;

  const trackedParentIds = parseParentIds(parentIds);
  const filePath = path.resolve(process.cwd(), file);
  const allCategories = parseCategoriesFile(filePath);

  const start = Number.isFinite(startIndex) && startIndex >= 0 ? startIndex : 0;
  const end =
    Number.isFinite(endIndex) && endIndex >= start ? endIndex + 1 : undefined;
  let categories = allCategories.slice(start, end);
  if (Number.isFinite(limit) && limit > 0) {
    categories = categories.slice(0, limit);
  }

  const categoryById = new Map();
  for (const item of allCategories) {
    const id = normalizeCategoryId(item);
    if (!id) continue;
    if (!categoryById.has(id)) categoryById.set(id, item);
  }

  const { roots, parents, children } = partitionCategories(categories);
  const ordered = [...roots, ...parents, ...children];

  const idMap = new Map();
  const mappingByCategoryId = {};
  const results = {
    created: 0,
    failed: 0,
    errors: [],
    total: ordered.length,
  };

  async function createAndTrack(item, phase, index) {
    const sourceId = normalizeCategoryId(item);
    const label = toStringValue(item?.categoryname) || sourceId || `#${index}`;
    const rootSourceId = normalizeRootId(item);
    const parentSourceId = normalizeParentId(item);

    try {
      if (!sourceId) throw new Error("Category id/categoryid is missing");

      const rules = buildTagRulesForCategory(item, categoryById);

      const collection = await createCollectionWithTagRules(item, rules);
      idMap.set(sourceId, collection.id);
      await publishCollectionToChannels(collection.id);

      const hierarchyMetafields = [];
      const ancestorCollectionIds = collectAncestorCollectionIds(
        item,
        categoryById,
        idMap,
      );
      if (ancestorCollectionIds.length > 0) {
        hierarchyMetafields.push({
          key: "parent_collection",
          type: "list.collection_reference",
          value: JSON.stringify(ancestorCollectionIds),
        });
      }

      if (rootSourceId && rootSourceId !== sourceId) {
        const rootCollectionId = idMap.get(rootSourceId);
        if (!rootCollectionId) {
          throw new Error(
            `Root collection mapping not found for rootid=${rootSourceId}`,
          );
        }
        hierarchyMetafields.push({
          key: "root_collection",
          type: "collection_reference",
          value: rootCollectionId,
        });
      }

      if (hierarchyMetafields.length > 0) {
        await setCollectionMetafields(collection.id, hierarchyMetafields);
      }

      mappingByCategoryId[sourceId] = {
        categoryid: sourceId,
        categoryname: toStringValue(item?.categoryname).trim(),
        shopifyCollectionId: collection.id,
        collectionUrl: buildCollectionUrlFromHandle(collection.handle),
      };

      results.created++;
      onProgress({
        index,
        total: ordered.length,
        status: "created",
        phase,
        category: item,
        collection,
        parentMatched: trackedParentIds.has(parentSourceId),
      });
    } catch (err) {
      results.failed++;
      const wrappedErrors = [{ message: err.message || String(err) }];
      results.errors.push({
        category: label,
        categoryId: sourceId || "",
        phase,
        errors: wrappedErrors,
      });
      onProgress({
        index,
        total: ordered.length,
        status: "failed",
        phase,
        category: item,
        errors: wrappedErrors,
      });
    }
  }

  function hasRequiredHierarchyMappings(item) {
    const sourceId = normalizeCategoryId(item);
    const rootSourceId = normalizeRootId(item);
    const parentSourceId = normalizeParentId(item);

    if (!sourceId) return false;

    if (parentSourceId !== "0" && !idMap.get(parentSourceId)) {
      return false;
    }

    if (rootSourceId && rootSourceId !== sourceId && !idMap.get(rootSourceId)) {
      return false;
    }

    return true;
  }

  let index = 0;
  for (const item of roots) {
    index++;
    await createAndTrack(item, "root", index);
    await sleep(delayMs);
  }
  // Resolve all non-roots in dependency order (supports arbitrary depth nesting).
  let pendingChildren = [...parents, ...children];
  let madeProgress = true;

  while (pendingChildren.length > 0 && madeProgress) {
    madeProgress = false;
    const nextPending = [];

    for (const item of pendingChildren) {
      if (!hasRequiredHierarchyMappings(item)) {
        nextPending.push(item);
        continue;
      }

      index++;
      const phase = normalizeParentId(item) === "0" ? "parent" : "child";
      await createAndTrack(item, phase, index);
      await sleep(delayMs);
      madeProgress = true;
    }

    pendingChildren = nextPending;
  }

  // Any remaining children are unresolved due to missing parent/root chain.
  for (const item of pendingChildren) {
    index++;
    const sourceId = normalizeCategoryId(item);
    const label = toStringValue(item?.categoryname) || sourceId || `#${index}`;
    const rootSourceId = normalizeRootId(item);
    const parentSourceId = normalizeParentId(item);
    const message = `Unresolved hierarchy dependency: parentid=${parentSourceId}, rootid=${rootSourceId}`;

    results.failed++;
    const wrappedErrors = [{ message }];
    results.errors.push({
      category: label,
      categoryId: sourceId || "",
      phase: "child",
      errors: wrappedErrors,
    });
    onProgress({
      index,
      total: ordered.length,
      status: "failed",
      phase: "child",
      category: item,
      errors: wrappedErrors,
    });
  }

  const mappingOutputPath = path.resolve(
    process.cwd(),
    "category-collection-url-map.json",
  );
  await upsertCategoryMappingFile(mappingOutputPath, mappingByCategoryId);

  return {
    created: results.created,
    failed: results.failed,
    errors: results.errors,
    total: results.total,
    mappingFile: mappingOutputPath,
  };
}

async function syncCategoryCollectionUrlMap(options = {}) {
  const {
    mappingFile = "category-collection-url-map.json",
    categoriesFile = "categories.json",
    delayMs = COLLECTION_BATCH_DELAY_MS,
    onProgress = () => {},
  } = options;

  const mappingPath = path.resolve(process.cwd(), mappingFile);
  const categoriesPath = path.resolve(process.cwd(), categoriesFile);
  const mappingRows = parseCategoriesFile(mappingPath);
  const allCategories = parseCategoriesFile(categoriesPath);
  const categoryById = new Map();
  for (const category of allCategories) {
    const id = normalizeCategoryId(category);
    if (!id || categoryById.has(id)) continue;
    categoryById.set(id, category);
  }

  const results = {
    checked: mappingRows.length,
    existing: 0,
    created: 0,
    updated: 0,
    failed: 0,
    errors: [],
    mappingFile: mappingPath,
  };
  const updatesByCategoryId = {};
  const checkedAt = new Date().toISOString();

  for (let i = 0; i < mappingRows.length; i++) {
    const row = mappingRows[i];
    const categoryId = toStringValue(row?.categoryid).trim();
    const categoryName =
      toStringValue(row?.categoryname).trim() || categoryId || `#${i + 1}`;
    const shopifyCollectionId = toStringValue(row?.shopifyCollectionId).trim();

    try {
      let collection = null;
      if (shopifyCollectionId) {
        const response = await requestGraphql(COLLECTION_BY_ID_QUERY, {
          id: shopifyCollectionId,
        });
        collection = response?.data?.collection || null;
      }

      let status = "existing";
      if (!collection?.id) {
        const categoryItem = categoryById.get(categoryId) || {
          categoryid: categoryId,
          categoryname: categoryName,
        };
        const rules = buildTagRulesForCategory(categoryItem, categoryById);
        collection = await createCollectionWithTagRules(categoryItem, rules);
        await publishCollectionToChannels(collection.id);
        results.created++;
        status = "created";
      } else {
        results.existing++;
      }

      const nextRow = {
        categoryid: categoryId,
        categoryname: categoryName,
        shopifyCollectionId: collection.id,
        collectionUrl: buildCollectionUrlFromHandle(collection.handle),
        syncStatus: status,
        lastSyncedAt: checkedAt,
      };
      const prevCollectionId = toStringValue(row?.shopifyCollectionId).trim();
      const prevCollectionUrl = toStringValue(row?.collectionUrl).trim();
      if (
        prevCollectionId !== nextRow.shopifyCollectionId ||
        prevCollectionUrl !== nextRow.collectionUrl
      ) {
        results.updated++;
      }
      // Always write back current verification metadata so JSON is updated each run.
      updatesByCategoryId[categoryId] = nextRow;

      onProgress({
        index: i + 1,
        total: mappingRows.length,
        status,
        category: row,
        collection,
      });
    } catch (err) {
      results.failed++;
      const wrappedErrors = [{ message: err.message || String(err) }];
      results.errors.push({
        category: categoryName,
        categoryId,
        errors: wrappedErrors,
      });
      onProgress({
        index: i + 1,
        total: mappingRows.length,
        status: "failed",
        category: row,
        errors: wrappedErrors,
      });
    }

    await sleep(delayMs);
  }

  if (Object.keys(updatesByCategoryId).length > 0) {
    await upsertCategoryMappingFile(mappingPath, updatesByCategoryId);
  }

  return results;
}

module.exports = {
  migrateCategoriesToCollections,
  syncCategoryCollectionUrlMap,
};
