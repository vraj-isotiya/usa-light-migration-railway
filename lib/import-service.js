/**
 * Volusion → Shopify Import Service
 * Uses productSet mutation for atomic product creation
 * Scalable structure for bulk import with rate limiting
 */

const shopifyClient = require("../config/shopify");
const sharp = require("sharp");
const crypto = require("crypto");
const fs = require("fs");
const path = require("path");
const {
  buildProductSetInput,
  buildMetafieldsSetPayload,
  buildMetafields,
  loadCategoryMap,
  buildSourceProductIdentity,
} = require("./migration-mapper");

const PRODUCT_SET_MUTATION = `
  mutation productSet($input: ProductSetInput!, $synchronous: Boolean) {
    productSet(input: $input, synchronous: $synchronous) {
      product {
        id
        title
        handle
        status
        variants(first: 100) {
          nodes {
            id
            sku
            inventoryItem {
              id
            }
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

const DEFAULT_DELAY_MS = 400;
const DEFAULT_MAPPING_FLUSH_EVERY = 100;
const METAFIELDS_BATCH_SIZE = 25;
const DEFAULT_SOURCE_FILE_BASE_URL = "https://www.usalight.com";
const MAX_SEQUENTIAL_PRODUCT_IMAGE_NUMBER = 10;

const FILE_CREATE_MUTATION = `
  mutation fileCreate($files: [FileCreateInput!]!) {
    fileCreate(files: $files) {
      files {
        ... on File {
          id
          fileStatus
        }
        ... on GenericFile {
          url
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
        code
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
      ... on GenericFile {
        url
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
        ... on GenericFile {
          url
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

// Mutation to publish a product to a publication (sales channel)
const PUBLISHABLE_PUBLISH_MUTATION = `
  mutation publishablePublish($id: ID!, $input: [PublicationInput!]!) {
    publishablePublish(id: $id, input: $input) {
      publishable {
        ... on Product {
          id
          title
        }
      }
      userErrors {
        field
        message
      }
    }
  }
`;

const PRODUCT_MANUFACTURER_LOGO_QUERY = `
  query productManufacturerLogo($id: ID!) {
    product(id: $id) {
      id
      manufacturerLogo: metafield(namespace: "product", key: "manufacturer_logo") {
        id
        value
      }
    }
  }
`;

const PRODUCT_VARIANTS_BY_SKU_QUERY = `
  query findProductVariantBySku($query: String!) {
    productVariants(first: 25, query: $query) {
      nodes {
        id
        sku
        product {
          id
          title
          handle
          variants(first: 100) {
            nodes {
              id
              sku
            }
          }
        }
      }
    }
  }
`;

const PRODUCTS_BY_TITLE_QUERY = `
  query findProductsByTitle($query: String!) {
    products(first: 25, query: $query) {
      nodes {
        id
        title
        handle
        variants(first: 100) {
          nodes {
            id
            sku
          }
        }
      }
    }
  }
`;

const PRODUCT_VERIFY_QUERY = `
  query productVerification($id: ID!) {
    product(id: $id) {
      id
      title
      status
      vendor
      tags
      handle
      metafields(first: 100, namespace: "product") {
        nodes {
          namespace
          key
          type
          value
        }
      }
      variants(first: 250) {
        nodes {
          id
          sku
          price
          compareAtPrice
          inventoryPolicy
          selectedOptions {
            name
            value
          }
          metafields(first: 30, namespace: "variant") {
            nodes {
              namespace
              key
              type
              value
            }
          }
        }
      }
    }
  }
`;

const PRODUCT_DELETE_MUTATION = `
  mutation deleteProduct($input: ProductDeleteInput!) {
    productDelete(input: $input) {
      deletedProductId
      userErrors {
        field
        message
      }
    }
  }
`;

const uploadedPdfUrlCache = new Map();
const productImageSequenceCache = new Map();
const productImageUrlExistsCache = new Map();
const uploadedWebpImageCache = new Map();
const uploadedDescriptionImageCache = new Map();
const manufacturerLogoMapCache = new Map();
const manufacturerLogoFileIdCache = new Map();
const fsp = fs.promises;
const PRODUCT_IMPORT_MAPPING_FILE = "product-import-mapping.json";
const MISSING_PRODUCT_IDENTITY_FILE = "missing-product-identity.json";

async function upsertMissingProductIdentityFile(filePath, newRows) {
  let existingRows = [];
  try {
    const raw = await fsp.readFile(filePath, "utf8");
    const parsed = JSON.parse(raw);
    if (Array.isArray(parsed)) existingRows = parsed;
  } catch (err) {
    if (err?.code !== "ENOENT") throw err;
  }

  const mergedByKey = new Map();
  for (const row of existingRows) {
    const key =
      String(row?.productcode || "").trim() ||
      String(row?.productid || "").trim();
    if (!key) continue;
    mergedByKey.set(key, row);
  }
  for (const row of newRows) {
    const key =
      String(row?.productcode || "").trim() ||
      String(row?.productid || "").trim();
    if (!key) continue;
    mergedByKey.set(key, row);
  }

  const mergedRows = [...mergedByKey.values()];
  await fsp.writeFile(filePath, JSON.stringify(mergedRows, null, 2), "utf8");
}

function normalizeSourceUrl(rawUrl, baseUrl = DEFAULT_SOURCE_FILE_BASE_URL) {
  const value = String(rawUrl || "").trim();
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
  if (/^https?:\/\//i.test(value)) {
    try {
      const parsed = new URL(value);
      if (
        parsed.hostname.toLowerCase() === "usalight.com" ||
        parsed.hostname.toLowerCase() === "www.usalight.com"
      ) {
        return encodePathAndQuery(
          `https://www.usalight.com${parsed.pathname}${parsed.search}${parsed.hash}`,
        );
      }
      return encodePathAndQuery(value);
    } catch {
      return value;
    }
  }
  if (value.startsWith("//")) return encodePathAndQuery(`https:${value}`);
  if (value.startsWith("/")) {
    return encodePathAndQuery(`${baseUrl.replace(/\/+$/, "")}${value}`);
  }
  return encodePathAndQuery(
    `${baseUrl.replace(/\/+$/, "")}/${value.replace(/^\/+/, "")}`,
  );
}

function isUsaLightSupportedFileLink(rawUrl) {
  const value = String(rawUrl || "").trim();
  if (!value) return false;
  if (value.startsWith("/")) {
    return /^\/v\/vspfiles(\/|$)/i.test(value);
  }
  if (/^https?:\/\//i.test(value)) {
    try {
      const parsed = new URL(value);
      const host = parsed.hostname.toLowerCase();
      if (host !== "www.usalight.com" && host !== "usalight.com") return false;
      return /^\/v\/vspfiles(\/|$)/i.test(parsed.pathname);
    } catch {
      return false;
    }
  }
  return false;
}

function extractLinkValues(html) {
  const result = [];
  const regex = /(href|src)\s*=\s*(["'])(.*?)\2/gi;
  const source = String(html || "");
  let match = regex.exec(source);
  while (match) {
    result.push(match[3]);
    match = regex.exec(source);
  }
  return result;
}

function fileNameFromUrl(url) {
  try {
    const pathname = new URL(url).pathname || "";
    return pathname.split("/").filter(Boolean).pop() || "document.pdf";
  } catch {
    return "document.pdf";
  }
}

function toShopifyNormalizedFileName(fileName) {
  let value = String(fileName || "").trim();
  if (!value) return "document.pdf";

  try {
    value = decodeURIComponent(value);
  } catch {
    // keep original value if decode fails
  }

  // Example: "UL Listed.png" -> "UL_20Listed.png"
  value = value.replace(/\s+/g, "_20");
  // Example: "UL%20Listed.png" -> "UL_20Listed.png"
  value = value.replace(/%/g, "_");

  return value;
}

function normalizedFileNameKey(fileName) {
  return toShopifyNormalizedFileName(fileName).toLowerCase();
}

function fileUrlFromNode(node) {
  return node?.url || node?.image?.url || null;
}

function replacePdfLinks(
  html,
  replacements,
  baseUrl = DEFAULT_SOURCE_FILE_BASE_URL,
) {
  return String(html || "").replace(
    /(href|src)\s*=\s*(["'])(.*?)\2/gi,
    (full, attr, quote, rawUrl) => {
      if (!isUsaLightSupportedFileLink(rawUrl)) return full;
      const sourceUrl = normalizeSourceUrl(rawUrl, baseUrl);
      const mappedUrl = replacements.get(sourceUrl);
      if (!mappedUrl) return full;
      return `${attr}=${quote}${mappedUrl}${quote}`;
    },
  );
}

function normalizeUsaLightPhotoUrl(rawUrl) {
  const normalized = normalizeSourceUrl(rawUrl, DEFAULT_SOURCE_FILE_BASE_URL);
  try {
    const parsed = new URL(normalized);
    const host = parsed.hostname.toLowerCase();
    if (host === "www.usalight.com" || host === "usalight.com") {
      return `https://www.usalight.com${parsed.pathname}${parsed.search}${parsed.hash}`;
    }
  } catch {
    // Ignore and return normalized fallback
  }
  return normalized;
}

function isGifPhotoUrl(url) {
  try {
    const pathname = new URL(String(url || "").trim()).pathname || "";
    return /\.gif$/i.test(pathname);
  } catch {
    return false;
  }
}

function withPhotoExtension(url, extensionWithDot) {
  try {
    const parsed = new URL(String(url || "").trim());
    parsed.pathname = parsed.pathname.replace(/\.[^./?#]+$/i, extensionWithDot);
    return parsed.toString();
  } catch {
    return String(url || "").trim();
  }
}

function parseSequentialPhotoPattern(photoUrl) {
  try {
    const parsed = new URL(photoUrl);
    const host = parsed.hostname.toLowerCase();
    if (host !== "www.usalight.com" && host !== "usalight.com") return null;
    if (!/^\/v\/vspfiles\/photos\//i.test(parsed.pathname)) return null;

    const parts = parsed.pathname.split("/");
    const fileName = parts.pop() || "";
    const match = fileName.match(/^(.*)-1(\.[^./?#]+)$/i);
    if (!match) return null;

    const baseName = match[1];
    const extension = match[2];
    const dirPath = parts.join("/");

    return {
      protocol: "https:",
      host: "www.usalight.com",
      dirPath,
      baseName,
      extension,
      search: parsed.search || "",
    };
  } catch {
    return null;
  }
}

function buildSequentialPhotoUrl(pattern, number) {
  return `${pattern.protocol}//${pattern.host}${pattern.dirPath}/${pattern.baseName}-${number}${pattern.extension}${pattern.search}`;
}

async function doesRemoteUrlExist(url) {
  if (productImageUrlExistsCache.has(url)) {
    return productImageUrlExistsCache.get(url);
  }

  let exists = false;
  try {
    const headRes = await fetch(url, { method: "HEAD", redirect: "follow" });
    if (headRes.ok) {
      exists = true;
    } else if ([403, 405].includes(headRes.status)) {
      const getRes = await fetch(url, { method: "GET", redirect: "follow" });
      exists = getRes.ok;
      try {
        await getRes.body?.cancel();
      } catch {
        // no-op
      }
    }
  } catch {
    exists = false;
  }

  productImageUrlExistsCache.set(url, exists);
  return exists;
}

async function discoverSequentialProductImages(primaryPhotoUrl) {
  const normalizedPrimary = normalizeUsaLightPhotoUrl(primaryPhotoUrl);
  const pattern = parseSequentialPhotoPattern(normalizedPrimary);
  if (!pattern) return [];

  if (productImageSequenceCache.has(normalizedPrimary)) {
    return productImageSequenceCache.get(normalizedPrimary);
  }

  const discovered = [];
  for (let n = 2; n <= MAX_SEQUENTIAL_PRODUCT_IMAGE_NUMBER; n++) {
    const candidate = buildSequentialPhotoUrl(pattern, n);
    const exists = await doesRemoteUrlExist(candidate);
    if (!exists) break;
    discovered.push(candidate);
  }

  productImageSequenceCache.set(normalizedPrimary, discovered);
  return discovered;
}

function buildPhotoUrlsFromClonedCode(clonedCode) {
  const code = String(clonedCode || "").trim();
  if (!code) return [];
  const encoded = encodeURIComponent(code);
  return [
    `https://www.usalight.com/v/vspfiles/photos/${encoded}-1.jpg`,
    `https://www.usalight.com/v/vspfiles/photos/${encoded}-1.png`,
  ].map((url) => normalizeUsaLightPhotoUrl(url));
}

async function resolvePrimaryPhotoForImport(rawPhotoUrl, sourceItem = null) {
  const primaryPhotoUrl = normalizeUsaLightPhotoUrl(rawPhotoUrl);
  if (!primaryPhotoUrl) {
    return { primaryPhotoUrl: "", allowSequentialDiscovery: false };
  }

  if (!isGifPhotoUrl(primaryPhotoUrl)) {
    return { primaryPhotoUrl, allowSequentialDiscovery: true };
  }

  const jpgCandidate = withPhotoExtension(primaryPhotoUrl, ".jpg");
  if (await doesRemoteUrlExist(jpgCandidate)) {
    return {
      primaryPhotoUrl: normalizeUsaLightPhotoUrl(jpgCandidate),
      allowSequentialDiscovery: true,
    };
  }

  const pngCandidate = withPhotoExtension(primaryPhotoUrl, ".png");
  if (await doesRemoteUrlExist(pngCandidate)) {
    return {
      primaryPhotoUrl: normalizeUsaLightPhotoUrl(pngCandidate),
      allowSequentialDiscovery: true,
    };
  }

  const clonedFromCandidates = buildPhotoUrlsFromClonedCode(
    sourceItem?.photos_cloned_from,
  );
  for (const candidate of clonedFromCandidates) {
    if (await doesRemoteUrlExist(candidate)) {
      return {
        primaryPhotoUrl: candidate,
        allowSequentialDiscovery: true,
      };
    }
  }

  // GIF source is intentionally ignored when no JPG/PNG fallback exists.
  return { primaryPhotoUrl: "", allowSequentialDiscovery: false };
}

async function waitForGenericFileReady(fileId, timeoutMs = 120000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const response = await shopifyClient.request(FILE_STATUS_QUERY, {
      variables: { id: fileId },
    });
    const fileNode = response.data?.node;
    if (!fileNode) throw new Error(`Generic file node not found: ${fileId}`);
    const fileUrl = fileUrlFromNode(fileNode);
    if (fileNode.fileStatus === "READY" && fileUrl) return fileUrl;
    if (fileNode.fileStatus === "FAILED") {
      throw new Error(`Generic file failed processing: ${fileId}`);
    }
    await sleep(1200);
  }
  throw new Error(`Timed out waiting for file READY: ${fileId}`);
}

function shopifyFileQueryFromFilename(fileName) {
  // filename search value should be quoted when special chars exist.
  const escaped = String(fileName || "").replace(/"/g, '\\"');
  return `filename:"${escaped}"`;
}

async function findExistingShopifyFileUrlByName(sourceOrFileName) {
  const isUrl = /^https?:\/\//i.test(String(sourceOrFileName || "").trim());
  const rawName = isUrl
    ? fileNameFromUrl(sourceOrFileName)
    : String(sourceOrFileName || "");
  const expectedName = toShopifyNormalizedFileName(rawName);
  const expectedKey = normalizedFileNameKey(expectedName);
  const response = await shopifyClient.request(FILES_BY_NAME_QUERY, {
    variables: {
      query: shopifyFileQueryFromFilename(expectedName),
    },
  });

  const nodes = response.data?.files?.nodes || [];
  for (const node of nodes) {
    if (!node?.id) continue;
    const nodeUrl = fileUrlFromNode(node);
    if (!nodeUrl) continue;
    let nodeFileName = "";
    try {
      nodeFileName = decodeURIComponent(
        new URL(nodeUrl).pathname.split("/").pop() || "",
      );
    } catch {
      continue;
    }
    if (normalizedFileNameKey(nodeFileName) !== expectedKey) continue;
    if (node.fileStatus === "READY") return nodeUrl;
    if (node.fileStatus === "PROCESSING") {
      return waitForGenericFileReady(node.id);
    }
  }
  return null;
}

async function uploadToStagedTarget(target, fileBuffer, filename, mimeType) {
  const form = new FormData();
  for (const p of target.parameters || []) {
    form.append(p.name, p.value);
  }
  const blob = new Blob([fileBuffer], { type: mimeType });
  form.append("file", blob, filename);

  const uploadRes = await fetch(target.url, {
    method: "POST",
    body: form,
  });

  if (!uploadRes.ok) {
    const body = await uploadRes.text();
    throw new Error(
      `Staged upload failed (${uploadRes.status}): ${body.slice(0, 300)}`,
    );
  }
}

function buildDeterministicWebpFilename(sourceUrl) {
  const sourceName = fileNameFromUrl(sourceUrl);
  const baseName = sourceName.replace(/\.[^./?#]+$/i, "");
  const hash = crypto
    .createHash("sha1")
    .update(String(sourceUrl))
    .digest("hex")
    .slice(0, 8);
  return toShopifyNormalizedFileName(`${baseName}-${hash}.webp`);
}

async function createShopifyWebpImageFromSourceUrl(sourceUrl) {
  if (uploadedWebpImageCache.has(sourceUrl)) {
    return uploadedWebpImageCache.get(sourceUrl);
  }

  const webpName = buildDeterministicWebpFilename(sourceUrl);

  const existing = await findExistingShopifyFileUrlByName(webpName);
  if (existing) {
    const existingObj = { id: null, url: existing, filename: webpName };
    uploadedWebpImageCache.set(sourceUrl, existingObj);
    return existingObj;
  }

  const imageRes = await fetch(sourceUrl);
  if (!imageRes.ok) {
    throw new Error(`Source image download failed (${imageRes.status})`);
  }
  const sourceBuffer = Buffer.from(await imageRes.arrayBuffer());
  const webpBuffer = await sharp(sourceBuffer)
    .webp({ quality: 100 })
    .toBuffer();

  async function requestStagedTarget(resourceType) {
    const response = await shopifyClient.request(
      STAGED_UPLOADS_CREATE_MUTATION,
      {
        variables: {
          input: [
            {
              filename: webpName,
              mimeType: "image/webp",
              resource: resourceType,
              httpMethod: "POST",
              fileSize: String(webpBuffer.length),
            },
          ],
        },
      },
    );

    const topLevelErrors = response?.errors || [];
    if (topLevelErrors.length > 0) {
      throw new Error(
        `stagedUploadsCreate(${resourceType}) GraphQL errors: ${topLevelErrors.map((e) => e.message).join("; ")}`,
      );
    }

    const stagedErrors = response.data?.stagedUploadsCreate?.userErrors || [];
    if (stagedErrors.length > 0) {
      throw new Error(
        `stagedUploadsCreate(${resourceType}) userErrors: ${stagedErrors.map((e) => e.message).join("; ")}`,
      );
    }

    const target = response.data?.stagedUploadsCreate?.stagedTargets?.[0];
    return { response, target };
  }

  // Primary: IMAGE, fallback: FILE (some stores/APIs return empty target for IMAGE+webp).
  let stagedResult = await requestStagedTarget("IMAGE");
  if (!stagedResult.target?.url || !stagedResult.target?.resourceUrl) {
    stagedResult = await requestStagedTarget("FILE");
  }

  const target = stagedResult.target;
  if (!target?.url || !target?.resourceUrl) {
    throw new Error(
      `No staged upload target returned for webp image (${sourceUrl}). stagedUploadsCreate payload: ${JSON.stringify(stagedResult.response?.data?.stagedUploadsCreate || {})}`,
    );
  }

  await uploadToStagedTarget(target, webpBuffer, webpName, "image/webp");

  const fileCreateResponse = await shopifyClient.request(FILE_CREATE_MUTATION, {
    variables: {
      files: [
        {
          contentType: "IMAGE",
          originalSource: target.resourceUrl,
          filename: webpName,
          duplicateResolutionMode: "APPEND_UUID",
        },
      ],
    },
  });

  const topLevelFileErrors = fileCreateResponse?.errors || [];
  if (topLevelFileErrors.length > 0) {
    throw new Error(
      `fileCreate GraphQL errors: ${topLevelFileErrors.map((e) => e.message).join("; ")}`,
    );
  }

  const fileErrors = fileCreateResponse.data?.fileCreate?.userErrors || [];
  if (fileErrors.length > 0) {
    throw new Error(fileErrors.map((e) => e.message).join("; "));
  }

  const file = fileCreateResponse.data?.fileCreate?.files?.[0];
  if (!file?.id) {
    throw new Error(
      `No image file returned from fileCreate. Response: ${JSON.stringify(fileCreateResponse.data?.fileCreate || {})}`,
    );
  }

  const fileUrl =
    file.fileStatus === "READY" && fileUrlFromNode(file)
      ? fileUrlFromNode(file)
      : await waitForGenericFileReady(file.id);

  const uploaded = { id: file.id, url: fileUrl, filename: webpName };
  uploadedWebpImageCache.set(sourceUrl, uploaded);
  return uploaded;
}

async function createShopifyFileFromSourceUrl(sourceUrl) {
  if (uploadedPdfUrlCache.has(sourceUrl)) {
    return uploadedPdfUrlCache.get(sourceUrl);
  }

  const existingUrl = await findExistingShopifyFileUrlByName(sourceUrl);
  if (existingUrl) {
    uploadedPdfUrlCache.set(sourceUrl, existingUrl);
    return existingUrl;
  }

  const response = await shopifyClient.request(FILE_CREATE_MUTATION, {
    variables: {
      files: [
        {
          contentType: "FILE",
          originalSource: sourceUrl,
          filename: toShopifyNormalizedFileName(fileNameFromUrl(sourceUrl)),
          duplicateResolutionMode: "APPEND_UUID",
        },
      ],
    },
  });

  const topLevelErrors = response?.errors || [];
  if (topLevelErrors.length > 0) {
    throw new Error(
      `fileCreate GraphQL errors: ${topLevelErrors.map((e) => e.message).join("; ")}`,
    );
  }

  const userErrors = response.data?.fileCreate?.userErrors || [];
  if (userErrors.length > 0) {
    throw new Error(
      userErrors.map((e) => `${e.code || "ERROR"}: ${e.message}`).join("; "),
    );
  }

  const file = response.data?.fileCreate?.files?.[0];
  if (!file?.id) {
    throw new Error(`No file returned from fileCreate for ${sourceUrl}`);
  }

  const url =
    file.fileStatus === "READY" && file.url
      ? file.url
      : await waitForGenericFileReady(file.id);

  uploadedPdfUrlCache.set(sourceUrl, url);
  return url;
}

async function createShopifyImageFromSourceUrl(sourceUrl) {
  if (uploadedDescriptionImageCache.has(sourceUrl)) {
    return uploadedDescriptionImageCache.get(sourceUrl);
  }

  const webpFile = await createShopifyWebpImageFromSourceUrl(sourceUrl);
  const url = webpFile?.url;
  if (!url) throw new Error(`No WebP image URL returned for ${sourceUrl}`);

  uploadedDescriptionImageCache.set(sourceUrl, url);
  return url;
}

async function prepareFileLinksForProduct(item) {
  const baseUrl =
    process.env.SOURCE_FILE_BASE_URL || DEFAULT_SOURCE_FILE_BASE_URL;
  const fieldsToProcess = [
    "productdescription_abovepricing",
    "productfeatures",
    "techspecs",
  ];

  const normalizedByField = {};
  const allSourceUrls = new Set();

  for (const field of fieldsToProcess) {
    const html = item?.[field];
    if (!html || !String(html).trim()) continue;

    const links = extractLinkValues(html);
    const normalizedUrls = [...new Set(links)]
      .filter((rawUrl) => isUsaLightSupportedFileLink(rawUrl))
      .map((rawUrl) => normalizeSourceUrl(rawUrl, baseUrl));

    if (normalizedUrls.length === 0) continue;
    normalizedByField[field] = { html, normalizedUrls };
    for (const url of normalizedUrls) allSourceUrls.add(url);
  }

  if (Object.keys(normalizedByField).length === 0) return item;

  const replacements = new Map();
  for (const sourceUrl of allSourceUrls) {
    try {
      const shopifyUrl = await createShopifyFileFromSourceUrl(sourceUrl);
      replacements.set(sourceUrl, shopifyUrl);
    } catch (err) {
      console.warn(
        `File upload failed for ${item.productcode || item.productname || "product"}: ${sourceUrl}`,
      );
      console.warn(err.message || String(err));
      replacements.set(sourceUrl, sourceUrl);
    }
  }

  const updated = { ...item };
  for (const field of Object.keys(normalizedByField)) {
    updated[field] = replacePdfHrefs(
      normalizedByField[field].html,
      replacements,
      baseUrl,
    );
  }
  return updated;
}

async function prepareProductDescriptionImages(item) {
  const html = item?.productdescription;
  if (!html || !String(html).trim()) return item;

  const baseUrl =
    process.env.SOURCE_FILE_BASE_URL || DEFAULT_SOURCE_FILE_BASE_URL;

  const srcRegex = /src\s*=\s*(["'])(.*?)\1/gi;
  const rawSrcs = [];
  let match = srcRegex.exec(String(html));
  while (match) {
    rawSrcs.push(match[2]);
    match = srcRegex.exec(String(html));
  }

  const sourceImageUrls = [...new Set(rawSrcs)]
    .filter((rawUrl) => isUsaLightSupportedFileLink(rawUrl))
    .map((rawUrl) => normalizeSourceUrl(rawUrl, baseUrl));

  if (sourceImageUrls.length === 0) return item;

  const replacements = new Map();
  for (const sourceUrl of sourceImageUrls) {
    try {
      const shopifyUrl = await createShopifyImageFromSourceUrl(sourceUrl);
      replacements.set(sourceUrl, shopifyUrl);
    } catch (err) {
      console.warn(
        `Description image upload failed for ${item.productcode || item.productname || "product"}: ${sourceUrl}`,
      );
      console.warn(err.message || String(err));
      replacements.set(sourceUrl, sourceUrl);
    }
  }

  const updatedHtml = String(html).replace(
    /src\s*=\s*(["'])(.*?)\1/gi,
    (full, quote, rawUrl) => {
      if (!isUsaLightSupportedFileLink(rawUrl)) return full;
      const normalized = normalizeSourceUrl(rawUrl, baseUrl);
      const replacement = replacements.get(normalized);
      if (!replacement) return full;
      return `src=${quote}${replacement}${quote}`;
    },
  );

  return {
    ...item,
    productdescription: updatedHtml,
  };
}

async function prepareProductImages(item) {
  const rawPrimary = item?.photourl;
  if (!rawPrimary || !String(rawPrimary).trim()) return item;

  const { primaryPhotoUrl, allowSequentialDiscovery } =
    await resolvePrimaryPhotoForImport(rawPrimary, item);
  if (!primaryPhotoUrl) {
    return {
      ...item,
      productImageFiles: [],
      photourl: "",
      additionalPhotoUrls: [],
    };
  }

  const additionalPhotoUrls = allowSequentialDiscovery
    ? await discoverSequentialProductImages(primaryPhotoUrl)
    : [];

  const sourcePhotoUrls = [
    primaryPhotoUrl,
    ...additionalPhotoUrls.map((url) => normalizeUsaLightPhotoUrl(url)),
  ];
  const gallerySourcePhotoUrls = sourcePhotoUrls.slice(1);

  const webpPhotoFiles = [];
  for (const sourceUrl of gallerySourcePhotoUrls) {
    try {
      const webpFile = await createShopifyWebpImageFromSourceUrl(sourceUrl);
      webpPhotoFiles.push(webpFile);
    } catch (err) {
      console.warn(
        `Product image upload failed for ${item?.productcode || item?.productname || "product"}: ${sourceUrl}`,
      );
      console.warn(err.message || String(err));
    }
  }

  return {
    ...item,
    productImageFiles: webpPhotoFiles,
    // Keep original primary image as source only; it is intentionally not uploaded.
    photourl: primaryPhotoUrl,
    additionalPhotoUrls: webpPhotoFiles.map((f) => f.url),
  };
}

// Backward-compatible alias used by prior code paths if referenced elsewhere.
function replacePdfHrefs(html, replacements, baseUrl) {
  return replacePdfLinks(html, replacements, baseUrl);
}

function shopifyStoreBaseUrl() {
  const value = String(process.env.SHOPIFY_STORE || "").trim();
  if (!value) return "";
  if (/^https?:\/\//i.test(value)) return value.replace(/\/+$/, "");
  return `https://${value.replace(/\/+$/, "")}`;
}

function buildShopifyProductUrl(handle) {
  const base = shopifyStoreBaseUrl();
  const h = String(handle || "").trim();
  if (!base || !h) return "";
  return `${base}/products/${h}`;
}

function normalizeManufacturerName(name) {
  const raw = String(name || "").trim();
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
  return aliases[key] || raw.toLowerCase();
}

function normalizeUrlForCompare(rawUrl) {
  try {
    const u = new URL(String(rawUrl || "").trim());
    return `${u.hostname.toLowerCase()}${decodeURIComponent(u.pathname)}`;
  } catch {
    return String(rawUrl || "")
      .trim()
      .toLowerCase();
  }
}

async function loadManufacturerLogoMap() {
  if (manufacturerLogoMapCache.size > 0) return manufacturerLogoMapCache;

  const filePath = path.join(process.cwd(), "product-manufacturer-logo.json");
  let parsed = {};
  try {
    const raw = await fsp.readFile(filePath, "utf8");
    parsed = JSON.parse(raw);
  } catch {
    parsed = {};
  }

  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    return manufacturerLogoMapCache;
  }

  for (const [manufacturerName, logoUrl] of Object.entries(parsed)) {
    const key = normalizeManufacturerName(manufacturerName);
    const value = String(logoUrl || "").trim();
    if (!key || !value) continue;
    manufacturerLogoMapCache.set(key, value);
  }

  return manufacturerLogoMapCache;
}

async function findExistingShopifyImageFileByUrl(sourceUrl) {
  const filename = fileNameFromUrl(sourceUrl);
  const expected = normalizeUrlForCompare(sourceUrl);
  const response = await shopifyClient.request(FILES_BY_NAME_QUERY, {
    variables: { query: shopifyFileQueryFromFilename(filename) },
  });

  const nodes = response?.data?.files?.nodes || [];
  for (const node of nodes) {
    const nodeUrl = fileUrlFromNode(node);
    if (!node?.id || !nodeUrl) continue;
    if (normalizeUrlForCompare(nodeUrl) !== expected) continue;
    if (node.fileStatus === "READY") return { id: node.id, url: nodeUrl };
    if (node.fileStatus === "PROCESSING") {
      const readyUrl = await waitForGenericFileReady(node.id);
      return { id: node.id, url: readyUrl };
    }
  }

  return null;
}

async function resolveManufacturerLogoFileId(sourceUrl) {
  const normalizedUrl = String(sourceUrl || "").trim();
  if (!normalizedUrl) return "";
  if (manufacturerLogoFileIdCache.has(normalizedUrl)) {
    return manufacturerLogoFileIdCache.get(normalizedUrl);
  }

  const existing = await findExistingShopifyImageFileByUrl(normalizedUrl);
  if (existing?.id) {
    manufacturerLogoFileIdCache.set(normalizedUrl, existing.id);
    return existing.id;
  }
  return "";
}

async function upsertProductMappingFile(mappingPath, newRows) {
  let existingRows = [];
  try {
    const raw = await fsp.readFile(mappingPath, "utf8");
    const parsed = JSON.parse(raw);
    if (Array.isArray(parsed)) existingRows = parsed;
  } catch (err) {
    if (err?.code !== "ENOENT") throw err;
  }

  const mergedByCode = new Map();
  for (const row of existingRows) {
    const key = String(row?.productcode || "").trim();
    if (!key) continue;
    mergedByCode.set(key, row);
  }
  for (const row of newRows) {
    const key = String(row?.productcode || "").trim();
    if (!key) continue;
    mergedByCode.set(key, row);
  }

  const mergedRows = [...mergedByCode.values()];
  await fsp.writeFile(mappingPath, JSON.stringify(mergedRows, null, 2), "utf8");
}

function normalizeIdentityValue(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

function normalizeSkuValue(value) {
  return String(value || "").trim().toLowerCase();
}

function quoteSearchValue(value) {
  const escaped = String(value || "")
    .replace(/\\/g, "\\\\")
    .replace(/"/g, '\\"');
  return `"${escaped}"`;
}

function buildVariantSkuMap(variantNodes = []) {
  const map = new Map();
  for (const variant of variantNodes) {
    const normalizedSku = normalizeSkuValue(variant?.sku);
    if (!normalizedSku || map.has(normalizedSku)) continue;
    map.set(normalizedSku, variant);
  }
  return map;
}

function pickMatchingProductByIdentity(products, sourceIdentity) {
  const normalizedName = normalizeIdentityValue(sourceIdentity?.name);
  const normalizedSku = normalizeSkuValue(sourceIdentity?.sku);

  for (const product of products || []) {
    const productName = normalizeIdentityValue(product?.title);
    if (normalizedName && productName !== normalizedName) continue;

    if (!normalizedSku) return product;

    const variants = product?.variants?.nodes || [];
    const hasSku = variants.some(
      (variant) => normalizeSkuValue(variant?.sku) === normalizedSku,
    );
    if (hasSku) return product;
  }

  return null;
}

function normalizeComparableText(value) {
  return String(value || "")
    .trim()
    .replace(/\s+/g, " ")
    .toLowerCase();
}

function normalizeComparableStatus(value) {
  return String(value || "")
    .trim()
    .toUpperCase();
}

function normalizeComparableMoney(value) {
  const raw = String(value ?? "").trim();
  if (!raw) return "";
  const n = Number.parseFloat(raw);
  if (!Number.isFinite(n)) return raw;
  return n.toFixed(2);
}

function normalizeComparableTags(value) {
  let tags = [];
  if (Array.isArray(value)) {
    tags = value;
  } else if (typeof value === "string") {
    tags = value.split(",");
  }

  return [
    ...new Set(
      tags
        .map((tag) => normalizeComparableText(tag))
        .filter(Boolean),
    ),
  ].sort();
}

function normalizeComparableOptionPairs(optionPairs = []) {
  return optionPairs
    .map((pair) => {
      const name = normalizeComparableText(pair?.name || pair?.optionName);
      const value = normalizeComparableText(pair?.value || pair?.optionValue);
      return name && value ? `${name}:${value}` : "";
    })
    .filter(Boolean)
    .sort();
}

function variantLookupKeyFromSnapshotLike(snapshotLike) {
  const normalizedSku = normalizeSkuValue(snapshotLike?.sku);
  if (normalizedSku) return `sku:${normalizedSku}`;

  const options = normalizeComparableOptionPairs(snapshotLike?.selectedOptions || []);
  return `opts:${options.join("|")}`;
}

function normalizeMetafieldValueByType(type, value) {
  const rawType = String(type || "").trim();
  const rawValue = String(value ?? "").trim();
  if (!rawValue) return "";

  if (rawType === "number_integer") {
    const n = Number.parseInt(rawValue, 10);
    return Number.isFinite(n) ? String(n) : rawValue;
  }
  if (rawType === "number_decimal") {
    return normalizeComparableMoney(rawValue);
  }
  if (rawType === "boolean") {
    return rawValue.toLowerCase() === "true" ? "true" : "false";
  }
  if (rawType === "json" || rawType.startsWith("list.")) {
    try {
      const parsed = JSON.parse(rawValue);
      return JSON.stringify(parsed);
    } catch {
      return rawValue;
    }
  }
  return rawValue;
}

function buildMetafieldMap(metafields = []) {
  const map = new Map();
  for (const mf of metafields || []) {
    const namespace = String(mf?.namespace || "").trim();
    const key = String(mf?.key || "").trim();
    if (!namespace || !key) continue;
    map.set(`${namespace}.${key}`, {
      type: String(mf?.type || "").trim(),
      value: String(mf?.value ?? ""),
    });
  }
  return map;
}

function buildExpectedProductSnapshot(source, locationId, options = {}) {
  const variantBuildReport = {};
  const input = buildProductSetInput(source, locationId, {
    categoryMap: options?.categoryMap || {},
    childProducts: Array.isArray(options?.childProducts) ? options.childProducts : [],
    variantBuildReport,
  });

  const expectedProductMetafields = buildMetafieldMap(buildMetafields(source));
  const expectedVariants = (input?.variants || []).map((variant) => {
    const selectedOptions = (variant?.optionValues || []).map((x) => ({
      name: x?.optionName,
      value: x?.name,
    }));
    const metafields = buildMetafieldMap(variant?.metafields || []);
    return {
      key: variantLookupKeyFromSnapshotLike({
        sku: variant?.sku,
        selectedOptions,
      }),
      sku: String(variant?.sku || ""),
      price: normalizeComparableMoney(variant?.price),
      compareAtPrice: normalizeComparableMoney(variant?.compareAtPrice),
      inventoryPolicy: normalizeComparableStatus(variant?.inventoryPolicy),
      selectedOptions: normalizeComparableOptionPairs(selectedOptions),
      metafields,
    };
  });

  return {
    title: normalizeComparableText(input?.title),
    status: normalizeComparableStatus(input?.status),
    vendor: normalizeComparableText(input?.vendor),
    tags: normalizeComparableTags(input?.tags),
    productMetafields: expectedProductMetafields,
    variants: expectedVariants,
    variantBuildReport,
  };
}

function buildActualProductSnapshot(product) {
  const productMetafields = buildMetafieldMap(
    product?.metafields?.nodes || [],
  );
  const variants = (product?.variants?.nodes || []).map((variant) => {
    const selectedOptions = (variant?.selectedOptions || []).map((x) => ({
      name: x?.name,
      value: x?.value,
    }));
    const metafields = buildMetafieldMap(variant?.metafields?.nodes || []);
    return {
      key: variantLookupKeyFromSnapshotLike({
        sku: variant?.sku,
        selectedOptions,
      }),
      sku: String(variant?.sku || ""),
      price: normalizeComparableMoney(variant?.price),
      compareAtPrice: normalizeComparableMoney(variant?.compareAtPrice),
      inventoryPolicy: normalizeComparableStatus(variant?.inventoryPolicy),
      selectedOptions: normalizeComparableOptionPairs(selectedOptions),
      metafields,
    };
  });

  return {
    title: normalizeComparableText(product?.title),
    status: normalizeComparableStatus(product?.status),
    vendor: normalizeComparableText(product?.vendor),
    tags: normalizeComparableTags(product?.tags || []),
    productMetafields,
    variants,
  };
}

function compareExpectedAndActualProductSnapshots(expected, actual) {
  const mismatches = [];
  const pushMismatch = (field, expectedValue, actualValue) => {
    mismatches.push({
      field,
      expected: expectedValue,
      actual: actualValue,
    });
  };

  if (expected.title !== actual.title) {
    pushMismatch("title", expected.title, actual.title);
  }
  if (expected.status !== actual.status) {
    pushMismatch("status", expected.status, actual.status);
  }
  if (expected.vendor !== actual.vendor) {
    pushMismatch("vendor", expected.vendor, actual.vendor);
  }

  const expectedTagsJson = JSON.stringify(expected.tags || []);
  const actualTagsJson = JSON.stringify(actual.tags || []);
  if (expectedTagsJson !== actualTagsJson) {
    pushMismatch("tags", expected.tags || [], actual.tags || []);
  }

  for (const [mfKey, expectedMf] of expected.productMetafields.entries()) {
    const actualMf = actual.productMetafields.get(mfKey);
    const expectedValue = normalizeMetafieldValueByType(
      expectedMf?.type,
      expectedMf?.value,
    );
    const actualValue = normalizeMetafieldValueByType(
      expectedMf?.type || actualMf?.type,
      actualMf?.value,
    );
    if (!actualMf) {
      pushMismatch(`product_metafield:${mfKey}`, expectedValue, null);
      continue;
    }
    if (expectedValue !== actualValue) {
      pushMismatch(`product_metafield:${mfKey}`, expectedValue, actualValue);
    }
  }

  const expectedVariantMap = new Map(
    expected.variants.map((variant) => [variant.key, variant]),
  );
  const actualVariantMap = new Map(
    actual.variants.map((variant) => [variant.key, variant]),
  );

  if (expectedVariantMap.size !== actualVariantMap.size) {
    pushMismatch(
      "variant_count",
      expectedVariantMap.size,
      actualVariantMap.size,
    );
  }

  for (const [variantKey, expectedVariant] of expectedVariantMap.entries()) {
    const actualVariant = actualVariantMap.get(variantKey);
    if (!actualVariant) {
      pushMismatch(`variant_missing:${variantKey}`, "present", "missing");
      continue;
    }

    if (normalizeSkuValue(expectedVariant.sku) !== normalizeSkuValue(actualVariant.sku)) {
      pushMismatch(
        `variant_sku:${variantKey}`,
        expectedVariant.sku,
        actualVariant.sku,
      );
    }
    if (expectedVariant.price !== actualVariant.price) {
      pushMismatch(
        `variant_price:${variantKey}`,
        expectedVariant.price,
        actualVariant.price,
      );
    }
    if (expectedVariant.compareAtPrice !== actualVariant.compareAtPrice) {
      pushMismatch(
        `variant_compareAtPrice:${variantKey}`,
        expectedVariant.compareAtPrice,
        actualVariant.compareAtPrice,
      );
    }
    if (expectedVariant.inventoryPolicy !== actualVariant.inventoryPolicy) {
      pushMismatch(
        `variant_inventoryPolicy:${variantKey}`,
        expectedVariant.inventoryPolicy,
        actualVariant.inventoryPolicy,
      );
    }

    const expectedOptions = JSON.stringify(expectedVariant.selectedOptions || []);
    const actualOptions = JSON.stringify(actualVariant.selectedOptions || []);
    if (expectedOptions !== actualOptions) {
      pushMismatch(
        `variant_selectedOptions:${variantKey}`,
        expectedVariant.selectedOptions || [],
        actualVariant.selectedOptions || [],
      );
    }

    for (const [mfKey, expectedMf] of expectedVariant.metafields.entries()) {
      const actualMf = actualVariant.metafields.get(mfKey);
      const expectedValue = normalizeMetafieldValueByType(
        expectedMf?.type,
        expectedMf?.value,
      );
      const actualValue = normalizeMetafieldValueByType(
        expectedMf?.type || actualMf?.type,
        actualMf?.value,
      );
      if (!actualMf) {
        pushMismatch(
          `variant_metafield:${variantKey}:${mfKey}`,
          expectedValue,
          null,
        );
        continue;
      }
      if (expectedValue !== actualValue) {
        pushMismatch(
          `variant_metafield:${variantKey}:${mfKey}`,
          expectedValue,
          actualValue,
        );
      }
    }
  }

  return {
    matches: mismatches.length === 0,
    mismatchCount: mismatches.length,
    mismatches,
  };
}

async function requestProductLookup(query, variables) {
  const response = await shopifyClient.request(query, { variables });
  const topLevelErrors = response?.errors || [];
  if (topLevelErrors.length > 0) {
    throw new Error(topLevelErrors.map((e) => e.message).join("; "));
  }
  return response;
}

async function findExistingShopifyProductByNameAndSku(sourceIdentity) {
  const name = String(sourceIdentity?.name || "").trim();
  const sku = String(sourceIdentity?.sku || "").trim();

  if (!name && !sku) return null;

  if (sku) {
    const bySkuResponse = await requestProductLookup(PRODUCT_VARIANTS_BY_SKU_QUERY, {
      query: `sku:${quoteSearchValue(sku)}`,
    });

    const skuProducts = (bySkuResponse?.data?.productVariants?.nodes || [])
      .map((node) => node?.product)
      .filter((node) => node && node.id);
    const matchedBySku = pickMatchingProductByIdentity(skuProducts, {
      name,
      sku,
    });
    if (matchedBySku) return matchedBySku;
  }

  if (!name) return null;

  const byTitleResponse = await requestProductLookup(PRODUCTS_BY_TITLE_QUERY, {
    query: `title:${quoteSearchValue(name)}`,
  });
  const titleProducts = byTitleResponse?.data?.products?.nodes || [];
  return pickMatchingProductByIdentity(titleProducts, { name, sku });
}

async function fetchShopifyProductForVerification(productId) {
  if (!String(productId || "").trim()) return null;
  const response = await shopifyClient.request(PRODUCT_VERIFY_QUERY, {
    variables: { id: productId },
  });
  const topLevelErrors = response?.errors || [];
  if (topLevelErrors.length > 0) {
    throw new Error(topLevelErrors.map((e) => e.message).join("; "));
  }
  return response?.data?.product || null;
}

async function deleteShopifyProductById(productId) {
  if (!String(productId || "").trim()) return { success: false, errors: [] };
  const response = await shopifyClient.request(PRODUCT_DELETE_MUTATION, {
    variables: { input: { id: productId } },
  });

  const topLevelErrors = response?.errors || [];
  if (topLevelErrors.length > 0) {
    return {
      success: false,
      errors: topLevelErrors.map((e) => ({ message: e.message })),
    };
  }

  const payload = response?.data?.productDelete;
  const userErrors = payload?.userErrors || [];
  if (userErrors.length > 0) {
    return { success: false, errors: userErrors };
  }

  return {
    success: Boolean(payload?.deletedProductId),
    deletedProductId: payload?.deletedProductId || "",
    errors: [],
  };
}

/**
 * Import a single product using productSet
 * @param {Object} source - Volusion product JSON
 * @param {string} locationId - Shopify location GID for inventory
 * @param {Object} importOptions - { categoryMap, childProducts }
 * @returns {Promise<{ success: boolean, product?: Object, errors?: Array }>}
 *          Variant line prices (including Choose Quantity = unit price × qty) are set in
 *          migration-mapper `buildProductSetInput`.
 */
async function importProduct(source, locationId, importOptions = {}) {
  const categoryMap = importOptions?.categoryMap || {};
  const childProducts = Array.isArray(importOptions?.childProducts)
    ? importOptions.childProducts
    : [];
  const variantBuildReport = {};
  const input = buildProductSetInput(source, locationId, {
    categoryMap,
    childProducts,
    variantBuildReport,
  });

  try {
    const response = await shopifyClient.request(PRODUCT_SET_MUTATION, {
      variables: { input, synchronous: true },
    });

    if (response.errors) {
      return { success: false, errors: response.errors };
    }

    const payload = response.data?.productSet;
    if (!payload) {
      return { success: false, errors: [{ message: "No payload returned" }] };
    }

    if (payload.userErrors?.length > 0) {
      return {
        success: false,
        errors: payload.userErrors,
        product: payload.product,
        variantBuildReport,
      };
    }

    return { success: true, product: payload.product, variantBuildReport };
  } catch (err) {
    return {
      success: false,
      errors: [{ message: err.message || String(err) }],
      variantBuildReport,
    };
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

/**
 * Publish a product to all available sales channels
 * @param {string} productId - gid://shopify/Product/xxx
 */

async function publishToAllChannels(productId) {
  const publicationIds = getPublicationIdsFromEnv();
  if (publicationIds.length === 0) {
    console.warn(
      "No publication IDs configured; set SHOPIFY_PUBLICATION_IDS or SHOPIFY_PUBLICATION_ID_1..N",
    );
    return { success: true };
  }

  const publicationInputs = publicationIds.map((id) => ({
    publicationId: id,
  }));

  try {
    const response = await shopifyClient.request(PUBLISHABLE_PUBLISH_MUTATION, {
      variables: {
        id: productId,
        input: publicationInputs,
      },
    });

    const userErrors = response.data?.publishablePublish?.userErrors || [];
    if (userErrors.length > 0) {
      const error = new Error("publishablePublish userErrors");
      error.userErrors = userErrors;
      throw error;
    }

    return { success: true };
  } catch (err) {
    // Don't fail product creation if publishing fails
    console.warn(
      `Failed to publish product ${productId} to channels:`,
      err.message || String(err),
    );
    return {
      success: false,
      errors: err.userErrors || [{ message: err.message || String(err) }],
    };
  }
}

/**
 * Ensure metafields exist via metafieldsSet (creates if not present)
 * Use when productCreate was used without metafields
 * @param {string} productId - gid://shopify/Product/xxx
 * @param {Object} source - Volusion product JSON
 */
async function ensureMetafields(productId, source) {
  const metafields = buildMetafieldsSetPayload(productId, source);
  if (metafields.length === 0) return { success: true };

  const batches = [];
  for (let i = 0; i < metafields.length; i += METAFIELDS_BATCH_SIZE) {
    batches.push(metafields.slice(i, i + METAFIELDS_BATCH_SIZE));
  }

  for (const batch of batches) {
    const response = await shopifyClient.request(METAFIELDS_SET_MUTATION, {
      variables: { metafields: batch },
    });
    const userErrors = response.data?.metafieldsSet?.userErrors || [];
    if (userErrors.length > 0) {
      // Surface detailed errors so the caller can log/inspect them
      const error = new Error("metafieldsSet userErrors");
      error.userErrors = userErrors;
      throw error;
    }
    await sleep(100);
  }
  return { success: true };
}

async function isManufacturerLogoAlreadySet(productId) {
  const response = await shopifyClient.request(
    PRODUCT_MANUFACTURER_LOGO_QUERY,
    {
      variables: { id: productId },
    },
  );
  const value = response?.data?.product?.manufacturerLogo?.value;
  return Boolean(String(value || "").trim());
}

async function ensureManufacturerLogoMetafield(productId, source) {
  const manufacturerName = normalizeManufacturerName(
    source?.productmanufacturer,
  );
  if (!manufacturerName)
    return { skipped: true, reason: "missing-manufacturer" };

  const logoMap = await loadManufacturerLogoMap();
  const logoUrl = String(logoMap.get(manufacturerName) || "").trim();
  if (!logoUrl) return { skipped: true, reason: "no-logo-match" };

  const alreadySet = await isManufacturerLogoAlreadySet(productId);
  if (alreadySet) return { skipped: true, reason: "already-populated" };

  const fileId = await resolveManufacturerLogoFileId(logoUrl);
  if (!fileId) return { skipped: true, reason: "file-id-missing" };

  const response = await shopifyClient.request(METAFIELDS_SET_MUTATION, {
    variables: {
      metafields: [
        {
          ownerId: productId,
          namespace: "product",
          key: "manufacturer_logo",
          type: "file_reference",
          value: fileId,
        },
      ],
    },
  });

  const userErrors = response?.data?.metafieldsSet?.userErrors || [];
  if (userErrors.length > 0) {
    throw new Error(userErrors.map((e) => e.message).join("; "));
  }

  return { success: true };
}

function normalizeProductCode(value) {
  return String(value || "").trim();
}

function isVariantControlledParent(product) {
  return (
    String(product?.enableoptions_inventorycontrol || "")
      .trim()
      .toUpperCase() === "Y" &&
    String(product?.optionids || "").trim() !== ""
  );
}

async function prepareChildVariantsForParent(parentItem, childItems = []) {
  if (!Array.isArray(childItems) || childItems.length === 0) return [];

  const parentResolved = await resolvePrimaryPhotoForImport(
    parentItem?.photourl || "",
    parentItem,
  );
  const parentPrimaryPhoto = parentResolved.primaryPhotoUrl;
  const preparedChildren = [];

  for (const child of childItems) {
    const preparedChild = { ...child };
    const rawPhoto = String(child?.photourl || "").trim();
    if (rawPhoto) {
      const childResolved = await resolvePrimaryPhotoForImport(rawPhoto, child);
      const normalizedPhoto = childResolved.primaryPhotoUrl;
      preparedChild.photourl = normalizedPhoto;
      if (normalizedPhoto && normalizedPhoto !== parentPrimaryPhoto) {
        try {
          preparedChild.variantImageFile =
            await createShopifyWebpImageFromSourceUrl(normalizedPhoto);
        } catch (err) {
          console.warn(
            `Variant image upload failed for ${child?.productcode || child?.productname || "variant"}: ${normalizedPhoto}`,
          );
          console.warn(err.message || String(err));
        }
      }
    }
    preparedChildren.push(preparedChild);
  }

  return preparedChildren;
}

/**
 * Bulk import products with rate limiting
 * @param {Array} products - Array of Volusion product JSON
 * @param {Object} options - { locationId, delayMs, onProgress }
 */
async function bulkImport(products, options = {}) {
  const {
    locationId = process.env.SHOPIFY_LOCATION_ID,
    delayMs = DEFAULT_DELAY_MS,
    flushEvery = DEFAULT_MAPPING_FLUSH_EVERY,
    onProgress = () => {},
    allProducts = null, // full products array for child-product lookup
    skipExistingLookup = false,
  } = options;

  if (!locationId) {
    console.warn(
      "SHOPIFY_LOCATION_ID not set; products will be created without inventory quantities",
    );
  }

  const categoryMap = loadCategoryMap();
  const results = { created: 0, failed: 0, skipped: 0, errors: [] };
  const mappingReportPath = path.join(
    process.cwd(),
    PRODUCT_IMPORT_MAPPING_FILE,
  );
  const pendingMappingRows = [];
  const mappingFlushEvery =
    Number.isFinite(flushEvery) && flushEvery > 0
      ? Math.floor(flushEvery)
      : DEFAULT_MAPPING_FLUSH_EVERY;
  const missingIdentityRows = [];
  const variantMappedChildCodes = new Set();

  async function flushMappingRows(force = false) {
    if (!force && pendingMappingRows.length < mappingFlushEvery) return;
    if (pendingMappingRows.length === 0) return;
    const rowsToWrite = pendingMappingRows.splice(0, pendingMappingRows.length);
    await upsertProductMappingFile(mappingReportPath, rowsToWrite);
  }

  async function queueMappingRows(rows) {
    const list = Array.isArray(rows) ? rows : [rows];
    for (const row of list) {
      if (!row || typeof row !== "object") continue;
      pendingMappingRows.push(row);
    }
    await flushMappingRows(false);
  }

  const sourceUniverse =
    Array.isArray(allProducts) && allProducts.length > 0 ? allProducts : products;
  const productByCode = new Map();
  const childrenByParentCode = new Map();

  for (const sourceItem of sourceUniverse) {
    const code = normalizeProductCode(sourceItem?.productcode);
    if (code) productByCode.set(code, sourceItem);

    const parentCode = normalizeProductCode(sourceItem?.ischildofproductcode);
    if (!parentCode) continue;
    if (!childrenByParentCode.has(parentCode)) {
      childrenByParentCode.set(parentCode, []);
    }
    childrenByParentCode.get(parentCode).push(sourceItem);
  }

  for (let i = 0; i < products.length; i++) {
    const item = products[i];
    const code = normalizeProductCode(item?.productcode);
    const parentCode = normalizeProductCode(item?.ischildofproductcode);

    if (parentCode) {
      const parentSource = productByCode.get(parentCode);
      if (parentSource && isVariantControlledParent(parentSource)) {
        results.skipped++;
        if (!variantMappedChildCodes.has(code)) {
          await queueMappingRows({
            productcode: code,
            productname: String(item?.productname || ""),
            producturl: String(item?.producturl || ""),
            parent_productcode: parentCode,
            is_variant: "Y",
            mapping_type: "child_variant",
            import_status: "skipped_child_row",
          });
        }
        onProgress({
          index: i + 1,
          total: products.length,
          product: item,
          status: "skipped",
        });
        continue;
      }
    }

    const productName = String(item?.productname || "").trim();
    const vendorPartNo = String(item?.vendor_partno || "").trim();
    if (!productName && !vendorPartNo) {
      results.skipped++;
      const missingRow = {
        productcode: code,
        productid: String(item?.productid || "").trim(),
        producturl: String(item?.producturl || ""),
        reason: "missing_productname_and_vendor_partno",
      };
      missingIdentityRows.push(missingRow);
      await queueMappingRows({
        productcode: code,
        productname: "",
        producturl: String(item?.producturl || ""),
        import_status: "skipped_missing_identity",
      });
      onProgress({
        index: i + 1,
        total: products.length,
        product: item,
        status: "skipped",
      });
      continue;
    }

    const sourceIdentity = buildSourceProductIdentity(item);
    const parentProductCode = normalizeProductCode(item?.productcode);
    const childRowsForVariant =
      isVariantControlledParent(item) && parentProductCode
        ? (childrenByParentCode.get(parentProductCode) || []).filter(
            (child) =>
              normalizeProductCode(child?.productcode) !== parentProductCode,
          )
        : [];

    let existingProduct = null;
    if (!skipExistingLookup) {
      try {
        existingProduct = await findExistingShopifyProductByNameAndSku(
          sourceIdentity,
        );
      } catch (lookupErr) {
        console.warn(
          `Existing product lookup failed for ${item?.productcode || item?.productname || `#${i + 1}`}`,
        );
        console.warn(lookupErr.message || String(lookupErr));
      }
    }

    if (existingProduct?.id) {
      results.skipped++;
      const existingVariantNodes = existingProduct?.variants?.nodes || [];
      const existingVariantBySku = buildVariantSkuMap(existingVariantNodes);

      await queueMappingRows({
        productcode: String(item?.productcode || ""),
        productname: String(item?.productname || ""),
        producturl: String(item?.producturl || ""),
        mapping_type:
          childRowsForVariant.length > 0 || existingVariantNodes.length > 1
            ? "parent_with_variants"
            : "standalone_product",
        variant_child_count: childRowsForVariant.length,
        variant_total_count: existingVariantNodes.length,
        variant_missing_combo_count: 0,
        variant_missing_option_ids: [],
        variant_missing_mappings: [],
        import_status: "mapped_existing_shopify_product",
        shopify_product_id: String(existingProduct?.id || ""),
        shopify_product_name: String(existingProduct?.title || ""),
        shopify_product_url: buildShopifyProductUrl(existingProduct?.handle),
        shopify_variant_skus: existingVariantNodes
          .map((variant) => String(variant?.sku || "").trim())
          .filter(Boolean),
      });

      if (childRowsForVariant.length > 0) {
        const childVariantRows = [];
        for (const childVariant of childRowsForVariant) {
          const childCode = normalizeProductCode(childVariant?.productcode);
          const childSku = normalizeProductCode(
            childVariant?.productcode || childVariant?.vendor_partno,
          );
          const matchedVariant = childSku
            ? existingVariantBySku.get(normalizeSkuValue(childSku))
            : null;
          if (childCode) variantMappedChildCodes.add(childCode);

          childVariantRows.push({
            productcode: childCode,
            productname: String(childVariant?.productname || ""),
            producturl: String(childVariant?.producturl || ""),
            parent_productcode: String(item?.productcode || ""),
            is_variant: "Y",
            mapping_type: "child_variant",
            import_status: matchedVariant?.id
              ? "variant_mapped_existing_shopify_product"
              : "variant_not_matched_existing_shopify_product",
            shopify_product_id: String(existingProduct?.id || ""),
            shopify_product_name: String(existingProduct?.title || ""),
            shopify_product_url: buildShopifyProductUrl(existingProduct?.handle),
            shopify_variant_id: String(matchedVariant?.id || ""),
            shopify_variant_sku: String(
              matchedVariant?.sku || childSku || "",
            ),
          });
        }

        if (childVariantRows.length > 0) {
          await queueMappingRows(childVariantRows);
        }
      }

      onProgress({
        index: i + 1,
        total: products.length,
        product: existingProduct,
        status: "skipped",
      });

      if (delayMs > 0) await sleep(delayMs);
      continue;
    }

    let preparedLinksItem = item;
    try {
      preparedLinksItem = await prepareFileLinksForProduct(item);
    } catch (err) {
      console.warn(
        `File-link preparation failed for ${item?.productcode || item?.productname || `#${i + 1}`}`,
      );
      console.warn(err.message || String(err));
    }

    let preparedDescriptionItem = preparedLinksItem;
    try {
      preparedDescriptionItem =
        await prepareProductDescriptionImages(preparedLinksItem);
    } catch (err) {
      console.warn(
        `Description image preparation failed for ${item?.productcode || item?.productname || `#${i + 1}`}`,
      );
      console.warn(err.message || String(err));
    }

    let preparedItem = preparedDescriptionItem;
    try {
      preparedItem = await prepareProductImages(preparedDescriptionItem);
    } catch (err) {
      console.warn(
        `Product image preparation failed for ${item?.productcode || item?.productname || `#${i + 1}`}`,
      );
      console.warn(err.message || String(err));
    }

    let preparedChildVariants = [];
    if (childRowsForVariant.length > 0) {
      try {
        preparedChildVariants = await prepareChildVariantsForParent(
          preparedItem,
          childRowsForVariant,
        );
      } catch (err) {
        console.warn(
          `Child variant image preparation failed for ${preparedItem?.productcode || preparedItem?.productname || `#${i + 1}`}`,
        );
        console.warn(err.message || String(err));
        preparedChildVariants = childRowsForVariant;
      }
    }

    const result = await importProduct(preparedItem, locationId, {
      categoryMap,
      childProducts: preparedChildVariants,
    });

    if (result.success) {
      // Ensure metafields are created or updated for this product
      try {
        await ensureMetafields(result.product.id, preparedItem);
      } catch (metaErr) {
        // If metafield setting fails, record the error but don't block product creation
        results.errors.push({
          product:
            preparedItem.productname || preparedItem.productcode || `#${i + 1}`,
          errors: [
            {
              message: `Metafields error: ${metaErr.message || String(metaErr)}`,
            },
          ],
        });
      }

      // Set product.manufacturer_logo from product-manufacturer-logo.json (skip if populated).
      try {
        await ensureManufacturerLogoMetafield(result.product.id, preparedItem);
      } catch (logoErr) {
        results.errors.push({
          product:
            preparedItem.productname || preparedItem.productcode || `#${i + 1}`,
          errors: [
            {
              message: `Manufacturer logo metafield error: ${logoErr.message || String(logoErr)}`,
            },
          ],
        });
      }

      // Publish product to all sales channels
      try {
        await publishToAllChannels(result.product.id);
      } catch (publishErr) {
        // Log but don't block - product is created, publishing can be retried
        console.warn(
          `Failed to publish product ${result.product.id} to channels:`,
          publishErr.message || String(publishErr),
        );
      }

      results.created++;
      const variantNodes = result?.product?.variants?.nodes || [];
      const variantBuildReport =
        result?.variantBuildReport && typeof result.variantBuildReport === "object"
          ? result.variantBuildReport
          : {};
      const variantBySku = buildVariantSkuMap(variantNodes);

      await queueMappingRows({
        productcode: String(preparedItem?.productcode || ""),
        productname: String(preparedItem?.productname || ""),
        producturl: String(preparedItem?.producturl || ""),
        mapping_type:
          preparedChildVariants.length > 0 ||
          String(variantBuildReport.source || "") === "child-products"
            ? "parent_with_variants"
            : "standalone_product",
        variant_child_count: preparedChildVariants.length,
        variant_total_count: Number(variantBuildReport.variantCount || variantNodes.length || 0),
        variant_missing_combo_count: Array.isArray(variantBuildReport.missingMappings)
          ? variantBuildReport.missingMappings.length
          : 0,
        variant_missing_option_ids: Array.isArray(variantBuildReport.missingOptionIds)
          ? variantBuildReport.missingOptionIds
          : [],
        variant_missing_mappings: Array.isArray(variantBuildReport.missingMappings)
          ? variantBuildReport.missingMappings
          : [],
        shopify_product_id: String(result?.product?.id || ""),
        shopify_product_name: String(result?.product?.title || ""),
        shopify_product_url: buildShopifyProductUrl(result?.product?.handle),
        shopify_variant_skus: variantNodes.map((v) => v.sku).filter(Boolean),
      });

      const childVariantRows = [];
      for (const childVariant of preparedChildVariants) {
        const childCode = normalizeProductCode(childVariant?.productcode);
        const childSku = normalizeProductCode(
          childVariant?.productcode || childVariant?.vendor_partno,
        );
        const matchedVariant = childSku
          ? variantBySku.get(normalizeSkuValue(childSku))
          : null;

        if (childCode) variantMappedChildCodes.add(childCode);
        childVariantRows.push({
          productcode: childCode,
          productname: String(childVariant?.productname || ""),
          producturl: String(childVariant?.producturl || ""),
          parent_productcode: String(preparedItem?.productcode || ""),
          is_variant: "Y",
          mapping_type: "child_variant",
          import_status: matchedVariant?.id ? "variant_mapped" : "variant_not_matched",
          shopify_product_id: String(result?.product?.id || ""),
          shopify_product_name: String(result?.product?.title || ""),
          shopify_product_url: buildShopifyProductUrl(result?.product?.handle),
          shopify_variant_id: String(matchedVariant?.id || ""),
          shopify_variant_sku: String(matchedVariant?.sku || childSku || ""),
        });
      }

      if (childVariantRows.length > 0) {
        await queueMappingRows(childVariantRows);
      }

      onProgress({
        index: i + 1,
        total: products.length,
        product: result.product,
        status: "created",
      });
    } else {
      results.failed++;
      results.errors.push({
        product:
          preparedItem.productname || preparedItem.productcode || `#${i + 1}`,
        errors: result.errors,
      });
      onProgress({
        index: i + 1,
        total: products.length,
        product: preparedItem,
        status: "failed",
        errors: result.errors,
      });
    }

    await sleep(delayMs);
  }

  await flushMappingRows(true);
  results.mappingReportFile = mappingReportPath;

  const missingIdentityPath = path.join(
    process.cwd(),
    MISSING_PRODUCT_IDENTITY_FILE,
  );
  await upsertMissingProductIdentityFile(missingIdentityPath, missingIdentityRows);
  results.missingIdentityFile = missingIdentityPath;

  return results;
}

/**
 * Verify already-migrated products against current mapper logic.
 * If mismatch is detected, delete and recreate product using current import flow.
 *
 * @param {Array} products
 * @param {Object} options
 * @returns {Promise<Object>}
 */
async function verifyAndRemigrateProducts(products, options = {}) {
  const {
    locationId = process.env.SHOPIFY_LOCATION_ID,
    delayMs = DEFAULT_DELAY_MS,
    flushEvery = DEFAULT_MAPPING_FLUSH_EVERY,
    onProgress = () => {},
    allProducts = null,
    dryRun = false,
  } = options;

  const categoryMap = loadCategoryMap();
  const mappingReportPath = path.join(
    process.cwd(),
    PRODUCT_IMPORT_MAPPING_FILE,
  );
  const pendingMappingRows = [];
  const mappingFlushEvery =
    Number.isFinite(flushEvery) && flushEvery > 0
      ? Math.floor(flushEvery)
      : DEFAULT_MAPPING_FLUSH_EVERY;

  const results = {
    checked: 0,
    matched: 0,
    mismatched: 0,
    recreated: 0,
    recreateFailed: 0,
    missing: 0,
    skipped: 0,
    dryRun: Boolean(dryRun),
    errors: [],
    mismatchSamples: [],
  };

  async function flushMappingRows(force = false) {
    if (!force && pendingMappingRows.length < mappingFlushEvery) return;
    if (pendingMappingRows.length === 0) return;
    const rowsToWrite = pendingMappingRows.splice(0, pendingMappingRows.length);
    await upsertProductMappingFile(mappingReportPath, rowsToWrite);
  }

  async function queueMappingRows(rows) {
    const list = Array.isArray(rows) ? rows : [rows];
    for (const row of list) {
      if (!row || typeof row !== "object") continue;
      pendingMappingRows.push(row);
    }
    await flushMappingRows(false);
  }

  const sourceUniverse =
    Array.isArray(allProducts) && allProducts.length > 0 ? allProducts : products;
  const productByCode = new Map();
  const childrenByParentCode = new Map();

  for (const sourceItem of sourceUniverse) {
    const code = normalizeProductCode(sourceItem?.productcode);
    if (code) productByCode.set(code, sourceItem);

    const parentCode = normalizeProductCode(sourceItem?.ischildofproductcode);
    if (!parentCode) continue;
    if (!childrenByParentCode.has(parentCode)) {
      childrenByParentCode.set(parentCode, []);
    }
    childrenByParentCode.get(parentCode).push(sourceItem);
  }

  for (let i = 0; i < products.length; i++) {
    const item = products[i];
    const code = normalizeProductCode(item?.productcode);
    const parentCode = normalizeProductCode(item?.ischildofproductcode);
    const checkedAt = new Date().toISOString();

    if (parentCode) {
      const parentSource = productByCode.get(parentCode);
      if (parentSource && isVariantControlledParent(parentSource)) {
        results.skipped++;
        onProgress({
          index: i + 1,
          total: products.length,
          product: item,
          status: "skipped_child_row",
        });
        continue;
      }
    }

    const productName = String(item?.productname || "").trim();
    const vendorPartNo = String(item?.vendor_partno || "").trim();
    if (!productName && !vendorPartNo) {
      results.skipped++;
      await queueMappingRows({
        productcode: code,
        productname: "",
        producturl: String(item?.producturl || ""),
        import_status: "verify_skipped_missing_identity",
        verification_status: "skipped",
        verification_checked_at: checkedAt,
      });
      onProgress({
        index: i + 1,
        total: products.length,
        product: item,
        status: "skipped_missing_identity",
      });
      continue;
    }

    results.checked++;
    const sourceIdentity = buildSourceProductIdentity(item);
    let existingProduct = null;

    try {
      existingProduct = await findExistingShopifyProductByNameAndSku(
        sourceIdentity,
      );
    } catch (lookupErr) {
      results.errors.push({
        product: code || productName || `#${i + 1}`,
        errors: [{ message: lookupErr.message || String(lookupErr) }],
      });
    }

    if (!existingProduct?.id) {
      results.missing++;
      await queueMappingRows({
        productcode: code,
        productname: String(item?.productname || ""),
        producturl: String(item?.producturl || ""),
        import_status: "verify_missing_shopify_product",
        verification_status: "missing",
        verification_checked_at: checkedAt,
      });
      onProgress({
        index: i + 1,
        total: products.length,
        product: item,
        status: "missing",
      });
      if (delayMs > 0) await sleep(delayMs);
      continue;
    }

    const parentProductCode = normalizeProductCode(item?.productcode);
    const childRowsForVariant =
      isVariantControlledParent(item) && parentProductCode
        ? (childrenByParentCode.get(parentProductCode) || []).filter(
            (child) =>
              normalizeProductCode(child?.productcode) !== parentProductCode,
          )
        : [];

    let actualProduct = null;
    try {
      actualProduct = await fetchShopifyProductForVerification(existingProduct.id);
    } catch (verifyFetchErr) {
      results.errors.push({
        product: code || productName || `#${i + 1}`,
        errors: [{ message: verifyFetchErr.message || String(verifyFetchErr) }],
      });
    }

    if (!actualProduct?.id) {
      results.missing++;
      await queueMappingRows({
        productcode: code,
        productname: String(item?.productname || ""),
        producturl: String(item?.producturl || ""),
        import_status: "verify_missing_shopify_product_after_lookup",
        verification_status: "missing",
        verification_checked_at: checkedAt,
      });
      onProgress({
        index: i + 1,
        total: products.length,
        product: item,
        status: "missing",
      });
      if (delayMs > 0) await sleep(delayMs);
      continue;
    }

    const expectedSnapshot = buildExpectedProductSnapshot(item, locationId, {
      categoryMap,
      childProducts: childRowsForVariant,
    });
    const actualSnapshot = buildActualProductSnapshot(actualProduct);
    const comparison = compareExpectedAndActualProductSnapshots(
      expectedSnapshot,
      actualSnapshot,
    );

    if (comparison.matches) {
      results.matched++;
      const actualVariants = actualProduct?.variants?.nodes || [];

      await queueMappingRows({
        productcode: code,
        productname: String(item?.productname || ""),
        producturl: String(item?.producturl || ""),
        mapping_type:
          childRowsForVariant.length > 0 || actualVariants.length > 1
            ? "parent_with_variants"
            : "standalone_product",
        variant_child_count: childRowsForVariant.length,
        variant_total_count: actualVariants.length,
        variant_missing_combo_count: 0,
        variant_missing_option_ids: [],
        variant_missing_mappings: [],
        import_status: "verified_match_existing_shopify_product",
        verification_status: "matched",
        verification_checked_at: checkedAt,
        verification_mismatch_count: 0,
        shopify_product_id: String(actualProduct?.id || ""),
        shopify_product_name: String(actualProduct?.title || ""),
        shopify_product_url: buildShopifyProductUrl(actualProduct?.handle),
        shopify_variant_skus: actualVariants
          .map((variant) => String(variant?.sku || "").trim())
          .filter(Boolean),
      });

      onProgress({
        index: i + 1,
        total: products.length,
        product: actualProduct,
        status: "matched",
      });
      if (delayMs > 0) await sleep(delayMs);
      continue;
    }

    results.mismatched++;
    const mismatchSummary = {
      productcode: code,
      shopify_product_id: String(actualProduct?.id || ""),
      mismatch_count: comparison.mismatchCount,
      mismatch_fields: comparison.mismatches.slice(0, 20).map((m) => m.field),
    };
    results.mismatchSamples.push(mismatchSummary);

    if (dryRun) {
      await queueMappingRows({
        productcode: code,
        productname: String(item?.productname || ""),
        producturl: String(item?.producturl || ""),
        import_status: "verify_mismatch_detected_dry_run",
        verification_status: "mismatched",
        verification_checked_at: checkedAt,
        verification_mismatch_count: comparison.mismatchCount,
        verification_mismatch_fields: mismatchSummary.mismatch_fields,
        shopify_product_id: String(actualProduct?.id || ""),
        shopify_product_name: String(actualProduct?.title || ""),
        shopify_product_url: buildShopifyProductUrl(actualProduct?.handle),
      });
      onProgress({
        index: i + 1,
        total: products.length,
        product: actualProduct,
        status: "mismatch_dry_run",
      });
      if (delayMs > 0) await sleep(delayMs);
      continue;
    }

    const deleteResult = await deleteShopifyProductById(actualProduct.id);
    if (!deleteResult.success) {
      results.recreateFailed++;
      results.errors.push({
        product: code || productName || `#${i + 1}`,
        errors: deleteResult.errors || [{ message: "Failed to delete product" }],
      });
      await queueMappingRows({
        productcode: code,
        productname: String(item?.productname || ""),
        producturl: String(item?.producturl || ""),
        import_status: "verify_mismatch_delete_failed",
        verification_status: "mismatched",
        verification_checked_at: checkedAt,
        verification_mismatch_count: comparison.mismatchCount,
        verification_mismatch_fields: mismatchSummary.mismatch_fields,
        shopify_product_id: String(actualProduct?.id || ""),
      });
      onProgress({
        index: i + 1,
        total: products.length,
        product: actualProduct,
        status: "delete_failed",
      });
      if (delayMs > 0) await sleep(delayMs);
      continue;
    }

    const recreateResult = await bulkImport([item], {
      locationId,
      delayMs: 0,
      flushEvery: 1,
      allProducts: sourceUniverse,
      skipExistingLookup: true,
      onProgress: () => {},
    });

    if ((recreateResult?.created || 0) > 0) {
      results.recreated++;
      onProgress({
        index: i + 1,
        total: products.length,
        product: item,
        status: "recreated",
      });
    } else {
      results.recreateFailed++;
      results.errors.push({
        product: code || productName || `#${i + 1}`,
        errors:
          recreateResult?.errors?.length > 0
            ? recreateResult.errors
            : [{ message: "Recreate failed after delete" }],
      });
      await queueMappingRows({
        productcode: code,
        productname: String(item?.productname || ""),
        producturl: String(item?.producturl || ""),
        import_status: "verify_mismatch_recreate_failed",
        verification_status: "mismatched",
        verification_checked_at: checkedAt,
        verification_mismatch_count: comparison.mismatchCount,
        verification_mismatch_fields: mismatchSummary.mismatch_fields,
      });
      onProgress({
        index: i + 1,
        total: products.length,
        product: item,
        status: "recreate_failed",
      });
    }

    if (delayMs > 0) await sleep(delayMs);
  }

  await flushMappingRows(true);
  results.mappingReportFile = mappingReportPath;
  return results;
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

module.exports = {
  importProduct,
  ensureMetafields,
  bulkImport,
  verifyAndRemigrateProducts,
  PRODUCT_SET_MUTATION,
  METAFIELDS_SET_MUTATION,
};
