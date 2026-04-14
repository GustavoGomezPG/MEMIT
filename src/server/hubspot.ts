const HUBSPOT_API = "https://api.hubapi.com";
const RATE_LIMIT = 100;
const RATE_WINDOW_MS = 10_000;

let requestTimestamps: number[] = [];

async function throttle(): Promise<void> {
  const now = Date.now();
  requestTimestamps = requestTimestamps.filter(
    (t) => now - t < RATE_WINDOW_MS
  );
  if (requestTimestamps.length >= RATE_LIMIT) {
    const oldest = requestTimestamps[0]!;
    const waitMs = RATE_WINDOW_MS - (now - oldest) + 50;
    await new Promise((r) => setTimeout(r, waitMs));
  }
  requestTimestamps.push(Date.now());
}

export async function hubspotFetch(
  token: string,
  path: string,
  options: RequestInit = {}
): Promise<Response> {
  await throttle();

  const url = `${HUBSPOT_API}${path}`;
  const res = await fetch(url, {
    ...options,
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
      ...options.headers,
    },
  });

  if (res.status === 429) {
    const retryAfter = parseInt(res.headers.get("Retry-After") || "1", 10);
    await new Promise((r) => setTimeout(r, retryAfter * 1000));
    return hubspotFetch(token, path, options);
  }

  return res;
}

export async function validateToken(
  token: string
): Promise<{ valid: boolean; portalId?: string; error?: string }> {
  try {
    // Use the account info endpoint — works with Service Keys, Private Apps, and OAuth
    const res = await hubspotFetch(token, "/account-info/v3/details");
    if (!res.ok) {
      if (res.status === 401 || res.status === 403) {
        return { valid: false, error: "Invalid or unauthorized access token" };
      }
      return { valid: false, error: `API returned status ${res.status}` };
    }

    const data = (await res.json()) as { portalId?: number; accountId?: number };
    const portalId = data.portalId || data.accountId;

    if (!portalId) {
      return { valid: false, error: "Could not resolve portal ID from response" };
    }

    return {
      valid: true,
      portalId: String(portalId),
    };
  } catch (err) {
    return {
      valid: false,
      error: err instanceof Error ? err.message : "Unknown error",
    };
  }
}

export interface HubSpotFolder {
  id: string;
  name: string;
  parentFolderId?: string;
  path: string;
}

export interface HubSpotFile {
  id: string;
  name: string;
  url: string;
  folderId?: string;
  path: string;
  size: number;
  type: string;
  extension: string;
  encoding: string;
  [key: string]: unknown;
}

export async function fetchAllFolders(token: string): Promise<HubSpotFolder[]> {
  const folders: HubSpotFolder[] = [];
  let after: string | undefined;

  do {
    const params = new URLSearchParams({ limit: "100" });
    if (after) params.set("after", after);

    const res = await hubspotFetch(
      token,
      `/files/v3/folders/search?${params.toString()}`
    );
    if (!res.ok) throw new Error(`Failed to fetch folders: ${res.status}`);

    const data = (await res.json()) as {
      results: HubSpotFolder[];
      paging?: { next?: { after: string } };
    };
    folders.push(...data.results);
    after = data.paging?.next?.after;
  } while (after);

  return folders;
}

export async function createFolder(
  token: string,
  path: string,
  parentFolderId?: string
): Promise<HubSpotFolder> {
  const body: Record<string, string> = { name: path.split("/").pop() || path };
  if (parentFolderId) body.parentFolderId = parentFolderId;

  const res = await hubspotFetch(token, "/files/v3/folders", {
    method: "POST",
    body: JSON.stringify(body),
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Failed to create folder "${path}": ${res.status} ${text}`);
  }

  return res.json() as Promise<HubSpotFolder>;
}

export async function fetchAllFiles(token: string): Promise<HubSpotFile[]> {
  const files: HubSpotFile[] = [];
  let after: string | undefined;

  do {
    const params = new URLSearchParams({ limit: "100" });
    if (after) params.set("after", after);

    const res = await hubspotFetch(
      token,
      `/files/v3/files/search?${params.toString()}`
    );
    if (!res.ok) throw new Error(`Failed to fetch files: ${res.status}`);

    const data = (await res.json()) as {
      results: HubSpotFile[];
      paging?: { next?: { after: string } };
    };
    files.push(...data.results);
    after = data.paging?.next?.after;
  } while (after);

  return files;
}

export async function getSignedUrl(
  token: string,
  fileId: string
): Promise<string> {
  const res = await hubspotFetch(
    token,
    `/files/v3/files/${fileId}/signed-url`
  );
  if (!res.ok) throw new Error(`Failed to get signed URL for file ${fileId}`);
  const data = (await res.json()) as { url: string };
  return data.url;
}

// ── Blog Posts ──

export interface HubSpotBlogPost {
  id: string;
  name: string;
  slug: string;
  htmlTitle: string;
  postBody: string;
  featuredImage: string;
  featuredImageAltText: string;
  metaDescription: string;
  state: string;
  blogAuthorId: string;
  contentGroupId: string;
  publishDate: string;
  created: string;
  updated: string;
  url: string;
  [key: string]: unknown;
}

export async function fetchAllBlogPosts(
  token: string
): Promise<HubSpotBlogPost[]> {
  const posts: HubSpotBlogPost[] = [];
  let after: string | undefined;

  do {
    const params = new URLSearchParams({ limit: "100" });
    if (after) params.set("after", after);

    const res = await hubspotFetch(
      token,
      `/cms/v3/blogs/posts?${params.toString()}`
    );
    if (!res.ok) throw new Error(`Failed to fetch blog posts: ${res.status}`);

    const data = (await res.json()) as {
      results: HubSpotBlogPost[];
      paging?: { next?: { after: string } };
    };
    posts.push(...data.results);
    after = data.paging?.next?.after;
  } while (after);

  return posts;
}

export async function createBlogPost(
  token: string,
  post: Record<string, unknown>
): Promise<HubSpotBlogPost> {
  const res = await hubspotFetch(token, "/cms/v3/blogs/posts", {
    method: "POST",
    body: JSON.stringify(post),
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Failed to create blog post: ${res.status} ${text}`);
  }

  return res.json() as Promise<HubSpotBlogPost>;
}

// ── Files ──

export async function uploadFile(
  token: string,
  fileBuffer: Buffer,
  fileName: string,
  folderId?: string
): Promise<HubSpotFile> {
  await throttle();

  const formData = new FormData();
  const blob = new Blob([new Uint8Array(fileBuffer)]);
  formData.append("file", blob, fileName);
  formData.append(
    "options",
    JSON.stringify({
      access: "PUBLIC_NOT_INDEXABLE",
      overwrite: false,
      duplicateValidationStrategy: "RETURN_EXISTING",
      duplicateValidationScope: "ENTIRE_PORTAL",
    })
  );
  if (folderId) {
    formData.append("folderId", folderId);
  }

  const res = await fetch(`${HUBSPOT_API}/files/v3/files`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
    },
    body: formData,
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Failed to upload "${fileName}": ${res.status} ${text}`);
  }

  return res.json() as Promise<HubSpotFile>;
}

// ── Blog Authors ──

export interface HubSpotBlogAuthor {
  id: string;
  fullName: string;
  email: string;
  slug: string;
  [key: string]: unknown;
}

export async function fetchAllBlogAuthors(
  token: string
): Promise<HubSpotBlogAuthor[]> {
  const authors: HubSpotBlogAuthor[] = [];
  let after: string | undefined;
  do {
    const params = new URLSearchParams({ limit: "100" });
    if (after) params.set("after", after);
    const res = await hubspotFetch(token, `/cms/v3/blogs/authors?${params}`);
    if (!res.ok) throw new Error(`Failed to fetch blog authors: ${res.status}`);
    const data = (await res.json()) as {
      results: HubSpotBlogAuthor[];
      paging?: { next?: { after: string } };
    };
    authors.push(...data.results);
    after = data.paging?.next?.after;
  } while (after);
  return authors;
}

export async function createBlogAuthor(
  token: string,
  data: { fullName: string; email: string; slug?: string }
): Promise<HubSpotBlogAuthor> {
  const res = await hubspotFetch(token, "/cms/v3/blogs/authors", {
    method: "POST",
    body: JSON.stringify(data),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Failed to create blog author: ${res.status} ${text}`);
  }
  return res.json() as Promise<HubSpotBlogAuthor>;
}

// ── Blog Tags ──

export interface HubSpotBlogTag {
  id: string;
  name: string;
  slug: string;
  [key: string]: unknown;
}

export async function fetchAllBlogTags(
  token: string
): Promise<HubSpotBlogTag[]> {
  // Try v3 first
  try {
    const tags: HubSpotBlogTag[] = [];
    let after: string | undefined;
    do {
      const params = new URLSearchParams({ limit: "100" });
      if (after) params.set("after", after);
      const res = await hubspotFetch(token, `/cms/v3/blogs/tags?${params}`);
      if (!res.ok) throw new Error(`v3 tags: ${res.status}`);
      const data = (await res.json()) as {
        results: HubSpotBlogTag[];
        paging?: { next?: { after: string } };
      };
      tags.push(...data.results);
      after = data.paging?.next?.after;
    } while (after);
    if (tags.length > 0) return tags;
  } catch {
    // Fall through to v2
  }

  // Fallback: v2 topics API (more widely available)
  try {
    const tags: HubSpotBlogTag[] = [];
    let offset = 0;
    let hasMore = true;
    while (hasMore) {
      const res = await hubspotFetch(
        token,
        `/blogs/v3/topics?limit=100&offset=${offset}`
      );
      if (!res.ok) {
        // Try the older v2 endpoint
        const res2 = await hubspotFetch(
          token,
          `/blogs/v2/topics?limit=100&offset=${offset}`
        );
        if (!res2.ok) throw new Error(`v2 topics: ${res2.status}`);
        const data = (await res2.json()) as {
          objects?: Array<{ id: number; name: string; slug: string }>;
          total?: number;
        };
        const objects = data.objects || [];
        tags.push(
          ...objects.map((o) => ({
            id: String(o.id),
            name: o.name,
            slug: o.slug,
          }))
        );
        hasMore = tags.length < (data.total || 0);
        offset += 100;
        continue;
      }
      const data = (await res.json()) as {
        objects?: Array<{ id: number; name: string; slug: string }>;
        results?: HubSpotBlogTag[];
        total?: number;
        paging?: { next?: { after: string } };
      };
      const results = data.results || (data.objects || []).map((o) => ({
        id: String(o.id),
        name: o.name,
        slug: o.slug,
      }));
      tags.push(...results);
      hasMore = data.paging?.next?.after
        ? true
        : tags.length < (data.total || 0);
      offset += 100;
      if (!data.paging?.next?.after && tags.length >= (data.total || tags.length)) break;
    }
    return tags;
  } catch {
    return [];
  }
}

export async function createBlogTag(
  token: string,
  data: { name: string; slug?: string }
): Promise<HubSpotBlogTag> {
  const res = await hubspotFetch(token, "/cms/v3/blogs/tags", {
    method: "POST",
    body: JSON.stringify(data),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Failed to create blog tag: ${res.status} ${text}`);
  }
  return res.json() as Promise<HubSpotBlogTag>;
}

// ── Blog search (idempotency) ──

export async function fetchBlogPostBySlug(
  token: string,
  slug: string
): Promise<HubSpotBlogPost | null> {
  const res = await hubspotFetch(
    token,
    `/cms/v3/blogs/posts?slug=${encodeURIComponent(slug)}&limit=1`
  );
  if (!res.ok) return null;
  const data = (await res.json()) as { results: HubSpotBlogPost[] };
  return data.results[0] || null;
}

// ── Content Groups (blogs) ──

export interface HubSpotContentGroup {
  id: string;
  name: string;
  slug: string;
  [key: string]: unknown;
}

export async function fetchContentGroups(
  token: string
): Promise<HubSpotContentGroup[]> {
  const groups: HubSpotContentGroup[] = [];
  let after: string | undefined;
  do {
    const params = new URLSearchParams({ limit: "100" });
    if (after) params.set("after", after);
    const res = await hubspotFetch(token, `/cms/v3/blogs?${params}`);
    if (!res.ok) throw new Error(`Failed to fetch content groups: ${res.status}`);
    const data = (await res.json()) as {
      results: HubSpotContentGroup[];
      paging?: { next?: { after: string } };
    };
    groups.push(...data.results);
    after = data.paging?.next?.after;
  } while (after);
  return groups;
}

// ── CMS Source Code (Design Manager) ──

export interface CmsSourceFile {
  path: string;
  source: string;
  type: string;
  folder: boolean;
  children?: string[];
  [key: string]: unknown;
}

/**
 * Fetch a template/module source file from the CMS Design Manager.
 * Environment is typically "developer" for draft or "published" for live.
 */
export async function fetchCmsSource(
  token: string,
  path: string,
  environment: string = "developer"
): Promise<CmsSourceFile | null> {
  const cleanPath = path.startsWith("/") ? path : `/${path}`;
  const res = await hubspotFetch(
    token,
    `/cms/v3/source-code/${environment}${cleanPath}`
  );
  if (!res.ok) return null;
  return res.json() as Promise<CmsSourceFile>;
}

/**
 * Upload a template/module source file to the CMS Design Manager.
 */
export async function uploadCmsSource(
  token: string,
  path: string,
  source: string,
  environment: string = "developer"
): Promise<boolean> {
  const cleanPath = path.startsWith("/") ? path : `/${path}`;
  const res = await hubspotFetch(
    token,
    `/cms/v3/source-code/${environment}${cleanPath}`,
    {
      method: "PUT",
      body: JSON.stringify({ source }),
    }
  );
  return res.ok;
}

/**
 * Fetch a module's full definition (folder listing + each file).
 * Modules are folders with meta.json, module.html, fields.json, etc.
 */
export async function fetchModuleFiles(
  token: string,
  modulePath: string,
  environment: string = "developer"
): Promise<Record<string, string>> {
  const files: Record<string, string> = {};
  const cleanPath = modulePath.endsWith(".module")
    ? modulePath
    : `${modulePath}.module`;

  // Get folder listing
  const folder = await fetchCmsSource(token, cleanPath, environment);
  if (!folder || !folder.children) {
    // Try as a single file
    const single = await fetchCmsSource(token, modulePath, environment);
    if (single && single.source) {
      files[modulePath] = single.source;
    }
    return files;
  }

  // Fetch each child file
  for (const childName of folder.children) {
    const childPath = `${cleanPath}/${childName}`;
    const child = await fetchCmsSource(token, childPath, environment);
    if (child && child.source) {
      files[childPath] = child.source;
    }
  }

  return files;
}

/**
 * Upload a module's files to the target portal's Design Manager.
 */
export async function uploadModuleFiles(
  token: string,
  moduleFiles: Record<string, string>,
  environment: string = "developer"
): Promise<{ uploaded: number; failed: number }> {
  let uploaded = 0;
  let failed = 0;

  for (const [path, source] of Object.entries(moduleFiles)) {
    const ok = await uploadCmsSource(token, path, source, environment);
    if (ok) uploaded++;
    else failed++;
  }

  return { uploaded, failed };
}

// ── Storage usage ──

export async function fetchStorageUsage(
  token: string
): Promise<{ bytesUsed: number; bytesLimit: number } | null> {
  try {
    const res = await hubspotFetch(token, "/files/v3/usage");
    if (!res.ok) return null;
    const data = (await res.json()) as {
      bytesUsed?: number;
      bytesLimit?: number;
      usage?: { bytesUsed?: number; bytesLimit?: number };
    };
    return {
      bytesUsed: data.bytesUsed ?? data.usage?.bytesUsed ?? 0,
      bytesLimit: data.bytesLimit ?? data.usage?.bytesLimit ?? 0,
    };
  } catch {
    return null;
  }
}

// ── HubDB ──

export interface HubDbColumn {
  id: number;
  name: string;
  label: string;
  type: string;
  options?: Array<{ id: string; name: string; type: string }>;
  [key: string]: unknown;
}

export interface HubDbTable {
  id: string;
  name: string;
  label: string;
  columns: HubDbColumn[];
  rowCount: number;
  published: boolean;
  createdAt: string;
  updatedAt: string;
  [key: string]: unknown;
}

export interface HubDbRow {
  id: string;
  values: Record<string, unknown>;
  path?: string;
  name?: string;
  [key: string]: unknown;
}

export async function fetchAllHubDbTables(
  token: string
): Promise<HubDbTable[]> {
  const tables: HubDbTable[] = [];
  let after: string | undefined;
  do {
    const params = new URLSearchParams({ limit: "100" });
    if (after) params.set("after", after);
    const res = await hubspotFetch(token, `/cms/v3/hubdb/tables?${params}`);
    if (!res.ok) throw new Error(`Failed to fetch HubDB tables: ${res.status}`);
    const data = (await res.json()) as {
      results: HubDbTable[];
      paging?: { next?: { after: string } };
    };
    tables.push(...data.results);
    after = data.paging?.next?.after;
  } while (after);
  return tables;
}

export async function fetchHubDbTable(
  token: string,
  tableIdOrName: string
): Promise<HubDbTable> {
  const res = await hubspotFetch(token, `/cms/v3/hubdb/tables/${tableIdOrName}`);
  if (!res.ok) throw new Error(`Failed to fetch HubDB table: ${res.status}`);
  return res.json() as Promise<HubDbTable>;
}

export async function fetchAllHubDbRows(
  token: string,
  tableId: string
): Promise<HubDbRow[]> {
  const rows: HubDbRow[] = [];
  let after: string | undefined;
  do {
    const params = new URLSearchParams({ limit: "100" });
    if (after) params.set("after", after);
    const res = await hubspotFetch(
      token,
      `/cms/v3/hubdb/tables/${tableId}/rows?${params}`
    );
    if (!res.ok) throw new Error(`Failed to fetch HubDB rows: ${res.status}`);
    const data = (await res.json()) as {
      results: HubDbRow[];
      paging?: { next?: { after: string } };
    };
    rows.push(...data.results);
    after = data.paging?.next?.after;
  } while (after);
  return rows;
}

export async function createHubDbTable(
  token: string,
  table: { name: string; label: string; columns: Omit<HubDbColumn, "id">[] }
): Promise<HubDbTable> {
  const res = await hubspotFetch(token, "/cms/v3/hubdb/tables", {
    method: "POST",
    body: JSON.stringify(table),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Failed to create HubDB table: ${res.status} ${text}`);
  }
  return res.json() as Promise<HubDbTable>;
}

export async function createHubDbRow(
  token: string,
  tableId: string,
  row: { values: Record<string, unknown>; path?: string; name?: string }
): Promise<HubDbRow> {
  const res = await hubspotFetch(
    token,
    `/cms/v3/hubdb/tables/${tableId}/rows`,
    { method: "POST", body: JSON.stringify(row) }
  );
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Failed to create HubDB row: ${res.status} ${text}`);
  }
  return res.json() as Promise<HubDbRow>;
}

export async function createHubDbRowsBatch(
  token: string,
  tableId: string,
  rows: Array<{ values: Record<string, unknown>; path?: string; name?: string }>
): Promise<{ results: HubDbRow[] }> {
  const res = await hubspotFetch(
    token,
    `/cms/v3/hubdb/tables/${tableId}/rows/batch/create`,
    { method: "POST", body: JSON.stringify({ inputs: rows }) }
  );
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Failed to batch create HubDB rows: ${res.status} ${text}`);
  }
  return res.json() as Promise<{ results: HubDbRow[] }>;
}

export async function publishHubDbTable(
  token: string,
  tableId: string
): Promise<HubDbTable> {
  const res = await hubspotFetch(
    token,
    `/cms/v3/hubdb/tables/${tableId}/draft/publish`,
    { method: "POST" }
  );
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Failed to publish HubDB table: ${res.status} ${text}`);
  }
  return res.json() as Promise<HubDbTable>;
}

export async function fetchHubDbTableByName(
  token: string,
  name: string
): Promise<HubDbTable | null> {
  try {
    const res = await hubspotFetch(token, `/cms/v3/hubdb/tables/${name}`);
    if (!res.ok) return null;
    return res.json() as Promise<HubDbTable>;
  } catch {
    return null;
  }
}

// ── Pages ──

export interface HubSpotPage {
  id: string;
  name: string;
  slug: string;
  htmlTitle: string;
  state: string;
  publishDate: string;
  created: string;
  updated: string;
  url: string;
  subcategory: string;
  featuredImage: string;
  featuredImageAltText: string;
  metaDescription: string;
  layoutSections: Record<string, unknown>;
  templatePath: string;
  widgetContainers: Record<string, unknown>;
  widgets: Record<string, unknown>;
  [key: string]: unknown;
}

export async function fetchAllSitePages(
  token: string
): Promise<HubSpotPage[]> {
  const pages: HubSpotPage[] = [];
  let after: string | undefined;
  do {
    const params = new URLSearchParams({ limit: "100" });
    if (after) params.set("after", after);
    const res = await hubspotFetch(token, `/cms/v3/pages/site-pages?${params}`);
    if (!res.ok) throw new Error(`Failed to fetch site pages: ${res.status}`);
    const data = (await res.json()) as {
      results: HubSpotPage[];
      paging?: { next?: { after: string } };
    };
    pages.push(...data.results);
    after = data.paging?.next?.after;
  } while (after);
  return pages;
}

export async function fetchAllLandingPages(
  token: string
): Promise<HubSpotPage[]> {
  const pages: HubSpotPage[] = [];
  let after: string | undefined;
  do {
    const params = new URLSearchParams({ limit: "100" });
    if (after) params.set("after", after);
    const res = await hubspotFetch(token, `/cms/v3/pages/landing-pages?${params}`);
    if (!res.ok) throw new Error(`Failed to fetch landing pages: ${res.status}`);
    const data = (await res.json()) as {
      results: HubSpotPage[];
      paging?: { next?: { after: string } };
    };
    pages.push(...data.results);
    after = data.paging?.next?.after;
  } while (after);
  return pages;
}

export async function createSitePage(
  token: string,
  page: Record<string, unknown>
): Promise<HubSpotPage> {
  const res = await hubspotFetch(token, "/cms/v3/pages/site-pages", {
    method: "POST",
    body: JSON.stringify(page),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Failed to create site page: ${res.status} ${text}`);
  }
  return res.json() as Promise<HubSpotPage>;
}

export async function createLandingPage(
  token: string,
  page: Record<string, unknown>
): Promise<HubSpotPage> {
  const res = await hubspotFetch(token, "/cms/v3/pages/landing-pages", {
    method: "POST",
    body: JSON.stringify(page),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Failed to create landing page: ${res.status} ${text}`);
  }
  return res.json() as Promise<HubSpotPage>;
}

export async function fetchPageBySlug(
  token: string,
  slug: string,
  subcategory: "site_page" | "landing_page" = "site_page"
): Promise<HubSpotPage | null> {
  const endpoint = subcategory === "landing_page"
    ? "/cms/v3/pages/landing-pages"
    : "/cms/v3/pages/site-pages";
  const res = await hubspotFetch(
    token,
    `${endpoint}?slug=${encodeURIComponent(slug)}&limit=1`
  );
  if (!res.ok) return null;
  const data = (await res.json()) as { results: HubSpotPage[] };
  return data.results[0] || null;
}