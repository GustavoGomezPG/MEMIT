import { db } from "../../db";
import { tasks } from "../../db/schema";
import type { Migration } from "../../db/schema";
import { eq } from "drizzle-orm";
import {
  createRunnerContext,
  logToTask,
  isTaskPaused,
  getExistingUrlMapping,
} from "./base";
import {
  readManifest,
  flushManifest,
  getDataDir,
} from "../manifest";
import {
  fetchAllBlogPosts,
  createBlogPost,
  uploadFile,
  fetchAllBlogAuthors,
  createBlogAuthor,
  fetchAllBlogTags,
  createBlogTag,
  fetchBlogPostBySlug,
  fetchContentGroups,
  fetchModuleFiles,
  uploadModuleFiles,
  fetchCmsSource,
  uploadCmsSource,
  type HubSpotBlogPost,
} from "../hubspot";
import {
  scanContent,
  extractHubLReferences,
  getUniquePaths,
  extractCtaGuids,
} from "../scanners";
import { writeCsvExport, BLOG_CSV_COLUMNS } from "../csv";
import { writeFile, mkdir, readFile } from "fs/promises";
import { resolve } from "path";

// ── Media extraction ──

const IMG_SRC_RE = /(?:src|data-src)=["']([^"']+)["']/gi;
const HUBSPOT_CDN_RE = /https?:\/\/[^"'\s]*hubspotusercontent[^"'\s]*/gi;
const HREF_MEDIA_RE = /href=["']([^"']+\.(png|jpe?g|gif|svg|webp|ico|bmp|tiff?|pdf|doc|docx|odt|rtf|xls|xlsx|xlsm|ods|ppt|pptx|odp|txt|md|csv|tsv|json|xml|mp4|mov|avi|webm|mkv|mp3|wav|ogg|flac|aac|zip|rar|7z|tar|gz|bz2|eps|ai|psd|indd)[^"']*)["']/gi;

function extractMediaUrls(post: HubSpotBlogPost): string[] {
  const urls = new Set<string>();
  if (post.featuredImage) urls.add(post.featuredImage);
  if (post.postBody) {
    let match: RegExpExecArray | null;
    IMG_SRC_RE.lastIndex = 0;
    while ((match = IMG_SRC_RE.exec(post.postBody)) !== null) {
      if (match[1]) urls.add(match[1]);
    }
    HUBSPOT_CDN_RE.lastIndex = 0;
    while ((match = HUBSPOT_CDN_RE.exec(post.postBody)) !== null) {
      urls.add(match[0]);
    }
    HREF_MEDIA_RE.lastIndex = 0;
    while ((match = HREF_MEDIA_RE.exec(post.postBody)) !== null) {
      if (match[1]) urls.add(match[1]);
    }
  }
  return Array.from(urls);
}

function rewriteUrls(html: string, mapping: Record<string, string>): string {
  let result = html;
  for (const [oldUrl, newUrl] of Object.entries(mapping)) {
    result = result.split(oldUrl).join(newUrl);
  }
  return result;
}

// ── EXPORT PHASE ──

export async function exportBlogPosts(
  taskId: number,
  migration: Migration
): Promise<void> {
  const ctx = await createRunnerContext(taskId, migration);
  if (!ctx) return;

  const { sourceToken, manifestPath, manifest } = ctx;
  const dataDir = getDataDir(migration.id, taskId);
  const mediaDir = resolve(dataDir, "media");
  await mkdir(mediaDir, { recursive: true });

  await db
    .update(tasks)
    .set({ status: "exporting", phase: "export", startedAt: new Date() })
    .where(eq(tasks.id, taskId));

  // Fetch posts
  await logToTask(taskId, "info", "Fetching blog posts from source portal...");
  let posts: HubSpotBlogPost[];
  try {
    posts = await fetchAllBlogPosts(sourceToken);
    await logToTask(taskId, "info", `Found ${posts.length} blog posts`);
  } catch (err) {
    await logToTask(taskId, "error", `Failed to fetch posts: ${err instanceof Error ? err.message : String(err)}`);
    await db.update(tasks).set({ status: "failed", completedAt: new Date() }).where(eq(tasks.id, taskId));
    return;
  }

  // Apply config filter
  const task = await db.select().from(tasks).where(eq(tasks.id, taskId)).then(r => r[0]);
  if (task?.config) {
    try {
      const config = JSON.parse(task.config) as { selectedPostIds?: string[] };
      if (config.selectedPostIds?.length) {
        const selectedSet = new Set(config.selectedPostIds);
        posts = posts.filter((p) => selectedSet.has(p.id));
        await logToTask(taskId, "info", `Filtered to ${posts.length} selected posts`);
      }
    } catch { /* use all posts */ }
  }

  // Populate manifest
  const existingIds = new Set(manifest.items.map((i) => i.id));
  for (const post of posts) {
    if (!existingIds.has(post.id)) {
      manifest.items.push({
        id: post.id,
        sourceUrl: post.url || "",
        localPath: null,
        targetUrl: null,
        targetId: null,
        status: "pending",
        error: null,
        size: 0,
        metadata: {
          name: post.name,
          slug: post.slug,
          htmlTitle: post.htmlTitle,
          state: post.state,
          contentGroupId: post.contentGroupId,
          blogAuthorId: post.blogAuthorId,
          tagIds: (post as Record<string, unknown>).tagIds || [],
          publishDate: post.publishDate,
          featuredImage: post.featuredImage,
          metaDescription: post.metaDescription,
          mediaUrls: extractMediaUrls(post),
        },
      });
    }
  }

  await db.update(tasks).set({ totalItems: manifest.items.length }).where(eq(tasks.id, taskId));
  flushManifest(manifestPath, manifest);

  // Export each post + its media
  await logToTask(taskId, "info", "Downloading posts and media...");
  let exported = 0;
  let failed = 0;
  let totalBytes = 0;

  for (const item of manifest.items) {
    if (item.status === "exported") {
      exported++;
      totalBytes += item.size;
      continue;
    }

    if (await isTaskPaused(taskId)) {
      await logToTask(taskId, "info", "Export paused");
      flushManifest(manifestPath, manifest);
      return;
    }

    const post = posts.find((p) => p.id === item.id);
    if (!post) {
      item.status = "failed";
      item.error = "Post not found in fetched data";
      failed++;
      continue;
    }

    try {
      // Save full post JSON
      const postPath = resolve(dataDir, `post-${post.id}.json`);
      const postJson = JSON.stringify(post, null, 2);
      await writeFile(postPath, postJson, "utf-8");
      item.localPath = postPath;
      item.size = Buffer.byteLength(postJson);

      // Download media
      const mediaUrls = item.metadata.mediaUrls as string[];
      const downloadResults = new Map<string, boolean>();

      for (const mediaUrl of mediaUrls) {
        try {
          const res = await fetch(mediaUrl, { signal: AbortSignal.timeout(30_000) });
          if (!res.ok) {
            downloadResults.set(mediaUrl, false);
            continue;
          }
          const buf = Buffer.from(await res.arrayBuffer());
          const urlPath = new URL(mediaUrl).pathname;
          const fileName = urlPath.split("/").pop() || `media-${Date.now()}`;
          await writeFile(resolve(mediaDir, fileName), buf);
          totalBytes += buf.length;
          downloadResults.set(mediaUrl, true);
        } catch {
          downloadResults.set(mediaUrl, false);
        }
      }

      // Scan content for warnings
      if (post.postBody) {
        const warnings = scanContent(post.postBody, post.id, downloadResults);
        for (const w of warnings) {
          manifest.warnings.push(`[${w.type}] Post "${post.name}": ${w.message} — ${w.snippet}`);
        }
      }

      item.status = "exported";
      totalBytes += item.size;
      exported++;

      if (exported % 10 === 0) {
        await db
          .update(tasks)
          .set({ exportedItems: exported, failedItems: failed, localStorageBytes: totalBytes })
          .where(eq(tasks.id, taskId));
        flushManifest(manifestPath, manifest);
      }

      if (exported % 25 === 0) {
        await logToTask(taskId, "info", `Export progress: ${exported}/${manifest.items.length} posts`);
      }
    } catch (err) {
      item.status = "failed";
      item.error = err instanceof Error ? err.message : String(err);
      failed++;
    }
  }

  // Re-scan ALL exported posts for content warnings (clears stale warnings from previous runs)
  manifest.warnings = [];
  await logToTask(taskId, "info", "Scanning content for warnings...");
  for (const item of manifest.items) {
    if (item.status !== "exported" || !item.localPath) continue;
    try {
      const postJson = await readFile(item.localPath, "utf-8");
      const post = JSON.parse(postJson) as HubSpotBlogPost;
      if (post.postBody) {
        const warnings = scanContent(post.postBody, post.id);
        for (const w of warnings) {
          manifest.warnings.push(
            `[${w.type}] Post "${post.name}": ${w.message} — ${w.snippet}`
          );
        }
      }
    } catch { /* skip */ }
  }
  manifest.warnings = [...new Set(manifest.warnings)];

  // Extract CTA GUIDs and save mapping file
  const allCtaGuids = new Map<string, string[]>();
  for (const item of manifest.items) {
    if (item.status !== "exported" || !item.localPath) continue;
    try {
      const postJson = await readFile(item.localPath, "utf-8");
      const post = JSON.parse(postJson) as HubSpotBlogPost;
      if (post.postBody) {
        const postGuids = extractCtaGuids(post.postBody, post.id);
        for (const [guid, postIds] of postGuids) {
          if (!allCtaGuids.has(guid)) allCtaGuids.set(guid, []);
          allCtaGuids.get(guid)!.push(...postIds);
        }
      }
    } catch { /* skip */ }
  }

  if (allCtaGuids.size > 0) {
    // Save CTA manifest for the mapping UI
    const ctaManifest = Array.from(allCtaGuids.entries()).map(
      ([guid, postIds]) => ({
        sourceGuid: guid,
        targetGuid: null as string | null,
        postIds: [...new Set(postIds)],
        postCount: new Set(postIds).size,
      })
    );
    await writeFile(
      resolve(dataDir, "_ctas.json"),
      JSON.stringify(ctaManifest, null, 2),
      "utf-8"
    );
    await logToTask(
      taskId,
      "info",
      `Found ${allCtaGuids.size} unique CTAs across ${new Set(Array.from(allCtaGuids.values()).flat()).size} posts — map them before import`
    );
  }

  flushManifest(manifestPath, manifest);
  if (manifest.warnings.length > 0) {
    await logToTask(taskId, "info", `Found ${manifest.warnings.length} content warnings`);
  }

  // ── Phase 2: HubL Template Extraction ──
  await logToTask(taskId, "info", "Analyzing HubL templates...");

  // Collect all HubL references from all exported posts
  const allHubLRefs: ReturnType<typeof extractHubLReferences> = [];
  for (const item of manifest.items) {
    if (item.status !== "exported" || !item.localPath) continue;
    try {
      const postJson = await readFile(item.localPath, "utf-8");
      const post = JSON.parse(postJson) as HubSpotBlogPost;
      if (post.postBody) {
        const refs = extractHubLReferences(post.postBody, post.id);
        allHubLRefs.push(...refs);
      }
    } catch {
      // skip unreadable posts
    }
  }

  const { modulePaths, includePaths } = getUniquePaths(allHubLRefs);
  const templatesDir = resolve(dataDir, "templates");
  await mkdir(templatesDir, { recursive: true });

  if (modulePaths.length > 0 || includePaths.length > 0) {
    await logToTask(
      taskId,
      "info",
      `Found ${modulePaths.length} custom modules and ${includePaths.length} template includes to extract`
    );

    // Download modules
    const moduleData: Record<string, Record<string, string>> = {};
    for (const modulePath of modulePaths) {
      if (await isTaskPaused(taskId)) {
        await logToTask(taskId, "info", "Export paused during template extraction");
        flushManifest(manifestPath, manifest);
        return;
      }

      try {
        const files = await fetchModuleFiles(sourceToken, modulePath);
        if (Object.keys(files).length > 0) {
          moduleData[modulePath] = files;
          // Save module files locally
          const moduleDir = resolve(templatesDir, "modules", modulePath.replace(/^\//, ""));
          await mkdir(moduleDir, { recursive: true });
          for (const [filePath, source] of Object.entries(files)) {
            const localFile = resolve(templatesDir, "modules", filePath.replace(/^\//, ""));
            await mkdir(resolve(localFile, ".."), { recursive: true });
            await writeFile(localFile, source, "utf-8");
          }
          await logToTask(taskId, "info", `Extracted module: ${modulePath} (${Object.keys(files).length} files)`);
        } else {
          await logToTask(taskId, "warn", `Module not found in Design Manager: ${modulePath}`);
        }
      } catch (err) {
        await logToTask(
          taskId,
          "warn",
          `Could not extract module "${modulePath}": ${err instanceof Error ? err.message : String(err)}`
        );
      }
    }

    // Download includes
    for (const includePath of includePaths) {
      if (await isTaskPaused(taskId)) {
        await logToTask(taskId, "info", "Export paused during template extraction");
        flushManifest(manifestPath, manifest);
        return;
      }

      try {
        const source = await fetchCmsSource(sourceToken, includePath);
        if (source && source.source) {
          const localFile = resolve(templatesDir, "includes", includePath.replace(/^\//, ""));
          await mkdir(resolve(localFile, ".."), { recursive: true });
          await writeFile(localFile, source.source, "utf-8");
          await logToTask(taskId, "info", `Extracted template: ${includePath}`);
        } else {
          await logToTask(taskId, "warn", `Template not found: ${includePath}`);
        }
      } catch (err) {
        await logToTask(
          taskId,
          "warn",
          `Could not extract template "${includePath}": ${err instanceof Error ? err.message : String(err)}`
        );
      }
    }

    // Save a summary of extracted templates for the import phase
    const templateManifest = {
      modules: Object.keys(moduleData).map((p) => ({
        path: p,
        files: Object.keys(moduleData[p]!),
      })),
      includes: includePaths,
    };
    await writeFile(
      resolve(templatesDir, "_templates.json"),
      JSON.stringify(templateManifest, null, 2),
      "utf-8"
    );

    await logToTask(
      taskId,
      "info",
      `Template extraction complete. ${Object.keys(moduleData).length} modules, ${includePaths.length} includes saved.`
    );
  } else {
    await logToTask(taskId, "info", "No custom HubL modules or includes found — posts use standard templates only");
  }

  // CSV export if requested
  if (ctx.outputType === "csv") {
    const csvRecords = manifest.items
      .filter((i) => i.status === "exported")
      .map((i) => ({
        id: i.id,
        name: i.metadata.name,
        slug: i.metadata.slug,
        htmlTitle: i.metadata.htmlTitle,
        state: i.metadata.state,
        publishDate: i.metadata.publishDate,
        url: i.sourceUrl,
        featuredImage: i.metadata.featuredImage,
        metaDescription: i.metadata.metaDescription,
        contentGroupId: i.metadata.contentGroupId,
        blogAuthorId: i.metadata.blogAuthorId,
      }));
    const csvPath = await writeCsvExport(migration.id, taskId, "blog_posts", csvRecords as Record<string, unknown>[], BLOG_CSV_COLUMNS);
    await logToTask(taskId, "info", `CSV export saved: ${csvPath}`);
  }

  // Deduplicate warnings
  manifest.warnings = [...new Set(manifest.warnings)];

  manifest.phase = "exported";
  manifest.exportedAt = new Date().toISOString();
  flushManifest(manifestPath, manifest);

  await db
    .update(tasks)
    .set({
      status: "exported",
      phase: "export",
      exportedItems: exported,
      failedItems: failed,
      localStorageBytes: totalBytes,
      exportedAt: new Date(),
    })
    .where(eq(tasks.id, taskId));

  const warningCount = manifest.warnings.length;
  await logToTask(
    taskId,
    "info",
    `Export completed. ${exported} posts downloaded, ${failed} failed.${warningCount > 0 ? ` ${warningCount} content warnings found.` : ""}`
  );
}

// ── STANDALONE TEMPLATE EXTRACTION ──
// Runs on an already-exported task that missed the template extraction step.

export async function extractTemplatesOnly(
  taskId: number,
  migration: Migration
): Promise<void> {
  const ctx = await createRunnerContext(taskId, migration);
  if (!ctx) return;

  const { sourceToken, manifestPath } = ctx;
  const manifest = readManifest(manifestPath);
  const dataDir = getDataDir(migration.id, taskId);

  await logToTask(taskId, "info", "Running standalone HubL template extraction...");

  // Collect HubL references from exported posts
  const allHubLRefs: ReturnType<typeof extractHubLReferences> = [];
  for (const item of manifest.items) {
    if (item.status !== "exported" || !item.localPath) continue;
    try {
      const postJson = await readFile(item.localPath, "utf-8");
      const post = JSON.parse(postJson) as HubSpotBlogPost;
      if (post.postBody) {
        allHubLRefs.push(...extractHubLReferences(post.postBody, post.id));
      }
    } catch {
      // skip
    }
  }

  const { modulePaths, includePaths } = getUniquePaths(allHubLRefs);
  const templatesDir = resolve(dataDir, "templates");
  await mkdir(templatesDir, { recursive: true });

  if (modulePaths.length === 0 && includePaths.length === 0) {
    await logToTask(taskId, "info", "No custom HubL modules or includes found in posts");
    return;
  }

  await logToTask(
    taskId,
    "info",
    `Found ${modulePaths.length} modules and ${includePaths.length} includes to extract`
  );

  const moduleData: Record<string, Record<string, string>> = {};

  for (const modulePath of modulePaths) {
    try {
      const files = await fetchModuleFiles(sourceToken, modulePath);
      if (Object.keys(files).length > 0) {
        moduleData[modulePath] = files;
        const moduleDir = resolve(templatesDir, "modules", modulePath.replace(/^\//, ""));
        await mkdir(moduleDir, { recursive: true });
        for (const [filePath, source] of Object.entries(files)) {
          const localFile = resolve(templatesDir, "modules", filePath.replace(/^\//, ""));
          await mkdir(resolve(localFile, ".."), { recursive: true });
          await writeFile(localFile, source, "utf-8");
        }
        await logToTask(taskId, "info", `Extracted module: ${modulePath} (${Object.keys(files).length} files)`);
      } else {
        await logToTask(taskId, "warn", `Module not found: ${modulePath}`);
      }
    } catch (err) {
      await logToTask(taskId, "warn", `Could not extract module "${modulePath}": ${err instanceof Error ? err.message : String(err)}`);
    }
  }

  for (const includePath of includePaths) {
    try {
      const source = await fetchCmsSource(sourceToken, includePath);
      if (source && source.source) {
        const localFile = resolve(templatesDir, "includes", includePath.replace(/^\//, ""));
        await mkdir(resolve(localFile, ".."), { recursive: true });
        await writeFile(localFile, source.source, "utf-8");
        await logToTask(taskId, "info", `Extracted template: ${includePath}`);
      } else {
        await logToTask(taskId, "warn", `Template not found: ${includePath}`);
      }
    } catch (err) {
      await logToTask(taskId, "warn", `Could not extract template "${includePath}": ${err instanceof Error ? err.message : String(err)}`);
    }
  }

  // Save template manifest
  const templateManifest = {
    modules: Object.keys(moduleData).map((p) => ({
      path: p,
      files: Object.keys(moduleData[p]!),
    })),
    includes: includePaths,
  };
  await writeFile(
    resolve(templatesDir, "_templates.json"),
    JSON.stringify(templateManifest, null, 2),
    "utf-8"
  );

  await logToTask(
    taskId,
    "info",
    `Template extraction complete. ${Object.keys(moduleData).length} modules, ${includePaths.length} includes saved locally.`
  );
}

// ── IMPORT PHASE ──

export async function importBlogPosts(
  taskId: number,
  migration: Migration,
  options: { dryRun: boolean } = { dryRun: false }
): Promise<void> {
  const ctx = await createRunnerContext(taskId, migration, options);
  if (!ctx) return;

  const { sourceToken, targetToken, manifestPath, dryRun } = ctx;
  const manifest = readManifest(manifestPath);
  const dataDir = getDataDir(migration.id, taskId);
  const mediaDir = resolve(dataDir, "media");

  await db
    .update(tasks)
    .set({ status: "importing", phase: "import" })
    .where(eq(tasks.id, taskId));

  if (dryRun) {
    await logToTask(taskId, "info", "DRY RUN — no posts will be created in target portal");
  }

  // Pre-requisite resolution
  const authorIdMapping: Record<string, string> = {};
  const tagIdMapping: Record<string, string> = {};
  let contentGroupMapping: Record<string, string> = {};

  // Load tag mapping from task config
  let tagMapping: Record<string, { action: string; name?: string; mergeInto?: string }> = {};
  try {
    const currentTask2 = await db.select().from(tasks).where(eq(tasks.id, taskId)).then((r) => r[0]);
    if (currentTask2?.config) {
      const config = JSON.parse(currentTask2.config) as { tagMapping?: Record<string, { action: string; name?: string; mergeInto?: string }> };
      if (config.tagMapping) tagMapping = config.tagMapping;
    }
    if (Object.keys(tagMapping).length > 0) {
      await logToTask(taskId, "info", `Loaded tag mapping: ${Object.keys(tagMapping).length} tags have custom actions`);
    }
  } catch { /* no tag mapping */ }

  if (!dryRun) {
    // Map blog authors
    await logToTask(taskId, "info", "Resolving blog authors...");
    try {
      const sourceAuthors = await fetchAllBlogAuthors(sourceToken);
      const targetAuthors = await fetchAllBlogAuthors(targetToken);
      const targetByEmail = new Map(targetAuthors.map((a) => [a.email, a]));

      const neededAuthorIds = new Set(
        manifest.items.map((i) => i.metadata.blogAuthorId as string).filter(Boolean)
      );

      for (const sourceAuthor of sourceAuthors) {
        if (!neededAuthorIds.has(sourceAuthor.id)) continue;
        const existing = targetByEmail.get(sourceAuthor.email);
        if (existing) {
          authorIdMapping[sourceAuthor.id] = existing.id;
        } else {
          try {
            const created = await createBlogAuthor(targetToken, {
              fullName: sourceAuthor.fullName,
              email: sourceAuthor.email,
              slug: sourceAuthor.slug,
            });
            authorIdMapping[sourceAuthor.id] = created.id;
          } catch {
            await logToTask(taskId, "warn", `Could not create author "${sourceAuthor.fullName}" in target`);
          }
        }
      }
      await logToTask(taskId, "info", `Mapped ${Object.keys(authorIdMapping).length} authors`);
    } catch (err) {
      await logToTask(taskId, "warn", `Author resolution failed: ${err instanceof Error ? err.message : String(err)}`);
    }

    // Map tags
    await logToTask(taskId, "info", "Resolving blog tags...");
    let targetByName = new Map<string, { id: string; name: string; [key: string]: unknown }>();
    try {
      const sourceTags = await fetchAllBlogTags(sourceToken);
      const targetTags = await fetchAllBlogTags(targetToken);
      targetByName = new Map(targetTags.map((t) => [t.name.toLowerCase(), t]));

      const neededTagIds = new Set(
        manifest.items.flatMap((i) => (i.metadata.tagIds as string[]) || [])
      );

      for (const sourceTag of sourceTags) {
        if (!neededTagIds.has(sourceTag.id)) continue;
        const existing = targetByName.get(sourceTag.name.toLowerCase());
        if (existing) {
          tagIdMapping[sourceTag.id] = existing.id;
        } else {
          try {
            const created = await createBlogTag(targetToken, {
              name: sourceTag.name,
              slug: sourceTag.slug,
            });
            tagIdMapping[sourceTag.id] = created.id;
          } catch {
            await logToTask(taskId, "warn", `Could not create tag "${sourceTag.name}" in target`);
          }
        }
      }
      await logToTask(taskId, "info", `Mapped ${Object.keys(tagIdMapping).length} tags`);
    } catch (err) {
      await logToTask(taskId, "warn", `Tag resolution failed: ${err instanceof Error ? err.message : String(err)}`);
    }

    // Apply tag mapping overrides
    for (const [sourceTagId, mapping] of Object.entries(tagMapping)) {
      if (mapping.action === "delete") {
        delete tagIdMapping[sourceTagId];
      } else if (mapping.action === "rename" && mapping.name) {
        const existingByNewName = targetByName.get(mapping.name.toLowerCase());
        if (existingByNewName) {
          tagIdMapping[sourceTagId] = existingByNewName.id;
        } else {
          try {
            const created = await createBlogTag(targetToken, { name: mapping.name });
            tagIdMapping[sourceTagId] = created.id;
          } catch {
            await logToTask(taskId, "warn", `Could not create renamed tag "${mapping.name}"`);
          }
        }
      } else if (mapping.action === "merge" && mapping.mergeInto) {
        const mergeTargetId = tagIdMapping[mapping.mergeInto];
        if (mergeTargetId) {
          tagIdMapping[sourceTagId] = mergeTargetId;
        }
      }
    }

    // Map content groups
    try {
      const sourceGroups = await fetchContentGroups(sourceToken);
      const targetGroups = await fetchContentGroups(targetToken);
      if (sourceGroups.length > 0 && targetGroups.length > 0) {
        // Map by name match, fall back to first target group
        const targetByName = new Map(targetGroups.map((g) => [g.name.toLowerCase(), g]));
        for (const sg of sourceGroups) {
          const match = targetByName.get(sg.name.toLowerCase());
          contentGroupMapping[sg.id] = match ? match.id : targetGroups[0]!.id;
        }
      }
    } catch {
      await logToTask(taskId, "warn", "Could not resolve content groups — posts will use source contentGroupId");
    }
  }

  // Upload discovered media
  const urlMapping: Record<string, string> = await getExistingUrlMapping(migration.id);

  // Load CTA mapping from task config or _ctas.json
  let ctaMapping: Record<string, string> = {};
  try {
    const currentTask = await db.select().from(tasks).where(eq(tasks.id, taskId)).then((r) => r[0]);
    if (currentTask?.config) {
      const config = JSON.parse(currentTask.config) as { ctaMapping?: Record<string, string> };
      if (config.ctaMapping) ctaMapping = config.ctaMapping;
    }
    if (Object.keys(ctaMapping).length === 0) {
      const ctasPath = resolve(dataDir, "_ctas.json");
      const ctasRaw = await readFile(ctasPath, "utf-8");
      const ctas = JSON.parse(ctasRaw) as Array<{ sourceGuid: string; targetGuid: string | null }>;
      for (const cta of ctas) {
        if (cta.targetGuid) ctaMapping[cta.sourceGuid] = cta.targetGuid;
      }
    }
    if (Object.keys(ctaMapping).length > 0) {
      await logToTask(taskId, "info", `Loaded CTA mapping: ${Object.keys(ctaMapping).length} CTAs will be rewritten`);
    }
  } catch { /* no CTA mapping */ }

  if (!dryRun) {
    // Upload HubL templates to target Design Manager
    const templatesDir = resolve(dataDir, "templates");
    try {
      const templateManifestPath = resolve(templatesDir, "_templates.json");
      const tmRaw = await readFile(templateManifestPath, "utf-8");
      const templateManifest = JSON.parse(tmRaw) as {
        modules: Array<{ path: string; files: string[] }>;
        includes: string[];
      };

      if (templateManifest.modules.length > 0 || templateManifest.includes.length > 0) {
        await logToTask(taskId, "info", "Uploading HubL templates to target Design Manager...");

        // Upload modules
        for (const mod of templateManifest.modules) {
          const moduleFiles: Record<string, string> = {};
          for (const filePath of mod.files) {
            try {
              const localPath = resolve(templatesDir, "modules", filePath.replace(/^\//, ""));
              const source = await readFile(localPath, "utf-8");
              moduleFiles[filePath] = source;
            } catch {
              // file missing locally
            }
          }
          if (Object.keys(moduleFiles).length > 0) {
            const result = await uploadModuleFiles(targetToken, moduleFiles);
            await logToTask(
              taskId,
              result.failed > 0 ? "warn" : "info",
              `Module ${mod.path}: ${result.uploaded} files uploaded, ${result.failed} failed`
            );
          }
        }

        // Upload includes
        for (const includePath of templateManifest.includes) {
          try {
            const localPath = resolve(templatesDir, "includes", includePath.replace(/^\//, ""));
            const source = await readFile(localPath, "utf-8");
            const ok = await uploadCmsSource(targetToken, includePath, source);
            await logToTask(
              taskId,
              ok ? "info" : "warn",
              `Template ${includePath}: ${ok ? "uploaded" : "upload failed"}`
            );
          } catch {
            await logToTask(taskId, "warn", `Could not upload template: ${includePath}`);
          }
        }

        await logToTask(taskId, "info", "HubL template upload complete");
      }
    } catch {
      // No templates to upload — that's fine
    }

    await logToTask(taskId, "info", "Uploading discovered media...");
    const allMediaUrls = new Set(
      manifest.items.flatMap((i) => (i.metadata.mediaUrls as string[]) || [])
    );
    let mediaUploaded = 0;
    let mediaSkipped = 0;

    for (const mediaUrl of allMediaUrls) {
      if (urlMapping[mediaUrl]) {
        mediaSkipped++;
        continue;
      }
      try {
        const urlPath = new URL(mediaUrl).pathname;
        const fileName = urlPath.split("/").pop() || `media-${Date.now()}`;
        const localPath = resolve(mediaDir, fileName);

        let fileBuffer: Buffer;
        try {
          fileBuffer = Buffer.from(await readFile(localPath));
        } catch {
          // File not downloaded locally, skip
          continue;
        }

        const uploaded = await uploadFile(targetToken, fileBuffer, fileName, undefined, ctx.uploadFolderPath);
        urlMapping[mediaUrl] = uploaded.url;
        mediaUploaded++;
        if (mediaUploaded % 10 === 0) {
          await logToTask(taskId, "info", `Media upload progress: ${mediaUploaded} uploaded, ${mediaSkipped} skipped`);
        }
      } catch (err) {
        await logToTask(taskId, "warn", `Failed to upload media: ${mediaUrl} — ${err instanceof Error ? err.message : String(err)}`);
      }
    }
    await logToTask(
      taskId,
      "info",
      `Media upload complete: ${mediaUploaded} uploaded, ${mediaSkipped} already mapped`
    );
  }

  // Import posts
  let imported = 0;
  let failed = 0;
  let skipped = 0;
  const exportedItems = manifest.items.filter((i) => i.status === "exported");

  for (const item of exportedItems) {
    if (await isTaskPaused(taskId)) {
      await logToTask(taskId, "info", "Import paused");
      flushManifest(manifestPath, manifest);
      await db
        .update(tasks)
        .set({ importedItems: imported, failedItems: failed, urlMapping: JSON.stringify(urlMapping) })
        .where(eq(tasks.id, taskId));
      return;
    }

    // Idempotency: check if post with same slug exists in target
    const slug = item.metadata.slug as string;
    if (!dryRun && slug) {
      const existing = await fetchBlogPostBySlug(targetToken, slug);
      if (existing) {
        item.status = "skipped";
        item.targetId = existing.id;
        item.targetUrl = existing.url;
        skipped++;
        continue;
      }
    }

    if (dryRun) {
      await logToTask(taskId, "info", `[DRY RUN] Would create post "${item.metadata.name}" (/${slug})`);
      item.status = "skipped";
      skipped++;
      continue;
    }

    try {
      // Read post JSON from local
      if (!item.localPath) throw new Error("No local post file");
      const postJson = await readFile(item.localPath, "utf-8");
      const post = JSON.parse(postJson) as HubSpotBlogPost;

      // Rewrite content — URLs and CTAs
      let rewrittenBody = rewriteUrls(post.postBody || "", urlMapping);
      // Rewrite CTA GUIDs
      for (const [sourceGuid, targetGuid] of Object.entries(ctaMapping)) {
        rewrittenBody = rewrittenBody
          .split(`cta('${sourceGuid}')`).join(`cta('${targetGuid}')`)
          .split(`cta("${sourceGuid}")`).join(`cta("${targetGuid}")`)
          .split(`data-cta-id="${sourceGuid}"`).join(`data-cta-id="${targetGuid}"`)
          .split(`data-cta-id='${sourceGuid}'`).join(`data-cta-id='${targetGuid}'`);
      }
      const rewrittenFeaturedImage = post.featuredImage
        ? urlMapping[post.featuredImage] || post.featuredImage
        : "";

      // Map IDs
      const targetAuthorId = authorIdMapping[post.blogAuthorId] || post.blogAuthorId;
      const targetContentGroupId = contentGroupMapping[post.contentGroupId] || post.contentGroupId;
      const sourceTagIds = ((post as Record<string, unknown>).tagIds as string[]) || [];
      const targetTagIds = sourceTagIds
        .filter((id) => {
          const mapping = tagMapping[id];
          return !mapping || mapping.action !== "delete";
        })
        .map((id) => tagIdMapping[id] || id)
        .filter(Boolean);

      await createBlogPost(targetToken, {
        name: post.name,
        slug: post.slug,
        htmlTitle: post.htmlTitle,
        postBody: rewrittenBody,
        featuredImage: rewrittenFeaturedImage,
        featuredImageAltText: post.featuredImageAltText,
        metaDescription: post.metaDescription,
        blogAuthorId: targetAuthorId,
        contentGroupId: targetContentGroupId,
        tagIds: targetTagIds,
        publishDate: post.publishDate,
        publishImmediately: false,
        state: "DRAFT",
      });

      item.status = "imported";
      imported++;

      if (imported % 10 === 0) {
        await db
          .update(tasks)
          .set({ importedItems: imported, failedItems: failed, urlMapping: JSON.stringify(urlMapping) })
          .where(eq(tasks.id, taskId));
        flushManifest(manifestPath, manifest);
      }

      if (imported % 25 === 0) {
        await logToTask(taskId, "info", `Import progress: ${imported}/${exportedItems.length} posts`);
      }
    } catch (err) {
      item.status = "failed";
      item.error = err instanceof Error ? err.message : String(err);
      failed++;
    }
  }

  // Final
  if (dryRun) {
    // Reset items back to exported so the real import can run
    for (const item of manifest.items) {
      if (item.status === "skipped") item.status = "exported";
    }
    manifest.phase = "exported";
    flushManifest(manifestPath, manifest);

    await db
      .update(tasks)
      .set({ status: "exported", phase: "export" })
      .where(eq(tasks.id, taskId));

    await logToTask(taskId, "info", `Dry run completed. ${skipped} posts previewed, ${failed} failed. Ready for real import.`);
  } else {
    manifest.phase = "completed";
    manifest.importedAt = new Date().toISOString();
    flushManifest(manifestPath, manifest);

    await db
      .update(tasks)
      .set({
        status: "completed",
        completedAt: new Date(),
        importedItems: imported,
        failedItems: failed,
        urlMapping: JSON.stringify(urlMapping),
      })
      .where(eq(tasks.id, taskId));

    await logToTask(taskId, "info", `Import completed. ${imported} posts created, ${skipped} skipped, ${failed} failed.`);
  }
}
