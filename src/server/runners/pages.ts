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
import { readManifest, flushManifest, getDataDir } from "../manifest";
import {
  fetchAllSitePages,
  fetchAllLandingPages,
  createSitePage,
  createLandingPage,
  fetchPageBySlug,
  uploadFile,
  type HubSpotPage,
} from "../hubspot";
import { scanContent } from "../scanners";
import { writeCsvExport, PAGE_CSV_COLUMNS } from "../csv";
import { writeFile, mkdir, readFile } from "fs/promises";
import { resolve } from "path";

const IMG_SRC_RE = /(?:src|data-src)=["']([^"']+)["']/gi;
const HUBSPOT_CDN_RE = /https?:\/\/[^"'\s]*hubspotusercontent[^"'\s]*/gi;

function extractMediaUrls(page: HubSpotPage): string[] {
  const urls = new Set<string>();
  if (page.featuredImage) urls.add(page.featuredImage);
  const jsonStr = JSON.stringify(page.layoutSections || {}) +
    JSON.stringify(page.widgetContainers || {}) +
    JSON.stringify(page.widgets || {});
  let match: RegExpExecArray | null;
  IMG_SRC_RE.lastIndex = 0;
  while ((match = IMG_SRC_RE.exec(jsonStr)) !== null) {
    if (match[1]) urls.add(match[1]);
  }
  HUBSPOT_CDN_RE.lastIndex = 0;
  while ((match = HUBSPOT_CDN_RE.exec(jsonStr)) !== null) {
    urls.add(match[0]);
  }
  return Array.from(urls);
}

function rewriteUrlsInObject(
  obj: unknown,
  mapping: Record<string, string>
): unknown {
  if (typeof obj === "string") {
    let result = obj;
    for (const [oldUrl, newUrl] of Object.entries(mapping)) {
      result = result.split(oldUrl).join(newUrl);
    }
    return result;
  }
  if (Array.isArray(obj)) {
    return obj.map((item) => rewriteUrlsInObject(item, mapping));
  }
  if (obj && typeof obj === "object") {
    const result: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(obj as Record<string, unknown>)) {
      result[key] = rewriteUrlsInObject(value, mapping);
    }
    return result;
  }
  return obj;
}

// ── EXPORT PHASE ──

export async function exportPages(
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

  await logToTask(taskId, "info", "Fetching pages from source portal...");
  let pages: HubSpotPage[] = [];
  try {
    const [sitePages, landingPages] = await Promise.all([
      fetchAllSitePages(sourceToken),
      fetchAllLandingPages(sourceToken).catch(() => []),
    ]);
    pages = [
      ...sitePages.map((p) => ({ ...p, subcategory: "site_page" as const })),
      ...landingPages.map((p) => ({ ...p, subcategory: "landing_page" as const })),
    ];
    await logToTask(taskId, "info", `Found ${sitePages.length} site pages and ${landingPages.length} landing pages`);
  } catch (err) {
    await logToTask(taskId, "error", `Failed to fetch pages: ${err instanceof Error ? err.message : String(err)}`);
    await db.update(tasks).set({ status: "failed", completedAt: new Date() }).where(eq(tasks.id, taskId));
    return;
  }

  // Config filter
  const task = await db.select().from(tasks).where(eq(tasks.id, taskId)).then((r) => r[0]);
  if (task?.config) {
    try {
      const config = JSON.parse(task.config) as { selectedPageIds?: string[] };
      if (config.selectedPageIds?.length) {
        const selectedSet = new Set(config.selectedPageIds);
        pages = pages.filter((p) => selectedSet.has(p.id));
        await logToTask(taskId, "info", `Filtered to ${pages.length} selected pages`);
      }
    } catch { /* use all */ }
  }

  // Populate manifest
  const existingIds = new Set(manifest.items.map((i) => i.id));
  for (const page of pages) {
    if (!existingIds.has(page.id)) {
      manifest.items.push({
        id: page.id,
        sourceUrl: page.url || "",
        localPath: null,
        targetUrl: null,
        targetId: null,
        status: "pending",
        error: null,
        size: 0,
        metadata: {
          name: page.name,
          slug: page.slug,
          htmlTitle: page.htmlTitle,
          state: page.state,
          subcategory: page.subcategory,
          templatePath: page.templatePath,
          publishDate: page.publishDate,
          featuredImage: page.featuredImage,
          metaDescription: page.metaDescription,
          mediaUrls: extractMediaUrls(page),
        },
      });
    }
  }

  await db.update(tasks).set({ totalItems: manifest.items.length }).where(eq(tasks.id, taskId));
  flushManifest(manifestPath, manifest);

  await logToTask(taskId, "info", "Downloading pages and media...");
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
    const page = pages.find((p) => p.id === item.id);
    if (!page) {
      item.status = "failed";
      item.error = "Page not found in fetched data";
      failed++;
      continue;
    }
    try {
      const pagePath = resolve(dataDir, `page-${page.id}.json`);
      const pageJson = JSON.stringify(page, null, 2);
      await writeFile(pagePath, pageJson, "utf-8");
      item.localPath = pagePath;
      item.size = Buffer.byteLength(pageJson);

      // Download media
      const mediaUrls = item.metadata.mediaUrls as string[];
      const downloadResults = new Map<string, boolean>();
      for (const mediaUrl of mediaUrls) {
        try {
          const res = await fetch(mediaUrl);
          if (!res.ok) { downloadResults.set(mediaUrl, false); continue; }
          const buf = Buffer.from(await res.arrayBuffer());
          const urlPath = new URL(mediaUrl).pathname;
          const fileName = urlPath.split("/").pop() || `media-${Date.now()}`;
          await writeFile(resolve(mediaDir, fileName), buf);
          totalBytes += buf.length;
          downloadResults.set(mediaUrl, true);
        } catch { downloadResults.set(mediaUrl, false); }
      }

      // Scan content for warnings
      const contentStr = JSON.stringify(page.layoutSections || {}) +
        JSON.stringify(page.widgetContainers || {}) +
        JSON.stringify(page.widgets || {});
      if (contentStr.length > 4) {
        const warnings = scanContent(contentStr, page.id, downloadResults);
        for (const w of warnings) {
          manifest.warnings.push(`[${w.type}] Page "${page.name}": ${w.message} — ${w.snippet}`);
        }
      }

      item.status = "exported";
      totalBytes += item.size;
      exported++;

      if (exported % 10 === 0) {
        await db.update(tasks).set({ exportedItems: exported, failedItems: failed, localStorageBytes: totalBytes }).where(eq(tasks.id, taskId));
        flushManifest(manifestPath, manifest);
      }
      if (exported % 25 === 0) {
        await logToTask(taskId, "info", `Export progress: ${exported}/${manifest.items.length} pages`);
      }
    } catch (err) {
      item.status = "failed";
      item.error = err instanceof Error ? err.message : String(err);
      failed++;
    }
  }

  if (ctx.outputType === "csv") {
    const csvRecords = manifest.items
      .filter((i) => i.status === "exported")
      .map((i) => ({
        id: i.id, name: i.metadata.name, slug: i.metadata.slug,
        htmlTitle: i.metadata.htmlTitle, state: i.metadata.state,
        subcategory: i.metadata.subcategory, publishDate: i.metadata.publishDate,
        url: i.sourceUrl, templatePath: i.metadata.templatePath,
        metaDescription: i.metadata.metaDescription,
      }));
    const csvPath = await writeCsvExport(migration.id, taskId, "pages", csvRecords as Record<string, unknown>[], PAGE_CSV_COLUMNS);
    await logToTask(taskId, "info", `CSV export saved: ${csvPath}`);
  }

  manifest.warnings = [...new Set(manifest.warnings)];
  manifest.phase = "exported";
  manifest.exportedAt = new Date().toISOString();
  flushManifest(manifestPath, manifest);

  await db.update(tasks).set({
    status: "exported", phase: "export", exportedItems: exported,
    failedItems: failed, localStorageBytes: totalBytes, exportedAt: new Date(),
  }).where(eq(tasks.id, taskId));

  const warningCount = manifest.warnings.length;
  await logToTask(taskId, "info", `Export completed. ${exported} pages downloaded, ${failed} failed.${warningCount > 0 ? ` ${warningCount} content warnings found.` : ""}`);
}

// ── IMPORT PHASE ──

export async function importPages(
  taskId: number,
  migration: Migration,
  options: { dryRun: boolean } = { dryRun: false }
): Promise<void> {
  const ctx = await createRunnerContext(taskId, migration, options);
  if (!ctx) return;

  const { targetToken, manifestPath, dryRun } = ctx;
  const manifest = readManifest(manifestPath);
  const dataDir = getDataDir(migration.id, taskId);
  const mediaDir = resolve(dataDir, "media");

  await db.update(tasks).set({ status: "importing", phase: "import" }).where(eq(tasks.id, taskId));

  if (dryRun) {
    await logToTask(taskId, "info", "DRY RUN — no pages will be created in target portal");
  }

  const urlMapping: Record<string, string> = await getExistingUrlMapping(migration.id);

  if (!dryRun) {
    await logToTask(taskId, "info", "Uploading discovered media...");
    const allMediaUrls = new Set(manifest.items.flatMap((i) => (i.metadata.mediaUrls as string[]) || []));
    let mediaUploaded = 0;
    let mediaSkipped = 0;
    for (const mediaUrl of allMediaUrls) {
      if (urlMapping[mediaUrl]) { mediaSkipped++; continue; }
      try {
        const urlPath = new URL(mediaUrl).pathname;
        const fileName = urlPath.split("/").pop() || `media-${Date.now()}`;
        const localPath = resolve(mediaDir, fileName);
        let fileBuffer: Buffer;
        try { fileBuffer = Buffer.from(await readFile(localPath)); } catch { continue; }
        const uploaded = await uploadFile(targetToken, fileBuffer, fileName);
        urlMapping[mediaUrl] = uploaded.url;
        mediaUploaded++;
      } catch { /* non-fatal */ }
    }
    await logToTask(taskId, "info", `Media: ${mediaUploaded} uploaded, ${mediaSkipped} already mapped`);
  }

  let imported = 0;
  let failed = 0;
  let skipped = 0;
  const exportedItems = manifest.items.filter((i) => i.status === "exported");

  for (const item of exportedItems) {
    if (await isTaskPaused(taskId)) {
      await logToTask(taskId, "info", "Import paused");
      flushManifest(manifestPath, manifest);
      await db.update(tasks).set({ importedItems: imported, failedItems: failed, urlMapping: JSON.stringify(urlMapping) }).where(eq(tasks.id, taskId));
      return;
    }

    const slug = item.metadata.slug as string;
    const subcategory = (item.metadata.subcategory as string) || "site_page";

    if (!dryRun && slug) {
      const existing = await fetchPageBySlug(targetToken, slug, subcategory as "site_page" | "landing_page");
      if (existing) {
        item.status = "skipped";
        item.targetId = existing.id;
        item.targetUrl = existing.url;
        skipped++;
        continue;
      }
    }

    if (dryRun) {
      await logToTask(taskId, "info", `[DRY RUN] Would create ${subcategory} "${item.metadata.name}" (/${slug})`);
      item.status = "skipped";
      skipped++;
      continue;
    }

    try {
      if (!item.localPath) throw new Error("No local page file");
      const pageJson = await readFile(item.localPath, "utf-8");
      const page = JSON.parse(pageJson) as HubSpotPage;

      const rewrittenLayoutSections = rewriteUrlsInObject(page.layoutSections, urlMapping) as Record<string, unknown>;
      const rewrittenWidgets = rewriteUrlsInObject(page.widgets, urlMapping) as Record<string, unknown>;
      const rewrittenWidgetContainers = rewriteUrlsInObject(page.widgetContainers, urlMapping) as Record<string, unknown>;
      const rewrittenFeaturedImage = page.featuredImage ? urlMapping[page.featuredImage] || page.featuredImage : "";

      const createFn = subcategory === "landing_page" ? createLandingPage : createSitePage;
      const created = await createFn(targetToken, {
        name: page.name, slug: page.slug, htmlTitle: page.htmlTitle,
        metaDescription: page.metaDescription, featuredImage: rewrittenFeaturedImage,
        featuredImageAltText: page.featuredImageAltText, templatePath: page.templatePath,
        layoutSections: rewrittenLayoutSections, widgets: rewrittenWidgets,
        widgetContainers: rewrittenWidgetContainers, publishDate: page.publishDate, state: "DRAFT",
      });

      item.status = "imported";
      item.targetId = created.id;
      item.targetUrl = created.url;
      imported++;

      if (imported % 10 === 0) {
        await db.update(tasks).set({ importedItems: imported, failedItems: failed, urlMapping: JSON.stringify(urlMapping) }).where(eq(tasks.id, taskId));
        flushManifest(manifestPath, manifest);
      }
      if (imported % 25 === 0) {
        await logToTask(taskId, "info", `Import progress: ${imported}/${exportedItems.length} pages`);
      }
    } catch (err) {
      item.status = "failed";
      item.error = err instanceof Error ? err.message : String(err);
      failed++;
    }
  }

  manifest.phase = "completed";
  manifest.importedAt = new Date().toISOString();
  flushManifest(manifestPath, manifest);

  await db.update(tasks).set({
    status: "completed", completedAt: new Date(), importedItems: imported,
    failedItems: failed, urlMapping: JSON.stringify(urlMapping),
  }).where(eq(tasks.id, taskId));

  await logToTask(taskId, "info", `Import ${dryRun ? "(DRY RUN) " : ""}completed. ${imported} pages created, ${skipped} skipped, ${failed} failed.`);
}
