import { createServerFn } from "@tanstack/react-start";
import { db } from "../db";
import { tasks, migrations, serviceKeys } from "../db/schema";
import { eq } from "drizzle-orm";
import { fetchAllBlogPosts, fetchAllFiles, fetchAllBlogTags, fetchContentGroups, hubspotFetch } from "./hubspot";
import { readManifest, flushManifest } from "./manifest";

// ── Logging ──

export type LogEntry = {
  timestamp: string;
  level: "info" | "warn" | "error";
  message: string;
};

export function appendLog(
  existingLog: string | null,
  level: LogEntry["level"],
  message: string
): string {
  const entries: LogEntry[] = existingLog ? JSON.parse(existingLog) : [];
  entries.push({
    timestamp: new Date().toISOString(),
    level,
    message,
  });
  return JSON.stringify(entries);
}

// ── Pre-flight: Media Summary ──

export const fetchMediaSummary = createServerFn({ method: "POST" })
  .inputValidator((migrationId: number) => migrationId)
  .handler(async ({ data: migrationId }) => {
    const [migration] = await db.select().from(migrations).where(eq(migrations.id, migrationId));
    if (!migration) throw new Error("Migration not found");

    const [sourceKey] = await db.select().from(serviceKeys).where(eq(serviceKeys.id, migration.sourceKeyId));
    const [targetKey] = await db.select().from(serviceKeys).where(eq(serviceKeys.id, migration.targetKeyId));
    if (!sourceKey) throw new Error("Source key not found");

    const files = await fetchAllFiles(sourceKey.accessToken);

    const totalFiles = files.length;
    const totalBytes = files.reduce((sum, f) => sum + (f.size || 0), 0);

    const byType: Record<string, { count: number; bytes: number }> = {};
    for (const f of files) {
      const ext = (f.extension || "other").toLowerCase();
      if (!byType[ext]) byType[ext] = { count: 0, bytes: 0 };
      byType[ext].count++;
      byType[ext].bytes += f.size || 0;
    }

    let targetStorage: { bytesUsed: number; bytesLimit: number } | null = null;
    if (targetKey) {
      try {
        const res = await hubspotFetch(targetKey.accessToken, "/files/v3/usage");
        if (res.ok) {
          const data = (await res.json()) as {
            bytesUsed?: number; bytesLimit?: number;
            usage?: { bytesUsed?: number; bytesLimit?: number };
          };
          targetStorage = {
            bytesUsed: data.bytesUsed ?? data.usage?.bytesUsed ?? 0,
            bytesLimit: data.bytesLimit ?? data.usage?.bytesLimit ?? 0,
          };
        }
      } catch { /* best-effort */ }
    }

    let spaceWarning: string | null = null;
    if (targetStorage && targetStorage.bytesLimit > 0) {
      const remaining = targetStorage.bytesLimit - targetStorage.bytesUsed;
      if (totalBytes > remaining) {
        spaceWarning = `Source files (${formatBytes(totalBytes)}) exceed target storage (${formatBytes(remaining)} free of ${formatBytes(targetStorage.bytesLimit)}).`;
      } else if (totalBytes > remaining * 0.8) {
        spaceWarning = `This migration will use most of the remaining storage (${formatBytes(remaining)} free).`;
      }
    }

    return {
      totalFiles,
      totalBytes,
      byType: Object.entries(byType)
        .sort((a, b) => b[1].count - a[1].count)
        .map(([ext, stats]) => ({ ext, ...stats })),
      targetStorage,
      spaceWarning,
    };
  });

function formatBytes(bytes: number): string {
  if (bytes === 0) return "0 B";
  const k = 1024;
  const sizes = ["B", "KB", "MB", "GB"];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return `${(bytes / Math.pow(k, i)).toFixed(1)} ${sizes[i]}`;
}

// ── Pre-flight: Blog Posts ──

export const fetchSourceBlogPosts = createServerFn({ method: "POST" })
  .inputValidator((migrationId: number) => migrationId)
  .handler(async ({ data: migrationId }) => {
    const [migration] = await db.select().from(migrations).where(eq(migrations.id, migrationId));
    if (!migration) throw new Error("Migration not found");

    const [sourceKey] = await db.select().from(serviceKeys).where(eq(serviceKeys.id, migration.sourceKeyId));
    if (!sourceKey) throw new Error("Source key not found");

    const token = sourceKey.accessToken;

    // Fetch posts, tags, and blogs in parallel
    const [posts, allTags, allBlogs] = await Promise.all([
      fetchAllBlogPosts(token),
      fetchAllBlogTags(token).catch((err) => {
        console.error("Failed to fetch blog tags:", err);
        return [];
      }),
      fetchContentGroups(token).catch(() => []),
    ]);

    // Build lookup maps
    const tagMap = new Map(allTags.map((t) => [t.id, t.name]));

    // If the tags API returned nothing, try to extract tags from post objects
    // Some HubSpot responses include tag_ids or topic_ids but the /blogs/tags endpoint needs different scopes
    if (tagMap.size === 0) {
      // Collect all unique tag IDs from posts
      for (const p of posts) {
        const raw = p as Record<string, unknown>;
        const ids = (raw.tagIds as string[]) || (raw.topicIds as string[]) || (raw.tag_ids as string[]) || [];
        for (const id of ids) {
          if (!tagMap.has(id)) {
            tagMap.set(id, `Tag ${id}`);
          }
        }
      }
      // Try to fetch individual tag names
      for (const [id] of tagMap) {
        try {
          const res = await hubspotFetch(token, `/cms/v3/blogs/tags/${id}`);
          if (res.ok) {
            const tag = (await res.json()) as { id: string; name: string };
            tagMap.set(id, tag.name);
          }
        } catch {
          // Keep the fallback name
        }
      }
    }
    const blogMap = new Map(allBlogs.map((b) => [b.id, b.name]));

    // If the blogs API didn't return results, derive from posts' contentGroupId
    // and try to get names from the URL pattern
    if (blogMap.size === 0) {
      const groupIds = new Set(posts.map((p) => p.contentGroupId).filter(Boolean));
      for (const gid of groupIds) {
        // Try to derive a name from the first post's URL in this group
        const sample = posts.find((p) => p.contentGroupId === gid);
        const urlPath = sample?.url ? new URL(sample.url).pathname.split("/")[1] : null;
        blogMap.set(gid, urlPath || `Blog ${gid}`);
      }
    }

    return {
      posts: posts.map((p) => {
        // HubSpot uses tagIds in v3, but some versions use topicIds (can be numbers)
        const raw = p as Record<string, unknown>;
        const rawIds = (raw.tagIds as Array<string | number> | undefined)
          || (raw.topicIds as Array<string | number> | undefined)
          || (raw.tag_ids as Array<string | number> | undefined)
          || [];
        const postTagIds = rawIds.map(String);
        return {
          id: p.id,
          name: p.name,
          slug: p.slug,
          state: p.state,
          featuredImage: p.featuredImage,
          publishDate: p.publishDate,
          contentGroupId: p.contentGroupId,
          contentGroupName: blogMap.get(p.contentGroupId) || p.contentGroupId,
          tagIds: postTagIds,
          tagNames: postTagIds.map((id) => tagMap.get(id) || id),
          url: p.url,
        };
      }),
      tags: allTags.map((t) => ({ id: t.id, name: t.name })),
      contentGroups: Array.from(blogMap.entries()).map(([id, name]) => ({ id, name })),
    };
  });

// ── CRUD ──

export const createTask = createServerFn({ method: "POST" })
  .inputValidator(
    (data: {
      migrationId: number;
      type: "media" | "blog_posts" | "hubdb" | "page" | "csv_import";
      label: string;
      outputType?: "same_as_source" | "hubdb" | "csv";
      config?: string;
      csvFileContent?: string;
      csvFileName?: string;
    }) => data
  )
  .handler(async ({ data }) => {
    const [task] = await db
      .insert(tasks)
      .values({
        migrationId: data.migrationId,
        type: data.type,
        label: data.label,
        outputType: data.outputType || "same_as_source",
        config: data.config || null,
        status: "pending",
        phase: "export",
        log: JSON.stringify([]),
      })
      .returning();

    // Save CSV file to disk if provided
    if (data.csvFileContent && data.csvFileName && task) {
      const { getDataDir } = await import("./manifest");
      const { resolve } = await import("path");
      const { mkdirSync, writeFileSync } = await import("fs");

      const dataDir = getDataDir(data.migrationId, task.id);
      mkdirSync(dataDir, { recursive: true });

      const csvPath = resolve(dataDir, data.csvFileName);
      writeFileSync(csvPath, data.csvFileContent, "utf-8");

      // Update config with file path
      let config: Record<string, unknown> = {};
      if (task.config) {
        try {
          config = JSON.parse(task.config);
        } catch {
          /* ignore */
        }
      }
      config.csvFilePath = csvPath;
      config.csvFileName = data.csvFileName;

      await db
        .update(tasks)
        .set({ config: JSON.stringify(config) })
        .where(eq(tasks.id, task.id));

      const [updated] = await db
        .select()
        .from(tasks)
        .where(eq(tasks.id, task.id));
      return updated;
    }

    return task;
  });

export const getTask = createServerFn({ method: "GET" })
  .inputValidator((id: number) => id)
  .handler(async ({ data: id }) => {
    const [task] = await db.select().from(tasks).where(eq(tasks.id, id));
    if (!task) throw new Error("Task not found");
    return task;
  });

export const deleteTask = createServerFn({ method: "POST" })
  .inputValidator((id: number) => id)
  .handler(async ({ data: id }) => {
    const [task] = await db.select().from(tasks).where(eq(tasks.id, id));
    if (task) {
      // Clean up local files
      try {
        const { getManifestDir } = await import("./manifest");
        const { rm } = await import("fs/promises");
        const dir = getManifestDir(task.migrationId, id);
        await rm(dir, { recursive: true, force: true });
      } catch { /* best-effort cleanup */ }
    }
    await db.delete(tasks).where(eq(tasks.id, id));
  });

// ── Storage Cleanup ──

export const getOrphanedStorage = createServerFn({ method: "GET" })
  .inputValidator(() => undefined)
  .handler(async () => {
    const { resolve } = await import("path");
    const { readdir, stat } = await import("fs/promises");

    const downloadsDir = resolve(process.cwd(), "memit-downloads");
    const orphaned: Array<{ path: string; migrationId: string; taskId: string; sizeBytes: number }> = [];

    // Get all task IDs and migration IDs from DB
    const allTasks = await db.select().from(tasks);
    const allMigrations = await db.select().from(migrations);
    const taskIds = new Set(allTasks.map((t) => String(t.id)));
    const migrationIds = new Set(allMigrations.map((m) => String(m.id)));

    let migrationDirs: string[];
    try {
      migrationDirs = await readdir(downloadsDir);
    } catch {
      return { orphaned: [], totalBytes: 0 };
    }

    for (const migDir of migrationDirs) {
      const migPath = resolve(downloadsDir, migDir);
      const migStat = await stat(migPath).catch(() => null);
      if (!migStat?.isDirectory()) continue;

      if (!migrationIds.has(migDir)) {
        // Entire migration folder is orphaned
        const size = await getDirSize(migPath);
        orphaned.push({ path: migPath, migrationId: migDir, taskId: "*", sizeBytes: size });
        continue;
      }

      // Check individual task folders
      let taskDirs: string[];
      try {
        taskDirs = await readdir(migPath);
      } catch {
        continue;
      }

      for (const taskDir of taskDirs) {
        const taskPath = resolve(migPath, taskDir);
        const taskStat = await stat(taskPath).catch(() => null);
        if (!taskStat?.isDirectory()) continue;

        if (!taskIds.has(taskDir)) {
          const size = await getDirSize(taskPath);
          orphaned.push({ path: taskPath, migrationId: migDir, taskId: taskDir, sizeBytes: size });
        }
      }
    }

    const totalBytes = orphaned.reduce((sum, o) => sum + o.sizeBytes, 0);
    return { orphaned, totalBytes };
  });

async function getDirSize(dirPath: string): Promise<number> {
  const { readdir, stat } = await import("fs/promises");
  const { resolve } = await import("path");
  let total = 0;
  try {
    const entries = await readdir(dirPath, { withFileTypes: true });
    for (const entry of entries) {
      const fullPath = resolve(dirPath, entry.name);
      if (entry.isDirectory()) {
        total += await getDirSize(fullPath);
      } else {
        const s = await stat(fullPath).catch(() => null);
        if (s) total += s.size;
      }
    }
  } catch { /* */ }
  return total;
}

export const cleanupOrphanedStorage = createServerFn({ method: "POST" })
  .inputValidator(() => undefined)
  .handler(async () => {
    const { rm } = await import("fs/promises");
    const result = await getOrphanedStorage();
    let cleaned = 0;
    let freedBytes = 0;

    for (const entry of result.orphaned) {
      try {
        await rm(entry.path, { recursive: true, force: true });
        cleaned++;
        freedBytes += entry.sizeBytes;
      } catch { /* best-effort */ }
    }

    return { cleaned, freedBytes, total: result.orphaned.length };
  });

export const getTasksForMigration = createServerFn({ method: "GET" })
  .inputValidator((migrationId: number) => migrationId)
  .handler(async ({ data: migrationId }) => {
    return db.select().from(tasks).where(eq(tasks.migrationId, migrationId)).orderBy(tasks.createdAt);
  });

// ── Two-Phase Orchestration ──

const runnerErrorHandler = async (taskId: number, err: unknown) => {
  const [current] = await db.select().from(tasks).where(eq(tasks.id, taskId));
  await db
    .update(tasks)
    .set({
      status: "failed",
      completedAt: new Date(),
      log: appendLog(
        current?.log ?? null,
        "error",
        `Fatal error: ${err instanceof Error ? err.message : String(err)}`
      ),
    })
    .where(eq(tasks.id, taskId));
};

export const exportTask = createServerFn({ method: "POST" })
  .inputValidator((data: { taskId: number }) => data)
  .handler(async ({ data }) => {
    const [task] = await db.select().from(tasks).where(eq(tasks.id, data.taskId));
    if (!task) throw new Error("Task not found");

    const validStatuses = ["pending", "export_paused", "exported", "failed"];
    if (!validStatuses.includes(task.status)) {
      throw new Error(`Cannot export task in "${task.status}" state`);
    }

    const [migration] = await db.select().from(migrations).where(eq(migrations.id, task.migrationId));
    if (!migration) throw new Error("Migration not found");

    // Update status
    await db
      .update(tasks)
      .set({
        status: "exporting",
        phase: "export",
        startedAt: task.startedAt || new Date(),
        log: appendLog(task.log, "info", `Starting export...`),
      })
      .where(eq(tasks.id, task.id));

    // Dispatch to runner
    if (task.type === "media") {
      const { exportMedia } = await import("./runners/media");
      exportMedia(task.id, migration).catch((err) => runnerErrorHandler(task.id, err));
    } else if (task.type === "blog_posts") {
      const { exportBlogPosts } = await import("./runners/blogs");
      exportBlogPosts(task.id, migration).catch((err) => runnerErrorHandler(task.id, err));
    } else if (task.type === "hubdb") {
      const { exportHubDb } = await import("./runners/hubdb");
      exportHubDb(task.id, migration).catch((err) => runnerErrorHandler(task.id, err));
    } else if (task.type === "page") {
      const { exportPages } = await import("./runners/pages");
      exportPages(task.id, migration).catch((err) => runnerErrorHandler(task.id, err));
    } else if (task.type === "csv_import") {
      const { exportCsvImport } = await import("./runners/csv-import");
      exportCsvImport(task.id, migration).catch((err) => runnerErrorHandler(task.id, err));
    } else {
      await db
        .update(tasks)
        .set({
          status: "failed",
          completedAt: new Date(),
          log: appendLog(task.log, "error", `Export for "${task.type}" is not yet implemented`),
        })
        .where(eq(tasks.id, task.id));
    }

    const [updated] = await db.select().from(tasks).where(eq(tasks.id, task.id));
    return updated;
  });

export const importTask = createServerFn({ method: "POST" })
  .inputValidator((data: { taskId: number; dryRun?: boolean }) => data)
  .handler(async ({ data }) => {
    const [task] = await db.select().from(tasks).where(eq(tasks.id, data.taskId));
    if (!task) throw new Error("Task not found");

    const validStatuses = ["exported", "import_paused", "failed", "completed"];
    if (!validStatuses.includes(task.status)) {
      throw new Error(`Cannot import task in "${task.status}" state`);
    }

    const [migration] = await db.select().from(migrations).where(eq(migrations.id, task.migrationId));
    if (!migration) throw new Error("Migration not found");

    const dryRun = data.dryRun || false;

    await db
      .update(tasks)
      .set({
        status: "importing",
        phase: "import",
        log: appendLog(task.log, "info", `Starting ${dryRun ? "dry run " : ""}import...`),
      })
      .where(eq(tasks.id, task.id));

    if (task.type === "media") {
      const { importMedia } = await import("./runners/media");
      importMedia(task.id, migration, { dryRun }).catch((err) => runnerErrorHandler(task.id, err));
    } else if (task.type === "blog_posts") {
      const { importBlogPosts } = await import("./runners/blogs");
      importBlogPosts(task.id, migration, { dryRun }).catch((err) => runnerErrorHandler(task.id, err));
    } else if (task.type === "hubdb") {
      const { importHubDb } = await import("./runners/hubdb");
      importHubDb(task.id, migration, { dryRun }).catch((err) => runnerErrorHandler(task.id, err));
    } else if (task.type === "page") {
      const { importPages } = await import("./runners/pages");
      importPages(task.id, migration, { dryRun }).catch((err) => runnerErrorHandler(task.id, err));
    } else if (task.type === "csv_import") {
      const { importCsvImport } = await import("./runners/csv-import");
      importCsvImport(task.id, migration, { dryRun }).catch((err) => runnerErrorHandler(task.id, err));
    } else {
      await db
        .update(tasks)
        .set({
          status: "failed",
          completedAt: new Date(),
          log: appendLog(task.log, "error", `Import for "${task.type}" is not yet implemented`),
        })
        .where(eq(tasks.id, task.id));
    }

    const [updated] = await db.select().from(tasks).where(eq(tasks.id, task.id));
    return updated;
  });

export const pauseTask = createServerFn({ method: "POST" })
  .inputValidator((taskId: number) => taskId)
  .handler(async ({ data: taskId }) => {
    const [task] = await db.select().from(tasks).where(eq(tasks.id, taskId));
    if (!task) throw new Error("Task not found");

    let newStatus: string;
    if (task.status === "exporting") {
      newStatus = "export_paused";
    } else if (task.status === "importing") {
      newStatus = "import_paused";
    } else {
      throw new Error(`Cannot pause task in "${task.status}" state`);
    }

    await db
      .update(tasks)
      .set({
        status: newStatus as typeof task.status,
        log: appendLog(task.log, "info", "Task paused by user"),
      })
      .where(eq(tasks.id, taskId));

    const [updated] = await db.select().from(tasks).where(eq(tasks.id, taskId));
    return updated;
  });

// ── Task Import Settings ──

export const updateTaskSettings = createServerFn({ method: "POST" })
  .inputValidator(
    (data: { taskId: number; hubdbTableName?: string; mediaFolderPath?: string }) => data
  )
  .handler(async ({ data }) => {
    const [task] = await db.select().from(tasks).where(eq(tasks.id, data.taskId));
    if (!task) throw new Error("Task not found");

    let config: Record<string, unknown> = {};
    if (task.config) {
      try { config = JSON.parse(task.config); } catch { /* */ }
    }

    if (data.hubdbTableName !== undefined) {
      config.hubdbTableName = data.hubdbTableName
        .toLowerCase()
        .replace(/[^a-z0-9_]/g, "_")
        .replace(/^_|_$/g, "");
    }
    if (data.mediaFolderPath !== undefined) {
      config.mediaFolderPath = "/" + data.mediaFolderPath
        .toLowerCase()
        .replace(/[^a-z0-9-/]/g, "-")
        .replace(/^[-/]+|[-/]+$/g, "");
    }

    await db
      .update(tasks)
      .set({ config: JSON.stringify(config) })
      .where(eq(tasks.id, data.taskId));

    return { saved: true, hubdbTableName: config.hubdbTableName, mediaFolderPath: config.mediaFolderPath };
  });

// ── Reset to Review ──

export const resetToExported = createServerFn({ method: "POST" })
  .inputValidator((taskId: number) => taskId)
  .handler(async ({ data: taskId }) => {
    const [task] = await db.select().from(tasks).where(eq(tasks.id, taskId));
    if (!task) throw new Error("Task not found");

    // Reset manifest items back to exported
    if (task.manifestPath) {
      const manifest = readManifest(task.manifestPath);
      for (const item of manifest.items) {
        if (item.status === "imported" || item.status === "skipped" || item.status === "failed") {
          item.status = "exported";
          item.targetUrl = null;
          item.targetId = null;
          item.error = null;
        }
      }
      manifest.phase = "exported";
      manifest.importedAt = null;
      flushManifest(task.manifestPath, manifest);
    }

    await db
      .update(tasks)
      .set({
        status: "exported",
        phase: "export",
        importedItems: 0,
        failedItems: 0,
        completedAt: null,
        log: appendLog(task.log, "info", "Reset to review step — ready for fresh import"),
      })
      .where(eq(tasks.id, taskId));

    const [updated] = await db.select().from(tasks).where(eq(tasks.id, taskId));
    return updated;
  });

// ── Retry Failed Media ──

export const retryFailedMediaDownloads = createServerFn({ method: "POST" })
  .inputValidator((taskId: number) => taskId)
  .handler(async ({ data: taskId }) => {
    const [task] = await db.select().from(tasks).where(eq(tasks.id, taskId));
    if (!task) throw new Error("Task not found");

    const [migration] = await db.select().from(migrations).where(eq(migrations.id, task.migrationId));
    if (!migration) throw new Error("Migration not found");

    // Local log helper (logToTask is in runners/base, not available here)
    async function log(level: "info" | "warn" | "error", message: string) {
      const [t] = await db.select().from(tasks).where(eq(tasks.id, taskId));
      if (t) {
        await db.update(tasks).set({ log: appendLog(t.log, level, message) }).where(eq(tasks.id, taskId));
      }
    }

    // Retry both download failures (localPath is null) and upload failures (not in urlMapping)
    const { getDataDir } = await import("./manifest");
    const { resolve } = await import("path");
    const { readFileSync } = await import("fs");
    const { writeFileSync, mkdirSync } = await import("fs");
    const { uploadFile, importFileFromUrl, fetchAllHubDbRows, updateHubDbRow, publishHubDbTable, fetchHubDbTableByName } = await import("./hubspot");

    // Load existing URL mapping
    const urlMapping: Record<string, string> = task.urlMapping ? JSON.parse(task.urlMapping) : {};

    // Load media catalog
    let mediaCatalog: Array<{ sourceUrl: string; localPath: string | null; size: number; foundIn?: string[] }> = [];
    try {
      const dataDir = getDataDir(task.migrationId, taskId);
      const catalogRaw = readFileSync(resolve(dataDir, "_media.json"), "utf-8");
      mediaCatalog = JSON.parse(catalogRaw);
    } catch {
      // No catalog — check manifest for media URLs not in mapping
    }

    // Find download failures (no local file) and upload failures (not in urlMapping)
    const downloadFailed = mediaCatalog.filter((e) => !e.localPath);
    const uploadFailed = mediaCatalog.filter((e) => e.localPath && !urlMapping[e.sourceUrl]);

    if (downloadFailed.length === 0 && uploadFailed.length === 0) {
      await log("info", "No failed media to retry — all media is downloaded and uploaded");
      return { retried: 0, succeeded: 0, failed: 0 };
    }

    // Resolve target token
    const [targetKey] = await db.select().from(serviceKeys).where(eq(serviceKeys.id, migration.targetKeyId));
    if (!targetKey) throw new Error("Target service key not found");
    const targetToken = targetKey.accessToken;

    // Derive upload folder (use config override or migration name)
    let uploadFolderPath = "/" + migration.name
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "-")
      .replace(/^-|-$/g, "");
    if (task.config) {
      try {
        const cfg = JSON.parse(task.config) as { mediaFolderPath?: string };
        if (cfg.mediaFolderPath) uploadFolderPath = cfg.mediaFolderPath;
      } catch { /* */ }
    }

    let succeeded = 0;
    let stillFailed = 0;

    // Retry downloads first
    if (downloadFailed.length > 0) {
      await log("info", `Retrying ${downloadFailed.length} failed media downloads...`);
      const dataDir = getDataDir(task.migrationId, taskId);
      const mediaDir = resolve(dataDir, "media");
      mkdirSync(mediaDir, { recursive: true });
      let fileIndex = mediaCatalog.filter((e) => e.localPath).length;

      for (const entry of downloadFailed) {
        try {
          const res = await fetch(entry.sourceUrl, { signal: AbortSignal.timeout(30_000) });
          if (!res.ok) { stillFailed++; continue; }
          const buf = Buffer.from(await res.arrayBuffer());
          const urlPath = new URL(entry.sourceUrl).pathname;
          const fileName = urlPath.split("/").pop() || `media-${Date.now()}`;
          const localPath = resolve(mediaDir, `${fileIndex}-${fileName}`);
          writeFileSync(localPath, buf);
          entry.localPath = localPath;
          entry.size = buf.length;
          fileIndex++;
          uploadFailed.push(entry); // Now try uploading it too
          await log("info", `Download retry succeeded: ${fileName}`);
        } catch {
          stillFailed++;
        }
      }
      // Save updated catalog
      writeFileSync(resolve(getDataDir(task.migrationId, taskId), "_media.json"), JSON.stringify(mediaCatalog, null, 2), "utf-8");
    }

    // Retry uploads
    if (uploadFailed.length > 0) {
      await log("info", `Retrying ${uploadFailed.length} failed media uploads...`);
      for (const entry of uploadFailed) {
        const fileName = entry.localPath!.split("/").pop() || `media-${Date.now()}`;
        try {
          // Try direct upload first
          const fileBuffer = Buffer.from(readFileSync(entry.localPath!));
          const uploaded = await uploadFile(targetToken, fileBuffer, fileName, undefined, uploadFolderPath);
          urlMapping[entry.sourceUrl] = uploaded.url;
          succeeded++;
          await log("info", `Upload retry succeeded: ${fileName} → ${uploaded.url}`);
        } catch {
          // Fallback: import-from-URL (HubSpot fetches the file directly — better for large files)
          try {
            await log("info", `Direct upload failed for ${fileName}, trying import-from-URL...`);
            const imported = await importFileFromUrl(targetToken, entry.sourceUrl, fileName, undefined, uploadFolderPath);
            urlMapping[entry.sourceUrl] = imported.url;
            succeeded++;
            await log("info", `Import-from-URL succeeded: ${fileName} → ${imported.url}`);
          } catch (err2) {
            stillFailed++;
            await log("warn", `All upload methods failed for ${fileName}: ${err2 instanceof Error ? err2.message : String(err2)}`);
          }
        }
      }
    }

    // Patch HubDB rows with newly mapped URLs (if this is a csv_import → hubdb task)
    if (succeeded > 0 && task.type === "csv_import" && task.outputType === "hubdb") {
      // Build a map of old → new URLs from this retry
      const newMappings: Record<string, string> = {};
      for (const entry of [...downloadFailed, ...uploadFailed]) {
        if (urlMapping[entry.sourceUrl]) {
          newMappings[entry.sourceUrl] = urlMapping[entry.sourceUrl]!;
        }
      }

      if (Object.keys(newMappings).length > 0) {
        // Find the HubDB table
        let tblConfig: Record<string, unknown> = {};
        if (task.config) {
          try { tblConfig = JSON.parse(task.config); } catch { /* */ }
        }
        const tblName = (tblConfig.hubdbTableName as string) ||
          ((tblConfig.csvFileName as string) || "csv_import").replace(/\.csv$/i, "").replace(/[^a-zA-Z0-9_]/g, "_").toLowerCase();

        try {
          const hubdbTable = await fetchHubDbTableByName(targetToken, tblName);
          if (hubdbTable) {
            await log("info", `Patching HubDB table "${tblName}" rows with ${Object.keys(newMappings).length} new media URLs...`);
            const rows = await fetchAllHubDbRows(targetToken, hubdbTable.id);
            let patchedCount = 0;

            for (const row of rows) {
              let rowNeedsUpdate = false;
              const updatedValues: Record<string, unknown> = {};

              for (const [colId, value] of Object.entries(row.values)) {
                if (typeof value === "string") {
                  let newValue = value;
                  for (const [oldUrl, newUrl] of Object.entries(newMappings)) {
                    if (newValue.includes(oldUrl)) {
                      newValue = newValue.split(oldUrl).join(newUrl);
                      rowNeedsUpdate = true;
                    }
                  }
                  updatedValues[colId] = newValue;
                } else {
                  updatedValues[colId] = value;
                }
              }

              if (rowNeedsUpdate) {
                try {
                  await updateHubDbRow(targetToken, hubdbTable.id, row.id, { values: updatedValues });
                  patchedCount++;
                } catch (patchErr) {
                  await log("warn", `Failed to patch row ${row.id}: ${patchErr instanceof Error ? patchErr.message : String(patchErr)}`);
                }
              }
            }

            if (patchedCount > 0) {
              // Re-publish the table to make changes live
              try {
                await publishHubDbTable(targetToken, hubdbTable.id);
                await log("info", `Patched ${patchedCount} rows and re-published table "${tblName}"`);
              } catch {
                await log("warn", `Patched ${patchedCount} rows but re-publish failed — publish manually in HubSpot`);
              }
            } else {
              await log("info", "No HubDB rows needed URL patching");
            }
          }
        } catch (err) {
          await log("warn", `Could not patch HubDB rows: ${err instanceof Error ? err.message : String(err)}`);
        }
      }
    }

    // Save updated mapping
    // Update urlMapping + mediaFailedCount
    const [latestTask] = await db.select().from(tasks).where(eq(tasks.id, taskId));
    let updatedConfig: Record<string, unknown> = {};
    if (latestTask?.config) {
      try { updatedConfig = JSON.parse(latestTask.config); } catch { /* */ }
    }
    updatedConfig.mediaFailedCount = stillFailed;

    await db
      .update(tasks)
      .set({ urlMapping: JSON.stringify(urlMapping), config: JSON.stringify(updatedConfig) })
      .where(eq(tasks.id, taskId));

    await log("info", `Retry complete: ${succeeded} succeeded, ${stillFailed} still failing`);
    return { retried: failed.length, succeeded, failed: stillFailed };
  });

// ── Manifest APIs ──

export const runTemplateExtraction = createServerFn({ method: "POST" })
  .inputValidator((taskId: number) => taskId)
  .handler(async ({ data: taskId }) => {
    const [task] = await db.select().from(tasks).where(eq(tasks.id, taskId));
    if (!task) throw new Error("Task not found");
    if (task.type !== "blog_posts") throw new Error("Template extraction only applies to blog tasks");
    if (!task.manifestPath) throw new Error("Task has no manifest — export first");

    const [migration] = await db
      .select()
      .from(migrations)
      .where(eq(migrations.id, task.migrationId));
    if (!migration) throw new Error("Migration not found");

    // Fire and forget
    const { extractTemplatesOnly } = await import("./runners/blogs");
    extractTemplatesOnly(task.id, migration).catch(async (err) => {
      const [current] = await db.select().from(tasks).where(eq(tasks.id, task.id));
      await db
        .update(tasks)
        .set({
          log: appendLog(
            current?.log ?? null,
            "error",
            `Template extraction failed: ${err instanceof Error ? err.message : String(err)}`
          ),
        })
        .where(eq(tasks.id, task.id));
    });

    return { started: true };
  });

export const getCtaMappings = createServerFn({ method: "POST" })
  .inputValidator((taskId: number) => taskId)
  .handler(async ({ data: taskId }) => {
    const [task] = await db.select().from(tasks).where(eq(tasks.id, taskId));
    if (!task || !task.manifestPath) return { ctas: [], mapping: {} };

    const { getDataDir } = await import("./manifest");
    const { resolve } = await import("path");
    const { readFileSync } = await import("fs");

    const dataDir = getDataDir(task.migrationId, taskId);

    // Try to read _ctas.json first, otherwise scan posts live
    let ctas: Array<{
      sourceGuid: string;
      targetGuid: string | null;
      postIds: string[];
      postCount: number;
    }> = [];
    try {
      const ctasRaw = readFileSync(resolve(dataDir, "_ctas.json"), "utf-8");
      ctas = JSON.parse(ctasRaw);
    } catch {
      // No _ctas.json — scan exported posts for CTA GUIDs on the fly
      const { extractCtaGuids } = await import("./scanners");
      const manifest = readManifest(task.manifestPath);
      const allGuids = new Map<string, string[]>();

      for (const item of manifest.items) {
        if (item.status !== "exported" || !item.localPath) continue;
        try {
          const postJson = readFileSync(item.localPath, "utf-8");
          const post = JSON.parse(postJson) as { postBody?: string; id: string };
          if (post.postBody) {
            const guids = extractCtaGuids(post.postBody, post.id);
            for (const [guid, postIds] of guids) {
              if (!allGuids.has(guid)) allGuids.set(guid, []);
              allGuids.get(guid)!.push(...postIds);
            }
          }
        } catch { /* skip */ }
      }

      if (allGuids.size === 0) return { ctas: [], mapping: {} };

      ctas = Array.from(allGuids.entries()).map(([guid, postIds]) => ({
        sourceGuid: guid,
        targetGuid: null,
        postIds: [...new Set(postIds)],
        postCount: new Set(postIds).size,
      }));

      // Save for next time
      try {
        const { writeFileSync: wfs } = await import("fs");
        wfs(resolve(dataDir, "_ctas.json"), JSON.stringify(ctas, null, 2), "utf-8");
      } catch { /* */ }
    }

    // Read existing mapping from task config
    let mapping: Record<string, string> = {};
    if (task.config) {
      try {
        const config = JSON.parse(task.config) as { ctaMapping?: Record<string, string> };
        if (config.ctaMapping) mapping = config.ctaMapping;
      } catch { /* */ }
    }

    // Merge any target GUIDs from _ctas.json
    for (const cta of ctas) {
      if (cta.targetGuid && !mapping[cta.sourceGuid]) {
        mapping[cta.sourceGuid] = cta.targetGuid;
      }
    }

    // Get post names for display
    const manifest = readManifest(task.manifestPath);
    const postNameMap: Record<string, string> = {};
    for (const item of manifest.items) {
      postNameMap[item.id] = (item.metadata?.name as string) || item.id;
    }

    return {
      ctas: ctas.map((c) => ({
        ...c,
        targetGuid: mapping[c.sourceGuid] || null,
        postNames: c.postIds.map((id) => postNameMap[id] || id),
      })),
      mapping,
    };
  });

export const saveCtaMappings = createServerFn({ method: "POST" })
  .inputValidator(
    (data: { taskId: number; mapping: Record<string, string> }) => data
  )
  .handler(async ({ data }) => {
    const [task] = await db.select().from(tasks).where(eq(tasks.id, data.taskId));
    if (!task) throw new Error("Task not found");

    // Merge into task config
    let config: Record<string, unknown> = {};
    if (task.config) {
      try {
        config = JSON.parse(task.config);
      } catch { /* */ }
    }
    config.ctaMapping = data.mapping;

    await db
      .update(tasks)
      .set({ config: JSON.stringify(config) })
      .where(eq(tasks.id, data.taskId));

    // Also update _ctas.json
    try {
      const { getDataDir } = await import("./manifest");
      const { resolve } = await import("path");
      const { readFileSync, writeFileSync } = await import("fs");

      const dataDir = getDataDir(task.migrationId, data.taskId);
      const ctasPath = resolve(dataDir, "_ctas.json");
      const ctasRaw = readFileSync(ctasPath, "utf-8");
      const ctas = JSON.parse(ctasRaw) as Array<{
        sourceGuid: string;
        targetGuid: string | null;
      }>;
      for (const cta of ctas) {
        if (data.mapping[cta.sourceGuid]) {
          cta.targetGuid = data.mapping[cta.sourceGuid]!;
        }
      }
      writeFileSync(ctasPath, JSON.stringify(ctas, null, 2), "utf-8");
    } catch { /* */ }

    return { saved: true };
  });

export const getTagData = createServerFn({ method: "POST" })
  .inputValidator((taskId: number) => taskId)
  .handler(async ({ data: taskId }) => {
    const [task] = await db.select().from(tasks).where(eq(tasks.id, taskId));
    if (!task || !task.manifestPath) return { tags: [], posts: [] };

    const manifest = readManifest(task.manifestPath);

    const tagPostMap = new Map<string, Set<string>>();
    const postNames: Record<string, string> = {};

    for (const item of manifest.items) {
      if (item.status !== "exported" && item.status !== "imported") continue;
      const tagIds = (item.metadata.tagIds as string[]) || [];
      const postId = item.id;
      postNames[postId] = (item.metadata.name as string) || postId;

      for (const tagId of tagIds) {
        if (!tagPostMap.has(tagId)) tagPostMap.set(tagId, new Set());
        tagPostMap.get(tagId)!.add(postId);
      }
    }

    const [migration] = await db.select().from(migrations).where(eq(migrations.id, task.migrationId));
    if (!migration) return { tags: [], posts: [] };

    const [sourceKey] = await db.select().from(serviceKeys).where(eq(serviceKeys.id, migration.sourceKeyId));
    let sourceTagNames: Record<string, string> = {};
    if (sourceKey) {
      try {
        const { fetchAllBlogTags } = await import("./hubspot");
        const sourceTags = await fetchAllBlogTags(sourceKey.accessToken);
        sourceTagNames = Object.fromEntries(sourceTags.map((t) => [t.id, t.name]));
      } catch { /* use IDs as fallback */ }
    }

    let tagMapping: Record<string, { action: string; name?: string; mergeInto?: string }> = {};
    if (task.config) {
      try {
        const config = JSON.parse(task.config);
        if (config.tagMapping) tagMapping = config.tagMapping;
      } catch { /* */ }
    }

    const tags = Array.from(tagPostMap.entries()).map(([tagId, postIds]) => ({
      id: tagId,
      name: sourceTagNames[tagId] || tagId,
      postCount: postIds.size,
      postIds: Array.from(postIds),
      mapping: tagMapping[tagId] || null,
    }));

    const posts = manifest.items
      .filter((i) => i.status === "exported" || i.status === "imported")
      .map((i) => ({
        id: i.id,
        name: (i.metadata.name as string) || i.id,
        slug: (i.metadata.slug as string) || "",
        tagIds: (i.metadata.tagIds as string[]) || [],
      }));

    return { tags, posts };
  });

export const saveTagMapping = createServerFn({ method: "POST" })
  .inputValidator(
    (data: {
      taskId: number;
      tagMapping: Record<string, { action: string; name?: string; mergeInto?: string }>;
      postTagUpdates?: Record<string, string[]>;
    }) => data
  )
  .handler(async ({ data }) => {
    const [task] = await db.select().from(tasks).where(eq(tasks.id, data.taskId));
    if (!task) throw new Error("Task not found");

    let config: Record<string, unknown> = {};
    if (task.config) {
      try { config = JSON.parse(task.config); } catch { /* */ }
    }
    config.tagMapping = data.tagMapping;

    if (data.postTagUpdates && task.manifestPath) {
      const manifest = readManifest(task.manifestPath);
      for (const item of manifest.items) {
        if (data.postTagUpdates[item.id]) {
          item.metadata.tagIds = data.postTagUpdates[item.id];
        }
      }
      flushManifest(task.manifestPath, manifest);
    }

    await db
      .update(tasks)
      .set({ config: JSON.stringify(config) })
      .where(eq(tasks.id, data.taskId));

    return { saved: true };
  });

export const getCsvPreviewData = createServerFn({ method: "POST" })
  .inputValidator((taskId: number) => taskId)
  .handler(async ({ data: taskId }) => {
    const [task] = await db.select().from(tasks).where(eq(tasks.id, taskId));
    if (!task?.config) return { headers: [], columnTypes: {}, rows: [], media: [] };

    const config = JSON.parse(task.config) as {
      csvFilePath?: string;
      csvHeaders?: string[];
      csvColumnTypes?: Record<string, string>;
    };

    if (!config.csvFilePath || !config.csvHeaders) {
      return { headers: [], columnTypes: {}, rows: [], media: [] };
    }

    // Read and parse CSV
    const { readFileSync } = await import("fs");
    let csvContent: string;
    try {
      csvContent = readFileSync(config.csvFilePath, "utf-8");
    } catch {
      return { headers: config.csvHeaders, columnTypes: config.csvColumnTypes || {}, rows: [], media: [] };
    }

    // Multi-line-aware CSV parser (handles quoted fields with newlines)
    function parseCsvContent(csv: string): { parsedHeaders: string[]; parsedRows: Record<string, string>[] } {
      const records: string[][] = [];
      let fields: string[] = [];
      let current = "";
      let inQuotes = false;

      for (let i = 0; i < csv.length; i++) {
        const char = csv[i]!;
        if (inQuotes) {
          if (char === '"') {
            if (csv[i + 1] === '"') { current += '"'; i++; }
            else inQuotes = false;
          } else {
            current += char;
          }
        } else {
          if (char === '"') { inQuotes = true; }
          else if (char === ",") { fields.push(current); current = ""; }
          else if (char === "\n" || char === "\r") {
            if (char === "\r" && csv[i + 1] === "\n") i++;
            fields.push(current); current = "";
            if (fields.some((f) => f !== "")) records.push(fields);
            fields = [];
          } else {
            current += char;
          }
        }
      }
      fields.push(current);
      if (fields.some((f) => f !== "")) records.push(fields);

      if (records.length === 0) return { parsedHeaders: [], parsedRows: [] };
      const parsedHeaders = records[0]!;
      const parsedRows = records.slice(1).map((values) => {
        const row: Record<string, string> = {};
        for (let i = 0; i < parsedHeaders.length; i++) {
          row[parsedHeaders[i]!] = values[i] || "";
        }
        return row;
      });
      return { parsedHeaders, parsedRows };
    }

    const { parsedHeaders: headers, parsedRows: rows } = parseCsvContent(csvContent);

    // Load media catalog
    let media: Array<{ sourceUrl: string; localPath: string | null; size: number; foundIn: string[] }> = [];
    try {
      const { getDataDir } = await import("./manifest");
      const { resolve } = await import("path");
      const catalogRaw = readFileSync(resolve(getDataDir(task.migrationId, taskId), "_media.json"), "utf-8");
      media = JSON.parse(catalogRaw);
    } catch { /* no media */ }

    return {
      headers: config.csvHeaders,
      columnTypes: config.csvColumnTypes || {},
      rows,
      media,
    };
  });

export const getManifestSummary = createServerFn({ method: "POST" })
  .inputValidator((taskId: number) => taskId)
  .handler(async ({ data: taskId }) => {
    const [task] = await db.select().from(tasks).where(eq(tasks.id, taskId));
    if (!task || !task.manifestPath) return null;

    try {
      const manifest = readManifest(task.manifestPath);
      return {
        summary: manifest.summary,
        warnings: manifest.warnings,
        phase: manifest.phase,
        exportedAt: manifest.exportedAt,
        importedAt: manifest.importedAt,
        itemCount: manifest.items.length,
      };
    } catch {
      return null;
    }
  });

export const getManifestItems = createServerFn({ method: "POST" })
  .inputValidator(
    (data: {
      taskId: number;
      offset: number;
      limit: number;
      statusFilter?: string;
    }) => data
  )
  .handler(async ({ data }) => {
    const [task] = await db.select().from(tasks).where(eq(tasks.id, data.taskId));
    if (!task || !task.manifestPath) return { items: [], total: 0 };

    try {
      const manifest = readManifest(task.manifestPath);
      let items = manifest.items;
      if (data.statusFilter && data.statusFilter !== "all") {
        items = items.filter((i) => i.status === data.statusFilter);
      }
      const total = items.length;
      const sliced = items.slice(data.offset, data.offset + data.limit);
      return {
        items: sliced.map((i) => ({
          id: i.id,
          sourceUrl: i.sourceUrl,
          localPath: i.localPath,
          targetUrl: i.targetUrl,
          status: i.status,
          error: i.error,
          size: i.size,
          name: (i.metadata?.name as string) || i.id,
        })),
        total,
      };
    } catch {
      return { items: [], total: 0 };
    }
  });
