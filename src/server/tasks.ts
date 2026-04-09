import { createServerFn } from "@tanstack/react-start";
import { db } from "../db";
import { tasks, migrations, serviceKeys } from "../db/schema";
import { eq } from "drizzle-orm";
import { fetchAllBlogPosts, fetchAllFiles, fetchAllBlogTags, fetchContentGroups, hubspotFetch } from "./hubspot";
import { readManifest } from "./manifest";

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
      type: "media" | "blog_posts" | "hubdb" | "page";
      label: string;
      outputType?: "same_as_source" | "hubdb" | "csv";
      config?: string;
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
    await db.delete(tasks).where(eq(tasks.id, id));
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

    const validStatuses = ["exported", "import_paused", "failed"];
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
