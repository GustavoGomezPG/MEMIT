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
  fetchAllFolders,
  fetchAllFiles,
  createFolder,
  getSignedUrl,
  uploadFile,
  fetchStorageUsage,
  type HubSpotFolder,
} from "../hubspot";
import { writeCsvExport, MEDIA_CSV_COLUMNS } from "../csv";
import { writeFile, mkdir, readFile } from "fs/promises";
import { resolve } from "path";

// ── EXPORT PHASE ──

export async function exportMedia(
  taskId: number,
  migration: Migration
): Promise<void> {
  const ctx = await createRunnerContext(taskId, migration);
  if (!ctx) return;

  const { sourceToken, manifestPath, manifest } = ctx;
  const dataDir = getDataDir(migration.id, taskId);

  // Set status
  await db
    .update(tasks)
    .set({ status: "exporting", phase: "export", startedAt: new Date() })
    .where(eq(tasks.id, taskId));

  // Step 1: Fetch all folders
  await logToTask(taskId, "info", "Fetching folders from source portal...");
  let sourceFolders: HubSpotFolder[];
  try {
    sourceFolders = await fetchAllFolders(sourceToken);
    await logToTask(
      taskId,
      "info",
      `Found ${sourceFolders.length} folders`
    );
  } catch (err) {
    await logToTask(taskId, "error", `Failed to fetch folders: ${err instanceof Error ? err.message : String(err)}`);
    await db.update(tasks).set({ status: "failed", completedAt: new Date() }).where(eq(tasks.id, taskId));
    return;
  }

  // Store folder structure in manifest metadata
  manifest.items = manifest.items.filter((i) => i.metadata?.itemType !== "folder");
  // We don't add folders as manifest items — they're metadata for the import phase
  // Store them as a JSON file
  await writeFile(
    resolve(dataDir, "_folders.json"),
    JSON.stringify(sourceFolders, null, 2),
    "utf-8"
  );

  // Step 2: Fetch all files
  await logToTask(taskId, "info", "Fetching files from source portal...");
  try {
    const sourceFiles = await fetchAllFiles(sourceToken);
    await logToTask(taskId, "info", `Found ${sourceFiles.length} files`);

    // Populate manifest with pending items (skip already-exported on resume)
    const existingIds = new Set(manifest.items.map((i) => i.id));
    for (const file of sourceFiles) {
      if (!existingIds.has(file.id)) {
        manifest.items.push({
          id: file.id,
          sourceUrl: (file as Record<string, unknown>).defaultHostingUrl as string || file.url,
          localPath: null,
          targetUrl: null,
          targetId: null,
          status: "pending",
          error: null,
          size: file.size || 0,
          metadata: {
            name: file.name,
            path: file.path,
            extension: file.extension,
            folderId: file.folderId || null,
            folderPath: file.path ? file.path.substring(0, file.path.lastIndexOf("/")) : "",
          },
        });
      }
    }

    await db
      .update(tasks)
      .set({ totalItems: manifest.items.length })
      .where(eq(tasks.id, taskId));
    flushManifest(manifestPath, manifest);
  } catch (err) {
    await logToTask(taskId, "error", `Failed to fetch files: ${err instanceof Error ? err.message : String(err)}`);
    await db.update(tasks).set({ status: "failed", completedAt: new Date() }).where(eq(tasks.id, taskId));
    return;
  }

  // Reset failed items to pending so they get retried
  const failedCount = manifest.items.filter((i) => i.status === "failed").length;
  if (failedCount > 0) {
    for (const item of manifest.items) {
      if (item.status === "failed") {
        item.status = "pending";
        item.error = null;
      }
    }
    await logToTask(taskId, "info", `Retrying ${failedCount} previously failed items`);
    flushManifest(manifestPath, manifest);
  }

  // Step 3: Download each file
  await logToTask(taskId, "info", "Downloading files...");
  let exported = 0;
  let failed = 0;
  let totalBytes = 0;

  for (const item of manifest.items) {
    if (item.status === "exported") {
      exported++;
      totalBytes += item.size;
      continue; // Already exported (resume)
    }

    if (await isTaskPaused(taskId)) {
      await logToTask(taskId, "info", "Export paused");
      flushManifest(manifestPath, manifest);
      return;
    }

    try {
      // Download
      let downloadUrl = item.sourceUrl;
      const response = await fetch(downloadUrl, { signal: AbortSignal.timeout(30_000) });
      if (!response.ok) {
        // Try signed URL
        downloadUrl = await getSignedUrl(sourceToken, item.id);
        const retryRes = await fetch(downloadUrl, { signal: AbortSignal.timeout(30_000) });
        if (!retryRes.ok) throw new Error(`Download failed: ${retryRes.status}`);
        var buffer = Buffer.from(await retryRes.arrayBuffer());
      } else {
        var buffer = Buffer.from(await response.arrayBuffer());
      }

      // Preserve folder structure locally
      const folderPath = (item.metadata.folderPath as string) || "";
      const fileDir = folderPath
        ? resolve(dataDir, ...folderPath.split("/").filter(Boolean))
        : dataDir;
      await mkdir(fileDir, { recursive: true });

      const fileName = (item.metadata.name as string) || `file-${item.id}`;
      const ext = item.metadata.extension as string;
      const fullName = ext && !fileName.endsWith(`.${ext}`) ? `${fileName}.${ext}` : fileName;
      const localPath = resolve(fileDir, fullName);
      await writeFile(localPath, buffer);

      // Update manifest item
      item.localPath = localPath;
      item.size = buffer.length;
      item.status = "exported";
      totalBytes += buffer.length;
      exported++;

      // Progress update every 10 files
      if (exported % 10 === 0) {
        await db
          .update(tasks)
          .set({ exportedItems: exported, failedItems: failed, localStorageBytes: totalBytes })
          .where(eq(tasks.id, taskId));
        flushManifest(manifestPath, manifest);
      }

      if (exported % 50 === 0) {
        await logToTask(taskId, "info", `Export progress: ${exported}/${manifest.items.length} files`);
      }
    } catch (err) {
      item.status = "failed";
      item.error = err instanceof Error ? err.message : String(err);
      failed++;
    }
  }

  // CSV export if requested
  if (ctx.outputType === "csv") {
    const csvRecords = manifest.items
      .filter((i) => i.status === "exported")
      .map((i) => ({
        id: i.id,
        name: i.metadata.name,
        sourceUrl: i.sourceUrl,
        localPath: i.localPath,
        size: i.size,
        extension: i.metadata.extension,
        folderPath: i.metadata.folderPath,
      }));
    const csvPath = await writeCsvExport(migration.id, taskId, "media", csvRecords as Record<string, unknown>[], MEDIA_CSV_COLUMNS);
    await logToTask(taskId, "info", `CSV export saved: ${csvPath}`);
  }

  // Final update
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

  await logToTask(
    taskId,
    "info",
    `Export completed. ${exported} files downloaded (${(totalBytes / 1024 / 1024).toFixed(1)} MB), ${failed} failed.`
  );
}

// ── IMPORT PHASE ──

export async function importMedia(
  taskId: number,
  migration: Migration,
  options: { dryRun: boolean } = { dryRun: false }
): Promise<void> {
  const ctx = await createRunnerContext(taskId, migration, options);
  if (!ctx) return;

  const { targetToken, manifestPath, dryRun } = ctx;
  const manifest = readManifest(manifestPath);
  const dataDir = getDataDir(migration.id, taskId);

  // Storage pre-check
  if (!dryRun) {
    const exportedBytes = manifest.items
      .filter((i) => i.status === "exported")
      .reduce((sum, i) => sum + i.size, 0);

    const storage = await fetchStorageUsage(targetToken);
    if (storage && storage.bytesLimit > 0) {
      const remaining = storage.bytesLimit - storage.bytesUsed;
      if (exportedBytes > remaining) {
        await logToTask(
          taskId,
          "error",
          `Insufficient storage on target portal. Need ${(exportedBytes / 1024 / 1024).toFixed(1)} MB but only ${(remaining / 1024 / 1024).toFixed(1)} MB free of ${(storage.bytesLimit / 1024 / 1024).toFixed(1)} MB total.`
        );
        await db.update(tasks).set({ status: "failed" }).where(eq(tasks.id, taskId));
        return;
      }
      if (exportedBytes > remaining * 0.8) {
        await logToTask(
          taskId,
          "warn",
          `This migration will use most of the remaining storage (${(remaining / 1024 / 1024).toFixed(1)} MB free).`
        );
      }
    }
  }

  // Set status
  await db
    .update(tasks)
    .set({ status: "importing", phase: "import" })
    .where(eq(tasks.id, taskId));

  if (dryRun) {
    await logToTask(taskId, "info", "DRY RUN — no files will be uploaded to target portal");
  }

  // Recreate folder structure in target
  let folderMap = new Map<string, string>();
  if (!dryRun) {
    try {
      const foldersJson = await readFile(resolve(dataDir, "_folders.json"), "utf-8");
      const sourceFolders = JSON.parse(foldersJson) as HubSpotFolder[];
      const sorted = [...sourceFolders].sort(
        (a, b) => (a.path?.split("/").length || 0) - (b.path?.split("/").length || 0)
      );

      await logToTask(taskId, "info", "Recreating folder structure in target...");
      for (const folder of sorted) {
        try {
          const parentTargetId = folder.parentFolderId
            ? folderMap.get(folder.parentFolderId)
            : undefined;
          const created = await createFolder(
            targetToken,
            folder.path || folder.name,
            parentTargetId
          );
          folderMap.set(folder.id, created.id);
        } catch {
          // Folder may already exist
        }
      }
      await logToTask(taskId, "info", `Created ${folderMap.size} folders in target`);
    } catch {
      await logToTask(taskId, "warn", "Could not read folder structure — uploading without folder mapping");
    }
  }

  // Load combined URL mapping for idempotency
  const urlMapping: Record<string, string> = await getExistingUrlMapping(migration.id);

  // Import each file
  let imported = 0;
  let failed = 0;
  let skipped = 0;

  const exportedItems = manifest.items.filter((i) => i.status === "exported");

  // Fast path for dry run — summary only, no per-item logging
  if (dryRun) {
    let wouldUpload = 0;
    let alreadyMapped = 0;
    let totalBytes = 0;

    for (const item of exportedItems) {
      if (urlMapping[item.sourceUrl]) {
        item.status = "skipped";
        item.targetUrl = urlMapping[item.sourceUrl];
        alreadyMapped++;
      } else {
        item.status = "skipped";
        wouldUpload++;
        totalBytes += item.size;
      }
    }

    await logToTask(
      taskId,
      "info",
      `[DRY RUN] Would upload ${wouldUpload} files (${(totalBytes / 1024 / 1024).toFixed(1)} MB), ${alreadyMapped} already mapped`
    );

    // Reset and finalize
    for (const item of manifest.items) {
      if (item.status === "skipped") item.status = "exported";
    }
    manifest.phase = "exported";
    flushManifest(manifestPath, manifest);

    await db.update(tasks).set({ status: "exported", phase: "export" }).where(eq(tasks.id, taskId));
    await logToTask(taskId, "info", "Dry run completed. Ready for real import.");
    return;
  }

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

    // Idempotency: skip if already mapped
    if (urlMapping[item.sourceUrl]) {
      item.status = "skipped";
      item.targetUrl = urlMapping[item.sourceUrl];
      skipped++;
      continue;
    }

    try {
      if (!item.localPath) throw new Error("No local file path");
      const fileBuffer = Buffer.from(await readFile(item.localPath));

      const folderId = item.metadata.folderId
        ? folderMap.get(item.metadata.folderId as string)
        : undefined;

      const fileName = (item.metadata.name as string) || `file-${item.id}`;
      const ext = item.metadata.extension as string;
      const fullName = ext && !fileName.endsWith(`.${ext}`) ? `${fileName}.${ext}` : fileName;

      const uploaded = await uploadFile(targetToken, fileBuffer, fullName, folderId, folderId ? undefined : ctx.uploadFolderPath);

      item.targetUrl = uploaded.url;
      item.targetId = uploaded.id;
      item.status = "imported";
      urlMapping[item.sourceUrl] = uploaded.url;
      imported++;

      if (imported % 10 === 0) {
        await db
          .update(tasks)
          .set({ importedItems: imported, failedItems: failed, urlMapping: JSON.stringify(urlMapping) })
          .where(eq(tasks.id, taskId));
        flushManifest(manifestPath, manifest);
      }

      if (imported % 50 === 0) {
        await logToTask(taskId, "info", `Import progress: ${imported}/${exportedItems.length} files`);
      }
    } catch (err) {
      item.status = "failed";
      item.error = err instanceof Error ? err.message : String(err);
      failed++;
      await logToTask(taskId, "warn", `Failed to upload: ${item.metadata.name as string} — ${item.error}`);
    }
  }

  // Final (dry run returns early above, so this is always the real import path)
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

  await logToTask(taskId, "info", `Import completed. ${imported} files uploaded, ${skipped} skipped (already mapped), ${failed} failed.`);
}
