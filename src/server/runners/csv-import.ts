import { db } from "../../db";
import { tasks } from "../../db/schema";
import type { Migration } from "../../db/schema";
import { eq } from "drizzle-orm";
import { createRunnerContext, logToTask, isTaskPaused, getExistingUrlMapping } from "./base";
import { flushManifest, getDataDir } from "../manifest";
import {
  createHubDbTable,
  createHubDbRowsBatch,
  createHubDbRow,
  publishHubDbTable,
  fetchHubDbTableByName,
  uploadFile,
} from "../hubspot";
import { writeCsvExport } from "../csv";
import { readFile, writeFile, mkdir } from "fs/promises";
import { resolve } from "path";

function parseCsv(content: string): { headers: string[]; rows: Record<string, string>[] } {
  if (!content.trim()) return { headers: [], rows: [] };

  // Parse character-by-character to handle multi-line quoted fields
  const records: string[][] = [];
  let fields: string[] = [];
  let current = "";
  let inQuotes = false;

  for (let i = 0; i < content.length; i++) {
    const char = content[i]!;

    if (inQuotes) {
      if (char === '"') {
        if (content[i + 1] === '"') {
          current += '"';
          i++;
        } else {
          inQuotes = false;
        }
      } else {
        current += char;
      }
    } else {
      if (char === '"') {
        inQuotes = true;
      } else if (char === ",") {
        fields.push(current);
        current = "";
      } else if (char === "\n" || char === "\r") {
        if (char === "\r" && content[i + 1] === "\n") i++;
        fields.push(current);
        current = "";
        if (fields.some((f) => f !== "")) {
          records.push(fields);
        }
        fields = [];
      } else {
        current += char;
      }
    }
  }
  // Last record
  fields.push(current);
  if (fields.some((f) => f !== "")) {
    records.push(fields);
  }

  if (records.length === 0) return { headers: [], rows: [] };

  const headers = records[0]!;
  const rows = records.slice(1).map((values) => {
    const row: Record<string, string> = {};
    for (let i = 0; i < headers.length; i++) {
      row[headers[i]!] = values[i] || "";
    }
    return row;
  });

  return { headers, rows };
}

function inferColumnType(values: string[]): string {
  const nonEmpty = values.filter(Boolean);
  if (nonEmpty.length === 0) return "TEXT";
  if (nonEmpty.every((v) => !isNaN(Number(v)) && v !== "")) return "NUMBER";
  if (nonEmpty.every((v) => ["true", "false", "yes", "no", "1", "0"].includes(v.toLowerCase()))) return "BOOLEAN";
  if (nonEmpty.every((v) => /^https?:\/\//i.test(v))) return "URL";
  if (nonEmpty.every((v) => !isNaN(Date.parse(v)) && /\d{4}/.test(v))) return "DATE";
  return "TEXT";
}

function formatBytes(bytes: number): string {
  if (bytes === 0) return "0 B";
  const k = 1024;
  const sizes = ["B", "KB", "MB", "GB"];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return `${(bytes / Math.pow(k, i)).toFixed(1)} ${sizes[i]}`;
}

// ── Media URL extraction ──

const IMG_SRC_RE = /(?:src|data-src)=["']([^"']+)["']/gi;
const HUBSPOT_CDN_RE = /https?:\/\/[^"'\s,]*hubspotusercontent[^"'\s,]*/gi;
const DIRECT_URL_RE = /^https?:\/\/.+\.(png|jpe?g|gif|svg|webp|ico|bmp|tiff?|pdf|doc|docx|odt|rtf|xls|xlsx|xlsm|ods|ppt|pptx|odp|txt|md|csv|tsv|json|xml|mp4|mov|avi|webm|mkv|mp3|wav|ogg|flac|aac|zip|rar|7z|tar|gz|bz2|woff2?|ttf|eot|otf|eps|ai|psd|indd)(\?[^\s]*)?$/i;

interface MediaEntry {
  sourceUrl: string;
  localPath: string | null;
  size: number;
  foundIn: string[];
}

function extractMediaFromCsv(
  rows: Record<string, string>[],
  headers: string[],
  columnTypes: Record<string, string>
): Map<string, MediaEntry> {
  const media = new Map<string, MediaEntry>();

  // Embed/streaming providers — these are not downloadable media
  const EMBED_HOSTS = [
    "player.vimeo.com", "vimeo.com",
    "youtube.com", "www.youtube.com", "youtu.be",
    "player.youtube.com", "www.youtube-nocookie.com",
    "fast.wistia.com", "fast.wistia.net",
    "play.vidyard.com", "embed.vidyard.com",
    "www.dailymotion.com", "dai.ly",
    "open.spotify.com", "embed.spotify.com",
    "w.soundcloud.com",
    "codepen.io", "jsfiddle.net",
    "docs.google.com", "drive.google.com",
    "maps.google.com", "www.google.com/maps",
    "calendly.com", "app.hubspot.com",
  ];

  function isEmbedUrl(url: string): boolean {
    try {
      const host = new URL(url).hostname;
      return EMBED_HOSTS.some((h) => host === h || host.endsWith(`.${h}`));
    } catch {
      return false;
    }
  }

  function addUrl(url: string, column: string) {
    const cleaned = url.trim().replace(/["'>\s]+$/, "");
    if (!cleaned || !cleaned.startsWith("http")) return;
    if (isEmbedUrl(cleaned)) return;

    if (media.has(cleaned)) {
      const entry = media.get(cleaned)!;
      if (!entry.foundIn.includes(column)) entry.foundIn.push(column);
    } else {
      media.set(cleaned, { sourceUrl: cleaned, localPath: null, size: 0, foundIn: [column] });
    }
  }

  for (const row of rows) {
    for (const header of headers) {
      const value = row[header] || "";
      if (!value) continue;

      const colType = columnTypes[header] || "TEXT";

      // 1. URL or IMAGE columns — entire value is a URL
      if (colType === "URL" || colType === "IMAGE") {
        if (/^https?:\/\//i.test(value)) {
          addUrl(value, header);
        }
        continue;
      }

      // 2. Direct URL to a media file
      DIRECT_URL_RE.lastIndex = 0;
      if (DIRECT_URL_RE.test(value.trim())) {
        addUrl(value.trim(), header);
        continue;
      }

      // 3. HubSpot CDN URLs embedded anywhere
      HUBSPOT_CDN_RE.lastIndex = 0;
      let match: RegExpExecArray | null;
      while ((match = HUBSPOT_CDN_RE.exec(value)) !== null) {
        addUrl(match[0], header);
      }

      // 4. <img> and <img data-src> in rich text / HTML
      IMG_SRC_RE.lastIndex = 0;
      while ((match = IMG_SRC_RE.exec(value)) !== null) {
        if (match[1]) addUrl(match[1], header);
      }

      // 5. href="..." pointing to media files
      const hrefRe = /href=["']([^"']+\.(png|jpe?g|gif|svg|webp|ico|bmp|tiff?|pdf|doc|docx|odt|rtf|xls|xlsx|xlsm|ods|ppt|pptx|odp|txt|md|csv|tsv|json|xml|mp4|mov|avi|webm|mkv|mp3|wav|ogg|flac|aac|zip|rar|7z|tar|gz|bz2|eps|ai|psd|indd)[^"']*)["']/gi;
      while ((match = hrefRe.exec(value)) !== null) {
        if (match[1]) addUrl(match[1], header);
      }
    }
  }

  return media;
}

/** Rewrite media URLs in a cell value using old→new mapping. Used by importCsvImport. */
export function rewriteUrls(value: string, mapping: Record<string, string>): string {
  let result = value;
  for (const [oldUrl, newUrl] of Object.entries(mapping)) {
    result = result.split(oldUrl).join(newUrl);
  }
  return result;
}

// ── EXPORT PHASE (parse + validate) ──

export async function exportCsvImport(
  taskId: number,
  migration: Migration
): Promise<void> {
  const ctx = await createRunnerContext(taskId, migration);
  if (!ctx) return;

  const { manifestPath, manifest } = ctx;

  await db
    .update(tasks)
    .set({ status: "exporting", phase: "export", startedAt: new Date() })
    .where(eq(tasks.id, taskId));

  const task = await db.select().from(tasks).where(eq(tasks.id, taskId)).then((r) => r[0]);
  let csvFilePath: string | null = null;
  let csvFileName: string | null = null;
  if (task?.config) {
    try {
      const config = JSON.parse(task.config) as { csvFilePath?: string; csvFileName?: string };
      csvFilePath = config.csvFilePath || null;
      csvFileName = config.csvFileName || null;
    } catch { /* */ }
  }

  if (!csvFilePath) {
    await logToTask(taskId, "error", "No CSV file path found in task config");
    await db.update(tasks).set({ status: "failed", completedAt: new Date() }).where(eq(tasks.id, taskId));
    return;
  }

  await logToTask(taskId, "info", `Parsing CSV file: ${csvFileName || csvFilePath}`);
  let csvContent: string;
  try {
    csvContent = await readFile(csvFilePath, "utf-8");
  } catch (err) {
    await logToTask(taskId, "error", `Failed to read CSV file: ${err instanceof Error ? err.message : String(err)}`);
    await db.update(tasks).set({ status: "failed", completedAt: new Date() }).where(eq(tasks.id, taskId));
    return;
  }

  const { headers, rows } = parseCsv(csvContent);

  if (headers.length === 0 || rows.length === 0) {
    await logToTask(taskId, "error", "CSV file is empty or has no data rows");
    await db.update(tasks).set({ status: "failed", completedAt: new Date() }).where(eq(tasks.id, taskId));
    return;
  }

  await logToTask(taskId, "info", `Parsed ${rows.length} rows with ${headers.length} columns: ${headers.join(", ")}`);

  const columnTypes: Record<string, string> = {};
  for (const header of headers) {
    const values = rows.map((r) => r[header] || "");
    columnTypes[header] = inferColumnType(values);
  }
  await logToTask(taskId, "info", `Column types: ${headers.map((h) => `${h} (${columnTypes[h]})`).join(", ")}`);

  let config: Record<string, unknown> = {};
  if (task?.config) {
    try { config = JSON.parse(task.config); } catch { /* */ }
  }

  // ── Media discovery and download ──
  manifest.warnings = [];
  const mediaDir = resolve(getDataDir(migration.id, taskId), "media");
  await mkdir(mediaDir, { recursive: true });

  await logToTask(taskId, "info", "Scanning CSV data for media URLs...");
  const mediaEntries = extractMediaFromCsv(rows, headers, columnTypes);

  if (mediaEntries.size > 0) {
    await logToTask(
      taskId,
      "info",
      `Found ${mediaEntries.size} unique media URLs across ${new Set(Array.from(mediaEntries.values()).flatMap((e) => e.foundIn)).size} columns`
    );

    let downloaded = 0;
    let downloadFailed = 0;

    for (const [url, entry] of mediaEntries) {
      if (await isTaskPaused(taskId)) {
        await logToTask(taskId, "info", "Export paused during media download");
        break;
      }

      try {
        const urlPath = new URL(url).pathname;
        const fileName = urlPath.split("/").pop() || `media-${Date.now()}`;
        await logToTask(taskId, "info", `Downloading (${downloaded + downloadFailed + 1}/${mediaEntries.size}): ${fileName}`);

        const res = await fetch(url, { signal: AbortSignal.timeout(30_000) });
        if (!res.ok) {
          downloadFailed++;
          const reason = `HTTP ${res.status}`;
          manifest.warnings.push(`[media] Failed to download: ${url} (${reason})`);
          await logToTask(taskId, "warn", `Media download failed: ${url} — ${reason}`);
          continue;
        }
        const buf = Buffer.from(await res.arrayBuffer());
        const safeName = `${downloaded}-${fileName}`;
        const localPath = resolve(mediaDir, safeName);
        await writeFile(localPath, buf);

        entry.localPath = localPath;
        entry.size = buf.length;
        downloaded++;
      } catch (err) {
        downloadFailed++;
        const reason = err instanceof Error ? err.message : String(err);
        manifest.warnings.push(`[media] Failed to download: ${url} (${reason})`);
        await logToTask(taskId, "warn", `Media download failed: ${url} — ${reason}`);
      }
    }

    // Save media catalog (all entries, including failed — retry needs them)
    const allMedia = Array.from(mediaEntries.values());
    await writeFile(
      resolve(getDataDir(migration.id, taskId), "_media.json"),
      JSON.stringify(allMedia, null, 2),
      "utf-8"
    );

    const totalMediaBytes = allMedia.filter((e) => e.localPath).reduce((sum, e) => sum + e.size, 0);
    await logToTask(
      taskId,
      "info",
      `Media download: ${downloaded} files (${formatBytes(totalMediaBytes)}), ${downloadFailed} failed`
    );

    config.mediaCount = downloaded;
    config.mediaFailedCount = downloadFailed;
  } else {
    await logToTask(taskId, "info", "No media URLs found in CSV data");
    config.mediaCount = 0;
  }

  const existingIds = new Set(manifest.items.map((i) => i.id));
  for (let i = 0; i < rows.length; i++) {
    const rowId = `row-${i}`;
    if (!existingIds.has(rowId)) {
      manifest.items.push({
        id: rowId,
        sourceUrl: "",
        localPath: csvFilePath,
        targetUrl: null,
        targetId: null,
        status: "exported",
        error: null,
        size: 0,
        metadata: { name: `Row ${i + 1}`, rowIndex: i, values: rows[i] },
      });
    }
  }

  config.csvHeaders = headers;
  config.csvColumnTypes = columnTypes;
  config.csvRowCount = rows.length;

  await db.update(tasks).set({
    config: JSON.stringify(config),
    totalItems: rows.length,
    exportedItems: rows.length,
    localStorageBytes: Buffer.byteLength(csvContent),
  }).where(eq(tasks.id, taskId));

  manifest.phase = "exported";
  manifest.exportedAt = new Date().toISOString();
  flushManifest(manifestPath, manifest);

  await db.update(tasks).set({ status: "exported", phase: "export", exportedAt: new Date() }).where(eq(tasks.id, taskId));

  await logToTask(taskId, "info", `CSV parsed successfully. ${rows.length} rows ready for import.`);
}

// ── RETRY FAILED MEDIA ──

export async function retryFailedMedia(
  taskId: number,
  migration: Migration
): Promise<{ retried: number; succeeded: number; failed: number }> {
  const dataDir = getDataDir(migration.id, taskId);
  const mediaDir = resolve(dataDir, "media");

  // Read catalog
  let catalog: MediaEntry[];
  try {
    const raw = await readFile(resolve(dataDir, "_media.json"), "utf-8");
    catalog = JSON.parse(raw) as MediaEntry[];
  } catch {
    return { retried: 0, succeeded: 0, failed: 0 };
  }

  const failedEntries = catalog.filter((e) => !e.localPath);
  if (failedEntries.length === 0) return { retried: 0, succeeded: 0, failed: 0 };

  await logToTask(taskId, "info", `Retrying ${failedEntries.length} failed media downloads...`);
  await mkdir(mediaDir, { recursive: true });

  // Count existing successful downloads for safe filename prefix
  let fileIndex = catalog.filter((e) => e.localPath).length;
  let succeeded = 0;
  let failed = 0;

  for (const entry of failedEntries) {
    try {
      const res = await fetch(entry.sourceUrl);
      if (!res.ok) {
        failed++;
        await logToTask(taskId, "warn", `Retry failed: ${entry.sourceUrl} — HTTP ${res.status}`);
        continue;
      }
      const buf = Buffer.from(await res.arrayBuffer());
      const urlPath = new URL(entry.sourceUrl).pathname;
      const fileName = urlPath.split("/").pop() || `media-${Date.now()}`;
      const safeName = `${fileIndex}-${fileName}`;
      const localPath = resolve(mediaDir, safeName);
      await writeFile(localPath, buf);

      entry.localPath = localPath;
      entry.size = buf.length;
      fileIndex++;
      succeeded++;
      await logToTask(taskId, "info", `Retry succeeded: ${entry.sourceUrl} (${formatBytes(buf.length)})`);
    } catch (err) {
      failed++;
      await logToTask(taskId, "warn", `Retry failed: ${entry.sourceUrl} — ${err instanceof Error ? err.message : String(err)}`);
    }
  }

  // Update catalog
  await writeFile(resolve(dataDir, "_media.json"), JSON.stringify(catalog, null, 2), "utf-8");

  // Update manifest warnings — remove resolved ones, keep remaining failures
  const { readManifest } = await import("../manifest");
  const [task] = await db.select().from(tasks).where(eq(tasks.id, taskId));
  if (task?.manifestPath) {
    const manifest = readManifest(task.manifestPath);
    const stillFailed = new Set(catalog.filter((e) => !e.localPath).map((e) => e.sourceUrl));
    manifest.warnings = manifest.warnings.filter((w) => {
      const urlMatch = w.match(/Failed to download: (\S+)/);
      return !urlMatch || stillFailed.has(urlMatch[1]!);
    });
    flushManifest(task.manifestPath, manifest);
  }

  // Update config counts
  if (task?.config) {
    try {
      const config = JSON.parse(task.config) as Record<string, unknown>;
      config.mediaCount = catalog.filter((e) => e.localPath).length;
      config.mediaFailedCount = catalog.filter((e) => !e.localPath).length;
      await db.update(tasks).set({ config: JSON.stringify(config) }).where(eq(tasks.id, taskId));
    } catch { /* */ }
  }

  await logToTask(taskId, "info", `Retry complete: ${succeeded} succeeded, ${failed} still failing`);
  return { retried: failedEntries.length, succeeded, failed };
}

// ── IMPORT PHASE ──

export async function importCsvImport(
  taskId: number,
  migration: Migration,
  options: { dryRun: boolean } = { dryRun: false }
): Promise<void> {
  const ctx = await createRunnerContext(taskId, migration, options);
  if (!ctx) return;

  const { targetToken, manifestPath, dryRun, outputType, uploadFolderPath } = ctx;
  const manifest = ctx.manifest;

  await db.update(tasks).set({ status: "importing", phase: "import" }).where(eq(tasks.id, taskId));

  const task = await db.select().from(tasks).where(eq(tasks.id, taskId)).then((r) => r[0]);
  if (!task?.config) {
    await logToTask(taskId, "error", "No CSV config found — run export first");
    await db.update(tasks).set({ status: "failed" }).where(eq(tasks.id, taskId));
    return;
  }

  const config = JSON.parse(task.config) as {
    csvFilePath: string;
    csvFileName: string;
    csvHeaders: string[];
    csvColumnTypes: Record<string, string>;
    csvRowCount: number;
    mediaCount?: number;
    hubdbTableName?: string;
    mediaFolderPath?: string;
  };

  const csvContent = await readFile(config.csvFilePath, "utf-8");
  const { rows } = parseCsv(csvContent);

  // ── Media upload and URL rewriting ──
  const urlMapping: Record<string, string> = await getExistingUrlMapping(migration.id);
  let mediaUploaded = 0;
  let mediaSkipped = 0;

  let mediaCatalog: MediaEntry[] = [];
  try {
    const catalogRaw = await readFile(
      resolve(getDataDir(migration.id, taskId), "_media.json"),
      "utf-8"
    );
    mediaCatalog = JSON.parse(catalogRaw) as MediaEntry[];
  } catch {
    // No media catalog — skip media handling
  }

  if (mediaCatalog.length > 0) {
    if (dryRun) {
      await logToTask(taskId, "info", `[DRY RUN] Would upload ${mediaCatalog.length} media files to target portal`);
      for (const entry of mediaCatalog.slice(0, 10)) {
        const fileName = entry.localPath?.split("/").pop() || "unknown";
        await logToTask(taskId, "info", `[DRY RUN]   ${fileName} (${formatBytes(entry.size)}) — found in: ${entry.foundIn.join(", ")}`);
      }
      if (mediaCatalog.length > 10) {
        await logToTask(taskId, "info", `[DRY RUN]   ...and ${mediaCatalog.length - 10} more files`);
      }
    } else {
      const uploadable = mediaCatalog.filter((e) => e.localPath && !urlMapping[e.sourceUrl]);
      const alreadyMapped = mediaCatalog.filter((e) => urlMapping[e.sourceUrl]);
      mediaSkipped = alreadyMapped.length;
      await logToTask(taskId, "info", `Uploading ${uploadable.length} media files to target portal (${mediaSkipped} already mapped)...`);

      for (let mi = 0; mi < uploadable.length; mi++) {
        const entry = uploadable[mi]!;
        const fileName = entry.localPath!.split("/").pop() || `media-${Date.now()}`;

        try {
          if ((mi + 1) % 10 === 0 || mi === 0) {
            await logToTask(taskId, "info", `Media upload progress: ${mi + 1}/${uploadable.length} — ${fileName}`);
          }
          const fileBuffer = Buffer.from(await readFile(entry.localPath!));
          const uploaded = await uploadFile(targetToken, fileBuffer, fileName, undefined, uploadFolderPath);
          urlMapping[entry.sourceUrl] = uploaded.url;
          mediaUploaded++;
        } catch {
          // Fallback: import-from-URL (HubSpot fetches the file — better for large files)
          try {
            await logToTask(taskId, "info", `Direct upload failed for ${fileName}, trying import-from-URL...`);
            const { importFileFromUrl } = await import("../hubspot");
            const imported = await importFileFromUrl(targetToken, entry.sourceUrl, fileName, undefined, uploadFolderPath);
            urlMapping[entry.sourceUrl] = imported.url;
            mediaUploaded++;
          } catch (err) {
            await logToTask(
              taskId,
              "warn",
              `Failed to upload media: ${fileName} — ${err instanceof Error ? err.message : String(err)}`
            );
          }
        }
      }

      await logToTask(taskId, "info", `Media upload complete: ${mediaUploaded} uploaded, ${mediaSkipped} already mapped`);

      // Update config with import-phase media failure count
      const stillUnmapped = mediaCatalog.filter((e) => e.localPath && !urlMapping[e.sourceUrl]).length;
      if (stillUnmapped > 0 || mediaUploaded > 0) {
        const latestTask = await db.select().from(tasks).where(eq(tasks.id, taskId)).then((r) => r[0]);
        if (latestTask?.config) {
          try {
            const cfg = JSON.parse(latestTask.config) as Record<string, unknown>;
            cfg.mediaFailedCount = stillUnmapped;
            await db.update(tasks).set({ config: JSON.stringify(cfg) }).where(eq(tasks.id, taskId));
          } catch { /* */ }
        }
      }
    }
  }

  if (outputType === "csv") {
    if (dryRun) {
      await logToTask(taskId, "info", `[DRY RUN] Would re-export ${rows.length} rows as CSV`);
    } else {
      const csvPath = await writeCsvExport(
        migration.id, taskId, "csv_import",
        rows as unknown as Record<string, unknown>[],
        config.csvHeaders
      );
      await logToTask(taskId, "info", `CSV re-exported to: ${csvPath}`);
    }

    for (const item of manifest.items) {
      if (item.status === "exported") item.status = "imported";
    }
    manifest.phase = "completed";
    manifest.importedAt = new Date().toISOString();
    flushManifest(manifestPath, manifest);

    await db.update(tasks).set({ status: "completed", completedAt: new Date(), importedItems: rows.length }).where(eq(tasks.id, taskId));
    await logToTask(taskId, "info", "CSV export completed.");
    return;
  }

  // Output type is HubDB
  if (dryRun) {
    const dryRunTableName = config.hubdbTableName ||
      (config.csvFileName || "csv_import").replace(/\.csv$/i, "").replace(/[^a-zA-Z0-9_]/g, "_").toLowerCase();
    await logToTask(taskId, "info", `[DRY RUN] Would create HubDB table "${dryRunTableName}" with ${config.csvHeaders.length} columns and ${rows.length} rows`);
    await logToTask(taskId, "info", `Columns: ${config.csvHeaders.map((h) => `${h} (${config.csvColumnTypes[h]})`).join(", ")}`);

    // Preview URL rewrites
    const mappingKeys = Object.keys(urlMapping);
    if (mappingKeys.length > 0) {
      let affectedCells = 0;
      for (const row of rows) {
        for (const header of config.csvHeaders) {
          const value = row[header] || "";
          for (const oldUrl of mappingKeys) {
            if (value.includes(oldUrl)) {
              affectedCells++;
              break;
            }
          }
        }
      }
      await logToTask(taskId, "info", `[DRY RUN] Would rewrite ${mappingKeys.length} media URLs across ~${affectedCells} cells`);
    }

    manifest.phase = "exported";
    flushManifest(manifestPath, manifest);
    await db.update(tasks).set({ status: "exported", phase: "export" }).where(eq(tasks.id, taskId));
    await logToTask(taskId, "info", "Dry run completed. Ready for real import.");
    return;
  }

  const tableName = config.hubdbTableName ||
    (config.csvFileName || "csv_import")
      .replace(/\.csv$/i, "")
      .replace(/[^a-zA-Z0-9_]/g, "_")
      .toLowerCase();

  const existingTable = await fetchHubDbTableByName(targetToken, tableName);
  if (existingTable) {
    await logToTask(taskId, "info", `HubDB table "${tableName}" already exists (ID: ${existingTable.id}), skipping creation`);
    for (const item of manifest.items) {
      if (item.status === "exported") item.status = "skipped";
    }
    manifest.phase = "completed";
    flushManifest(manifestPath, manifest);
    await db.update(tasks).set({ status: "completed", completedAt: new Date() }).where(eq(tasks.id, taskId));
    return;
  }

  const columns = config.csvHeaders.map((header) => ({
    name: header.replace(/[^a-zA-Z0-9_]/g, "_").toLowerCase(),
    label: header,
    type: config.csvColumnTypes[header] || "TEXT",
  }));

  await logToTask(taskId, "info", `Creating HubDB table "${tableName}" with ${columns.length} columns...`);

  let createdTable;
  try {
    createdTable = await createHubDbTable(targetToken, {
      name: tableName,
      label: config.hubdbTableName || config.csvFileName?.replace(/\.csv$/i, "") || tableName,
      columns,
    });
    await logToTask(taskId, "info", `Table created (ID: ${createdTable.id}), inserting rows...`);
  } catch (err) {
    await logToTask(taskId, "error", `Failed to create HubDB table: ${err instanceof Error ? err.message : String(err)}`);
    await db.update(tasks).set({ status: "failed", completedAt: new Date() }).where(eq(tasks.id, taskId));
    return;
  }

  const colNameToId: Record<string, string> = {};
  for (const col of createdTable.columns) {
    colNameToId[col.name] = String(col.id);
  }

  // Rewrite media URLs in row data before insertion
  const hasUrlMapping = Object.keys(urlMapping).length > 0;
  let rewrittenRows = rows;
  if (hasUrlMapping) {
    await logToTask(taskId, "info", `Rewriting ${Object.keys(urlMapping).length} media URLs in row data...`);
    rewrittenRows = rows.map((row) => {
      const rewritten: Record<string, string> = {};
      for (const header of config.csvHeaders) {
        const value = row[header] || "";
        rewritten[header] = rewriteUrls(value, urlMapping);
      }
      return rewritten;
    });
    await logToTask(taskId, "info", "URL rewriting complete");
  }

  const BATCH_SIZE = 100;
  let imported = 0;
  let failed = 0;

  for (let i = 0; i < rewrittenRows.length; i += BATCH_SIZE) {
    if (await isTaskPaused(taskId)) {
      await logToTask(taskId, "info", "Import paused");
      flushManifest(manifestPath, manifest);
      await db.update(tasks).set({ importedItems: imported, failedItems: failed }).where(eq(tasks.id, taskId));
      return;
    }

    const batch = rewrittenRows.slice(i, i + BATCH_SIZE);
    const mappedRows = batch.map((row) => {
      const values: Record<string, unknown> = {};
      for (const header of config.csvHeaders) {
        const colName = header.replace(/[^a-zA-Z0-9_]/g, "_").toLowerCase();
        const colId = colNameToId[colName];
        if (!colId) continue;
        const rawValue = row[header] || "";
        const colType = config.csvColumnTypes[header] || "TEXT";

        // Skip empty values — send null instead of empty strings
        if (!rawValue.trim()) {
          values[colId] = null;
          continue;
        }

        if (colType === "NUMBER") {
          const num = Number(rawValue);
          values[colId] = isNaN(num) ? null : num;
        } else if (colType === "BOOLEAN") {
          values[colId] = ["true", "yes", "1"].includes(rawValue.toLowerCase());
        } else {
          values[colId] = rawValue;
        }
      }
      return { values };
    });

    try {
      await createHubDbRowsBatch(targetToken, createdTable.id, mappedRows);
      for (let j = i; j < i + batch.length; j++) {
        const item = manifest.items.find((it) => it.id === `row-${j}`);
        if (item) item.status = "imported";
      }
      imported += batch.length;
    } catch (batchErr) {
      await logToTask(taskId, "warn", `Batch insert failed (rows ${i + 1}-${i + batch.length}), falling back to individual inserts: ${batchErr instanceof Error ? batchErr.message : String(batchErr)}`);
      for (let j = 0; j < mappedRows.length; j++) {
        try {
          await createHubDbRow(targetToken, createdTable.id, mappedRows[j]!);
          const item = manifest.items.find((it) => it.id === `row-${i + j}`);
          if (item) item.status = "imported";
          imported++;
        } catch (rowErr) {
          const item = manifest.items.find((it) => it.id === `row-${i + j}`);
          if (item) { item.status = "failed"; item.error = rowErr instanceof Error ? rowErr.message : String(rowErr); }
          failed++;
          await logToTask(taskId, "warn", `Row ${i + j + 1} failed: ${rowErr instanceof Error ? rowErr.message : String(rowErr)}`);
        }
      }
    }

    if (imported % 100 === 0) {
      await db.update(tasks).set({ importedItems: imported, failedItems: failed }).where(eq(tasks.id, taskId));
      flushManifest(manifestPath, manifest);
      await logToTask(taskId, "info", `Insert progress: ${imported}/${rewrittenRows.length} rows`);
    }
  }

  try {
    await publishHubDbTable(targetToken, createdTable.id);
    await logToTask(taskId, "info", `Published HubDB table "${tableName}"`);
  } catch (err) {
    await logToTask(taskId, "warn", `Table created but publish failed: ${err instanceof Error ? err.message : String(err)}`);
  }

  manifest.phase = "completed";
  manifest.importedAt = new Date().toISOString();
  flushManifest(manifestPath, manifest);

  await db.update(tasks).set({
    status: "completed", completedAt: new Date(), importedItems: imported, failedItems: failed,
    urlMapping: JSON.stringify(urlMapping),
  }).where(eq(tasks.id, taskId));

  await logToTask(taskId, "info", `Import completed. HubDB table "${tableName}" created with ${imported} rows. ${failed} failed.`);
}
