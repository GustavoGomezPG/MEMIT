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
  const lines = content.split("\n").map((l) => l.trim()).filter(Boolean);
  if (lines.length === 0) return { headers: [], rows: [] };

  function parseLine(line: string): string[] {
    const fields: string[] = [];
    let current = "";
    let inQuotes = false;
    for (let i = 0; i < line.length; i++) {
      const char = line[i]!;
      if (char === '"') {
        if (inQuotes && line[i + 1] === '"') {
          current += '"';
          i++;
        } else {
          inQuotes = !inQuotes;
        }
      } else if (char === "," && !inQuotes) {
        fields.push(current);
        current = "";
      } else {
        current += char;
      }
    }
    fields.push(current);
    return fields;
  }

  const headers = parseLine(lines[0]!);
  const rows = lines.slice(1).map((line) => {
    const values = parseLine(line);
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
const DIRECT_URL_RE = /^https?:\/\/.+\.(png|jpe?g|gif|svg|webp|ico|bmp|pdf|doc|docx|xls|xlsx|ppt|pptx|mp4|mov|avi|webm|mp3|wav|zip|rar|csv|txt|woff2?|ttf|eot)(\?[^\s]*)?$/i;

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

  function addUrl(url: string, column: string) {
    const cleaned = url.trim().replace(/["'>\s]+$/, "");
    if (!cleaned || !cleaned.startsWith("http")) return;

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
      const hrefRe = /href=["']([^"']+\.(png|jpe?g|gif|svg|webp|pdf|doc|docx|xls|xlsx|ppt|pptx|mp4|mov|zip|rar|csv)[^"']*)["']/gi;
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
        const res = await fetch(url);
        if (!res.ok) {
          downloadFailed++;
          manifest.warnings.push(`[media] Failed to download: ${url} (HTTP ${res.status})`);
          continue;
        }
        const buf = Buffer.from(await res.arrayBuffer());
        const urlPath = new URL(url).pathname;
        const fileName = urlPath.split("/").pop() || `media-${Date.now()}`;
        const safeName = `${downloaded}-${fileName}`;
        const localPath = resolve(mediaDir, safeName);
        await writeFile(localPath, buf);

        entry.localPath = localPath;
        entry.size = buf.length;
        downloaded++;
      } catch (err) {
        downloadFailed++;
        manifest.warnings.push(
          `[media] Failed to download: ${url} (${err instanceof Error ? err.message : String(err)})`
        );
      }
    }

    // Save media catalog
    const mediaCatalog = Array.from(mediaEntries.values()).filter((e) => e.localPath);
    await writeFile(
      resolve(getDataDir(migration.id, taskId), "_media.json"),
      JSON.stringify(mediaCatalog, null, 2),
      "utf-8"
    );

    const totalMediaBytes = mediaCatalog.reduce((sum, e) => sum + e.size, 0);
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

// ── IMPORT PHASE ──

export async function importCsvImport(
  taskId: number,
  migration: Migration,
  options: { dryRun: boolean } = { dryRun: false }
): Promise<void> {
  const ctx = await createRunnerContext(taskId, migration, options);
  if (!ctx) return;

  const { targetToken, manifestPath, dryRun, outputType } = ctx;
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
      await logToTask(taskId, "info", `Uploading ${mediaCatalog.length} media files to target portal...`);

      for (const entry of mediaCatalog) {
        if (urlMapping[entry.sourceUrl]) {
          mediaSkipped++;
          continue;
        }
        if (!entry.localPath) continue;

        try {
          const fileBuffer = Buffer.from(await readFile(entry.localPath));
          const fileName = entry.localPath.split("/").pop() || `media-${Date.now()}`;
          const uploaded = await uploadFile(targetToken, fileBuffer, fileName);
          urlMapping[entry.sourceUrl] = uploaded.url;
          mediaUploaded++;
        } catch (err) {
          await logToTask(
            taskId,
            "warn",
            `Failed to upload media: ${entry.sourceUrl} (${err instanceof Error ? err.message : String(err)})`
          );
        }
      }

      await logToTask(taskId, "info", `Media upload: ${mediaUploaded} uploaded, ${mediaSkipped} already mapped`);
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
    await logToTask(taskId, "info", `[DRY RUN] Would create HubDB table with ${config.csvHeaders.length} columns and ${rows.length} rows`);
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

  const tableName = (config.csvFileName || "csv_import")
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
      label: config.csvFileName?.replace(/\.csv$/i, "") || tableName,
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
        if (colType === "NUMBER") {
          values[colId] = rawValue ? Number(rawValue) : null;
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
    } catch {
      for (let j = 0; j < mappedRows.length; j++) {
        try {
          await createHubDbRow(targetToken, createdTable.id, mappedRows[j]!);
          const item = manifest.items.find((it) => it.id === `row-${i + j}`);
          if (item) item.status = "imported";
          imported++;
        } catch {
          const item = manifest.items.find((it) => it.id === `row-${i + j}`);
          if (item) { item.status = "failed"; item.error = "Failed to insert row"; }
          failed++;
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
