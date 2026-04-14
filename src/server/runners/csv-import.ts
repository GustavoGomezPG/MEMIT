import { db } from "../../db";
import { tasks } from "../../db/schema";
import type { Migration } from "../../db/schema";
import { eq } from "drizzle-orm";
import { createRunnerContext, logToTask, isTaskPaused } from "./base";
import { flushManifest } from "../manifest";
import {
  createHubDbTable,
  createHubDbRowsBatch,
  createHubDbRow,
  publishHubDbTable,
  fetchHubDbTableByName,
} from "../hubspot";
import { writeCsvExport } from "../csv";
import { readFile } from "fs/promises";

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

  let config: Record<string, unknown> = {};
  if (task?.config) {
    try { config = JSON.parse(task.config); } catch { /* */ }
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
  };

  const csvContent = await readFile(config.csvFilePath, "utf-8");
  const { rows } = parseCsv(csvContent);

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
    for (const item of manifest.items) {
      if (item.status === "exported") item.status = "skipped";
    }
    manifest.phase = "completed";
    flushManifest(manifestPath, manifest);
    await db.update(tasks).set({ status: "completed", completedAt: new Date() }).where(eq(tasks.id, taskId));
    await logToTask(taskId, "info", "Dry run completed.");
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

  const BATCH_SIZE = 100;
  let imported = 0;
  let failed = 0;

  for (let i = 0; i < rows.length; i += BATCH_SIZE) {
    if (await isTaskPaused(taskId)) {
      await logToTask(taskId, "info", "Import paused");
      flushManifest(manifestPath, manifest);
      await db.update(tasks).set({ importedItems: imported, failedItems: failed }).where(eq(tasks.id, taskId));
      return;
    }

    const batch = rows.slice(i, i + BATCH_SIZE);
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
      await logToTask(taskId, "info", `Insert progress: ${imported}/${rows.length} rows`);
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
  }).where(eq(tasks.id, taskId));

  await logToTask(taskId, "info", `Import completed. HubDB table "${tableName}" created with ${imported} rows. ${failed} failed.`);
}
