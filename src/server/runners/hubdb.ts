import { db } from "../../db";
import { tasks } from "../../db/schema";
import type { Migration } from "../../db/schema";
import { eq } from "drizzle-orm";
import {
  createRunnerContext,
  logToTask,
  isTaskPaused,
} from "./base";
import { readManifest, flushManifest, getDataDir } from "../manifest";
import {
  fetchAllHubDbTables,
  fetchAllHubDbRows,
  createHubDbTable,
  createHubDbRow,
  createHubDbRowsBatch,
  publishHubDbTable,
  fetchHubDbTableByName,
  type HubDbTable,
  type HubDbRow,
} from "../hubspot";
import { writeCsvExport, HUBDB_CSV_COLUMNS } from "../csv";
import { writeFile, mkdir, readFile } from "fs/promises";
import { resolve } from "path";

// ── EXPORT PHASE ──

export async function exportHubDb(
  taskId: number,
  migration: Migration
): Promise<void> {
  const ctx = await createRunnerContext(taskId, migration);
  if (!ctx) return;

  const { sourceToken, manifestPath, manifest } = ctx;
  const dataDir = getDataDir(migration.id, taskId);
  await mkdir(dataDir, { recursive: true });

  await db
    .update(tasks)
    .set({ status: "exporting", phase: "export", startedAt: new Date() })
    .where(eq(tasks.id, taskId));

  await logToTask(taskId, "info", "Fetching HubDB tables from source portal...");
  let tables: HubDbTable[];
  try {
    tables = await fetchAllHubDbTables(sourceToken);
    await logToTask(taskId, "info", `Found ${tables.length} HubDB tables`);
  } catch (err) {
    await logToTask(taskId, "error", `Failed to fetch tables: ${err instanceof Error ? err.message : String(err)}`);
    await db.update(tasks).set({ status: "failed", completedAt: new Date() }).where(eq(tasks.id, taskId));
    return;
  }

  if (tables.length === 0) {
    await logToTask(taskId, "info", "No HubDB tables found in source portal");
    manifest.phase = "exported";
    manifest.exportedAt = new Date().toISOString();
    flushManifest(manifestPath, manifest);
    await db.update(tasks).set({ status: "exported", exportedAt: new Date() }).where(eq(tasks.id, taskId));
    return;
  }

  // Apply config filter
  const task = await db.select().from(tasks).where(eq(tasks.id, taskId)).then((r) => r[0]);
  if (task?.config) {
    try {
      const config = JSON.parse(task.config) as { selectedTableIds?: string[] };
      if (config.selectedTableIds?.length) {
        const selectedSet = new Set(config.selectedTableIds);
        tables = tables.filter((t) => selectedSet.has(t.id));
        await logToTask(taskId, "info", `Filtered to ${tables.length} selected tables`);
      }
    } catch { /* use all tables */ }
  }

  // Populate manifest
  const existingIds = new Set(manifest.items.map((i) => i.id));
  for (const table of tables) {
    if (!existingIds.has(table.id)) {
      manifest.items.push({
        id: table.id,
        sourceUrl: "",
        localPath: null,
        targetUrl: null,
        targetId: null,
        status: "pending",
        error: null,
        size: 0,
        metadata: {
          name: table.label || table.name,
          tableName: table.name,
          rowCount: table.rowCount,
          columnCount: table.columns.length,
        },
      });
    }
  }

  await db.update(tasks).set({ totalItems: manifest.items.length }).where(eq(tasks.id, taskId));
  flushManifest(manifestPath, manifest);

  await logToTask(taskId, "info", "Downloading table schemas and rows...");
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

    const table = tables.find((t) => t.id === item.id);
    if (!table) {
      item.status = "failed";
      item.error = "Table not found in fetched data";
      failed++;
      continue;
    }

    try {
      const rows = await fetchAllHubDbRows(sourceToken, table.id);
      const tableData = { ...table, rows };
      const tablePath = resolve(dataDir, `table-${table.id}.json`);
      const tableJson = JSON.stringify(tableData, null, 2);
      await writeFile(tablePath, tableJson, "utf-8");

      item.localPath = tablePath;
      item.size = Buffer.byteLength(tableJson);
      item.status = "exported";
      item.metadata.rowCount = rows.length;
      totalBytes += item.size;
      exported++;

      await logToTask(taskId, "info", `Exported table "${table.label || table.name}": ${rows.length} rows, ${table.columns.length} columns`);

      if (exported % 10 === 0) {
        await db
          .update(tasks)
          .set({ exportedItems: exported, failedItems: failed, localStorageBytes: totalBytes })
          .where(eq(tasks.id, taskId));
        flushManifest(manifestPath, manifest);
      }
    } catch (err) {
      item.status = "failed";
      item.error = err instanceof Error ? err.message : String(err);
      failed++;
      await logToTask(taskId, "warn", `Failed to export table "${table.label}": ${item.error}`);
    }
  }

  // CSV export if requested
  if (ctx.outputType === "csv") {
    const csvRecords: Record<string, unknown>[] = [];
    for (const item of manifest.items) {
      if (item.status !== "exported" || !item.localPath) continue;
      try {
        const raw = await readFile(item.localPath, "utf-8");
        const tableData = JSON.parse(raw) as HubDbTable & { rows: HubDbRow[] };
        for (const row of tableData.rows) {
          csvRecords.push({
            tableId: tableData.id,
            tableName: tableData.name,
            rowId: row.id,
            path: row.path || "",
            name: row.name || "",
            values: JSON.stringify(row.values),
          });
        }
      } catch { /* skip */ }
    }
    if (csvRecords.length > 0) {
      const csvPath = await writeCsvExport(migration.id, taskId, "hubdb", csvRecords, HUBDB_CSV_COLUMNS);
      await logToTask(taskId, "info", `CSV export saved: ${csvPath}`);
    }
  }

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

  await logToTask(taskId, "info", `Export completed. ${exported} tables downloaded, ${failed} failed.`);
}

// ── IMPORT PHASE ──

export async function importHubDb(
  taskId: number,
  migration: Migration,
  options: { dryRun: boolean } = { dryRun: false }
): Promise<void> {
  const ctx = await createRunnerContext(taskId, migration, options);
  if (!ctx) return;

  const { targetToken, manifestPath, dryRun } = ctx;
  const manifest = readManifest(manifestPath);

  await db
    .update(tasks)
    .set({ status: "importing", phase: "import" })
    .where(eq(tasks.id, taskId));

  if (dryRun) {
    await logToTask(taskId, "info", "DRY RUN — no tables will be created in target portal");
  }

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
        .set({ importedItems: imported, failedItems: failed })
        .where(eq(tasks.id, taskId));
      return;
    }

    if (!item.localPath) {
      item.status = "failed";
      item.error = "No local data file";
      failed++;
      continue;
    }

    try {
      const raw = await readFile(item.localPath, "utf-8");
      const tableData = JSON.parse(raw) as HubDbTable & { rows: HubDbRow[] };

      // Idempotency
      if (!dryRun) {
        const existing = await fetchHubDbTableByName(targetToken, tableData.name);
        if (existing) {
          item.status = "skipped";
          item.targetId = existing.id;
          skipped++;
          await logToTask(taskId, "info", `Skipped table "${tableData.label}" — already exists in target (ID: ${existing.id})`);
          continue;
        }
      }

      if (dryRun) {
        await logToTask(taskId, "info", `[DRY RUN] Would create table "${tableData.label}" with ${tableData.columns.length} columns and ${tableData.rows.length} rows`);
        item.status = "skipped";
        skipped++;
        continue;
      }

      // Create table
      const columnsForCreate = tableData.columns.map((col) => ({
        name: col.name,
        label: col.label,
        type: col.type,
        ...(col.options ? { options: col.options } : {}),
      }));

      const createdTable = await createHubDbTable(targetToken, {
        name: tableData.name,
        label: tableData.label,
        columns: columnsForCreate,
      });

      await logToTask(taskId, "info", `Created table "${tableData.label}" (ID: ${createdTable.id}), inserting ${tableData.rows.length} rows...`);

      // Column ID remapping
      const sourceColIdToName: Record<string, string> = {};
      for (const col of tableData.columns) {
        sourceColIdToName[String(col.id)] = col.name;
      }
      const targetColNameToId: Record<string, string> = {};
      for (const col of createdTable.columns) {
        targetColNameToId[col.name] = String(col.id);
      }

      // Insert rows in batches of 100
      const BATCH_SIZE = 100;
      let rowsInserted = 0;

      for (let i = 0; i < tableData.rows.length; i += BATCH_SIZE) {
        const batch = tableData.rows.slice(i, i + BATCH_SIZE);
        const mappedRows = batch.map((row) => {
          const mappedValues: Record<string, unknown> = {};
          for (const [key, value] of Object.entries(row.values)) {
            const colName = sourceColIdToName[key] || key;
            const targetColId = targetColNameToId[colName];
            if (targetColId) {
              mappedValues[targetColId] = value;
            }
          }
          return {
            values: mappedValues,
            ...(row.path ? { path: row.path } : {}),
            ...(row.name ? { name: row.name } : {}),
          };
        });

        try {
          await createHubDbRowsBatch(targetToken, createdTable.id, mappedRows);
          rowsInserted += batch.length;
        } catch {
          for (const row of mappedRows) {
            try {
              await createHubDbRow(targetToken, createdTable.id, row);
              rowsInserted++;
            } catch { /* non-fatal per row */ }
          }
        }
      }

      // Publish
      try {
        await publishHubDbTable(targetToken, createdTable.id);
        await logToTask(taskId, "info", `Published table "${tableData.label}" — ${rowsInserted} rows inserted`);
      } catch (pubErr) {
        await logToTask(taskId, "warn", `Table created but publish failed: ${pubErr instanceof Error ? pubErr.message : String(pubErr)}`);
      }

      item.status = "imported";
      item.targetId = createdTable.id;
      imported++;

      if (imported % 10 === 0) {
        await db
          .update(tasks)
          .set({ importedItems: imported, failedItems: failed })
          .where(eq(tasks.id, taskId));
        flushManifest(manifestPath, manifest);
      }
    } catch (err) {
      item.status = "failed";
      item.error = err instanceof Error ? err.message : String(err);
      failed++;
      await logToTask(taskId, "warn", `Failed to import table: ${item.error}`);
    }
  }

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
    })
    .where(eq(tasks.id, taskId));

  await logToTask(taskId, "info", `Import ${dryRun ? "(DRY RUN) " : ""}completed. ${imported} tables created, ${skipped} skipped, ${failed} failed.`);
}
