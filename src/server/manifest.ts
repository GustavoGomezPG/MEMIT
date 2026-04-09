import { writeFileSync, readFileSync, renameSync, existsSync } from "fs";
import { mkdir } from "fs/promises";
import { resolve } from "path";

// ── Types ──

export type ManifestItemStatus =
  | "pending"
  | "exported"
  | "imported"
  | "failed"
  | "skipped";

export interface ManifestItem {
  id: string;
  sourceUrl: string;
  localPath: string | null;
  targetUrl: string | null;
  targetId: string | null;
  status: ManifestItemStatus;
  error: string | null;
  size: number;
  metadata: Record<string, unknown>;
}

export interface ManifestSummary {
  total: number;
  exported: number;
  imported: number;
  failed: number;
  skipped: number;
}

export interface Manifest {
  version: 1;
  taskId: number;
  migrationId: number;
  type: string;
  phase: "pending" | "exporting" | "exported" | "importing" | "completed";
  exportedAt: string | null;
  importedAt: string | null;
  items: ManifestItem[];
  warnings: string[];
  summary: ManifestSummary;
}

// ── Directory helpers ──

export function getManifestDir(migrationId: number, taskId: number): string {
  return resolve(
    process.cwd(),
    "memit-downloads",
    String(migrationId),
    String(taskId)
  );
}

export function getDataDir(migrationId: number, taskId: number): string {
  return resolve(getManifestDir(migrationId, taskId), "data");
}

export function getExportsDir(migrationId: number, taskId: number): string {
  return resolve(getManifestDir(migrationId, taskId), "exports");
}

// ── Manifest CRUD ──

export async function createManifest(
  taskId: number,
  migrationId: number,
  type: string
): Promise<string> {
  const dir = getManifestDir(migrationId, taskId);
  await mkdir(dir, { recursive: true });
  await mkdir(resolve(dir, "data"), { recursive: true });
  await mkdir(resolve(dir, "exports"), { recursive: true });

  const manifest: Manifest = {
    version: 1,
    taskId,
    migrationId,
    type,
    phase: "pending",
    exportedAt: null,
    importedAt: null,
    items: [],
    warnings: [],
    summary: { total: 0, exported: 0, imported: 0, failed: 0, skipped: 0 },
  };

  const manifestPath = resolve(dir, "manifest.json");
  writeManifest(manifestPath, manifest);
  return manifestPath;
}

export function readManifest(manifestPath: string): Manifest {
  const raw = readFileSync(manifestPath, "utf-8");
  return JSON.parse(raw) as Manifest;
}

/**
 * Atomic write: write to .tmp then rename to avoid corruption on crash.
 */
export function writeManifest(manifestPath: string, manifest: Manifest): void {
  manifest.summary = recalculateSummary(manifest);
  const tmpPath = manifestPath + ".tmp";
  writeFileSync(tmpPath, JSON.stringify(manifest, null, 2), "utf-8");
  renameSync(tmpPath, manifestPath);
}

export function manifestExists(manifestPath: string): boolean {
  return existsSync(manifestPath);
}

// ── Item operations ──

export function updateManifestItem(
  manifestPath: string,
  itemId: string,
  updates: Partial<ManifestItem>
): void {
  const manifest = readManifest(manifestPath);
  const item = manifest.items.find((i) => i.id === itemId);
  if (item) {
    Object.assign(item, updates);
    writeManifest(manifestPath, manifest);
  }
}

export function batchUpdateManifestItems(
  manifestPath: string,
  updates: Array<{ id: string; updates: Partial<ManifestItem> }>
): void {
  const manifest = readManifest(manifestPath);
  for (const { id, updates: itemUpdates } of updates) {
    const item = manifest.items.find((i) => i.id === id);
    if (item) {
      Object.assign(item, itemUpdates);
    }
  }
  writeManifest(manifestPath, manifest);
}

/**
 * Batch-efficient: read manifest, apply updates to multiple items, write once.
 * Call this every N items in a loop to minimize I/O.
 */
export function flushManifest(
  manifestPath: string,
  manifest: Manifest
): void {
  writeManifest(manifestPath, manifest);
}

// ── Summary ──

export function recalculateSummary(manifest: Manifest): ManifestSummary {
  const summary: ManifestSummary = {
    total: manifest.items.length,
    exported: 0,
    imported: 0,
    failed: 0,
    skipped: 0,
  };
  for (const item of manifest.items) {
    if (item.status === "exported") summary.exported++;
    else if (item.status === "imported") summary.imported++;
    else if (item.status === "failed") summary.failed++;
    else if (item.status === "skipped") summary.skipped++;
  }
  return summary;
}
