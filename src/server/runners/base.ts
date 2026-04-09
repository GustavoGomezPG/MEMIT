import { db } from "../../db";
import { tasks, serviceKeys } from "../../db/schema";
import type { Migration, Task } from "../../db/schema";
import { eq } from "drizzle-orm";
import { appendLog } from "../tasks";
import {
  createManifest,
  readManifest,
  manifestExists,
  type Manifest,
} from "../manifest";

// ── DB helpers ──

export async function getTaskFromDb(
  taskId: number
): Promise<Task | undefined> {
  const [task] = await db.select().from(tasks).where(eq(tasks.id, taskId));
  return task;
}

export async function logToTask(
  taskId: number,
  level: "info" | "warn" | "error",
  message: string
): Promise<void> {
  const task = await getTaskFromDb(taskId);
  if (!task) return;
  await db
    .update(tasks)
    .set({ log: appendLog(task.log, level, message) })
    .where(eq(tasks.id, taskId));
}

export async function isTaskPaused(taskId: number): Promise<boolean> {
  const task = await getTaskFromDb(taskId);
  return (
    task?.status === "export_paused" || task?.status === "import_paused"
  );
}

// ── Token resolution ──

export async function resolveTokens(
  migration: Migration
): Promise<{ sourceToken: string; targetToken: string } | null> {
  const [sourceKey] = await db
    .select()
    .from(serviceKeys)
    .where(eq(serviceKeys.id, migration.sourceKeyId));
  const [targetKey] = await db
    .select()
    .from(serviceKeys)
    .where(eq(serviceKeys.id, migration.targetKeyId));

  if (!sourceKey || !targetKey) return null;

  return {
    sourceToken: sourceKey.accessToken,
    targetToken: targetKey.accessToken,
  };
}

// ── URL Mapping aggregation ──

/**
 * Merge URL mappings from all tasks in a migration.
 * This enables cross-task deduplication (e.g., media task + blog task).
 */
export async function getExistingUrlMapping(
  migrationId: number
): Promise<Record<string, string>> {
  const allTasks = await db
    .select()
    .from(tasks)
    .where(eq(tasks.migrationId, migrationId));

  const combined: Record<string, string> = {};
  for (const t of allTasks) {
    if (t.urlMapping) {
      Object.assign(combined, JSON.parse(t.urlMapping));
    }
  }
  return combined;
}

// ── Runner context ──

export interface RunnerContext {
  taskId: number;
  migration: Migration;
  sourceToken: string;
  targetToken: string;
  manifestPath: string;
  manifest: Manifest;
  dryRun: boolean;
  outputType: "same_as_source" | "hubdb" | "csv";
}

export async function createRunnerContext(
  taskId: number,
  migration: Migration,
  options: { dryRun?: boolean } = {}
): Promise<RunnerContext | null> {
  const tokens = await resolveTokens(migration);
  if (!tokens) {
    await logToTask(taskId, "error", "Source or target service key not found");
    await db
      .update(tasks)
      .set({ status: "failed", completedAt: new Date() })
      .where(eq(tasks.id, taskId));
    return null;
  }

  const task = await getTaskFromDb(taskId);
  if (!task) return null;

  // Read existing manifest or create new one
  let manifestPath = task.manifestPath;
  let manifest: Manifest;

  if (manifestPath && manifestExists(manifestPath)) {
    manifest = readManifest(manifestPath);
  } else {
    manifestPath = await createManifest(taskId, migration.id, task.type);
    await db
      .update(tasks)
      .set({ manifestPath })
      .where(eq(tasks.id, taskId));
    manifest = readManifest(manifestPath);
  }

  return {
    taskId,
    migration,
    sourceToken: tokens.sourceToken,
    targetToken: tokens.targetToken,
    manifestPath,
    manifest,
    dryRun: options.dryRun || false,
    outputType: (task.outputType as RunnerContext["outputType"]) || "same_as_source",
  };
}
