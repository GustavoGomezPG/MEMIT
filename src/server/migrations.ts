import { createServerFn } from "@tanstack/react-start";
import { db } from "../db";
import { migrations, tasks, serviceKeys } from "../db/schema";
import { eq } from "drizzle-orm";
import { validateToken } from "./hubspot";

// ── Service Keys ──

export const getServiceKeys = createServerFn({ method: "GET" }).handler(
  async () => {
    return db.select().from(serviceKeys).orderBy(serviceKeys.createdAt);
  }
);

export const validateAndCreateKey = createServerFn({ method: "POST" })
  .inputValidator((data: { name: string; accessToken: string }) => data)
  .handler(async ({ data }) => {
    const result = await validateToken(data.accessToken);
    if (!result.valid) {
      return { success: false as const, error: result.error };
    }

    const [key] = await db
      .insert(serviceKeys)
      .values({
        name: data.name,
        accessToken: data.accessToken,
        portalId: result.portalId || null,
      })
      .returning();

    return { success: true as const, key };
  });

export const deleteServiceKey = createServerFn({ method: "POST" })
  .inputValidator((id: number) => id)
  .handler(async ({ data: id }) => {
    await db.delete(serviceKeys).where(eq(serviceKeys.id, id));
  });

// ── Migrations ──

export const getMigrations = createServerFn({ method: "GET" }).handler(
  async () => {
    const allMigrations = await db
      .select()
      .from(migrations)
      .orderBy(migrations.createdAt);

    const allTasks = await db.select().from(tasks);
    const allKeys = await db.select().from(serviceKeys);

    return allMigrations.map((m) => {
      const sourceKey = allKeys.find((k) => k.id === m.sourceKeyId);
      const targetKey = allKeys.find((k) => k.id === m.targetKeyId);
      const migrationTasks = allTasks.filter((t) => t.migrationId === m.id);
      const completedTasks = migrationTasks.filter(
        (t) => t.status === "completed"
      );
      return {
        ...m,
        sourceKeyName: sourceKey?.name || "—",
        sourcePortalId: sourceKey?.portalId || null,
        targetKeyName: targetKey?.name || "—",
        targetPortalId: targetKey?.portalId || null,
        taskCount: migrationTasks.length,
        completedTaskCount: completedTasks.length,
      };
    });
  }
);

export const getMigration = createServerFn({ method: "GET" })
  .inputValidator((id: number) => id)
  .handler(async ({ data: id }) => {
    const [migration] = await db
      .select()
      .from(migrations)
      .where(eq(migrations.id, id));
    if (!migration) throw new Error("Migration not found");

    const [sourceKey] = await db
      .select()
      .from(serviceKeys)
      .where(eq(serviceKeys.id, migration.sourceKeyId));
    const [targetKey] = await db
      .select()
      .from(serviceKeys)
      .where(eq(serviceKeys.id, migration.targetKeyId));

    const migrationTasks = await db
      .select()
      .from(tasks)
      .where(eq(tasks.migrationId, id))
      .orderBy(tasks.createdAt);

    return {
      ...migration,
      sourceKey: sourceKey || null,
      targetKey: targetKey || null,
      tasks: migrationTasks,
    };
  });

export const createMigration = createServerFn({ method: "POST" })
  .inputValidator(
    (data: {
      name: string;
      sourceKeyId: number;
      targetKeyId: number;
    }) => data
  )
  .handler(async ({ data }) => {
    const [migration] = await db
      .insert(migrations)
      .values({
        name: data.name,
        sourceKeyId: data.sourceKeyId,
        targetKeyId: data.targetKeyId,
        status: "draft",
      })
      .returning();

    return migration;
  });

export const swapMigrationDirection = createServerFn({ method: "POST" })
  .inputValidator((id: number) => id)
  .handler(async ({ data: id }) => {
    const [migration] = await db
      .select()
      .from(migrations)
      .where(eq(migrations.id, id));
    if (!migration) throw new Error("Migration not found");

    await db
      .update(migrations)
      .set({
        sourceKeyId: migration.targetKeyId,
        targetKeyId: migration.sourceKeyId,
        updatedAt: new Date(),
      })
      .where(eq(migrations.id, id));
  });

export const updateMigrationStatus = createServerFn({ method: "POST" })
  .inputValidator(
    (data: { id: number; status: "draft" | "active" | "completed" }) => data
  )
  .handler(async ({ data }) => {
    await db
      .update(migrations)
      .set({ status: data.status, updatedAt: new Date() })
      .where(eq(migrations.id, data.id));
  });

export const deleteMigration = createServerFn({ method: "POST" })
  .inputValidator((id: number) => id)
  .handler(async ({ data: id }) => {
    await db.delete(migrations).where(eq(migrations.id, id));
  });
