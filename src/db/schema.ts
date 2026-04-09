import { sqliteTable, text, integer } from "drizzle-orm/sqlite-core";

export const serviceKeys = sqliteTable("service_keys", {
  id: integer("id").primaryKey({ autoIncrement: true }),
  name: text("name").notNull(),
  accessToken: text("access_token").notNull(),
  portalId: text("portal_id"),
  createdAt: integer("created_at", { mode: "timestamp" })
    .notNull()
    .$defaultFn(() => new Date()),
});

export const migrations = sqliteTable("migrations", {
  id: integer("id").primaryKey({ autoIncrement: true }),
  name: text("name").notNull(),
  sourceKeyId: integer("source_key_id")
    .notNull()
    .references(() => serviceKeys.id),
  targetKeyId: integer("target_key_id")
    .notNull()
    .references(() => serviceKeys.id),
  status: text("status", {
    enum: ["draft", "active", "completed"],
  })
    .notNull()
    .default("draft"),
  createdAt: integer("created_at", { mode: "timestamp" })
    .notNull()
    .$defaultFn(() => new Date()),
  updatedAt: integer("updated_at", { mode: "timestamp" })
    .notNull()
    .$defaultFn(() => new Date()),
});

export const tasks = sqliteTable("tasks", {
  id: integer("id").primaryKey({ autoIncrement: true }),
  migrationId: integer("migration_id")
    .notNull()
    .references(() => migrations.id, { onDelete: "cascade" }),
  type: text("type", {
    enum: [
      "media",
      "blog_posts",
      "hubdb",
      "page",
    ],
  }).notNull(),
  status: text("status", {
    enum: [
      "pending",
      "exporting",
      "export_paused",
      "exported",
      "importing",
      "import_paused",
      "completed",
      "failed",
    ],
  })
    .notNull()
    .default("pending"),
  phase: text("phase", {
    enum: ["export", "import"],
  })
    .notNull()
    .default("export"),
  label: text("label").notNull(),
  outputType: text("output_type", {
    enum: ["same_as_source", "hubdb", "csv"],
  })
    .notNull()
    .default("same_as_source"),
  config: text("config"),
  totalItems: integer("total_items").default(0),
  processedItems: integer("processed_items").default(0),
  exportedItems: integer("exported_items").default(0),
  importedItems: integer("imported_items").default(0),
  failedItems: integer("failed_items").default(0),
  urlMapping: text("url_mapping"),
  manifestPath: text("manifest_path"),
  localStorageBytes: integer("local_storage_bytes").default(0),
  log: text("log"),
  exportedAt: integer("exported_at", { mode: "timestamp" }),
  startedAt: integer("started_at", { mode: "timestamp" }),
  completedAt: integer("completed_at", { mode: "timestamp" }),
  createdAt: integer("created_at", { mode: "timestamp" })
    .notNull()
    .$defaultFn(() => new Date()),
});

export type ServiceKey = typeof serviceKeys.$inferSelect;
export type NewServiceKey = typeof serviceKeys.$inferInsert;
export type Migration = typeof migrations.$inferSelect;
export type NewMigration = typeof migrations.$inferInsert;
export type Task = typeof tasks.$inferSelect;
export type NewTask = typeof tasks.$inferInsert;
