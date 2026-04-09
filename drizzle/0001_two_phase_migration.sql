-- Two-phase migration architecture: export (source → local) then import (local → target)
-- New task statuses: pending, exporting, export_paused, exported, importing, import_paused, completed, failed
-- SQLite text columns have no enum enforcement, so status changes are schema-level only.

ALTER TABLE `tasks` ADD COLUMN `phase` text DEFAULT 'export' NOT NULL;
--> statement-breakpoint
ALTER TABLE `tasks` ADD COLUMN `exported_items` integer DEFAULT 0;
--> statement-breakpoint
ALTER TABLE `tasks` ADD COLUMN `imported_items` integer DEFAULT 0;
--> statement-breakpoint
ALTER TABLE `tasks` ADD COLUMN `exported_at` integer;
--> statement-breakpoint
ALTER TABLE `tasks` ADD COLUMN `manifest_path` text;
--> statement-breakpoint
ALTER TABLE `tasks` ADD COLUMN `local_storage_bytes` integer DEFAULT 0;
--> statement-breakpoint
-- Remove the old processedItems column by leaving it in place (SQLite cannot DROP COLUMN on older versions).
-- The column is kept for backward compatibility but the new exportedItems/importedItems are canonical.
