CREATE TABLE `migrations` (
	`id` integer PRIMARY KEY AUTOINCREMENT NOT NULL,
	`name` text NOT NULL,
	`source_key_id` integer NOT NULL,
	`target_key_id` integer NOT NULL,
	`status` text DEFAULT 'draft' NOT NULL,
	`created_at` integer NOT NULL,
	`updated_at` integer NOT NULL,
	FOREIGN KEY (`source_key_id`) REFERENCES `service_keys`(`id`) ON UPDATE no action ON DELETE no action,
	FOREIGN KEY (`target_key_id`) REFERENCES `service_keys`(`id`) ON UPDATE no action ON DELETE no action
);
--> statement-breakpoint
CREATE TABLE `service_keys` (
	`id` integer PRIMARY KEY AUTOINCREMENT NOT NULL,
	`name` text NOT NULL,
	`access_token` text NOT NULL,
	`portal_id` text,
	`created_at` integer NOT NULL
);
--> statement-breakpoint
CREATE TABLE `tasks` (
	`id` integer PRIMARY KEY AUTOINCREMENT NOT NULL,
	`migration_id` integer NOT NULL,
	`type` text NOT NULL,
	`status` text DEFAULT 'pending' NOT NULL,
	`label` text NOT NULL,
	`output_type` text DEFAULT 'same_as_source' NOT NULL,
	`config` text,
	`total_items` integer DEFAULT 0,
	`processed_items` integer DEFAULT 0,
	`failed_items` integer DEFAULT 0,
	`url_mapping` text,
	`log` text,
	`started_at` integer,
	`completed_at` integer,
	`created_at` integer NOT NULL,
	FOREIGN KEY (`migration_id`) REFERENCES `migrations`(`id`) ON UPDATE no action ON DELETE cascade
);
