import { stringify } from "csv-stringify/sync";
import { writeFile } from "fs/promises";
import { resolve } from "path";
import { getExportsDir } from "./manifest";
import { mkdir } from "fs/promises";

/**
 * Generate a CSV string from an array of objects.
 */
export function generateCsv(
  records: Record<string, unknown>[],
  columns?: string[]
): string {
  if (records.length === 0) return "";

  const cols = columns || Object.keys(records[0]!);

  return stringify(records, {
    header: true,
    columns: cols,
    cast: {
      boolean: (value) => (value ? "true" : "false"),
      object: (value) => JSON.stringify(value),
    },
  });
}

/**
 * Write a CSV export file to the task's exports directory.
 * Returns the absolute path to the written file.
 */
export async function writeCsvExport(
  migrationId: number,
  taskId: number,
  taskType: string,
  records: Record<string, unknown>[],
  columns?: string[]
): Promise<string> {
  const csv = generateCsv(records, columns);
  const dir = getExportsDir(migrationId, taskId);
  await mkdir(dir, { recursive: true });

  const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
  const fileName = `${taskType}-${timestamp}.csv`;
  const filePath = resolve(dir, fileName);

  await writeFile(filePath, csv, "utf-8");
  return filePath;
}

/** Media CSV columns */
export const MEDIA_CSV_COLUMNS = [
  "id",
  "name",
  "sourceUrl",
  "localPath",
  "size",
  "extension",
  "folderPath",
];

/** Blog post CSV columns */
export const BLOG_CSV_COLUMNS = [
  "id",
  "name",
  "slug",
  "htmlTitle",
  "state",
  "publishDate",
  "url",
  "featuredImage",
  "metaDescription",
  "contentGroupId",
  "blogAuthorId",
];

/** HubDB CSV columns */
export const HUBDB_CSV_COLUMNS = [
  "tableId", "tableName", "rowId", "path", "name", "values",
];
