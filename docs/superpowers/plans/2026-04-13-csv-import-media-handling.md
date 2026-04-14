# CSV Import Media Handling Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add media discovery, download, upload, and URL rewriting to the CSV-to-HubDB import pipeline so that media referenced in CSV cell values is migrated alongside the data.

**Architecture:** During the export phase, scan every CSV cell value for media URLs (direct links, `<img>` tags, HubSpot CDN patterns), download them to local storage, and save a media catalog. During the import phase, upload media from local storage to the target portal, build a URL mapping, rewrite all cell values before inserting HubDB rows. Dry run previews what media would be uploaded and what URLs would change.

**Tech Stack:** Node.js fs, HubSpot Files API v3 (`uploadFile` with `RETURN_EXISTING`), existing runner infrastructure

---

## File Structure

### Modified files
```
src/server/runners/csv-import.ts  — Add media scanning to export, media upload + URL rewriting to import
```

No new files needed. All changes are contained in the existing CSV import runner, using existing utilities (`uploadFile`, `getExistingUrlMapping` from base, `getDataDir` from manifest).

---

## Task 1: Add media URL extraction to export phase

**Files:**
- Modify: `src/server/runners/csv-import.ts`

- [ ] **Step 1: Add new imports**

At the top of the file, add the missing imports needed for media handling:

```ts
import { getDataDir, getExistingUrlMapping } from "../manifest";
// Change existing import to also include getDataDir:
import { flushManifest, getDataDir } from "../manifest";
// Add getExistingUrlMapping from base:
import { createRunnerContext, logToTask, isTaskPaused, getExistingUrlMapping } from "./base";
// Add uploadFile from hubspot:
import { uploadFile } from "../hubspot";
// Add fs utilities:
import { readFile, writeFile, mkdir } from "fs/promises";
import { resolve } from "path";
```

The complete import block should become:

```ts
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
```

- [ ] **Step 2: Add media URL extraction function**

Add after `inferColumnType` and before the export function:

```ts
// ── Media URL extraction ──

const IMG_SRC_RE = /(?:src|data-src)=["']([^"']+)["']/gi;
const HUBSPOT_CDN_RE = /https?:\/\/[^"'\s,]*hubspotusercontent[^"'\s,]*/gi;
const DIRECT_URL_RE = /^https?:\/\/.+\.(png|jpe?g|gif|svg|webp|ico|bmp|pdf|doc|docx|xls|xlsx|ppt|pptx|mp4|mov|avi|webm|mp3|wav|zip|rar|csv|txt|woff2?|ttf|eot)(\?[^\s]*)?$/i;

interface MediaEntry {
  sourceUrl: string;
  localPath: string | null;
  size: number;
  foundIn: string[]; // column names where this URL was found
}

/**
 * Scan all cell values in a CSV dataset for media URLs.
 * Detects:
 *  - Direct URLs to common file types (images, docs, video, etc.)
 *  - HubSpot CDN URLs anywhere in a string
 *  - <img src="..."> and <img data-src="..."> in rich text / HTML
 *  - href="..." pointing to media file extensions
 */
function extractMediaFromCsv(
  rows: Record<string, string>[],
  headers: string[],
  columnTypes: Record<string, string>
): Map<string, MediaEntry> {
  const media = new Map<string, MediaEntry>();

  function addUrl(url: string, column: string) {
    // Clean up URL — remove trailing quotes, whitespace
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

      // 1. URL or IMAGE columns — treat entire value as a direct URL
      if (colType === "URL" || colType === "IMAGE") {
        if (/^https?:\/\//i.test(value)) {
          addUrl(value, header);
        }
        continue;
      }

      // 2. Check if the value is a direct URL to a media file
      DIRECT_URL_RE.lastIndex = 0;
      if (DIRECT_URL_RE.test(value.trim())) {
        addUrl(value.trim(), header);
        continue;
      }

      // 3. Scan for HubSpot CDN URLs embedded anywhere
      HUBSPOT_CDN_RE.lastIndex = 0;
      let match: RegExpExecArray | null;
      while ((match = HUBSPOT_CDN_RE.exec(value)) !== null) {
        addUrl(match[0], header);
      }

      // 4. Scan for <img> and other HTML media references
      IMG_SRC_RE.lastIndex = 0;
      while ((match = IMG_SRC_RE.exec(value)) !== null) {
        if (match[1]) addUrl(match[1], header);
      }

      // 5. Scan for href="..." pointing to media files
      const hrefRe = /href=["']([^"']+\.(png|jpe?g|gif|svg|webp|pdf|doc|docx|xls|xlsx|ppt|pptx|mp4|mov|zip|rar|csv)[^"']*)["']/gi;
      while ((match = hrefRe.exec(value)) !== null) {
        if (match[1]) addUrl(match[1], header);
      }
    }
  }

  return media;
}

/**
 * Rewrite all media URLs in a cell value using the URL mapping.
 */
function rewriteUrls(value: string, mapping: Record<string, string>): string {
  let result = value;
  for (const [oldUrl, newUrl] of Object.entries(mapping)) {
    result = result.split(oldUrl).join(newUrl);
  }
  return result;
}
```

- [ ] **Step 3: Add media download to export phase**

In `exportCsvImport`, after the column type inference section (after `await logToTask(taskId, "info", \`Column types: ...`)`) and BEFORE the manifest item creation loop, add the media scanning and download logic:

```ts
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
        // Deduplicate filenames with a hash prefix
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

    // Store media count in config for import phase
    config.mediaCount = downloaded;
    config.mediaFailedCount = downloadFailed;
  } else {
    await logToTask(taskId, "info", "No media URLs found in CSV data");
    config.mediaCount = 0;
  }
```

Also add a `formatBytes` helper at the top of the file (after the `inferColumnType` function):

```ts
function formatBytes(bytes: number): string {
  if (bytes === 0) return "0 B";
  const k = 1024;
  const sizes = ["B", "KB", "MB", "GB"];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return `${(bytes / Math.pow(k, i)).toFixed(1)} ${sizes[i]}`;
}
```

- [ ] **Step 4: Verify — check TypeScript compilation**

Run: `pnpm dev` — confirm no errors in terminal.

- [ ] **Step 5: Commit**

```bash
git add src/server/runners/csv-import.ts
git commit -m "feat: add media URL extraction and download to CSV import export phase"
```

---

## Task 2: Add media upload and URL rewriting to import phase

**Files:**
- Modify: `src/server/runners/csv-import.ts`

- [ ] **Step 1: Add media upload logic to the HubDB import path**

In `importCsvImport`, after reading the CSV content and parsing rows (`const { rows } = parseCsv(csvContent);`), and BEFORE the `if (outputType === "csv")` check, add the media upload section:

```ts
  // ── Media upload and URL rewriting ──
  const urlMapping: Record<string, string> = await getExistingUrlMapping(migration.id);
  let mediaUploaded = 0;
  let mediaSkipped = 0;

  // Load media catalog from export
  const mediaDir = resolve(getDataDir(migration.id, taskId), "media");
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

      await logToTask(
        taskId,
        "info",
        `Media upload: ${mediaUploaded} uploaded, ${mediaSkipped} already mapped`
      );
    }
  }
```

- [ ] **Step 2: Update the dry run HubDB path to include media preview info**

Replace the existing HubDB dry run block:

```ts
  // Output type is HubDB
  if (dryRun) {
    await logToTask(taskId, "info", `[DRY RUN] Would create HubDB table with ${config.csvHeaders.length} columns and ${rows.length} rows`);
    await logToTask(taskId, "info", `Columns: ${config.csvHeaders.map((h) => `${h} (${config.csvColumnTypes[h]})`).join(", ")}`);
    manifest.phase = "exported";
    flushManifest(manifestPath, manifest);
    await db.update(tasks).set({ status: "exported", phase: "export" }).where(eq(tasks.id, taskId));
    await logToTask(taskId, "info", "Dry run completed. Ready for real import.");
    return;
  }
```

With:

```ts
  // Output type is HubDB
  if (dryRun) {
    await logToTask(taskId, "info", `[DRY RUN] Would create HubDB table with ${config.csvHeaders.length} columns and ${rows.length} rows`);
    await logToTask(taskId, "info", `Columns: ${config.csvHeaders.map((h) => `${h} (${config.csvColumnTypes[h]})`).join(", ")}`);

    // Preview URL rewrites
    const rewriteCount = Object.keys(urlMapping).length;
    if (rewriteCount > 0) {
      let affectedCells = 0;
      for (const row of rows) {
        for (const header of config.csvHeaders) {
          const value = row[header] || "";
          for (const oldUrl of Object.keys(urlMapping)) {
            if (value.includes(oldUrl)) {
              affectedCells++;
              break;
            }
          }
        }
      }
      await logToTask(taskId, "info", `[DRY RUN] Would rewrite ${rewriteCount} media URLs across ~${affectedCells} cells`);
    }

    manifest.phase = "exported";
    flushManifest(manifestPath, manifest);
    await db.update(tasks).set({ status: "exported", phase: "export" }).where(eq(tasks.id, taskId));
    await logToTask(taskId, "info", "Dry run completed. Ready for real import.");
    return;
  }
```

- [ ] **Step 3: Add URL rewriting before row insertion**

In the real (non-dry-run) HubDB import path, AFTER the table is created and column mapping is built (`colNameToId`), and BEFORE the batch insertion loop, add the URL rewriting step:

```ts
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
```

Then update the batch insertion loop to use `rewrittenRows` instead of `rows`:

Change:
```ts
  for (let i = 0; i < rows.length; i += BATCH_SIZE) {
    // ...
    const batch = rows.slice(i, i + BATCH_SIZE);
```

To:
```ts
  for (let i = 0; i < rewrittenRows.length; i += BATCH_SIZE) {
    // ...
    const batch = rewrittenRows.slice(i, i + BATCH_SIZE);
```

And update the progress log and remaining references from `rows.length` to `rewrittenRows.length` within the batch loop.

- [ ] **Step 4: Save URL mapping to task at the end**

In the final section (after publishing the table), update the DB write to include urlMapping:

Change:
```ts
  await db.update(tasks).set({
    status: "completed", completedAt: new Date(), importedItems: imported, failedItems: failed,
  }).where(eq(tasks.id, taskId));
```

To:
```ts
  await db.update(tasks).set({
    status: "completed", completedAt: new Date(), importedItems: imported, failedItems: failed,
    urlMapping: JSON.stringify(urlMapping),
  }).where(eq(tasks.id, taskId));
```

- [ ] **Step 5: Verify — TypeScript compilation**

Run: `pnpm dev` — confirm no errors.

- [ ] **Step 6: Commit**

```bash
git add src/server/runners/csv-import.ts
git commit -m "feat: add media upload and URL rewriting to CSV import phase"
```

---

## Summary

| Phase | What happens | Media handling |
|-------|-------------|---------------|
| **Export** | Parse CSV, infer types, scan all cells for media URLs, download media to local storage, save `_media.json` catalog | Discovery + download |
| **Dry Run** | Preview media uploads, count affected cells, show URL rewrite plan | Preview only |
| **Import** | Upload media from local to target, build URL mapping, rewrite all cell values, create HubDB table, insert rows | Upload + rewrite + insert |

Detection covers:
- URL/IMAGE column types (entire value)
- Direct URLs to media file extensions (`.png`, `.jpg`, `.pdf`, etc.)
- HubSpot CDN URLs embedded in any string
- `<img src="...">` / `<img data-src="...">` in rich text / HTML
- `<a href="...">` pointing to downloadable files
