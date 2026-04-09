# MEMIT — Media & Content Migration Tool

A full-stack local application for migrating content and media between HubSpot portals.

## Tech Stack

- **Framework:** TanStack Start (React 19, file-based routing, server functions)
- **UI:** shadcn/ui v4 (uses `@base-ui/react`, NOT Radix) + Tailwind CSS v4
- **Database:** SQLite via Drizzle ORM (`memit.db` at project root)
- **Runtime:** Node.js, **pnpm** as package manager
- **Charts:** chart.js + react-chartjs-2 (doughnut chart for file type breakdown)
- **CSV:** csv-stringify (sync API) for export files

## Key Conventions

### TanStack Start Server Functions

Server functions use the `.inputValidator().handler()` chain — NOT `.validator()`:

```ts
export const myFn = createServerFn({ method: "POST" })
  .inputValidator((data: { id: number }) => data)
  .handler(async ({ data }) => { ... });
```

Callers pass data as `myFn({ data: { id: 1 } })`.

### shadcn/ui (base-ui, not Radix)

Built on `@base-ui/react`, not `@radix-ui`:
- **No `asChild` prop.** Use `render` for composition.
- **Accordion** has no `type="single"` or `collapsible` props.
- **Select** `onValueChange` passes `string | null`, not `string`.

### Dark Mode

Tailwind v4 with `@custom-variant dark (&:is(.dark *))`. Toggled on `<html>` via localStorage.

### Design System

"Architectural Ledger" editorial aesthetic:
- **Tonal layering** — no 1px borders for layout. Use `surface-low`, `surface-high` background shifts.
- **Signature gradient** — `linear-gradient(135deg, var(--primary), #ff7a59)` for primary CTAs.
- **Typography** — Manrope for headings, Inter for body.
- **Ghost borders** — `rgba(223, 192, 184, 0.15)`, never opaque.
- **Ambient shadows** — `0 12px 32px rgba(0, 29, 53, 0.06)`.

## HubSpot API

### Authentication: Service Keys (not Private Apps)

Tokens are **Service Keys** (beta), obtained from:
**Settings > Account Management > Keys > Service Keys**

Validation: `/account-info/v3/details` → `{ portalId }`. Do NOT use `/oauth/v1/access-tokens/`.

### Required Scopes

- `files` — File Manager read/write
- `files.ui_hidden.read` — System/hidden files
- `content` — CMS content read/write
- `hubdb` — HubDB tables read/write

### Rate Limiting

100 req/10s with auto-retry on 429. Search endpoints: 4 req/sec.

### Storage Limits

Free: 250MB, Starter: 500MB, Professional/Enterprise: 5GB. Max file: 300MB (2GB on paid).

### File Upload Idempotency

`uploadFile` uses `duplicateValidationStrategy: "RETURN_EXISTING"` — re-running imports does NOT create duplicates. HubSpot returns the existing file if it matches.

### Source Portal

Immediate use case: portal **489418** (`489418.fs1.hubspotusercontent-na1.net`).

## Database Schema

Three tables (see `src/db/schema.ts`):

- **service_keys** — Named HubSpot tokens with portal IDs. Reusable, swappable.
- **migrations** — Source/target key references, status. Direction is swappable.
- **tasks** — Two-phase migration jobs with:
  - `status`: `pending | exporting | export_paused | exported | importing | import_paused | completed | failed`
  - `phase`: `export | import`
  - `exportedItems`, `importedItems`, `failedItems` — per-phase progress
  - `manifestPath` — path to on-disk manifest.json
  - `localStorageBytes` — total size of exported data
  - `urlMapping` — JSON `{ oldUrl: newUrl }` for media rewriting
  - `config` — JSON task config (e.g., `{ selectedPostIds: [...] }`)
  - `outputType`: `same_as_source | hubdb | csv`

## Database Migrations — IMPORTANT

**NEVER delete `memit.db` to fix schema issues.** Use Drizzle migrations:

1. Change `src/db/schema.ts`
2. Run `pnpm db:generate` or hand-write SQL in `drizzle/`
3. Run `pnpm db:push` to apply

If `drizzle-kit generate` fails (TTY required), write `ALTER TABLE ... ADD COLUMN` SQL manually.

## Project Structure

```
src/
  routes/
    __root.tsx              # App shell, nav, theme toggle, notFound
    index.tsx               # Dashboard — migration cards grid
    migrations/
      new.tsx               # New migration form with key management
      $id/
        index.tsx           # Migration detail — two-phase task cards
  components/
    ui/                     # shadcn/ui components (DO NOT edit)
    MigrationCard.tsx       # Dashboard card
    TaskCard.tsx            # Two-phase task card (export/import progress)
    CreateTaskModal.tsx     # Multi-step task creation
    ManifestBrowser.tsx     # Browse exported data modal
    PermissionsGuide.tsx    # Service key setup guide
  server/
    hubspot.ts              # HubSpot API client (rate-limited)
    migrations.ts           # Migration + service key CRUD
    tasks.ts                # Task orchestration (export/import/pause)
    manifest.ts             # On-disk manifest manager
    scanners.ts             # Content warning scanners (HubL, forms, CTAs)
    csv.ts                  # CSV generation utility
    runners/
      base.ts               # Shared runner infrastructure
      media.ts              # Media: exportMedia + importMedia
      blogs.ts              # Blog: exportBlogPosts + importBlogPosts
  db/
    schema.ts               # Drizzle schema
    index.ts                # SQLite connection (WAL mode)
  lib/
    utils.ts                # cn() helper
drizzle/                    # Migration SQL files
memit-downloads/            # Local storage (gitignored)
  {migrationId}/
    {taskId}/
      manifest.json         # Item-level tracking
      data/                 # Downloaded files
      exports/              # CSV exports
```

## Commands

```bash
pnpm dev              # Start dev server on port 3000
pnpm build            # Production build
pnpm db:generate      # Generate Drizzle migration SQL
pnpm db:push          # Push schema to SQLite
pnpm db:studio        # Open Drizzle Studio
```

## Two-Phase Migration Architecture

**Every task type follows: Export (source → local) then Import (local → target).**

### Status Flow

```
pending → exporting → exported → importing → completed
            ↕                       ↕
       export_paused          import_paused
       
Any state → failed (retryable from failed phase)
```

`exported` is a stable checkpoint — user reviews data before triggering import.

### Export Phase
- Fetches all items from source portal
- Downloads to `memit-downloads/{migrationId}/{taskId}/data/`
- Creates manifest.json tracking every item's status
- Scans content for warnings (HubL, form embeds, CTAs)
- Generates CSV if outputType is "csv"
- Non-destructive — never writes to target

### Import Phase
- Reads from local storage — never touches source portal
- **Storage pre-check** — compares export size vs target portal free space
- **Idempotency** — skips items already in URL mapping or already existing in target
- **Dry run mode** — logs what would happen without writing to target
- Resumable — picks up from last manifest checkpoint on crash/pause

### Manifest (on-disk, not DB)

Per-task `manifest.json` tracks every item:
```json
{
  "items": [
    { "id": "...", "sourceUrl": "...", "localPath": "...", "targetUrl": null, "status": "exported", "size": 12345 }
  ],
  "warnings": ["[hubl] Post 'X': HubL token found..."],
  "summary": { "total": 100, "exported": 100, "imported": 0, "failed": 0, "skipped": 0 }
}
```
Written atomically (write .tmp → rename) every 10 items for crash safety.

### Media Runner

**Export:** Fetch folders + files → download each file preserving folder structure → save folder map as `_folders.json`.

**Import:** Storage pre-check → recreate folders in target → upload files with `RETURN_EXISTING` → build URL mapping.

### Blog Posts Runner

**Export:** Fetch posts (filtered by selected IDs) → save each as JSON + download inline media → scan for HubL/forms/CTAs → generate warnings.

**Import:** Resolve pre-requisites (create authors, tags, content groups in target) → upload media → rewrite URLs + IDs → create posts as DRAFT with preserved publish dates. Idempotency: skip posts with matching slug in target.

## Content Scanning (`src/server/scanners.ts`)

During blog export, HTML is scanned for portal-specific content that breaks after migration:
- **HubL tokens** — `{% %}` and `{{ }}` template references
- **Form embeds** — `hbspt.forms.create` with portal-specific IDs
- **CTA embeds** — CTA GUIDs that must be recreated manually
- **Broken media** — URLs that 404 during download

Warnings are stored in the manifest and shown in the UI as an amber badge.

## CRITICAL: Dry Run Requirement

**Every import phase MUST support dry run mode.** Non-negotiable.

- Export always runs (downloading is non-destructive)
- Dry run gates the import phase only — logs what would happen, never writes to target
- UI shows "Dry Run" button alongside "Import" on exported tasks
- CSV export provides a manual fallback path via HubSpot's CSV import tool
