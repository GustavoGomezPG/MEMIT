# MEMIT - Media & Content Migration Tool

A full-stack local application for migrating content and media between HubSpot portals with a two-phase, local-first architecture.

## Overview

MEMIT provides a safe, resumable, and auditable way to migrate content between HubSpot portals. Instead of streaming data directly from source to target (risky), MEMIT downloads everything locally first, lets you review and configure, then uploads to the target portal.

### Key Features

- **Two-Phase Architecture** - Export (source to local) then Import (local to target), with a review step in between
- **Resumable Operations** - Pause/resume at any point. Crash recovery via on-disk manifest tracking every item
- **Dry Run Mode** - Preview what would happen without writing to the target portal
- **Idempotent Imports** - Re-running imports skips already-migrated items (no duplicates)
- **Content Scanning** - Automatically detects HubL tokens, form embeds, and CTA embeds that need attention
- **CTA Mapping** - Extract CTA GUIDs from posts, map them to target portal CTAs, auto-rewrite during import
- **HubL Template Extraction** - Automatically downloads custom modules and includes from the source Design Manager
- **CSV Export** - Export any migration data as CSV for manual import via HubSpot's CSV tool
- **Storage Pre-Check** - Compares export size against target portal's free storage before importing
- **Multi-Select Filters** - Filter blog posts by blog, tags, and status with multi-select dropdowns

## Tech Stack

| Layer | Technology |
|-------|-----------|
| **Framework** | [TanStack Start](https://tanstack.com/start) (React 19, file-based routing, server functions) |
| **UI Components** | [shadcn/ui](https://ui.shadcn.com/) v4 (base-ui) + Tailwind CSS v4 |
| **Database** | SQLite via [Drizzle ORM](https://orm.drizzle.team/) |
| **Charts** | chart.js + react-chartjs-2 |
| **Virtual Scrolling** | @tanstack/react-virtual |
| **CSV** | csv-stringify |
| **Runtime** | Node.js |
| **Package Manager** | pnpm |

## Design System

MEMIT uses an editorial enterprise design system ("The Architectural Ledger"):

- **Tonal Layering** - No 1px borders for layout; depth through background color shifts
- **Signature Gradient** - Primary CTAs use a gradient from deep red to HubSpot orange
- **Typography** - Manrope for headings, Inter for body text
- **Ghost Borders** - Semi-transparent borders only where absolutely necessary
- **Dark Mode** - Full dark mode support via Tailwind CSS class strategy

## Architecture

### Two-Phase Migration Flow

```
pending -> exporting -> exported -> importing -> completed
              |                        |
        export_paused           import_paused

Any state -> failed (retryable from failed phase)
```

1. **Export Phase** - Downloads all data from the source HubSpot portal to local storage
2. **Review Phase** - User inspects exported data, configures CTA mappings, reviews warnings
3. **Import Phase** - Uploads from local storage to the target portal with full idempotency

### Local Storage Structure

```
memit-downloads/
  {migrationId}/
    {taskId}/
      manifest.json         # Item-level tracking (atomic writes)
      _ctas.json            # CTA GUID mapping
      _folders.json         # Folder structure (media tasks)
      _templates.json       # HubL template manifest
      data/
        image.png           # Downloaded media files
        post-12345.json     # Blog post JSON
        templates/
          modules/          # Extracted HubL modules
          includes/         # Extracted HubL includes
      exports/
        media-2026-04-09.csv
        blog_posts-2026-04-09.csv
```

### Manifest System

Every item in a migration task is tracked individually in `manifest.json`:

```json
{
  "version": 1,
  "items": [
    {
      "id": "hubspot-file-id",
      "sourceUrl": "https://...",
      "localPath": "data/image.png",
      "targetUrl": null,
      "status": "exported",
      "size": 12345
    }
  ],
  "warnings": ["[cta_embed] Post \"Title\": CTA must be recreated..."],
  "summary": { "total": 100, "exported": 100, "imported": 0, "failed": 0, "skipped": 0 }
}
```

## Supported Migration Types

| Type | Export | Import | CSV | Notes |
|------|--------|--------|-----|-------|
| **Media Files** | Folders + files with structure preservation | Upload with RETURN_EXISTING idempotency | File listing | Signed URL fallback for private files |
| **Blog Posts** | Posts + inline media + HubL templates | Authors, tags, content groups auto-created | Post metadata | CTA mapping, content scanning, selective post import |
| **HubDB** | Planned | Planned | Planned | - |
| **Pages** | Planned | Planned | Planned | - |

### Blog Posts - Smart Migration

The blog runner handles everything in one task:

1. **Download posts** - Save full JSON + extract inline media
2. **Content scanning** - Detect HubL tokens, form embeds, CTA embeds, broken media
3. **HubL extraction** - Download custom modules and includes from Design Manager
4. **CTA extraction** - Build GUID mapping for manual CTA recreation
5. **Import prerequisites** - Auto-create authors (match by email), tags (match by name), content groups
6. **Media upload** - Upload discovered media with deduplication across tasks
7. **Content rewriting** - Rewrite media URLs, CTA GUIDs, author/tag/content group IDs
8. **Post creation** - Create as DRAFT with preserved publish dates

## HubSpot API

### Authentication

MEMIT uses **Service Keys** (beta), obtained from:
**Settings > Account Management > Keys > Service Keys**

### Required Scopes

| Scope | Purpose |
|-------|---------|
| `files` | File Manager read/write |
| `files.ui_hidden.read` | System/hidden files (editor uploads, module assets) |
| `content` | CMS content read/write (blog posts, pages) |
| `hubdb` | HubDB tables read/write |

### Rate Limiting

- 100 requests per 10 seconds (auto-throttled)
- Automatic retry on HTTP 429 with Retry-After backoff
- Search endpoints: 4 requests/second

### Storage Limits

| Plan | File Storage |
|------|-------------|
| Free | 250 MB |
| Starter | 500 MB |
| Professional | 5 GB |
| Enterprise | 5 GB |

MEMIT checks target storage before import and warns if insufficient.

## Project Structure

```
src/
  routes/
    __root.tsx              # App shell, nav, theme toggle
    index.tsx               # Dashboard
    migrations/
      new.tsx               # New migration + key management
      $id/index.tsx         # Migration detail (two-phase task cards)
  components/
    ui/                     # shadcn/ui (DO NOT edit)
    MigrationCard.tsx       # Dashboard card
    TaskCard.tsx            # Two-phase task card
    CreateTaskModal.tsx     # Multi-step task creation
    ManifestBrowser.tsx     # Virtual-scrolled data browser
    WarningsPanel.tsx       # Content warning details + resolution
    CtaMappingModal.tsx     # CTA GUID mapping interface
    MultiSelect.tsx         # Multi-select dropdown
    PermissionsGuide.tsx    # Setup instructions
  server/
    hubspot.ts              # HubSpot API client
    migrations.ts           # Migration + key CRUD
    tasks.ts                # Task orchestration
    manifest.ts             # On-disk manifest manager
    scanners.ts             # Content warning scanners
    csv.ts                  # CSV generation
    runners/
      base.ts               # Shared runner infrastructure
      media.ts              # Media: export + import
      blogs.ts              # Blog: export + import
  db/
    schema.ts               # Drizzle schema
    index.ts                # SQLite connection
```

## Getting Started

### Prerequisites

- Node.js 20+
- pnpm

### Installation

```bash
git clone git@github.com:GustavoGomezPG/MEMIT.git
cd MEMIT
pnpm install
pnpm db:push
```

### Development

```bash
pnpm dev
```

Open [http://localhost:3000](http://localhost:3000)

### Commands

| Command | Description |
|---------|-------------|
| `pnpm dev` | Start dev server on port 3000 |
| `pnpm build` | Production build |
| `pnpm db:generate` | Generate Drizzle migration SQL |
| `pnpm db:push` | Push schema to SQLite |
| `pnpm db:studio` | Open Drizzle Studio |

## Database

SQLite database (`memit.db`) with three tables:

- **service_keys** - Named HubSpot Service Key tokens
- **migrations** - Source/target portal pairs (swappable direction)
- **tasks** - Two-phase migration jobs with manifest tracking

Schema changes use Drizzle migrations in `drizzle/`. Never delete `memit.db`.

## Safety & Idempotency

- **Export is read-only** - Only GET requests to the source portal. Nothing is modified or deleted
- **Import uses RETURN_EXISTING** - HubSpot returns existing files instead of creating duplicates
- **Blog slug check** - Posts with matching slugs in the target are skipped
- **Manifest checkpointing** - Atomic writes every 10 items for crash recovery
- **Pause at any time** - Runner checks pause status between every item

## Important: Local-Only Application

**MEMIT is designed to run locally on your machine only.** It is NOT intended to be deployed to a server, cloud hosting, or any publicly accessible environment.

- HubSpot access tokens are stored in a local SQLite database without encryption
- Downloaded media files are stored as plain files on disk
- There is no user authentication, session management, or access control
- The application trusts `localhost` connections implicitly

**Do not expose this application to the internet.** If a hosted/multi-user version is needed in the future, significant security work would be required (token encryption, auth layer, secure file storage, HTTPS, etc.).

## License

Private - Datamax internal tool.
