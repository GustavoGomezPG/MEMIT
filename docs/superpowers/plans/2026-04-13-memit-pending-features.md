# MEMIT Pending Features Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the four remaining MEMIT features — tag re-mapping for blog posts, HubDB migration, page migration, and CSV-to-HubDB import.

**Architecture:** Each feature follows the existing two-phase pattern (export → review → import). New runners are added under `src/server/runners/`, new API functions go in `src/server/hubspot.ts`, and UI components follow the modal pattern established by `CtaMappingModal` and `CreateTaskModal`. The CSV import runner reuses the HubDB API functions added in Phase 2.

**Tech Stack:** TanStack Start (React 19), shadcn/ui (base-ui), Drizzle ORM (SQLite), HubSpot CMS API v3

---

## Build Order & Dependencies

```
Phase 1: Tag Re-Mapping Screen          (independent)
Phase 2: HubDB API + Migration Runner   (independent)
Phase 3: Page API + Migration Runner     (independent)
Phase 4: CSV Import Runner              (depends on Phase 2 HubDB API)
```

## File Structure

### New files
```
src/server/runners/hubdb.ts           — HubDB export + import runner
src/server/runners/pages.ts           — Page export + import runner
src/server/runners/csv-import.ts      — CSV import runner (parse CSV → HubDB)
src/components/TagMappingModal.tsx     — Tag re-mapping UI for blog review step
```

### Modified files
```
src/server/hubspot.ts                 — Add HubDB + Page API functions
src/server/tasks.ts                   — Add tag mapping server fns, route new task types
src/server/csv.ts                     — Add HubDB + Page CSV column definitions
src/components/TaskCard.tsx            — Add csv_import icon, tag mapping button
src/components/CreateTaskModal.tsx     — Add page selector step, HubDB table preview
src/routes/migrations/$id/index.tsx   — Wire TagMappingModal, pass onDelete to tasks
```

---

## Phase 1: Tag Re-Mapping Screen

### Task 1: Server functions for tag data extraction

**Files:**
- Modify: `src/server/tasks.ts`

- [ ] **Step 1: Add `getTagData` server function**

This function reads all exported blog posts from the manifest, extracts unique tags with their post associations, and returns them for the UI. Add after `saveCtaMappings`:

```ts
export const getTagData = createServerFn({ method: "POST" })
  .inputValidator((taskId: number) => taskId)
  .handler(async ({ data: taskId }) => {
    const [task] = await db.select().from(tasks).where(eq(tasks.id, taskId));
    if (!task || !task.manifestPath) return { tags: [], posts: [] };

    const manifest = readManifest(task.manifestPath);

    // Build tag → posts mapping from exported items
    const tagPostMap = new Map<string, Set<string>>();
    const postNames: Record<string, string> = {};

    for (const item of manifest.items) {
      if (item.status !== "exported" && item.status !== "imported") continue;
      const tagIds = (item.metadata.tagIds as string[]) || [];
      const postId = item.id;
      postNames[postId] = (item.metadata.name as string) || postId;

      for (const tagId of tagIds) {
        if (!tagPostMap.has(tagId)) tagPostMap.set(tagId, new Set());
        tagPostMap.get(tagId)!.add(postId);
      }
    }

    // Try to get tag names from source portal
    const [migration] = await db.select().from(migrations).where(eq(migrations.id, task.migrationId));
    if (!migration) return { tags: [], posts: [] };

    const [sourceKey] = await db.select().from(serviceKeys).where(eq(serviceKeys.id, migration.sourceKeyId));
    let sourceTagNames: Record<string, string> = {};
    if (sourceKey) {
      try {
        const { fetchAllBlogTags } = await import("./hubspot");
        const sourceTags = await fetchAllBlogTags(sourceKey.accessToken);
        sourceTagNames = Object.fromEntries(sourceTags.map((t) => [t.id, t.name]));
      } catch { /* use IDs as fallback */ }
    }

    // Load existing tag mapping from task config
    let tagMapping: Record<string, { action: string; name?: string; mergeInto?: string }> = {};
    if (task.config) {
      try {
        const config = JSON.parse(task.config);
        if (config.tagMapping) tagMapping = config.tagMapping;
      } catch { /* */ }
    }

    const tags = Array.from(tagPostMap.entries()).map(([tagId, postIds]) => ({
      id: tagId,
      name: sourceTagNames[tagId] || tagId,
      postCount: postIds.size,
      postIds: Array.from(postIds),
      mapping: tagMapping[tagId] || null,
    }));

    const posts = manifest.items
      .filter((i) => i.status === "exported" || i.status === "imported")
      .map((i) => ({
        id: i.id,
        name: (i.metadata.name as string) || i.id,
        slug: (i.metadata.slug as string) || "",
        tagIds: (i.metadata.tagIds as string[]) || [],
      }));

    return { tags, posts };
  });
```

- [ ] **Step 2: Add `saveTagMapping` server function**

```ts
export const saveTagMapping = createServerFn({ method: "POST" })
  .inputValidator(
    (data: {
      taskId: number;
      tagMapping: Record<string, { action: string; name?: string; mergeInto?: string }>;
      postTagUpdates?: Record<string, string[]>;
    }) => data
  )
  .handler(async ({ data }) => {
    const [task] = await db.select().from(tasks).where(eq(tasks.id, data.taskId));
    if (!task) throw new Error("Task not found");

    // Merge into task config
    let config: Record<string, unknown> = {};
    if (task.config) {
      try { config = JSON.parse(task.config); } catch { /* */ }
    }
    config.tagMapping = data.tagMapping;

    // If post-level tag reassignments were made, update manifest metadata
    if (data.postTagUpdates && task.manifestPath) {
      const manifest = readManifest(task.manifestPath);
      for (const item of manifest.items) {
        if (data.postTagUpdates[item.id]) {
          item.metadata.tagIds = data.postTagUpdates[item.id];
        }
      }
      const { flushManifest } = await import("./manifest");
      flushManifest(task.manifestPath, manifest);
    }

    await db
      .update(tasks)
      .set({ config: JSON.stringify(config) })
      .where(eq(tasks.id, data.taskId));

    return { saved: true };
  });
```

- [ ] **Step 3: Add missing import for `flushManifest` at top of tasks.ts**

Add `flushManifest` to the import from `./manifest`:

```ts
import { readManifest, flushManifest } from "./manifest";
```

- [ ] **Step 4: Verify — restart dev server, confirm no TypeScript errors**

Run: `pnpm dev` — check terminal for compilation errors.

- [ ] **Step 5: Commit**

```bash
git add src/server/tasks.ts
git commit -m "feat: add tag data extraction and mapping server functions"
```

---

### Task 2: TagMappingModal component

**Files:**
- Create: `src/components/TagMappingModal.tsx`

- [ ] **Step 1: Create the TagMappingModal component**

This modal displays all tags from exported blog posts with controls to rename, delete, merge, and reassign. Structure:

```tsx
import { useState, useEffect, useMemo } from "react";
import {
  X,
  Search,
  Tag,
  Trash2,
  Edit3,
  GitMerge,
  Check,
  Loader2,
  ChevronDown,
  ChevronUp,
} from "lucide-react";
import { getTagData, saveTagMapping } from "../server/tasks";

interface TagData {
  id: string;
  name: string;
  postCount: number;
  postIds: string[];
  mapping: { action: string; name?: string; mergeInto?: string } | null;
}

interface PostData {
  id: string;
  name: string;
  slug: string;
  tagIds: string[];
}

type TagAction =
  | { action: "keep" }
  | { action: "rename"; name: string }
  | { action: "delete" }
  | { action: "merge"; mergeInto: string };

interface TagMappingModalProps {
  open: boolean;
  onClose: () => void;
  taskId: number;
}

export function TagMappingModal({ open, onClose, taskId }: TagMappingModalProps) {
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [tags, setTags] = useState<TagData[]>([]);
  const [posts, setPosts] = useState<PostData[]>([]);
  const [actions, setActions] = useState<Record<string, TagAction>>({});
  const [searchQuery, setSearchQuery] = useState("");
  const [expandedTag, setExpandedTag] = useState<string | null>(null);
  const [editingTag, setEditingTag] = useState<string | null>(null);
  const [editName, setEditName] = useState("");
  const [bulkSelected, setBulkSelected] = useState<Set<string>>(new Set());
  const [bulkTargetTag, setBulkTargetTag] = useState<string | null>(null);
  // Track per-post tag reassignments
  const [postTagOverrides, setPostTagOverrides] = useState<Record<string, string[]>>({});

  useEffect(() => {
    if (!open) return;
    setLoading(true);
    getTagData({ data: taskId })
      .then((result) => {
        setTags(result.tags);
        setPosts(result.posts);
        // Initialize actions from existing mappings
        const initial: Record<string, TagAction> = {};
        for (const tag of result.tags) {
          if (tag.mapping) {
            initial[tag.id] = tag.mapping as TagAction;
          } else {
            initial[tag.id] = { action: "keep" };
          }
        }
        setActions(initial);
      })
      .finally(() => setLoading(false));
  }, [open, taskId]);

  const filteredTags = useMemo(() => {
    if (!searchQuery) return tags;
    const q = searchQuery.toLowerCase();
    return tags.filter(
      (t) => t.name.toLowerCase().includes(q) || t.id.includes(q)
    );
  }, [tags, searchQuery]);

  const hasChanges = useMemo(() => {
    return Object.values(actions).some((a) => a.action !== "keep") ||
      Object.keys(postTagOverrides).length > 0;
  }, [actions, postTagOverrides]);

  function setTagAction(tagId: string, action: TagAction) {
    setActions((prev) => ({ ...prev, [tagId]: action }));
  }

  function startRename(tag: TagData) {
    setEditingTag(tag.id);
    const current = actions[tag.id];
    setEditName(current?.action === "rename" ? current.name : tag.name);
  }

  function confirmRename(tagId: string) {
    if (editName.trim()) {
      setTagAction(tagId, { action: "rename", name: editName.trim() });
    }
    setEditingTag(null);
  }

  function toggleBulkSelect(tagId: string) {
    setBulkSelected((prev) => {
      const next = new Set(prev);
      if (next.has(tagId)) next.delete(tagId);
      else next.add(tagId);
      return next;
    });
  }

  function applyBulkMerge() {
    if (!bulkTargetTag || bulkSelected.size === 0) return;
    const next = { ...actions };
    for (const tagId of bulkSelected) {
      if (tagId !== bulkTargetTag) {
        next[tagId] = { action: "merge", mergeInto: bulkTargetTag };
      }
    }
    setActions(next);
    setBulkSelected(new Set());
    setBulkTargetTag(null);
  }

  function reassignPost(postId: string, removeTagId: string, addTagId: string) {
    const post = posts.find((p) => p.id === postId);
    if (!post) return;
    const currentTags = postTagOverrides[postId] || [...post.tagIds];
    const updated = currentTags
      .filter((id) => id !== removeTagId)
      .concat(addTagId)
      .filter((v, i, a) => a.indexOf(v) === i); // dedupe
    setPostTagOverrides((prev) => ({ ...prev, [postId]: updated }));
  }

  async function handleSave() {
    setSaving(true);
    try {
      await saveTagMapping({
        data: {
          taskId,
          tagMapping: actions,
          postTagUpdates: Object.keys(postTagOverrides).length > 0
            ? postTagOverrides
            : undefined,
        },
      });
      onClose();
    } finally {
      setSaving(false);
    }
  }

  if (!open) return null;

  const actionSummary = {
    keep: Object.values(actions).filter((a) => a.action === "keep").length,
    rename: Object.values(actions).filter((a) => a.action === "rename").length,
    delete: Object.values(actions).filter((a) => a.action === "delete").length,
    merge: Object.values(actions).filter((a) => a.action === "merge").length,
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-foreground/40 backdrop-blur-sm">
      <div className="glass-modal shadow-ambient flex max-h-[90vh] w-full max-w-3xl flex-col overflow-hidden rounded-2xl">
        {/* Header */}
        <div className="flex items-center justify-between bg-[var(--surface-high)] px-8 py-6">
          <div>
            <h2 className="text-2xl font-extrabold tracking-tight">
              Tag Mapping
            </h2>
            <p className="mt-1 text-sm font-medium italic text-muted-foreground">
              Rename, merge, or remove tags before import.
            </p>
          </div>
          <button
            type="button"
            onClick={onClose}
            className="flex h-10 w-10 items-center justify-center rounded-full transition-colors hover:bg-white/50 dark:hover:bg-white/10"
          >
            <X className="h-5 w-5" />
          </button>
        </div>

        {/* Body */}
        <div className="overflow-y-auto p-8">
          {loading ? (
            <div className="flex flex-col items-center justify-center py-16">
              <Loader2 className="h-8 w-8 animate-spin text-primary" />
              <p className="mt-3 text-sm text-muted-foreground">
                Loading tag data...
              </p>
            </div>
          ) : tags.length === 0 ? (
            <p className="py-8 text-center text-sm text-muted-foreground">
              No tags found in exported posts.
            </p>
          ) : (
            <div className="space-y-4">
              {/* Summary badges */}
              <div className="flex flex-wrap gap-2 text-xs">
                <span className="rounded bg-secondary px-2 py-1 font-bold text-secondary-foreground">
                  {tags.length} tags
                </span>
                {actionSummary.rename > 0 && (
                  <span className="rounded bg-blue-100 px-2 py-1 font-bold text-blue-700 dark:bg-blue-900/30 dark:text-blue-300">
                    {actionSummary.rename} renamed
                  </span>
                )}
                {actionSummary.delete > 0 && (
                  <span className="rounded bg-red-100 px-2 py-1 font-bold text-red-700 dark:bg-red-900/30 dark:text-red-300">
                    {actionSummary.delete} deleted
                  </span>
                )}
                {actionSummary.merge > 0 && (
                  <span className="rounded bg-purple-100 px-2 py-1 font-bold text-purple-700 dark:bg-purple-900/30 dark:text-purple-300">
                    {actionSummary.merge} merged
                  </span>
                )}
              </div>

              {/* Bulk merge controls */}
              {bulkSelected.size > 1 && (
                <div className="flex items-center gap-3 rounded-xl bg-[var(--surface-low)] p-4">
                  <span className="text-xs font-bold text-muted-foreground">
                    Merge {bulkSelected.size} tags into:
                  </span>
                  <select
                    value={bulkTargetTag || ""}
                    onChange={(e) => setBulkTargetTag(e.target.value || null)}
                    className="rounded-lg bg-card px-3 py-1.5 text-xs font-semibold outline-none"
                  >
                    <option value="">Select target...</option>
                    {tags
                      .filter((t) => bulkSelected.has(t.id))
                      .map((t) => (
                        <option key={t.id} value={t.id}>
                          {t.name}
                        </option>
                      ))}
                  </select>
                  <button
                    type="button"
                    onClick={applyBulkMerge}
                    disabled={!bulkTargetTag}
                    className="rounded-lg bg-primary px-3 py-1.5 text-xs font-bold text-primary-foreground disabled:opacity-50"
                  >
                    Merge
                  </button>
                  <button
                    type="button"
                    onClick={() => setBulkSelected(new Set())}
                    className="text-xs text-muted-foreground hover:text-foreground"
                  >
                    Cancel
                  </button>
                </div>
              )}

              {/* Search */}
              <div className="relative">
                <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
                <input
                  type="text"
                  placeholder="Search tags..."
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                  className="w-full rounded-lg bg-[var(--surface-low)] py-2.5 pl-10 pr-4 text-sm outline-none placeholder:text-muted-foreground focus:ring-2 focus:ring-primary/30"
                />
              </div>

              {/* Tag list */}
              <div className="space-y-2">
                {filteredTags.map((tag) => {
                  const action = actions[tag.id] || { action: "keep" };
                  const isExpanded = expandedTag === tag.id;
                  const isEditing = editingTag === tag.id;
                  const isMerged = action.action === "merge";
                  const mergeTarget = isMerged
                    ? tags.find((t) => t.id === (action as { mergeInto: string }).mergeInto)
                    : null;

                  return (
                    <div key={tag.id} className="rounded-xl border border-border bg-card">
                      <div className="flex items-center gap-3 p-4">
                        {/* Bulk select checkbox */}
                        <button
                          type="button"
                          onClick={() => toggleBulkSelect(tag.id)}
                          className={`flex h-5 w-5 shrink-0 items-center justify-center rounded ${
                            bulkSelected.has(tag.id)
                              ? "bg-primary"
                              : "border border-border"
                          }`}
                        >
                          {bulkSelected.has(tag.id) && (
                            <Check className="h-3 w-3 text-primary-foreground" />
                          )}
                        </button>

                        <Tag className="h-4 w-4 shrink-0 text-muted-foreground" />

                        {/* Tag name or edit input */}
                        <div className="min-w-0 flex-1">
                          {isEditing ? (
                            <div className="flex items-center gap-2">
                              <input
                                type="text"
                                value={editName}
                                onChange={(e) => setEditName(e.target.value)}
                                onKeyDown={(e) => {
                                  if (e.key === "Enter") confirmRename(tag.id);
                                  if (e.key === "Escape") setEditingTag(null);
                                }}
                                autoFocus
                                className="flex-1 rounded bg-[var(--surface-low)] px-2 py-1 text-sm font-semibold outline-none focus:ring-2 focus:ring-primary/30"
                              />
                              <button
                                type="button"
                                onClick={() => confirmRename(tag.id)}
                                className="rounded bg-primary px-2 py-1 text-xs font-bold text-primary-foreground"
                              >
                                Save
                              </button>
                            </div>
                          ) : (
                            <div>
                              <span
                                className={`text-sm font-semibold ${
                                  action.action === "delete"
                                    ? "text-destructive line-through"
                                    : action.action === "rename"
                                      ? "text-blue-600 dark:text-blue-400"
                                      : isMerged
                                        ? "text-purple-600 dark:text-purple-400"
                                        : ""
                                }`}
                              >
                                {action.action === "rename"
                                  ? (action as { name: string }).name
                                  : tag.name}
                              </span>
                              {action.action === "rename" && (
                                <span className="ml-2 text-xs text-muted-foreground">
                                  (was: {tag.name})
                                </span>
                              )}
                              {isMerged && mergeTarget && (
                                <span className="ml-2 text-xs text-purple-500">
                                  → merges into "{mergeTarget.name}"
                                </span>
                              )}
                            </div>
                          )}
                        </div>

                        {/* Post count */}
                        <span className="shrink-0 text-xs tabular-nums text-muted-foreground">
                          {tag.postCount} post{tag.postCount !== 1 ? "s" : ""}
                        </span>

                        {/* Action buttons */}
                        {!isEditing && (
                          <div className="flex shrink-0 items-center gap-1">
                            <button
                              type="button"
                              onClick={() => startRename(tag)}
                              className="rounded p-1.5 text-muted-foreground transition-colors hover:bg-[var(--surface-low)] hover:text-foreground"
                              title="Rename"
                            >
                              <Edit3 className="h-3.5 w-3.5" />
                            </button>
                            <button
                              type="button"
                              onClick={() =>
                                setTagAction(
                                  tag.id,
                                  action.action === "delete"
                                    ? { action: "keep" }
                                    : { action: "delete" }
                                )
                              }
                              className={`rounded p-1.5 transition-colors ${
                                action.action === "delete"
                                  ? "bg-destructive/10 text-destructive"
                                  : "text-muted-foreground hover:bg-[var(--surface-low)] hover:text-foreground"
                              }`}
                              title={action.action === "delete" ? "Restore" : "Delete"}
                            >
                              <Trash2 className="h-3.5 w-3.5" />
                            </button>
                            <button
                              type="button"
                              onClick={() =>
                                setExpandedTag(isExpanded ? null : tag.id)
                              }
                              className="rounded p-1.5 text-muted-foreground transition-colors hover:bg-[var(--surface-low)] hover:text-foreground"
                              title="View posts"
                            >
                              {isExpanded ? (
                                <ChevronUp className="h-3.5 w-3.5" />
                              ) : (
                                <ChevronDown className="h-3.5 w-3.5" />
                              )}
                            </button>
                          </div>
                        )}
                      </div>

                      {/* Expanded: posts in this tag */}
                      {isExpanded && (
                        <div className="border-t border-border px-4 pb-4 pt-3">
                          <div className="space-y-1.5">
                            {tag.postIds.map((postId) => {
                              const post = posts.find((p) => p.id === postId);
                              if (!post) return null;
                              return (
                                <div
                                  key={postId}
                                  className="flex items-center justify-between rounded-lg bg-[var(--surface-low)] px-3 py-2"
                                >
                                  <div className="min-w-0 flex-1">
                                    <p className="truncate text-xs font-semibold">
                                      {post.name}
                                    </p>
                                    <p className="truncate text-[10px] font-mono text-muted-foreground">
                                      /{post.slug}
                                    </p>
                                  </div>
                                  <select
                                    value={tag.id}
                                    onChange={(e) => {
                                      if (e.target.value !== tag.id) {
                                        reassignPost(postId, tag.id, e.target.value);
                                      }
                                    }}
                                    className="ml-2 rounded bg-card px-2 py-1 text-[10px] font-semibold outline-none"
                                    title="Reassign to another tag"
                                  >
                                    {tags.map((t) => (
                                      <option key={t.id} value={t.id}>
                                        {t.name}
                                      </option>
                                    ))}
                                  </select>
                                </div>
                              );
                            })}
                          </div>
                        </div>
                      )}
                    </div>
                  );
                })}
              </div>
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="flex items-center justify-between bg-card px-8 py-6">
          <div className="text-xs text-muted-foreground">
            {Object.keys(postTagOverrides).length > 0 && (
              <span>{Object.keys(postTagOverrides).length} posts reassigned</span>
            )}
          </div>
          <div className="flex items-center gap-4">
            <button
              type="button"
              onClick={onClose}
              className="px-6 py-3 font-bold text-secondary-foreground transition-colors hover:text-foreground"
            >
              Cancel
            </button>
            <button
              type="button"
              onClick={handleSave}
              disabled={saving || !hasChanges}
              className="signature-gradient flex items-center gap-2 rounded-xl px-10 py-3 font-bold text-white shadow-lg shadow-primary/20 transition-all hover:scale-[1.02] active:scale-[0.98] disabled:opacity-50 disabled:hover:scale-100"
            >
              {saving ? (
                <Loader2 className="h-4 w-4 animate-spin" />
              ) : (
                <Check className="h-4 w-4" />
              )}
              Save Mapping
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
```

- [ ] **Step 2: Verify — check that the file compiles**

Run: `pnpm dev` — check terminal for errors on HMR.

- [ ] **Step 3: Commit**

```bash
git add src/components/TagMappingModal.tsx
git commit -m "feat: add TagMappingModal component for blog post tag management"
```

---

### Task 3: Wire TagMappingModal to TaskCard and migration detail

**Files:**
- Modify: `src/components/TaskCard.tsx`
- Modify: `src/routes/migrations/$id/index.tsx`

- [ ] **Step 1: Add `csv_import` icon and `onTagMapping` prop to TaskCard**

In `TaskCard.tsx`, add to the `typeIcons` map:

```ts
import { FileSpreadsheet } from "lucide-react";
// ... in typeIcons:
const typeIcons: Record<string, React.ElementType> = {
  media: Image,
  blog_posts: FileText,
  hubdb: Database,
  page: Layout,
  csv_import: FileSpreadsheet,
};
```

Add `onTagMapping` to `TaskCardProps`:

```ts
interface TaskCardProps {
  task: Task;
  onExport: (taskId: number) => void;
  onImport: (taskId: number, dryRun: boolean) => void;
  onPause: (taskId: number) => void;
  onDelete: (taskId: number) => void;
  onBrowse?: (taskId: number) => void;
  onTagMapping?: (taskId: number) => void;
  warningCount?: number;
  onWarningsClick?: () => void;
  isRunning?: boolean;
}
```

Destructure `onTagMapping` in the component function parameters.

- [ ] **Step 2: Add "Map Tags" button in exported blog_posts actions**

In the `task.status === "exported"` action button block, add after the Browse button:

```tsx
{task.type === "blog_posts" && onTagMapping && (
  <Button size="sm" variant="outline" onClick={() => onTagMapping(task.id)}>
    <Tag className="mr-1 h-3.5 w-3.5" /> Map Tags
  </Button>
)}
```

Add `Tag` to the lucide-react imports.

- [ ] **Step 3: Wire TagMappingModal in migration detail page**

In `src/routes/migrations/$id/index.tsx`, add imports:

```ts
import { TagMappingModal } from "../../../components/TagMappingModal";
```

Add state:

```ts
const [tagMapTaskId, setTagMapTaskId] = useState<number | null>(null);
```

Pass `onTagMapping` to TaskCard:

```tsx
<TaskCard
  key={task.id}
  task={task}
  onExport={handleExportTask}
  onImport={handleImportTask}
  onPause={handlePauseTask}
  onDelete={handleDeleteTask}
  onBrowse={(id) => setBrowseTaskId(id)}
  onTagMapping={(id) => setTagMapTaskId(id)}
  warningCount={taskWarnings[task.id]?.length}
  onWarningsClick={() => setWarningsTaskId(task.id)}
  isRunning={hasActiveTask}
/>
```

Render the modal (add alongside the other modals):

```tsx
{tagMapTaskId && (
  <TagMappingModal
    open={!!tagMapTaskId}
    onClose={() => setTagMapTaskId(null)}
    taskId={tagMapTaskId}
  />
)}
```

- [ ] **Step 4: Verify in browser — navigate to a blog_posts task in "exported" state, confirm Map Tags button appears**

- [ ] **Step 5: Commit**

```bash
git add src/components/TaskCard.tsx src/routes/migrations/$id/index.tsx
git commit -m "feat: wire TagMappingModal to TaskCard for blog post tasks"
```

---

### Task 4: Update blog import runner to apply tag mappings

**Files:**
- Modify: `src/server/runners/blogs.ts`

- [ ] **Step 1: Load tag mapping in the import function**

In `importBlogPosts`, after loading CTA mappings (~line 686), add tag mapping loading:

```ts
// Load tag mapping from task config
let tagMapping: Record<string, { action: string; name?: string; mergeInto?: string }> = {};
try {
  const currentTask = await db.select().from(tasks).where(eq(tasks.id, taskId)).then((r) => r[0]);
  if (currentTask?.config) {
    const config = JSON.parse(currentTask.config) as { tagMapping?: Record<string, { action: string; name?: string; mergeInto?: string }> };
    if (config.tagMapping) tagMapping = config.tagMapping;
  }
  if (Object.keys(tagMapping).length > 0) {
    await logToTask(taskId, "info", `Loaded tag mapping: ${Object.keys(tagMapping).length} tags have custom actions`);
  }
} catch { /* no tag mapping */ }
```

- [ ] **Step 2: Apply tag mapping during tag resolution**

Replace the tag mapping section in the import function (where `tagIdMapping` is built). After matching source tags to target tags by name, apply custom actions:

```ts
// Apply tag mapping overrides
for (const [sourceTagId, mapping] of Object.entries(tagMapping)) {
  if (mapping.action === "delete") {
    // Remove from tagIdMapping — posts with this tag will skip it
    delete tagIdMapping[sourceTagId];
  } else if (mapping.action === "rename" && mapping.name) {
    // Create tag with new name in target
    const existingByNewName = targetByName.get(mapping.name.toLowerCase());
    if (existingByNewName) {
      tagIdMapping[sourceTagId] = existingByNewName.id;
    } else {
      try {
        const created = await createBlogTag(targetToken, { name: mapping.name });
        tagIdMapping[sourceTagId] = created.id;
      } catch {
        await logToTask(taskId, "warn", `Could not create renamed tag "${mapping.name}"`);
      }
    }
  } else if (mapping.action === "merge" && mapping.mergeInto) {
    // Point this tag to the merge target's resolved ID
    const mergeTargetId = tagIdMapping[mapping.mergeInto];
    if (mergeTargetId) {
      tagIdMapping[sourceTagId] = mergeTargetId;
    }
  }
}
```

- [ ] **Step 3: Filter out deleted tags when building targetTagIds for each post**

In the post creation loop, update the tag ID resolution to skip deleted tags:

```ts
const sourceTagIds = ((post as Record<string, unknown>).tagIds as string[]) || [];
const targetTagIds = sourceTagIds
  .filter((id) => {
    const mapping = tagMapping[id];
    return !mapping || mapping.action !== "delete";
  })
  .map((id) => tagIdMapping[id] || id)
  .filter(Boolean);
```

- [ ] **Step 4: Verify — check TypeScript compilation**

Run: `pnpm dev` — check terminal for errors.

- [ ] **Step 5: Commit**

```bash
git add src/server/runners/blogs.ts
git commit -m "feat: apply tag mappings during blog post import"
```

---

## Phase 2: HubDB Migration

### Task 5: Add HubDB API functions to hubspot.ts

**Files:**
- Modify: `src/server/hubspot.ts`

- [ ] **Step 1: Add HubDB types**

Add after the `HubSpotContentGroup` interface:

```ts
// ── HubDB ──

export interface HubDbColumn {
  id: number;
  name: string;
  label: string;
  type: string; // TEXT, NUMBER, DATE, DATETIME, URL, IMAGE, SELECT, MULTISELECT, BOOLEAN, CURRENCY, RICHTEXT
  options?: Array<{ id: string; name: string; type: string }>;
  [key: string]: unknown;
}

export interface HubDbTable {
  id: string;
  name: string;
  label: string;
  columns: HubDbColumn[];
  rowCount: number;
  published: boolean;
  createdAt: string;
  updatedAt: string;
  [key: string]: unknown;
}

export interface HubDbRow {
  id: string;
  values: Record<string, unknown>;
  path?: string;
  name?: string;
  [key: string]: unknown;
}
```

- [ ] **Step 2: Add HubDB fetch functions**

```ts
export async function fetchAllHubDbTables(
  token: string
): Promise<HubDbTable[]> {
  const tables: HubDbTable[] = [];
  let after: string | undefined;
  do {
    const params = new URLSearchParams({ limit: "100" });
    if (after) params.set("after", after);
    const res = await hubspotFetch(token, `/cms/v3/hubdb/tables?${params}`);
    if (!res.ok) throw new Error(`Failed to fetch HubDB tables: ${res.status}`);
    const data = (await res.json()) as {
      results: HubDbTable[];
      paging?: { next?: { after: string } };
    };
    tables.push(...data.results);
    after = data.paging?.next?.after;
  } while (after);
  return tables;
}

export async function fetchHubDbTable(
  token: string,
  tableIdOrName: string
): Promise<HubDbTable> {
  const res = await hubspotFetch(token, `/cms/v3/hubdb/tables/${tableIdOrName}`);
  if (!res.ok) throw new Error(`Failed to fetch HubDB table: ${res.status}`);
  return res.json() as Promise<HubDbTable>;
}

export async function fetchAllHubDbRows(
  token: string,
  tableId: string
): Promise<HubDbRow[]> {
  const rows: HubDbRow[] = [];
  let after: string | undefined;
  do {
    const params = new URLSearchParams({ limit: "100" });
    if (after) params.set("after", after);
    const res = await hubspotFetch(
      token,
      `/cms/v3/hubdb/tables/${tableId}/rows?${params}`
    );
    if (!res.ok) throw new Error(`Failed to fetch HubDB rows: ${res.status}`);
    const data = (await res.json()) as {
      results: HubDbRow[];
      paging?: { next?: { after: string } };
    };
    rows.push(...data.results);
    after = data.paging?.next?.after;
  } while (after);
  return rows;
}
```

- [ ] **Step 3: Add HubDB write functions**

```ts
export async function createHubDbTable(
  token: string,
  table: { name: string; label: string; columns: Omit<HubDbColumn, "id">[] }
): Promise<HubDbTable> {
  const res = await hubspotFetch(token, "/cms/v3/hubdb/tables", {
    method: "POST",
    body: JSON.stringify(table),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Failed to create HubDB table: ${res.status} ${text}`);
  }
  return res.json() as Promise<HubDbTable>;
}

export async function createHubDbRow(
  token: string,
  tableId: string,
  row: { values: Record<string, unknown>; path?: string; name?: string }
): Promise<HubDbRow> {
  const res = await hubspotFetch(
    token,
    `/cms/v3/hubdb/tables/${tableId}/rows`,
    { method: "POST", body: JSON.stringify(row) }
  );
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Failed to create HubDB row: ${res.status} ${text}`);
  }
  return res.json() as Promise<HubDbRow>;
}

export async function createHubDbRowsBatch(
  token: string,
  tableId: string,
  rows: Array<{ values: Record<string, unknown>; path?: string; name?: string }>
): Promise<{ results: HubDbRow[] }> {
  const res = await hubspotFetch(
    token,
    `/cms/v3/hubdb/tables/${tableId}/rows/batch/create`,
    { method: "POST", body: JSON.stringify({ inputs: rows }) }
  );
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Failed to batch create HubDB rows: ${res.status} ${text}`);
  }
  return res.json() as Promise<{ results: HubDbRow[] }>;
}

export async function publishHubDbTable(
  token: string,
  tableId: string
): Promise<HubDbTable> {
  const res = await hubspotFetch(
    token,
    `/cms/v3/hubdb/tables/${tableId}/draft/publish`,
    { method: "POST" }
  );
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Failed to publish HubDB table: ${res.status} ${text}`);
  }
  return res.json() as Promise<HubDbTable>;
}

export async function fetchHubDbTableByName(
  token: string,
  name: string
): Promise<HubDbTable | null> {
  try {
    const res = await hubspotFetch(token, `/cms/v3/hubdb/tables/${name}`);
    if (!res.ok) return null;
    return res.json() as Promise<HubDbTable>;
  } catch {
    return null;
  }
}
```

- [ ] **Step 4: Commit**

```bash
git add src/server/hubspot.ts
git commit -m "feat: add HubDB API functions (tables, rows, batch create, publish)"
```

---

### Task 6: HubDB export runner

**Files:**
- Create: `src/server/runners/hubdb.ts`
- Modify: `src/server/csv.ts` (add HUBDB_CSV_COLUMNS)

- [ ] **Step 1: Add HubDB CSV columns**

In `src/server/csv.ts`, add:

```ts
export const HUBDB_CSV_COLUMNS = [
  "tableId", "tableName", "rowId", "path", "name", "values",
];
```

- [ ] **Step 2: Create the HubDB export runner**

Create `src/server/runners/hubdb.ts`:

```ts
import { db } from "../../db";
import { tasks } from "../../db/schema";
import type { Migration } from "../../db/schema";
import { eq } from "drizzle-orm";
import {
  createRunnerContext,
  logToTask,
  isTaskPaused,
  getExistingUrlMapping,
} from "./base";
import { readManifest, flushManifest, getDataDir } from "../manifest";
import {
  fetchAllHubDbTables,
  fetchAllHubDbRows,
  createHubDbTable,
  createHubDbRow,
  createHubDbRowsBatch,
  publishHubDbTable,
  fetchHubDbTableByName,
  type HubDbTable,
  type HubDbRow,
} from "../hubspot";
import { writeCsvExport, HUBDB_CSV_COLUMNS } from "../csv";
import { writeFile, mkdir } from "fs/promises";
import { resolve } from "path";
import { readFile } from "fs/promises";

// ── EXPORT PHASE ──

export async function exportHubDb(
  taskId: number,
  migration: Migration
): Promise<void> {
  const ctx = await createRunnerContext(taskId, migration);
  if (!ctx) return;

  const { sourceToken, manifestPath, manifest } = ctx;
  const dataDir = getDataDir(migration.id, taskId);
  await mkdir(dataDir, { recursive: true });

  await db
    .update(tasks)
    .set({ status: "exporting", phase: "export", startedAt: new Date() })
    .where(eq(tasks.id, taskId));

  // Fetch all HubDB tables
  await logToTask(taskId, "info", "Fetching HubDB tables from source portal...");
  let tables: HubDbTable[];
  try {
    tables = await fetchAllHubDbTables(sourceToken);
    await logToTask(taskId, "info", `Found ${tables.length} HubDB tables`);
  } catch (err) {
    await logToTask(taskId, "error", `Failed to fetch tables: ${err instanceof Error ? err.message : String(err)}`);
    await db.update(tasks).set({ status: "failed", completedAt: new Date() }).where(eq(tasks.id, taskId));
    return;
  }

  if (tables.length === 0) {
    await logToTask(taskId, "info", "No HubDB tables found in source portal");
    await db.update(tasks).set({ status: "exported", exportedAt: new Date() }).where(eq(tasks.id, taskId));
    return;
  }

  // Apply config filter if specific tables were selected
  const task = await db.select().from(tasks).where(eq(tasks.id, taskId)).then((r) => r[0]);
  if (task?.config) {
    try {
      const config = JSON.parse(task.config) as { selectedTableIds?: string[] };
      if (config.selectedTableIds?.length) {
        const selectedSet = new Set(config.selectedTableIds);
        tables = tables.filter((t) => selectedSet.has(t.id));
        await logToTask(taskId, "info", `Filtered to ${tables.length} selected tables`);
      }
    } catch { /* use all tables */ }
  }

  // Populate manifest items (one per table)
  const existingIds = new Set(manifest.items.map((i) => i.id));
  for (const table of tables) {
    if (!existingIds.has(table.id)) {
      manifest.items.push({
        id: table.id,
        sourceUrl: "",
        localPath: null,
        targetUrl: null,
        targetId: null,
        status: "pending",
        error: null,
        size: 0,
        metadata: {
          name: table.label || table.name,
          tableName: table.name,
          rowCount: table.rowCount,
          columnCount: table.columns.length,
        },
      });
    }
  }

  await db.update(tasks).set({ totalItems: manifest.items.length }).where(eq(tasks.id, taskId));
  flushManifest(manifestPath, manifest);

  // Export each table (schema + rows)
  await logToTask(taskId, "info", "Downloading table schemas and rows...");
  let exported = 0;
  let failed = 0;
  let totalBytes = 0;

  for (const item of manifest.items) {
    if (item.status === "exported") {
      exported++;
      totalBytes += item.size;
      continue;
    }

    if (await isTaskPaused(taskId)) {
      await logToTask(taskId, "info", "Export paused");
      flushManifest(manifestPath, manifest);
      return;
    }

    const table = tables.find((t) => t.id === item.id);
    if (!table) {
      item.status = "failed";
      item.error = "Table not found in fetched data";
      failed++;
      continue;
    }

    try {
      // Fetch all rows for this table
      const rows = await fetchAllHubDbRows(sourceToken, table.id);

      // Save table JSON (schema + rows)
      const tableData = {
        ...table,
        rows,
      };
      const tablePath = resolve(dataDir, `table-${table.id}.json`);
      const tableJson = JSON.stringify(tableData, null, 2);
      await writeFile(tablePath, tableJson, "utf-8");

      item.localPath = tablePath;
      item.size = Buffer.byteLength(tableJson);
      item.status = "exported";
      item.metadata.rowCount = rows.length;
      totalBytes += item.size;
      exported++;

      await logToTask(
        taskId,
        "info",
        `Exported table "${table.label || table.name}": ${rows.length} rows, ${table.columns.length} columns`
      );

      if (exported % 10 === 0) {
        await db
          .update(tasks)
          .set({ exportedItems: exported, failedItems: failed, localStorageBytes: totalBytes })
          .where(eq(tasks.id, taskId));
        flushManifest(manifestPath, manifest);
      }
    } catch (err) {
      item.status = "failed";
      item.error = err instanceof Error ? err.message : String(err);
      failed++;
      await logToTask(taskId, "warn", `Failed to export table "${table.label}": ${item.error}`);
    }
  }

  // CSV export if requested
  if (ctx.outputType === "csv") {
    const csvRecords: Record<string, unknown>[] = [];
    for (const item of manifest.items) {
      if (item.status !== "exported" || !item.localPath) continue;
      try {
        const raw = await readFile(item.localPath, "utf-8");
        const tableData = JSON.parse(raw) as HubDbTable & { rows: HubDbRow[] };
        for (const row of tableData.rows) {
          csvRecords.push({
            tableId: tableData.id,
            tableName: tableData.name,
            rowId: row.id,
            path: row.path || "",
            name: row.name || "",
            values: JSON.stringify(row.values),
          });
        }
      } catch { /* skip */ }
    }
    if (csvRecords.length > 0) {
      const csvPath = await writeCsvExport(migration.id, taskId, "hubdb", csvRecords, HUBDB_CSV_COLUMNS);
      await logToTask(taskId, "info", `CSV export saved: ${csvPath}`);
    }
  }

  manifest.phase = "exported";
  manifest.exportedAt = new Date().toISOString();
  flushManifest(manifestPath, manifest);

  await db
    .update(tasks)
    .set({
      status: "exported",
      phase: "export",
      exportedItems: exported,
      failedItems: failed,
      localStorageBytes: totalBytes,
      exportedAt: new Date(),
    })
    .where(eq(tasks.id, taskId));

  await logToTask(
    taskId,
    "info",
    `Export completed. ${exported} tables downloaded, ${failed} failed.`
  );
}
```

- [ ] **Step 3: Commit**

```bash
git add src/server/runners/hubdb.ts src/server/csv.ts
git commit -m "feat: add HubDB export runner with CSV output support"
```

---

### Task 7: HubDB import runner

**Files:**
- Modify: `src/server/runners/hubdb.ts`

- [ ] **Step 1: Add the import function**

Append to `src/server/runners/hubdb.ts`:

```ts
// ── IMPORT PHASE ──

export async function importHubDb(
  taskId: number,
  migration: Migration,
  options: { dryRun: boolean } = { dryRun: false }
): Promise<void> {
  const ctx = await createRunnerContext(taskId, migration, options);
  if (!ctx) return;

  const { targetToken, manifestPath, dryRun } = ctx;
  const manifest = readManifest(manifestPath);
  const dataDir = getDataDir(migration.id, taskId);

  await db
    .update(tasks)
    .set({ status: "importing", phase: "import" })
    .where(eq(tasks.id, taskId));

  if (dryRun) {
    await logToTask(taskId, "info", "DRY RUN — no tables will be created in target portal");
  }

  let imported = 0;
  let failed = 0;
  let skipped = 0;
  const exportedItems = manifest.items.filter((i) => i.status === "exported");

  for (const item of exportedItems) {
    if (await isTaskPaused(taskId)) {
      await logToTask(taskId, "info", "Import paused");
      flushManifest(manifestPath, manifest);
      await db
        .update(tasks)
        .set({ importedItems: imported, failedItems: failed })
        .where(eq(tasks.id, taskId));
      return;
    }

    if (!item.localPath) {
      item.status = "failed";
      item.error = "No local data file";
      failed++;
      continue;
    }

    try {
      const raw = await readFile(item.localPath, "utf-8");
      const tableData = JSON.parse(raw) as HubDbTable & { rows: HubDbRow[] };

      // Idempotency: check if table with same name exists in target
      if (!dryRun) {
        const existing = await fetchHubDbTableByName(targetToken, tableData.name);
        if (existing) {
          item.status = "skipped";
          item.targetId = existing.id;
          skipped++;
          await logToTask(taskId, "info", `Skipped table "${tableData.label}" — already exists in target (ID: ${existing.id})`);
          continue;
        }
      }

      if (dryRun) {
        await logToTask(
          taskId,
          "info",
          `[DRY RUN] Would create table "${tableData.label}" with ${tableData.columns.length} columns and ${tableData.rows.length} rows`
        );
        item.status = "skipped";
        skipped++;
        continue;
      }

      // Create table with schema
      const columnsForCreate = tableData.columns.map((col) => ({
        name: col.name,
        label: col.label,
        type: col.type,
        ...(col.options ? { options: col.options } : {}),
      }));

      const createdTable = await createHubDbTable(targetToken, {
        name: tableData.name,
        label: tableData.label,
        columns: columnsForCreate,
      });

      await logToTask(
        taskId,
        "info",
        `Created table "${tableData.label}" (ID: ${createdTable.id}), inserting ${tableData.rows.length} rows...`
      );

      // Build column name mapping (source column IDs → target column names)
      // HubDB rows use column IDs as keys in `values`, but when creating in a new table
      // the column IDs change. We need to map by column name.
      const sourceColIdToName: Record<string, string> = {};
      for (const col of tableData.columns) {
        sourceColIdToName[String(col.id)] = col.name;
      }
      const targetColNameToId: Record<string, string> = {};
      for (const col of createdTable.columns) {
        targetColNameToId[col.name] = String(col.id);
      }

      // Insert rows in batches of 100
      const BATCH_SIZE = 100;
      let rowsInserted = 0;

      for (let i = 0; i < tableData.rows.length; i += BATCH_SIZE) {
        const batch = tableData.rows.slice(i, i + BATCH_SIZE);
        const mappedRows = batch.map((row) => {
          // Remap column IDs: source row values use source column IDs
          const mappedValues: Record<string, unknown> = {};
          for (const [key, value] of Object.entries(row.values)) {
            const colName = sourceColIdToName[key] || key;
            const targetColId = targetColNameToId[colName];
            if (targetColId) {
              mappedValues[targetColId] = value;
            }
          }
          return {
            values: mappedValues,
            ...(row.path ? { path: row.path } : {}),
            ...(row.name ? { name: row.name } : {}),
          };
        });

        try {
          await createHubDbRowsBatch(targetToken, createdTable.id, mappedRows);
          rowsInserted += batch.length;
        } catch (batchErr) {
          // Fall back to individual inserts
          for (const row of mappedRows) {
            try {
              await createHubDbRow(targetToken, createdTable.id, row);
              rowsInserted++;
            } catch {
              // Non-fatal per row
            }
          }
        }
      }

      // Publish the table
      try {
        await publishHubDbTable(targetToken, createdTable.id);
        await logToTask(taskId, "info", `Published table "${tableData.label}" — ${rowsInserted} rows inserted`);
      } catch (pubErr) {
        await logToTask(taskId, "warn", `Table created but publish failed: ${pubErr instanceof Error ? pubErr.message : String(pubErr)}`);
      }

      item.status = "imported";
      item.targetId = createdTable.id;
      imported++;

      if (imported % 10 === 0) {
        await db
          .update(tasks)
          .set({ importedItems: imported, failedItems: failed })
          .where(eq(tasks.id, taskId));
        flushManifest(manifestPath, manifest);
      }
    } catch (err) {
      item.status = "failed";
      item.error = err instanceof Error ? err.message : String(err);
      failed++;
      await logToTask(taskId, "warn", `Failed to import table: ${item.error}`);
    }
  }

  manifest.phase = "completed";
  manifest.importedAt = new Date().toISOString();
  flushManifest(manifestPath, manifest);

  await db
    .update(tasks)
    .set({
      status: "completed",
      completedAt: new Date(),
      importedItems: imported,
      failedItems: failed,
    })
    .where(eq(tasks.id, taskId));

  await logToTask(
    taskId,
    "info",
    `Import ${dryRun ? "(DRY RUN) " : ""}completed. ${imported} tables created, ${skipped} skipped, ${failed} failed.`
  );
}
```

- [ ] **Step 2: Commit**

```bash
git add src/server/runners/hubdb.ts
git commit -m "feat: add HubDB import runner with batch row insert and publish"
```

---

### Task 8: Route HubDB tasks in orchestrator

**Files:**
- Modify: `src/server/tasks.ts`

- [ ] **Step 1: Add HubDB dispatch to exportTask**

In the `exportTask` handler, add after the `blog_posts` branch:

```ts
} else if (task.type === "hubdb") {
  const { exportHubDb } = await import("./runners/hubdb");
  exportHubDb(task.id, migration).catch((err) => runnerErrorHandler(task.id, err));
}
```

- [ ] **Step 2: Add HubDB dispatch to importTask**

In the `importTask` handler, add after the `blog_posts` branch:

```ts
} else if (task.type === "hubdb") {
  const { importHubDb } = await import("./runners/hubdb");
  importHubDb(task.id, migration, { dryRun }).catch((err) => runnerErrorHandler(task.id, err));
}
```

- [ ] **Step 3: Verify — run dev server, create a HubDB task and confirm it no longer shows "not yet implemented"**

- [ ] **Step 4: Commit**

```bash
git add src/server/tasks.ts
git commit -m "feat: route HubDB tasks to hubdb runner in orchestrator"
```

---

## Phase 3: Page Migration

### Task 9: Add Page API functions to hubspot.ts

**Files:**
- Modify: `src/server/hubspot.ts`

- [ ] **Step 1: Add Page types**

```ts
// ── Pages ──

export interface HubSpotPage {
  id: string;
  name: string;
  slug: string;
  htmlTitle: string;
  pageExpiryEnabled: boolean;
  state: string;
  publishDate: string;
  created: string;
  updated: string;
  url: string;
  subcategory: string; // "site_page" or "landing_page"
  featuredImage: string;
  featuredImageAltText: string;
  metaDescription: string;
  layoutSections: Record<string, unknown>;
  templatePath: string;
  widgetContainers: Record<string, unknown>;
  widgets: Record<string, unknown>;
  [key: string]: unknown;
}
```

- [ ] **Step 2: Add Page fetch functions**

```ts
export async function fetchAllSitePages(
  token: string
): Promise<HubSpotPage[]> {
  const pages: HubSpotPage[] = [];
  let after: string | undefined;
  do {
    const params = new URLSearchParams({ limit: "100" });
    if (after) params.set("after", after);
    const res = await hubspotFetch(token, `/cms/v3/pages/site-pages?${params}`);
    if (!res.ok) throw new Error(`Failed to fetch site pages: ${res.status}`);
    const data = (await res.json()) as {
      results: HubSpotPage[];
      paging?: { next?: { after: string } };
    };
    pages.push(...data.results);
    after = data.paging?.next?.after;
  } while (after);
  return pages;
}

export async function fetchAllLandingPages(
  token: string
): Promise<HubSpotPage[]> {
  const pages: HubSpotPage[] = [];
  let after: string | undefined;
  do {
    const params = new URLSearchParams({ limit: "100" });
    if (after) params.set("after", after);
    const res = await hubspotFetch(token, `/cms/v3/pages/landing-pages?${params}`);
    if (!res.ok) throw new Error(`Failed to fetch landing pages: ${res.status}`);
    const data = (await res.json()) as {
      results: HubSpotPage[];
      paging?: { next?: { after: string } };
    };
    pages.push(...data.results);
    after = data.paging?.next?.after;
  } while (after);
  return pages;
}

export async function createSitePage(
  token: string,
  page: Record<string, unknown>
): Promise<HubSpotPage> {
  const res = await hubspotFetch(token, "/cms/v3/pages/site-pages", {
    method: "POST",
    body: JSON.stringify(page),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Failed to create site page: ${res.status} ${text}`);
  }
  return res.json() as Promise<HubSpotPage>;
}

export async function createLandingPage(
  token: string,
  page: Record<string, unknown>
): Promise<HubSpotPage> {
  const res = await hubspotFetch(token, "/cms/v3/pages/landing-pages", {
    method: "POST",
    body: JSON.stringify(page),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Failed to create landing page: ${res.status} ${text}`);
  }
  return res.json() as Promise<HubSpotPage>;
}

export async function fetchPageBySlug(
  token: string,
  slug: string,
  subcategory: "site_page" | "landing_page" = "site_page"
): Promise<HubSpotPage | null> {
  const endpoint = subcategory === "landing_page"
    ? "/cms/v3/pages/landing-pages"
    : "/cms/v3/pages/site-pages";
  const res = await hubspotFetch(
    token,
    `${endpoint}?slug=${encodeURIComponent(slug)}&limit=1`
  );
  if (!res.ok) return null;
  const data = (await res.json()) as { results: HubSpotPage[] };
  return data.results[0] || null;
}
```

- [ ] **Step 3: Commit**

```bash
git add src/server/hubspot.ts
git commit -m "feat: add Page API functions (site pages + landing pages)"
```

---

### Task 10: Page export runner

**Files:**
- Create: `src/server/runners/pages.ts`
- Modify: `src/server/csv.ts`

- [ ] **Step 1: Add Page CSV columns**

In `src/server/csv.ts`:

```ts
export const PAGE_CSV_COLUMNS = [
  "id", "name", "slug", "htmlTitle", "state", "subcategory",
  "publishDate", "url", "templatePath", "metaDescription",
];
```

- [ ] **Step 2: Create the Page export runner**

Create `src/server/runners/pages.ts`:

```ts
import { db } from "../../db";
import { tasks } from "../../db/schema";
import type { Migration } from "../../db/schema";
import { eq } from "drizzle-orm";
import {
  createRunnerContext,
  logToTask,
  isTaskPaused,
  getExistingUrlMapping,
} from "./base";
import { readManifest, flushManifest, getDataDir } from "../manifest";
import {
  fetchAllSitePages,
  fetchAllLandingPages,
  createSitePage,
  createLandingPage,
  fetchPageBySlug,
  uploadFile,
  type HubSpotPage,
} from "../hubspot";
import { scanContent } from "../scanners";
import { writeCsvExport, PAGE_CSV_COLUMNS } from "../csv";
import { writeFile, mkdir, readFile } from "fs/promises";
import { resolve } from "path";

// ── Media extraction (shared pattern with blogs) ──

const IMG_SRC_RE = /(?:src|data-src)=["']([^"']+)["']/gi;
const HUBSPOT_CDN_RE = /https?:\/\/[^"'\s]*hubspotusercontent[^"'\s]*/gi;

function extractMediaUrls(page: HubSpotPage): string[] {
  const urls = new Set<string>();
  if (page.featuredImage) urls.add(page.featuredImage);

  // Scan layoutSections and widgets for media URLs
  const jsonStr = JSON.stringify(page.layoutSections || {}) +
    JSON.stringify(page.widgetContainers || {}) +
    JSON.stringify(page.widgets || {});

  let match: RegExpExecArray | null;
  IMG_SRC_RE.lastIndex = 0;
  while ((match = IMG_SRC_RE.exec(jsonStr)) !== null) {
    if (match[1]) urls.add(match[1]);
  }
  HUBSPOT_CDN_RE.lastIndex = 0;
  while ((match = HUBSPOT_CDN_RE.exec(jsonStr)) !== null) {
    urls.add(match[0]);
  }

  return Array.from(urls);
}

function rewriteUrlsInObject(
  obj: unknown,
  mapping: Record<string, string>
): unknown {
  if (typeof obj === "string") {
    let result = obj;
    for (const [oldUrl, newUrl] of Object.entries(mapping)) {
      result = result.split(oldUrl).join(newUrl);
    }
    return result;
  }
  if (Array.isArray(obj)) {
    return obj.map((item) => rewriteUrlsInObject(item, mapping));
  }
  if (obj && typeof obj === "object") {
    const result: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(obj as Record<string, unknown>)) {
      result[key] = rewriteUrlsInObject(value, mapping);
    }
    return result;
  }
  return obj;
}

// ── EXPORT PHASE ──

export async function exportPages(
  taskId: number,
  migration: Migration
): Promise<void> {
  const ctx = await createRunnerContext(taskId, migration);
  if (!ctx) return;

  const { sourceToken, manifestPath, manifest } = ctx;
  const dataDir = getDataDir(migration.id, taskId);
  const mediaDir = resolve(dataDir, "media");
  await mkdir(mediaDir, { recursive: true });

  await db
    .update(tasks)
    .set({ status: "exporting", phase: "export", startedAt: new Date() })
    .where(eq(tasks.id, taskId));

  // Fetch pages (both site pages and landing pages)
  await logToTask(taskId, "info", "Fetching pages from source portal...");
  let pages: HubSpotPage[] = [];
  try {
    const [sitePages, landingPages] = await Promise.all([
      fetchAllSitePages(sourceToken),
      fetchAllLandingPages(sourceToken).catch(() => []),
    ]);
    pages = [
      ...sitePages.map((p) => ({ ...p, subcategory: "site_page" as const })),
      ...landingPages.map((p) => ({ ...p, subcategory: "landing_page" as const })),
    ];
    await logToTask(
      taskId,
      "info",
      `Found ${sitePages.length} site pages and ${landingPages.length} landing pages`
    );
  } catch (err) {
    await logToTask(taskId, "error", `Failed to fetch pages: ${err instanceof Error ? err.message : String(err)}`);
    await db.update(tasks).set({ status: "failed", completedAt: new Date() }).where(eq(tasks.id, taskId));
    return;
  }

  // Apply config filter
  const task = await db.select().from(tasks).where(eq(tasks.id, taskId)).then((r) => r[0]);
  if (task?.config) {
    try {
      const config = JSON.parse(task.config) as { selectedPageIds?: string[] };
      if (config.selectedPageIds?.length) {
        const selectedSet = new Set(config.selectedPageIds);
        pages = pages.filter((p) => selectedSet.has(p.id));
        await logToTask(taskId, "info", `Filtered to ${pages.length} selected pages`);
      }
    } catch { /* use all pages */ }
  }

  // Populate manifest
  const existingIds = new Set(manifest.items.map((i) => i.id));
  for (const page of pages) {
    if (!existingIds.has(page.id)) {
      manifest.items.push({
        id: page.id,
        sourceUrl: page.url || "",
        localPath: null,
        targetUrl: null,
        targetId: null,
        status: "pending",
        error: null,
        size: 0,
        metadata: {
          name: page.name,
          slug: page.slug,
          htmlTitle: page.htmlTitle,
          state: page.state,
          subcategory: page.subcategory,
          templatePath: page.templatePath,
          publishDate: page.publishDate,
          featuredImage: page.featuredImage,
          metaDescription: page.metaDescription,
          mediaUrls: extractMediaUrls(page),
        },
      });
    }
  }

  await db.update(tasks).set({ totalItems: manifest.items.length }).where(eq(tasks.id, taskId));
  flushManifest(manifestPath, manifest);

  // Export each page + its media
  await logToTask(taskId, "info", "Downloading pages and media...");
  let exported = 0;
  let failed = 0;
  let totalBytes = 0;

  for (const item of manifest.items) {
    if (item.status === "exported") {
      exported++;
      totalBytes += item.size;
      continue;
    }

    if (await isTaskPaused(taskId)) {
      await logToTask(taskId, "info", "Export paused");
      flushManifest(manifestPath, manifest);
      return;
    }

    const page = pages.find((p) => p.id === item.id);
    if (!page) {
      item.status = "failed";
      item.error = "Page not found in fetched data";
      failed++;
      continue;
    }

    try {
      // Save full page JSON
      const pagePath = resolve(dataDir, `page-${page.id}.json`);
      const pageJson = JSON.stringify(page, null, 2);
      await writeFile(pagePath, pageJson, "utf-8");
      item.localPath = pagePath;
      item.size = Buffer.byteLength(pageJson);

      // Download media
      const mediaUrls = item.metadata.mediaUrls as string[];
      const downloadResults = new Map<string, boolean>();
      for (const mediaUrl of mediaUrls) {
        try {
          const res = await fetch(mediaUrl);
          if (!res.ok) {
            downloadResults.set(mediaUrl, false);
            continue;
          }
          const buf = Buffer.from(await res.arrayBuffer());
          const urlPath = new URL(mediaUrl).pathname;
          const fileName = urlPath.split("/").pop() || `media-${Date.now()}`;
          await writeFile(resolve(mediaDir, fileName), buf);
          totalBytes += buf.length;
          downloadResults.set(mediaUrl, true);
        } catch {
          downloadResults.set(mediaUrl, false);
        }
      }

      // Scan content for warnings (reuse blog scanners on serialized content)
      const contentStr = JSON.stringify(page.layoutSections || {}) +
        JSON.stringify(page.widgetContainers || {}) +
        JSON.stringify(page.widgets || {});
      if (contentStr.length > 4) {
        const warnings = scanContent(contentStr, page.id, downloadResults);
        for (const w of warnings) {
          manifest.warnings.push(`[${w.type}] Page "${page.name}": ${w.message} — ${w.snippet}`);
        }
      }

      item.status = "exported";
      totalBytes += item.size;
      exported++;

      if (exported % 10 === 0) {
        await db
          .update(tasks)
          .set({ exportedItems: exported, failedItems: failed, localStorageBytes: totalBytes })
          .where(eq(tasks.id, taskId));
        flushManifest(manifestPath, manifest);
      }

      if (exported % 25 === 0) {
        await logToTask(taskId, "info", `Export progress: ${exported}/${manifest.items.length} pages`);
      }
    } catch (err) {
      item.status = "failed";
      item.error = err instanceof Error ? err.message : String(err);
      failed++;
    }
  }

  // CSV export if requested
  if (ctx.outputType === "csv") {
    const csvRecords = manifest.items
      .filter((i) => i.status === "exported")
      .map((i) => ({
        id: i.id,
        name: i.metadata.name,
        slug: i.metadata.slug,
        htmlTitle: i.metadata.htmlTitle,
        state: i.metadata.state,
        subcategory: i.metadata.subcategory,
        publishDate: i.metadata.publishDate,
        url: i.sourceUrl,
        templatePath: i.metadata.templatePath,
        metaDescription: i.metadata.metaDescription,
      }));
    const csvPath = await writeCsvExport(migration.id, taskId, "pages", csvRecords as Record<string, unknown>[], PAGE_CSV_COLUMNS);
    await logToTask(taskId, "info", `CSV export saved: ${csvPath}`);
  }

  manifest.warnings = [...new Set(manifest.warnings)];
  manifest.phase = "exported";
  manifest.exportedAt = new Date().toISOString();
  flushManifest(manifestPath, manifest);

  await db
    .update(tasks)
    .set({
      status: "exported",
      phase: "export",
      exportedItems: exported,
      failedItems: failed,
      localStorageBytes: totalBytes,
      exportedAt: new Date(),
    })
    .where(eq(tasks.id, taskId));

  const warningCount = manifest.warnings.length;
  await logToTask(
    taskId,
    "info",
    `Export completed. ${exported} pages downloaded, ${failed} failed.${warningCount > 0 ? ` ${warningCount} content warnings found.` : ""}`
  );
}
```

- [ ] **Step 3: Commit**

```bash
git add src/server/runners/pages.ts src/server/csv.ts
git commit -m "feat: add Page export runner with media download and content scanning"
```

---

### Task 11: Page import runner

**Files:**
- Modify: `src/server/runners/pages.ts`

- [ ] **Step 1: Add the import function**

Append to `src/server/runners/pages.ts`:

```ts
// ── IMPORT PHASE ──

export async function importPages(
  taskId: number,
  migration: Migration,
  options: { dryRun: boolean } = { dryRun: false }
): Promise<void> {
  const ctx = await createRunnerContext(taskId, migration, options);
  if (!ctx) return;

  const { targetToken, manifestPath, dryRun } = ctx;
  const manifest = readManifest(manifestPath);
  const dataDir = getDataDir(migration.id, taskId);
  const mediaDir = resolve(dataDir, "media");

  await db
    .update(tasks)
    .set({ status: "importing", phase: "import" })
    .where(eq(tasks.id, taskId));

  if (dryRun) {
    await logToTask(taskId, "info", "DRY RUN — no pages will be created in target portal");
  }

  // Upload discovered media
  const urlMapping: Record<string, string> = await getExistingUrlMapping(migration.id);

  if (!dryRun) {
    await logToTask(taskId, "info", "Uploading discovered media...");
    const allMediaUrls = new Set(
      manifest.items.flatMap((i) => (i.metadata.mediaUrls as string[]) || [])
    );
    let mediaUploaded = 0;
    let mediaSkipped = 0;

    for (const mediaUrl of allMediaUrls) {
      if (urlMapping[mediaUrl]) {
        mediaSkipped++;
        continue;
      }
      try {
        const urlPath = new URL(mediaUrl).pathname;
        const fileName = urlPath.split("/").pop() || `media-${Date.now()}`;
        const localPath = resolve(mediaDir, fileName);

        let fileBuffer: Buffer;
        try {
          fileBuffer = Buffer.from(await readFile(localPath));
        } catch {
          continue;
        }

        const uploaded = await uploadFile(targetToken, fileBuffer, fileName);
        urlMapping[mediaUrl] = uploaded.url;
        mediaUploaded++;
      } catch {
        // Non-fatal
      }
    }
    await logToTask(taskId, "info", `Media: ${mediaUploaded} uploaded, ${mediaSkipped} already mapped`);
  }

  // Import pages
  let imported = 0;
  let failed = 0;
  let skipped = 0;
  const exportedItems = manifest.items.filter((i) => i.status === "exported");

  for (const item of exportedItems) {
    if (await isTaskPaused(taskId)) {
      await logToTask(taskId, "info", "Import paused");
      flushManifest(manifestPath, manifest);
      await db
        .update(tasks)
        .set({ importedItems: imported, failedItems: failed, urlMapping: JSON.stringify(urlMapping) })
        .where(eq(tasks.id, taskId));
      return;
    }

    const slug = item.metadata.slug as string;
    const subcategory = (item.metadata.subcategory as string) || "site_page";

    // Idempotency check
    if (!dryRun && slug) {
      const existing = await fetchPageBySlug(
        targetToken,
        slug,
        subcategory as "site_page" | "landing_page"
      );
      if (existing) {
        item.status = "skipped";
        item.targetId = existing.id;
        item.targetUrl = existing.url;
        skipped++;
        continue;
      }
    }

    if (dryRun) {
      await logToTask(taskId, "info", `[DRY RUN] Would create ${subcategory} "${item.metadata.name}" (/${slug})`);
      item.status = "skipped";
      skipped++;
      continue;
    }

    try {
      if (!item.localPath) throw new Error("No local page file");
      const pageJson = await readFile(item.localPath, "utf-8");
      const page = JSON.parse(pageJson) as HubSpotPage;

      // Rewrite media URLs in layoutSections, widgets, widgetContainers
      const rewrittenLayoutSections = rewriteUrlsInObject(page.layoutSections, urlMapping) as Record<string, unknown>;
      const rewrittenWidgets = rewriteUrlsInObject(page.widgets, urlMapping) as Record<string, unknown>;
      const rewrittenWidgetContainers = rewriteUrlsInObject(page.widgetContainers, urlMapping) as Record<string, unknown>;
      const rewrittenFeaturedImage = page.featuredImage
        ? urlMapping[page.featuredImage] || page.featuredImage
        : "";

      const createFn = subcategory === "landing_page" ? createLandingPage : createSitePage;

      const created = await createFn(targetToken, {
        name: page.name,
        slug: page.slug,
        htmlTitle: page.htmlTitle,
        metaDescription: page.metaDescription,
        featuredImage: rewrittenFeaturedImage,
        featuredImageAltText: page.featuredImageAltText,
        templatePath: page.templatePath,
        layoutSections: rewrittenLayoutSections,
        widgets: rewrittenWidgets,
        widgetContainers: rewrittenWidgetContainers,
        publishDate: page.publishDate,
        state: "DRAFT",
      });

      item.status = "imported";
      item.targetId = created.id;
      item.targetUrl = created.url;
      imported++;

      if (imported % 10 === 0) {
        await db
          .update(tasks)
          .set({ importedItems: imported, failedItems: failed, urlMapping: JSON.stringify(urlMapping) })
          .where(eq(tasks.id, taskId));
        flushManifest(manifestPath, manifest);
      }

      if (imported % 25 === 0) {
        await logToTask(taskId, "info", `Import progress: ${imported}/${exportedItems.length} pages`);
      }
    } catch (err) {
      item.status = "failed";
      item.error = err instanceof Error ? err.message : String(err);
      failed++;
    }
  }

  manifest.phase = "completed";
  manifest.importedAt = new Date().toISOString();
  flushManifest(manifestPath, manifest);

  await db
    .update(tasks)
    .set({
      status: "completed",
      completedAt: new Date(),
      importedItems: imported,
      failedItems: failed,
      urlMapping: JSON.stringify(urlMapping),
    })
    .where(eq(tasks.id, taskId));

  await logToTask(
    taskId,
    "info",
    `Import ${dryRun ? "(DRY RUN) " : ""}completed. ${imported} pages created, ${skipped} skipped, ${failed} failed.`
  );
}
```

- [ ] **Step 2: Commit**

```bash
git add src/server/runners/pages.ts
git commit -m "feat: add Page import runner with URL rewriting and idempotency"
```

---

### Task 12: Route Page tasks in orchestrator

**Files:**
- Modify: `src/server/tasks.ts`

- [ ] **Step 1: Add Page dispatch to exportTask**

In the `exportTask` handler, add after the `hubdb` branch:

```ts
} else if (task.type === "page") {
  const { exportPages } = await import("./runners/pages");
  exportPages(task.id, migration).catch((err) => runnerErrorHandler(task.id, err));
}
```

- [ ] **Step 2: Add Page dispatch to importTask**

In the `importTask` handler, add after the `hubdb` branch:

```ts
} else if (task.type === "page") {
  const { importPages } = await import("./runners/pages");
  importPages(task.id, migration, { dryRun }).catch((err) => runnerErrorHandler(task.id, err));
}
```

- [ ] **Step 3: Commit**

```bash
git add src/server/tasks.ts
git commit -m "feat: route Page tasks to pages runner in orchestrator"
```

---

## Phase 4: CSV Import Runner

### Task 13: CSV import runner

**Files:**
- Create: `src/server/runners/csv-import.ts`

- [ ] **Step 1: Create the CSV import runner**

This runner reads a CSV file (saved during task creation), parses it, and on import pushes it to HubDB or re-exports as CSV.

```ts
import { db } from "../../db";
import { tasks } from "../../db/schema";
import type { Migration } from "../../db/schema";
import { eq } from "drizzle-orm";
import {
  createRunnerContext,
  logToTask,
  isTaskPaused,
} from "./base";
import { flushManifest, getDataDir } from "../manifest";
import {
  createHubDbTable,
  createHubDbRowsBatch,
  createHubDbRow,
  publishHubDbTable,
  fetchHubDbTableByName,
} from "../hubspot";
import { writeCsvExport } from "../csv";
import { readFile } from "fs/promises";

function parseCsv(content: string): { headers: string[]; rows: Record<string, string>[] } {
  const lines = content.split("\n").map((l) => l.trim()).filter(Boolean);
  if (lines.length === 0) return { headers: [], rows: [] };

  // Simple CSV parser — handles quoted fields with commas
  function parseLine(line: string): string[] {
    const fields: string[] = [];
    let current = "";
    let inQuotes = false;

    for (let i = 0; i < line.length; i++) {
      const char = line[i]!;
      if (char === '"') {
        if (inQuotes && line[i + 1] === '"') {
          current += '"';
          i++;
        } else {
          inQuotes = !inQuotes;
        }
      } else if (char === "," && !inQuotes) {
        fields.push(current);
        current = "";
      } else {
        current += char;
      }
    }
    fields.push(current);
    return fields;
  }

  const headers = parseLine(lines[0]!);
  const rows = lines.slice(1).map((line) => {
    const values = parseLine(line);
    const row: Record<string, string> = {};
    for (let i = 0; i < headers.length; i++) {
      row[headers[i]!] = values[i] || "";
    }
    return row;
  });

  return { headers, rows };
}

function inferColumnType(values: string[]): string {
  const nonEmpty = values.filter(Boolean);
  if (nonEmpty.length === 0) return "TEXT";

  // Check if all values are numbers
  if (nonEmpty.every((v) => !isNaN(Number(v)) && v !== "")) return "NUMBER";

  // Check if all values are booleans
  if (nonEmpty.every((v) => ["true", "false", "yes", "no", "1", "0"].includes(v.toLowerCase()))) return "BOOLEAN";

  // Check if all values look like URLs
  if (nonEmpty.every((v) => /^https?:\/\//i.test(v))) return "URL";

  // Check if all values look like dates (ISO 8601)
  if (nonEmpty.every((v) => !isNaN(Date.parse(v)) && /\d{4}/.test(v))) return "DATE";

  return "TEXT";
}

// ── EXPORT PHASE (parse + validate) ──

export async function exportCsvImport(
  taskId: number,
  migration: Migration
): Promise<void> {
  const ctx = await createRunnerContext(taskId, migration);
  if (!ctx) return;

  const { manifestPath, manifest } = ctx;
  const dataDir = getDataDir(migration.id, taskId);

  await db
    .update(tasks)
    .set({ status: "exporting", phase: "export", startedAt: new Date() })
    .where(eq(tasks.id, taskId));

  // Read CSV file path from config
  const task = await db.select().from(tasks).where(eq(tasks.id, taskId)).then((r) => r[0]);
  let csvFilePath: string | null = null;
  let csvFileName: string | null = null;
  if (task?.config) {
    try {
      const config = JSON.parse(task.config) as { csvFilePath?: string; csvFileName?: string };
      csvFilePath = config.csvFilePath || null;
      csvFileName = config.csvFileName || null;
    } catch { /* */ }
  }

  if (!csvFilePath) {
    await logToTask(taskId, "error", "No CSV file path found in task config");
    await db.update(tasks).set({ status: "failed", completedAt: new Date() }).where(eq(tasks.id, taskId));
    return;
  }

  // Parse CSV
  await logToTask(taskId, "info", `Parsing CSV file: ${csvFileName || csvFilePath}`);
  let csvContent: string;
  try {
    csvContent = await readFile(csvFilePath, "utf-8");
  } catch (err) {
    await logToTask(taskId, "error", `Failed to read CSV file: ${err instanceof Error ? err.message : String(err)}`);
    await db.update(tasks).set({ status: "failed", completedAt: new Date() }).where(eq(tasks.id, taskId));
    return;
  }

  const { headers, rows } = parseCsv(csvContent);

  if (headers.length === 0 || rows.length === 0) {
    await logToTask(taskId, "error", "CSV file is empty or has no data rows");
    await db.update(tasks).set({ status: "failed", completedAt: new Date() }).where(eq(tasks.id, taskId));
    return;
  }

  await logToTask(taskId, "info", `Parsed ${rows.length} rows with ${headers.length} columns: ${headers.join(", ")}`);

  // Infer column types
  const columnTypes: Record<string, string> = {};
  for (const header of headers) {
    const values = rows.map((r) => r[header] || "");
    columnTypes[header] = inferColumnType(values);
  }
  await logToTask(
    taskId,
    "info",
    `Column types: ${headers.map((h) => `${h} (${columnTypes[h]})`).join(", ")}`
  );

  // Create manifest items (one per row)
  const existingIds = new Set(manifest.items.map((i) => i.id));
  for (let i = 0; i < rows.length; i++) {
    const rowId = `row-${i}`;
    if (!existingIds.has(rowId)) {
      manifest.items.push({
        id: rowId,
        sourceUrl: "",
        localPath: csvFilePath,
        targetUrl: null,
        targetId: null,
        status: "exported", // Already "exported" since data is local
        error: null,
        size: 0,
        metadata: {
          name: `Row ${i + 1}`,
          rowIndex: i,
          values: rows[i],
        },
      });
    }
  }

  // Store parsed schema in config for import phase
  let config: Record<string, unknown> = {};
  if (task?.config) {
    try { config = JSON.parse(task.config); } catch { /* */ }
  }
  config.csvHeaders = headers;
  config.csvColumnTypes = columnTypes;
  config.csvRowCount = rows.length;

  await db
    .update(tasks)
    .set({
      config: JSON.stringify(config),
      totalItems: rows.length,
      exportedItems: rows.length,
      localStorageBytes: Buffer.byteLength(csvContent),
    })
    .where(eq(tasks.id, taskId));

  manifest.phase = "exported";
  manifest.exportedAt = new Date().toISOString();
  flushManifest(manifestPath, manifest);

  await db
    .update(tasks)
    .set({
      status: "exported",
      phase: "export",
      exportedAt: new Date(),
    })
    .where(eq(tasks.id, taskId));

  await logToTask(taskId, "info", `CSV parsed successfully. ${rows.length} rows ready for import.`);
}

// ── IMPORT PHASE ──

export async function importCsvImport(
  taskId: number,
  migration: Migration,
  options: { dryRun: boolean } = { dryRun: false }
): Promise<void> {
  const ctx = await createRunnerContext(taskId, migration, options);
  if (!ctx) return;

  const { targetToken, manifestPath, dryRun, outputType } = ctx;
  const manifest = readManifest(manifestPath);

  await db
    .update(tasks)
    .set({ status: "importing", phase: "import" })
    .where(eq(tasks.id, taskId));

  // Load parsed CSV data from config
  const task = await db.select().from(tasks).where(eq(tasks.id, taskId)).then((r) => r[0]);
  if (!task?.config) {
    await logToTask(taskId, "error", "No CSV config found — run export first");
    await db.update(tasks).set({ status: "failed" }).where(eq(tasks.id, taskId));
    return;
  }

  const config = JSON.parse(task.config) as {
    csvFilePath: string;
    csvFileName: string;
    csvHeaders: string[];
    csvColumnTypes: Record<string, string>;
    csvRowCount: number;
  };

  // Re-read and parse the CSV (manifest items have row references)
  const csvContent = await readFile(config.csvFilePath, "utf-8");
  const { rows } = parseCsv(csvContent);

  if (outputType === "csv") {
    // Re-export as CSV (pass-through)
    if (dryRun) {
      await logToTask(taskId, "info", `[DRY RUN] Would re-export ${rows.length} rows as CSV`);
    } else {
      const csvPath = await writeCsvExport(
        migration.id,
        taskId,
        "csv_import",
        rows as unknown as Record<string, unknown>[],
        config.csvHeaders
      );
      await logToTask(taskId, "info", `CSV re-exported to: ${csvPath}`);
    }

    // Mark all items as imported
    for (const item of manifest.items) {
      if (item.status === "exported") item.status = "imported";
    }
    manifest.phase = "completed";
    manifest.importedAt = new Date().toISOString();
    flushManifest(manifestPath, manifest);

    await db
      .update(tasks)
      .set({
        status: "completed",
        completedAt: new Date(),
        importedItems: rows.length,
      })
      .where(eq(tasks.id, taskId));

    await logToTask(taskId, "info", "CSV export completed.");
    return;
  }

  // Output type is HubDB — create table and insert rows
  if (dryRun) {
    await logToTask(taskId, "info", `[DRY RUN] Would create HubDB table with ${config.csvHeaders.length} columns and ${rows.length} rows`);
    await logToTask(taskId, "info", `Columns: ${config.csvHeaders.map((h) => `${h} (${config.csvColumnTypes[h]})`).join(", ")}`);

    for (const item of manifest.items) {
      if (item.status === "exported") item.status = "skipped";
    }
    manifest.phase = "completed";
    flushManifest(manifestPath, manifest);

    await db.update(tasks).set({ status: "completed", completedAt: new Date() }).where(eq(tasks.id, taskId));
    await logToTask(taskId, "info", "Dry run completed.");
    return;
  }

  // Create HubDB table
  const tableName = (config.csvFileName || "csv_import")
    .replace(/\.csv$/i, "")
    .replace(/[^a-zA-Z0-9_]/g, "_")
    .toLowerCase();

  // Idempotency: check if table exists
  const existingTable = await fetchHubDbTableByName(targetToken, tableName);
  if (existingTable) {
    await logToTask(taskId, "info", `HubDB table "${tableName}" already exists (ID: ${existingTable.id}), skipping creation`);
    for (const item of manifest.items) {
      if (item.status === "exported") item.status = "skipped";
    }
    manifest.phase = "completed";
    flushManifest(manifestPath, manifest);

    await db.update(tasks).set({ status: "completed", completedAt: new Date() }).where(eq(tasks.id, taskId));
    return;
  }

  const columns = config.csvHeaders.map((header) => ({
    name: header.replace(/[^a-zA-Z0-9_]/g, "_").toLowerCase(),
    label: header,
    type: config.csvColumnTypes[header] || "TEXT",
  }));

  await logToTask(taskId, "info", `Creating HubDB table "${tableName}" with ${columns.length} columns...`);

  let createdTable;
  try {
    createdTable = await createHubDbTable(targetToken, {
      name: tableName,
      label: config.csvFileName?.replace(/\.csv$/i, "") || tableName,
      columns,
    });
    await logToTask(taskId, "info", `Table created (ID: ${createdTable.id}), inserting rows...`);
  } catch (err) {
    await logToTask(taskId, "error", `Failed to create HubDB table: ${err instanceof Error ? err.message : String(err)}`);
    await db.update(tasks).set({ status: "failed", completedAt: new Date() }).where(eq(tasks.id, taskId));
    return;
  }

  // Build column name mapping
  const colNameToId: Record<string, string> = {};
  for (const col of createdTable.columns) {
    colNameToId[col.name] = String(col.id);
  }

  // Insert rows in batches
  const BATCH_SIZE = 100;
  let imported = 0;
  let failed = 0;

  for (let i = 0; i < rows.length; i += BATCH_SIZE) {
    if (await isTaskPaused(taskId)) {
      await logToTask(taskId, "info", "Import paused");
      flushManifest(manifestPath, manifest);
      await db.update(tasks).set({ importedItems: imported, failedItems: failed }).where(eq(tasks.id, taskId));
      return;
    }

    const batch = rows.slice(i, i + BATCH_SIZE);
    const mappedRows = batch.map((row) => {
      const values: Record<string, unknown> = {};
      for (const header of config.csvHeaders) {
        const colName = header.replace(/[^a-zA-Z0-9_]/g, "_").toLowerCase();
        const colId = colNameToId[colName];
        if (!colId) continue;

        const rawValue = row[header] || "";
        const colType = config.csvColumnTypes[header] || "TEXT";

        // Type coercion
        if (colType === "NUMBER") {
          values[colId] = rawValue ? Number(rawValue) : null;
        } else if (colType === "BOOLEAN") {
          values[colId] = ["true", "yes", "1"].includes(rawValue.toLowerCase());
        } else {
          values[colId] = rawValue;
        }
      }
      return { values };
    });

    try {
      await createHubDbRowsBatch(targetToken, createdTable.id, mappedRows);
      // Mark manifest items as imported
      for (let j = i; j < i + batch.length; j++) {
        const item = manifest.items.find((it) => it.id === `row-${j}`);
        if (item) item.status = "imported";
      }
      imported += batch.length;
    } catch {
      // Fall back to individual inserts
      for (let j = 0; j < mappedRows.length; j++) {
        try {
          await createHubDbRow(targetToken, createdTable.id, mappedRows[j]!);
          const item = manifest.items.find((it) => it.id === `row-${i + j}`);
          if (item) item.status = "imported";
          imported++;
        } catch {
          const item = manifest.items.find((it) => it.id === `row-${i + j}`);
          if (item) {
            item.status = "failed";
            item.error = "Failed to insert row";
          }
          failed++;
        }
      }
    }

    if (imported % 100 === 0) {
      await db
        .update(tasks)
        .set({ importedItems: imported, failedItems: failed })
        .where(eq(tasks.id, taskId));
      flushManifest(manifestPath, manifest);
      await logToTask(taskId, "info", `Insert progress: ${imported}/${rows.length} rows`);
    }
  }

  // Publish the table
  try {
    await publishHubDbTable(targetToken, createdTable.id);
    await logToTask(taskId, "info", `Published HubDB table "${tableName}"`);
  } catch (err) {
    await logToTask(taskId, "warn", `Table created but publish failed: ${err instanceof Error ? err.message : String(err)}`);
  }

  manifest.phase = "completed";
  manifest.importedAt = new Date().toISOString();
  flushManifest(manifestPath, manifest);

  await db
    .update(tasks)
    .set({
      status: "completed",
      completedAt: new Date(),
      importedItems: imported,
      failedItems: failed,
    })
    .where(eq(tasks.id, taskId));

  await logToTask(
    taskId,
    "info",
    `Import completed. HubDB table "${tableName}" created with ${imported} rows. ${failed} failed.`
  );
}
```

- [ ] **Step 2: Commit**

```bash
git add src/server/runners/csv-import.ts
git commit -m "feat: add CSV import runner with HubDB table creation and type inference"
```

---

### Task 14: Route CSV import tasks in orchestrator

**Files:**
- Modify: `src/server/tasks.ts`

- [ ] **Step 1: Add csv_import dispatch to exportTask**

In the `exportTask` handler, add after the `page` branch:

```ts
} else if (task.type === "csv_import") {
  const { exportCsvImport } = await import("./runners/csv-import");
  exportCsvImport(task.id, migration).catch((err) => runnerErrorHandler(task.id, err));
}
```

- [ ] **Step 2: Add csv_import dispatch to importTask**

In the `importTask` handler, add after the `page` branch:

```ts
} else if (task.type === "csv_import") {
  const { importCsvImport } = await import("./runners/csv-import");
  importCsvImport(task.id, migration, { dryRun }).catch((err) => runnerErrorHandler(task.id, err));
}
```

- [ ] **Step 3: Remove the catch-all "not yet implemented" fallback**

The else branch at the end of both `exportTask` and `importTask` that marks unknown types as failed can now remain as a safety net for truly unknown types, but all 5 types are now routed.

- [ ] **Step 4: Verify — run dev server, create a CSV import task, confirm the export phase parses the CSV successfully**

- [ ] **Step 5: Commit**

```bash
git add src/server/tasks.ts
git commit -m "feat: route csv_import, hubdb, and page tasks to their runners"
```

---

## Summary

| Phase | Tasks | New Files | Feature |
|-------|-------|-----------|---------|
| 1 | Tasks 1-4 | `TagMappingModal.tsx` | Tag re-mapping for blog post review step |
| 2 | Tasks 5-8 | `runners/hubdb.ts` | HubDB table migration (export + import) |
| 3 | Tasks 9-12 | `runners/pages.ts` | Site page + landing page migration |
| 4 | Tasks 13-14 | `runners/csv-import.ts` | CSV file → HubDB import |

All runners follow the established patterns:
- Two-phase (export → import) with manifest-driven progress tracking
- Dry run support on all import phases
- Pause/resume at item boundaries
- Idempotency checks before writing to target
- CSV export as alternative output type
- Progress updates every 10 items + log every 25 items
