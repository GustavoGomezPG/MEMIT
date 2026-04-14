import { useState, useEffect, useMemo } from "react";
import {
  X,
  Search,
  Tag,
  Trash2,
  Edit3,
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
  const [postTagOverrides, setPostTagOverrides] = useState<Record<string, string[]>>({});

  useEffect(() => {
    if (!open) return;
    setLoading(true);
    getTagData({ data: taskId })
      .then((result) => {
        setTags(result.tags);
        setPosts(result.posts);
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
    setEditName(current?.action === "rename" ? (current as { name: string }).name : tag.name);
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
      .filter((v, i, a) => a.indexOf(v) === i);
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
                                  → merges into &ldquo;{mergeTarget.name}&rdquo;
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
