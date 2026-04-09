import { useState, useEffect, useRef } from "react";
import { useVirtualizer } from "@tanstack/react-virtual";
import { getManifestItems, getManifestSummary } from "../server/tasks";
import { X, AlertTriangle, Loader2 } from "lucide-react";

interface ManifestBrowserProps {
  open: boolean;
  onClose: () => void;
  taskId: number;
}

type ManifestItemRow = {
  id: string;
  sourceUrl: string;
  localPath: string | null;
  targetUrl: string | null;
  status: string;
  error: string | null;
  size: number;
  name: string;
};

function fmtBytes(bytes: number): string {
  if (!bytes) return "0 B";
  const k = 1024;
  const sizes = ["B", "KB", "MB", "GB"];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return `${(bytes / Math.pow(k, i)).toFixed(1)} ${sizes[i]}`;
}

const statusColors: Record<string, string> = {
  pending: "text-muted-foreground",
  exported: "text-accent-foreground",
  imported: "text-green-600 dark:text-green-400",
  failed: "text-destructive",
  skipped: "text-muted-foreground",
};

export function ManifestBrowser({
  open,
  onClose,
  taskId,
}: ManifestBrowserProps) {
  const [allItems, setAllItems] = useState<ManifestItemRow[]>([]);
  const [statusFilter, setStatusFilter] = useState("all");
  const [warnings, setWarnings] = useState<string[]>([]);
  const [summary, setSummary] = useState<{
    total: number;
    exported: number;
    imported: number;
    failed: number;
    skipped: number;
  } | null>(null);
  const [loading, setLoading] = useState(false);

  const parentRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!open) return;
    loadAll();
  }, [open, taskId]);

  async function loadAll() {
    setLoading(true);
    try {
      const [summaryResult, itemsResult] = await Promise.all([
        getManifestSummary({ data: taskId }),
        getManifestItems({ data: { taskId, offset: 0, limit: 100000, statusFilter: "all" } }),
      ]);
      if (summaryResult) {
        setSummary(summaryResult.summary);
        setWarnings(summaryResult.warnings || []);
      }
      setAllItems(itemsResult.items);
    } finally {
      setLoading(false);
    }
  }

  const filteredItems =
    statusFilter === "all"
      ? allItems
      : allItems.filter((i) => i.status === statusFilter);

  const virtualizer = useVirtualizer({
    count: filteredItems.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => 64,
    overscan: 20,
  });

  if (!open) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-foreground/40 backdrop-blur-sm">
      <div className="glass-modal shadow-ambient flex max-h-[90vh] w-full max-w-4xl flex-col overflow-hidden rounded-2xl">
        {/* Header */}
        <div className="flex items-center justify-between bg-[var(--surface-high)] px-8 py-6">
          <div>
            <h2 className="text-2xl font-extrabold tracking-tight">
              Browse Exported Data
            </h2>
            {summary && (
              <p className="mt-1 text-sm text-muted-foreground">
                {summary.exported} exported &middot; {summary.imported} imported &middot;{" "}
                {summary.failed} failed &middot; {summary.skipped} skipped
              </p>
            )}
          </div>
          <button
            type="button"
            onClick={onClose}
            className="flex h-10 w-10 items-center justify-center rounded-full transition-colors hover:bg-white/50 dark:hover:bg-white/10"
          >
            <X className="h-5 w-5" />
          </button>
        </div>

        {/* Warnings */}
        {warnings.length > 0 && (
          <div className="bg-[var(--primary-fixed)] px-8 py-3">
            <div className="flex items-start gap-2">
              <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0 text-primary" />
              <div className="space-y-1 text-xs">
                <p className="font-semibold">
                  {warnings.length} content warnings
                </p>
                {warnings.slice(0, 5).map((w, i) => (
                  <p key={i} className="text-muted-foreground">
                    {w}
                  </p>
                ))}
                {warnings.length > 5 && (
                  <p className="text-muted-foreground">
                    ...and {warnings.length - 5} more
                  </p>
                )}
              </div>
            </div>
          </div>
        )}

        {/* Filters */}
        <div className="flex items-center gap-3 px-8 py-4">
          <select
            value={statusFilter}
            onChange={(e) => setStatusFilter(e.target.value)}
            className="rounded-lg bg-[var(--surface-low)] px-3 py-2 text-sm outline-none"
          >
            <option value="all">All statuses</option>
            <option value="exported">Exported</option>
            <option value="imported">Imported</option>
            <option value="failed">Failed</option>
            <option value="skipped">Skipped</option>
            <option value="pending">Pending</option>
          </select>
          <span className="text-xs text-muted-foreground">
            {filteredItems.length} items
          </span>
        </div>

        {/* Virtual list */}
        {loading ? (
          <div className="flex flex-1 items-center justify-center py-16">
            <Loader2 className="h-6 w-6 animate-spin text-primary" />
          </div>
        ) : (
          <div
            ref={parentRef}
            className="flex-1 overflow-y-auto px-8 pb-6"
          >
            <div
              style={{
                height: `${virtualizer.getTotalSize()}px`,
                width: "100%",
                position: "relative",
              }}
            >
              {virtualizer.getVirtualItems().map((virtualRow) => {
                const item = filteredItems[virtualRow.index]!;
                return (
                  <div
                    key={item.id}
                    style={{
                      position: "absolute",
                      top: 0,
                      left: 0,
                      width: "100%",
                      height: `${virtualRow.size}px`,
                      transform: `translateY(${virtualRow.start}px)`,
                    }}
                  >
                    <div className="flex items-center gap-3 rounded-lg bg-card px-4 py-2.5">
                      <div className="min-w-0 flex-1">
                        <p className="truncate text-sm font-medium">
                          {item.name}
                        </p>
                        <p className="truncate text-xs text-muted-foreground">
                          {item.sourceUrl}
                        </p>
                        {item.error && (
                          <p className="mt-0.5 truncate text-xs text-destructive">
                            {item.error}
                          </p>
                        )}
                      </div>
                      <span className="shrink-0 text-xs tabular-nums text-muted-foreground">
                        {fmtBytes(item.size)}
                      </span>
                      <span
                        className={`w-20 shrink-0 text-right text-xs font-semibold uppercase ${statusColors[item.status] || ""}`}
                      >
                        {item.status}
                      </span>
                    </div>
                  </div>
                );
              })}
            </div>
            {filteredItems.length === 0 && (
              <p className="py-8 text-center text-sm text-muted-foreground">
                No items match this filter.
              </p>
            )}
          </div>
        )}
      </div>
    </div>
  );
}
