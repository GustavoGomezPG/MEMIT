import { useState, useEffect, useRef, useMemo } from "react";
import { useVirtualizer } from "@tanstack/react-virtual";
import { X, Search, Loader2, ExternalLink } from "lucide-react";
import { getCsvPreviewData } from "../server/tasks";

interface CsvDataBrowserProps {
  open: boolean;
  onClose: () => void;
  taskId: number;
}

const TYPE_BADGES: Record<string, { label: string; color: string }> = {
  TEXT: { label: "TEXT", color: "bg-secondary text-secondary-foreground" },
  NUMBER: { label: "NUM", color: "bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-300" },
  URL: { label: "URL", color: "bg-purple-100 text-purple-700 dark:bg-purple-900/30 dark:text-purple-300" },
  IMAGE: { label: "IMG", color: "bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-300" },
  BOOLEAN: { label: "BOOL", color: "bg-amber-100 text-amber-700 dark:bg-amber-900/30 dark:text-amber-300" },
  DATE: { label: "DATE", color: "bg-cyan-100 text-cyan-700 dark:bg-cyan-900/30 dark:text-cyan-300" },
  RICHTEXT: { label: "HTML", color: "bg-rose-100 text-rose-700 dark:bg-rose-900/30 dark:text-rose-300" },
};

const IMG_EXT_RE = /\.(png|jpe?g|gif|webp|svg|ico|bmp)(\?.*)?$/i;
const HTML_TAG_RE = /<[a-z][\s\S]*>/i;

function detectCellType(value: string, colType: string): string {
  if (!value) return colType;
  if (colType === "URL" || colType === "IMAGE") {
    if (IMG_EXT_RE.test(value)) return "IMAGE";
    return "URL";
  }
  if (HTML_TAG_RE.test(value)) return "RICHTEXT";
  return colType;
}

function getColumnWidth(colType: string, header: string, sampleValues: string[]): number {
  if (colType === "NUMBER" || colType === "BOOLEAN") return 100;
  if (colType === "IMAGE") return 220;
  if (colType === "URL") return 220;
  // Check if any values contain HTML
  const hasHtml = sampleValues.some((v) => HTML_TAG_RE.test(v));
  if (hasHtml) return 320;
  // Check average length
  const avgLen = sampleValues.reduce((sum, v) => sum + v.length, 0) / (sampleValues.length || 1);
  if (avgLen > 100) return 280;
  if (avgLen > 40) return 200;
  if (header.length > 15) return 180;
  return 150;
}

function CellRenderer({ value, cellType }: { value: string; cellType: string }) {
  const [expanded, setExpanded] = useState(false);

  if (!value) {
    return <span className="text-xs text-muted-foreground/50">—</span>;
  }

  switch (cellType) {
    case "IMAGE":
      return (
        <div className="flex items-center gap-2">
          <img
            src={value}
            alt=""
            className="h-10 w-10 shrink-0 rounded border border-border object-cover"
            onError={(e) => {
              (e.target as HTMLImageElement).style.display = "none";
            }}
          />
          <a
            href={value}
            target="_blank"
            rel="noopener noreferrer"
            className="truncate text-[11px] text-primary hover:underline"
          >
            {value.split("/").pop()?.split("?")[0] || value}
          </a>
        </div>
      );

    case "URL":
      return (
        <a
          href={value}
          target="_blank"
          rel="noopener noreferrer"
          className="flex items-center gap-1 truncate text-[11px] text-primary hover:underline"
        >
          <ExternalLink className="h-3 w-3 shrink-0" />
          <span className="truncate">{value}</span>
        </a>
      );

    case "RICHTEXT":
      return (
        <div className="relative">
          <div
            className={`prose prose-xs dark:prose-invert max-w-none text-[11px] leading-relaxed ${
              expanded ? "" : "max-h-20 overflow-hidden"
            }`}
            dangerouslySetInnerHTML={{ __html: value }}
          />
          {!expanded && value.length > 200 && (
            <button
              type="button"
              onClick={() => setExpanded(true)}
              className="absolute bottom-0 left-0 right-0 bg-gradient-to-t from-card to-transparent pt-4 text-center text-[10px] font-bold text-primary"
            >
              Show more
            </button>
          )}
          {expanded && (
            <button
              type="button"
              onClick={() => setExpanded(false)}
              className="mt-1 text-[10px] font-bold text-primary"
            >
              Show less
            </button>
          )}
        </div>
      );

    case "NUMBER":
      return (
        <span className="text-right text-xs tabular-nums">
          {isNaN(Number(value)) ? value : Number(value).toLocaleString()}
        </span>
      );

    case "BOOLEAN":
      return (
        <span
          className={`inline-block rounded px-1.5 py-0.5 text-[10px] font-bold ${
            ["true", "yes", "1"].includes(value.toLowerCase())
              ? "bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-300"
              : "bg-secondary text-secondary-foreground"
          }`}
        >
          {value}
        </span>
      );

    default: {
      const truncated = value.length > 120;
      return (
        <div>
          <span className="text-xs">
            {expanded ? value : truncated ? value.slice(0, 120) + "..." : value}
          </span>
          {truncated && (
            <button
              type="button"
              onClick={() => setExpanded(!expanded)}
              className="ml-1 text-[10px] font-bold text-primary"
            >
              {expanded ? "less" : "more"}
            </button>
          )}
        </div>
      );
    }
  }
}

export function CsvDataBrowser({ open, onClose, taskId }: CsvDataBrowserProps) {
  const [loading, setLoading] = useState(true);
  const [headers, setHeaders] = useState<string[]>([]);
  const [columnTypes, setColumnTypes] = useState<Record<string, string>>({});
  const [allRows, setAllRows] = useState<Record<string, string>[]>([]);
  const [searchQuery, setSearchQuery] = useState("");
  const parentRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!open) return;
    setLoading(true);
    getCsvPreviewData({ data: taskId })
      .then((result) => {
        setHeaders(result.headers);
        setColumnTypes(result.columnTypes);
        setAllRows(result.rows);
      })
      .finally(() => setLoading(false));
  }, [open, taskId]);

  const filteredRows = useMemo(() => {
    if (!searchQuery) return allRows;
    const q = searchQuery.toLowerCase();
    return allRows.filter((row) =>
      Object.values(row).some((v) => v.toLowerCase().includes(q))
    );
  }, [allRows, searchQuery]);

  const columnWidths = useMemo(() => {
    const widths: Record<string, number> = {};
    for (const header of headers) {
      const colType = columnTypes[header] || "TEXT";
      const samples = allRows.slice(0, 50).map((r) => r[header] || "");
      widths[header] = getColumnWidth(colType, header, samples);
    }
    return widths;
  }, [headers, columnTypes, allRows]);

  const totalWidth = useMemo(
    () => 50 + headers.reduce((sum, h) => sum + (columnWidths[h] || 150), 0),
    [headers, columnWidths]
  );

  const virtualizer = useVirtualizer({
    count: filteredRows.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => 52,
    overscan: 15,
  });

  if (!open) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-foreground/40 backdrop-blur-sm">
      <div className="glass-modal shadow-ambient flex max-h-[90vh] w-full max-w-6xl flex-col overflow-hidden rounded-2xl">
        {/* Header */}
        <div className="flex items-center justify-between bg-[var(--surface-high)] px-8 py-6">
          <div>
            <h2 className="text-2xl font-extrabold tracking-tight">
              Browse CSV Data
            </h2>
            <p className="mt-1 text-sm text-muted-foreground">
              {allRows.length} rows &middot; {headers.length} columns
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

        {/* Search */}
        <div className="px-8 py-3">
          <div className="relative">
            <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
            <input
              type="text"
              placeholder="Search across all columns..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="w-full rounded-lg bg-[var(--surface-low)] py-2.5 pl-10 pr-4 text-sm outline-none placeholder:text-muted-foreground focus:ring-2 focus:ring-primary/30"
            />
          </div>
          {searchQuery && (
            <p className="mt-1 text-xs text-muted-foreground">
              {filteredRows.length} of {allRows.length} rows match
            </p>
          )}
        </div>

        {/* Table */}
        {loading ? (
          <div className="flex flex-1 items-center justify-center py-16">
            <Loader2 className="h-8 w-8 animate-spin text-primary" />
          </div>
        ) : (
          <div className="flex-1 overflow-x-auto px-4 pb-4">
            {/* Sticky column headers */}
            <div
              className="sticky top-0 z-10 flex border-b border-border bg-[var(--surface-high)]"
              style={{ width: `${totalWidth}px`, minWidth: "100%" }}
            >
              {/* Row number column */}
              <div className="flex w-[50px] shrink-0 items-center justify-center px-2 py-3 text-[10px] font-bold text-muted-foreground">
                #
              </div>
              {headers.map((header) => {
                const colType = columnTypes[header] || "TEXT";
                const badge = TYPE_BADGES[colType] || TYPE_BADGES.TEXT!;
                return (
                  <div
                    key={header}
                    className="flex shrink-0 flex-col gap-1 border-l border-border/50 px-3 py-2"
                    style={{ width: `${columnWidths[header] || 150}px` }}
                  >
                    <span className="truncate text-xs font-bold">{header}</span>
                    <span
                      className={`inline-block w-fit rounded px-1.5 py-0.5 text-[9px] font-bold ${badge.color}`}
                    >
                      {badge.label}
                    </span>
                  </div>
                );
              })}
            </div>

            {/* Virtual rows */}
            <div
              ref={parentRef}
              className="max-h-[60vh] overflow-y-auto"
            >
              <div
                style={{
                  height: `${virtualizer.getTotalSize()}px`,
                  width: `${totalWidth}px`,
                  minWidth: "100%",
                  position: "relative",
                }}
              >
                {virtualizer.getVirtualItems().map((virtualRow) => {
                  const row = filteredRows[virtualRow.index]!;
                  const rowIdx = virtualRow.index;
                  return (
                    <div
                      key={virtualRow.key}
                      className={`absolute left-0 top-0 flex border-b border-border/30 ${
                        rowIdx % 2 === 0 ? "bg-card" : "bg-[var(--surface-low)]"
                      }`}
                      style={{
                        width: `${totalWidth}px`,
                        minWidth: "100%",
                        minHeight: `${virtualRow.size}px`,
                        transform: `translateY(${virtualRow.start}px)`,
                      }}
                    >
                      {/* Row number */}
                      <div className="flex w-[50px] shrink-0 items-start justify-center px-2 pt-3 text-[10px] tabular-nums text-muted-foreground">
                        {rowIdx + 1}
                      </div>
                      {headers.map((header) => {
                        const value = row[header] || "";
                        const colType = columnTypes[header] || "TEXT";
                        const cellType = detectCellType(value, colType);
                        return (
                          <div
                            key={header}
                            className="shrink-0 border-l border-border/30 px-3 py-2"
                            style={{ width: `${columnWidths[header] || 150}px` }}
                          >
                            <CellRenderer value={value} cellType={cellType} />
                          </div>
                        );
                      })}
                    </div>
                  );
                })}
              </div>
              {filteredRows.length === 0 && (
                <p className="py-8 text-center text-sm text-muted-foreground">
                  {searchQuery ? "No rows match your search." : "No data."}
                </p>
              )}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
