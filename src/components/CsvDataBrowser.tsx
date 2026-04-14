import { useState, useEffect, useMemo } from "react";
import {
  useReactTable,
  getCoreRowModel,
  getFilteredRowModel,
  getSortedRowModel,
  flexRender,
  createColumnHelper,
  type SortingState,
  type ColumnDef,
} from "@tanstack/react-table";
import { X, Search, Loader2, ExternalLink, ArrowUpDown, ArrowUp, ArrowDown } from "lucide-react";
import { Markup } from "interweave";
import { getCsvPreviewData } from "../server/tasks";

interface CsvDataBrowserProps {
  open: boolean;
  onClose: () => void;
  taskId: number;
}

type CsvRow = Record<string, string>;

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

function SmartCell({ value, colType }: { value: string; colType: string }) {
  if (!value) {
    return <span className="text-xs text-muted-foreground/50">—</span>;
  }

  const cellType = detectCellType(value, colType);

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
            className="min-w-0 truncate text-[11px] text-primary hover:underline"
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
          className="flex items-center gap-1 text-[11px] text-primary hover:underline"
        >
          <ExternalLink className="h-3 w-3 shrink-0" />
          <span className="min-w-0 truncate">{value}</span>
        </a>
      );

    case "RICHTEXT":
      return (
        <div className="prose prose-sm dark:prose-invert max-w-none break-words">
          <Markup
            content={value}
            blockList={["script", "style", "iframe", "object", "embed"]}
            tagName="div"
          />
        </div>
      );

    case "NUMBER":
      return (
        <span className="block text-right text-xs tabular-nums">
          {value}
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

    default:
      return (
        <span className="whitespace-pre-wrap break-words text-xs">
          {value}
        </span>
      );
  }
}

function getColumnSize(colType: string, header: string, sampleValues: string[]): number {
  if (colType === "NUMBER" || colType === "BOOLEAN") return 110;
  if (colType === "IMAGE") return 230;
  if (colType === "URL") return 230;
  const hasHtml = sampleValues.some((v) => HTML_TAG_RE.test(v));
  if (hasHtml) return 320;
  const avgLen = sampleValues.reduce((sum, v) => sum + v.length, 0) / (sampleValues.length || 1);
  if (avgLen > 100) return 280;
  if (avgLen > 40) return 200;
  if (header.length > 15) return 180;
  return 150;
}

export function CsvDataBrowser({ open, onClose, taskId }: CsvDataBrowserProps) {
  const [loading, setLoading] = useState(true);
  const [csvHeaders, setCsvHeaders] = useState<string[]>([]);
  const [columnTypes, setColumnTypes] = useState<Record<string, string>>({});
  const [data, setData] = useState<CsvRow[]>([]);
  const [globalFilter, setGlobalFilter] = useState("");
  const [sorting, setSorting] = useState<SortingState>([]);

  useEffect(() => {
    if (!open) return;
    setLoading(true);
    setGlobalFilter("");
    setSorting([]);
    getCsvPreviewData({ data: taskId })
      .then((result) => {
        setCsvHeaders(result.headers);
        setColumnTypes(result.columnTypes);
        setData(result.rows);
      })
      .finally(() => setLoading(false));
  }, [open, taskId]);

  const columns = useMemo<ColumnDef<CsvRow, string>[]>(() => {
    const columnHelper = createColumnHelper<CsvRow>();

    // Row number column
    const rowNumCol = columnHelper.display({
      id: "_rowNum",
      header: "#",
      cell: (info) => (
        <span className="text-[10px] tabular-nums text-muted-foreground">
          {info.row.index + 1}
        </span>
      ),
      size: 50,
      enableSorting: false,
      enableGlobalFilter: false,
    });

    // Data columns from CSV headers
    const dataCols = csvHeaders.map((header) => {
      const colType = columnTypes[header] || "TEXT";
      const samples = data.slice(0, 50).map((r) => r[header] || "");
      const size = getColumnSize(colType, header, samples);
      const badge = TYPE_BADGES[colType] || TYPE_BADGES.TEXT!;

      return columnHelper.accessor((row) => row[header] || "", {
        id: header,
        header: () => (
          <div className="flex flex-col gap-1">
            <span className="truncate text-xs font-bold">{header}</span>
            <span
              className={`inline-block w-fit rounded px-1.5 py-0.5 text-[9px] font-bold ${badge.color}`}
            >
              {badge.label}
            </span>
          </div>
        ),
        cell: (info) => (
          <SmartCell value={info.getValue()} colType={colType} />
        ),
        size,
        sortingFn: colType === "NUMBER" ? "alphanumeric" : "text",
      });
    });

    return [rowNumCol, ...dataCols] as ColumnDef<CsvRow, string>[];
  }, [csvHeaders, columnTypes, data]);

  const table = useReactTable({
    data,
    columns,
    state: { globalFilter, sorting },
    onGlobalFilterChange: setGlobalFilter,
    onSortingChange: setSorting,
    getCoreRowModel: getCoreRowModel(),
    getFilteredRowModel: getFilteredRowModel(),
    getSortedRowModel: getSortedRowModel(),
    globalFilterFn: "includesString",
  });

  if (!open) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-foreground/40 backdrop-blur-sm">
      <div className="glass-modal shadow-ambient flex max-h-[90vh] w-full max-w-[95vw] flex-col overflow-hidden rounded-2xl">
        {/* Header */}
        <div className="flex items-center justify-between bg-[var(--surface-high)] px-8 py-6">
          <div>
            <h2 className="text-2xl font-extrabold tracking-tight">
              Browse CSV Data
            </h2>
            <p className="mt-1 text-sm text-muted-foreground">
              {table.getFilteredRowModel().rows.length} of {data.length} rows
              &middot; {csvHeaders.length} columns
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
              value={globalFilter}
              onChange={(e) => setGlobalFilter(e.target.value)}
              className="w-full rounded-lg bg-[var(--surface-low)] py-2.5 pl-10 pr-4 text-sm outline-none placeholder:text-muted-foreground focus:ring-2 focus:ring-primary/30"
            />
          </div>
        </div>

        {/* Table */}
        {loading ? (
          <div className="flex flex-1 items-center justify-center py-16">
            <Loader2 className="h-8 w-8 animate-spin text-primary" />
          </div>
        ) : (
          <div className="csv-table-scroll flex-1 px-4 pb-4">
            <table className="w-max min-w-full border-collapse">
              <thead className="sticky top-0 z-10">
                {table.getHeaderGroups().map((headerGroup) => (
                  <tr key={headerGroup.id} className="bg-[var(--surface-high)]">
                    {headerGroup.headers.map((header) => (
                      <th
                        key={header.id}
                        className="border-b border-border px-3 py-2 text-left align-bottom"
                        style={{ width: header.getSize(), minWidth: header.getSize() }}
                      >
                        {header.isPlaceholder ? null : (
                          <div
                            className={`flex items-end gap-1.5 ${
                              header.column.getCanSort()
                                ? "cursor-pointer select-none"
                                : ""
                            }`}
                            onClick={header.column.getToggleSortingHandler()}
                          >
                            {flexRender(
                              header.column.columnDef.header,
                              header.getContext()
                            )}
                            {header.column.getCanSort() && (
                              <span className="mb-0.5 shrink-0 text-muted-foreground">
                                {header.column.getIsSorted() === "asc" ? (
                                  <ArrowUp className="h-3 w-3" />
                                ) : header.column.getIsSorted() === "desc" ? (
                                  <ArrowDown className="h-3 w-3" />
                                ) : (
                                  <ArrowUpDown className="h-3 w-3 opacity-40" />
                                )}
                              </span>
                            )}
                          </div>
                        )}
                      </th>
                    ))}
                  </tr>
                ))}
              </thead>
              <tbody>
                {table.getRowModel().rows.map((row, rowIdx) => (
                  <tr
                    key={row.id}
                    className={
                      rowIdx % 2 === 0
                        ? "bg-card"
                        : "bg-[var(--surface-low)]"
                    }
                  >
                    {row.getVisibleCells().map((cell) => (
                      <td
                        key={cell.id}
                        className="border-b border-border/30 p-0 align-top"
                        style={{ width: cell.column.getSize(), minWidth: cell.column.getSize() }}
                      >
                        <div className="max-h-[140px] overflow-y-auto px-3 py-2">
                          {flexRender(
                            cell.column.columnDef.cell,
                            cell.getContext()
                          )}
                        </div>
                      </td>
                    ))}
                  </tr>
                ))}
                {table.getRowModel().rows.length === 0 && (
                  <tr>
                    <td
                      colSpan={columns.length}
                      className="py-8 text-center text-sm text-muted-foreground"
                    >
                      {globalFilter
                        ? "No rows match your search."
                        : "No data."}
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}
