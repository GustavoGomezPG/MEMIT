import { Card, CardContent, CardHeader, CardTitle } from "./ui/card";
import { Badge } from "./ui/badge";
import { Button } from "./ui/button";
import { Progress } from "./ui/progress";
import {
  Image,
  FileText,
  Database,
  Layout,
  FileSpreadsheet,
  Tag,
  Play,
  Pause,
  RotateCcw,
  Upload,
  Download,
  Eye,
  FolderOpen,
  ChevronDown,
  ChevronUp,
  AlertTriangle,
  HardDrive,
  CheckCircle,
  Trash2,
} from "lucide-react";
import { useState, useRef, useEffect } from "react";
import type { Task } from "../db/schema";
import type { LogEntry } from "../server/tasks";

const typeIcons: Record<string, React.ElementType> = {
  media: Image,
  blog_posts: FileText,
  hubdb: Database,
  page: Layout,
  csv_import: FileSpreadsheet,
};

const statusConfig: Record<
  string,
  { label: string; variant: "default" | "secondary" | "outline" | "destructive" }
> = {
  pending: { label: "Ready", variant: "secondary" },
  exporting: { label: "Exporting", variant: "default" },
  export_paused: { label: "Export Paused", variant: "outline" },
  exported: { label: "Exported", variant: "outline" },
  importing: { label: "Importing", variant: "default" },
  import_paused: { label: "Import Paused", variant: "outline" },
  completed: { label: "Completed", variant: "outline" },
  failed: { label: "Failed", variant: "destructive" },
};

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

function fmtBytes(bytes: number): string {
  if (!bytes) return "0 B";
  const k = 1024;
  const sizes = ["B", "KB", "MB", "GB"];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return `${(bytes / Math.pow(k, i)).toFixed(1)} ${sizes[i]}`;
}

// Phase step indicator
function PhaseSteps({ task }: { task: Task }) {
  const steps = [
    { key: "export", label: "Export", description: "Download from source" },
    { key: "review", label: "Review", description: "Inspect local data" },
    { key: "import", label: "Import", description: "Upload to target" },
  ];

  let activeStep = 0;
  if (
    task.status === "exporting" ||
    task.status === "export_paused"
  ) {
    activeStep = 0;
  } else if (task.status === "exported") {
    activeStep = 1;
  } else if (
    task.status === "importing" ||
    task.status === "import_paused"
  ) {
    activeStep = 2;
  } else if (task.status === "completed") {
    activeStep = 3;
  } else if (task.status === "failed") {
    activeStep = task.phase === "import" ? 2 : 0;
  }

  return (
    <div className="flex items-center gap-1">
      {steps.map((step, i) => {
        const isDone = i < activeStep;
        const isCurrent = i === activeStep && task.status !== "completed";
        return (
          <div key={step.key} className="flex items-center gap-1">
            {i > 0 && (
              <div
                className={`h-px w-6 ${isDone ? "bg-accent-foreground" : "bg-border"}`}
              />
            )}
            <div className="flex items-center gap-1.5">
              <div
                className={`flex h-5 w-5 items-center justify-center rounded-full text-[10px] font-bold ${
                  isDone
                    ? "bg-accent-foreground text-white"
                    : isCurrent
                      ? "bg-primary text-primary-foreground"
                      : "bg-[var(--surface-low)] text-muted-foreground"
                }`}
              >
                {isDone ? (
                  <CheckCircle className="h-3 w-3" />
                ) : (
                  i + 1
                )}
              </div>
              <div>
                <span
                  className={`text-xs font-semibold ${
                    isCurrent ? "text-foreground" : "text-muted-foreground"
                  }`}
                >
                  {step.label}
                </span>
              </div>
            </div>
          </div>
        );
      })}
    </div>
  );
}

// Status-specific explanation
function StatusDescription({ task }: { task: Task }) {
  const descriptions: Record<string, string> = {
    pending:
      "Start by exporting data from the source portal. Files will be downloaded to your local machine for review before importing.",
    exporting:
      "Downloading data from the source portal to local storage. You can pause at any time.",
    export_paused:
      "Export is paused. Resume to continue downloading remaining items.",
    exported:
      task.failedItems && task.failedItems > 0
        ? `Export finished with ${task.failedItems} failed item${task.failedItems > 1 ? "s" : ""}. You can retry the failed files or proceed to import.`
        : "All data has been downloaded locally. Review the files, then import to the target portal or run a dry run first.",
    importing:
      "Uploading data to the target portal. Duplicate items are skipped automatically.",
    import_paused:
      "Import is paused. Resume to continue uploading remaining items to the target.",
    completed:
      "Migration complete. All items have been exported and imported successfully.",
    failed:
      task.phase === "import"
        ? "Import failed. Your exported data is safe locally. You can retry the import."
        : "Export failed. Check the logs for details and retry.",
  };

  return (
    <p className="text-xs text-muted-foreground">
      {descriptions[task.status] || ""}
    </p>
  );
}

export function TaskCard({
  task,
  onExport,
  onImport,
  onPause,
  onDelete,
  onBrowse,
  onTagMapping,
  warningCount,
  onWarningsClick,
  isRunning,
}: TaskCardProps) {
  const [logOpen, setLogOpen] = useState(false);
  const [confirmDelete, setConfirmDelete] = useState(false);
  const Icon = typeIcons[task.type] || FileText;
  const status = statusConfig[task.status] || statusConfig.pending!;

  const exportProgress =
    task.totalItems && task.totalItems > 0
      ? Math.round(((task.exportedItems || 0) / task.totalItems) * 100)
      : 0;
  const importProgress =
    task.totalItems && task.totalItems > 0
      ? Math.round(((task.importedItems || 0) / task.totalItems) * 100)
      : 0;

  const logEntries: LogEntry[] = task.log ? JSON.parse(task.log) : [];

  const showExportBar = [
    "exporting",
    "export_paused",
    "exported",
    "importing",
    "import_paused",
    "completed",
  ].includes(task.status);

  const showImportBar = [
    "importing",
    "import_paused",
    "completed",
  ].includes(task.status);

  return (
    <Card className="overflow-hidden">
      <CardHeader className="pb-2">
        <div className="flex items-start justify-between">
          <div className="flex items-center gap-2.5">
            <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-[var(--surface-low)]">
              <Icon className="h-4 w-4 text-accent-foreground" />
            </div>
            <div>
              <CardTitle className="text-sm">{task.label}</CardTitle>
              <span className="text-[10px] uppercase tracking-wider text-muted-foreground">
                {task.outputType === "csv"
                  ? "CSV Export"
                  : task.outputType === "hubdb"
                    ? "HubDB Output"
                    : "Direct Migration"}
              </span>
            </div>
          </div>
          <div className="flex items-center gap-2">
            {warningCount && warningCount > 0 ? (
              <button type="button" onClick={onWarningsClick}>
                <Badge
                  variant="outline"
                  className="cursor-pointer text-yellow-600 hover:bg-yellow-100 dark:text-yellow-400 dark:hover:bg-yellow-900/30"
                >
                  <AlertTriangle className="mr-1 h-3 w-3" />
                  {warningCount} warnings
                </Badge>
              </button>
            ) : null}
            <Badge variant={status.variant}>{status.label}</Badge>
            {task.status !== "exporting" && task.status !== "importing" && (
              confirmDelete ? (
                <div className="flex items-center gap-1">
                  <button
                    type="button"
                    onClick={() => onDelete(task.id)}
                    className="rounded px-2 py-1 text-[10px] font-bold text-destructive transition-colors hover:bg-destructive hover:text-destructive-foreground"
                  >
                    Confirm
                  </button>
                  <button
                    type="button"
                    onClick={() => setConfirmDelete(false)}
                    className="rounded px-2 py-1 text-[10px] font-bold text-muted-foreground transition-colors hover:text-foreground"
                  >
                    Cancel
                  </button>
                </div>
              ) : (
                <button
                  type="button"
                  onClick={() => setConfirmDelete(true)}
                  className="flex h-6 w-6 items-center justify-center rounded text-muted-foreground transition-colors hover:bg-destructive/10 hover:text-destructive"
                  title="Delete task"
                >
                  <Trash2 className="h-3.5 w-3.5" />
                </button>
              )
            )}
          </div>
        </div>
      </CardHeader>
      <CardContent className="space-y-4">
        {/* Phase stepper */}
        <PhaseSteps task={task} />

        {/* Status description */}
        <StatusDescription task={task} />

        {/* Progress bars */}
        {showExportBar && (
          <div className="rounded-lg bg-[var(--surface-low)] p-3">
            <div className="mb-1.5 flex items-center justify-between text-xs">
              <span className="flex items-center gap-1.5 font-semibold">
                <Download className="h-3.5 w-3.5 text-accent-foreground" />
                Phase 1 — Export
              </span>
              <span className="text-muted-foreground">
                {task.exportedItems || 0} of {task.totalItems || 0} items
                {task.localStorageBytes ? (
                  <span className="ml-2">
                    <HardDrive className="mr-0.5 inline h-3 w-3" />
                    {fmtBytes(task.localStorageBytes)}
                  </span>
                ) : null}
              </span>
            </div>
            <Progress value={exportProgress} />
            <p className="mt-1 text-[10px] text-muted-foreground">
              Downloading files from source portal to local storage
            </p>
          </div>
        )}

        {showImportBar && (
          <div className="rounded-lg bg-[var(--surface-low)] p-3">
            <div className="mb-1.5 flex items-center justify-between text-xs">
              <span className="flex items-center gap-1.5 font-semibold">
                <Upload className="h-3.5 w-3.5 text-accent-foreground" />
                Phase 2 — Import
              </span>
              <span className="text-muted-foreground">
                {task.importedItems || 0} of {task.totalItems || 0} items
                {task.failedItems ? (
                  <span className="ml-2 text-destructive">
                    {task.failedItems} failed
                  </span>
                ) : null}
              </span>
            </div>
            <Progress value={importProgress} />
            <p className="mt-1 text-[10px] text-muted-foreground">
              Uploading files to target portal (duplicates are skipped
              automatically)
            </p>
          </div>
        )}

        {/* Action buttons */}
        <div className="flex flex-wrap gap-2">
          {task.status === "pending" && (
            <Button size="sm" onClick={() => onExport(task.id)} disabled={isRunning} className="signature-gradient border-0 text-white">
              <Download className="mr-1 h-3.5 w-3.5" /> Start Export
            </Button>
          )}

          {(task.status === "exporting" || task.status === "importing") && (
            <Button size="sm" variant="outline" onClick={() => onPause(task.id)}>
              <Pause className="mr-1 h-3.5 w-3.5" /> Pause
            </Button>
          )}

          {task.status === "export_paused" && (
            <Button size="sm" onClick={() => onExport(task.id)} disabled={isRunning}>
              <Play className="mr-1 h-3.5 w-3.5" /> Resume
            </Button>
          )}

          {task.status === "exported" && (
            <>
              <Button size="sm" variant="outline" onClick={() => onExport(task.id)} disabled={isRunning}>
                <RotateCcw className="mr-1 h-3.5 w-3.5" />
                {task.failedItems && task.failedItems > 0
                  ? `Retry ${task.failedItems} Failed`
                  : "Re-export"}
              </Button>
              <Button size="sm" onClick={() => onImport(task.id, false)} disabled={isRunning} className="signature-gradient border-0 text-white">
                <Upload className="mr-1 h-3.5 w-3.5" /> Start Import
              </Button>
              <Button size="sm" variant="outline" onClick={() => onImport(task.id, true)} disabled={isRunning}>
                <Eye className="mr-1 h-3.5 w-3.5" /> Dry Run
              </Button>
              {onBrowse && (
                <Button size="sm" variant="outline" onClick={() => onBrowse(task.id)}>
                  <FolderOpen className="mr-1 h-3.5 w-3.5" /> Browse
                </Button>
              )}
              {task.type === "blog_posts" && onTagMapping && (
                <Button size="sm" variant="outline" onClick={() => onTagMapping(task.id)}>
                  <Tag className="mr-1 h-3.5 w-3.5" /> Map Tags
                </Button>
              )}
            </>
          )}

          {task.status === "import_paused" && (
            <Button size="sm" onClick={() => onImport(task.id, false)} disabled={isRunning}>
              <Play className="mr-1 h-3.5 w-3.5" /> Resume
            </Button>
          )}

          {task.status === "completed" && task.localStorageBytes ? (
            <div className="flex items-center gap-1.5 text-xs text-muted-foreground">
              <HardDrive className="h-3.5 w-3.5" />
              {fmtBytes(task.localStorageBytes)} stored locally
            </div>
          ) : null}

          {task.status === "failed" && (
            <Button size="sm" variant="outline" onClick={() => { task.phase === "import" ? onImport(task.id, false) : onExport(task.id); }} disabled={isRunning}>
              <RotateCcw className="mr-1 h-3.5 w-3.5" /> Retry
            </Button>
          )}
        </div>

        {/* Log viewer */}
        {logEntries.length > 0 && (
          <div>
            <button
              type="button"
              onClick={() => setLogOpen(!logOpen)}
              className="flex w-full items-center justify-between text-xs text-muted-foreground hover:text-foreground"
            >
              <span>Activity Log ({logEntries.length} entries)</span>
              {logOpen ? (
                <ChevronUp className="h-3 w-3" />
              ) : (
                <ChevronDown className="h-3 w-3" />
              )}
            </button>
            {logOpen && (
              <LogScroller entries={logEntries} />
            )}
          </div>
        )}
      </CardContent>
    </Card>
  );
}

function LogScroller({ entries }: { entries: LogEntry[] }) {
  const endRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    endRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [entries.length]);

  return (
    <div className="mt-2 h-40 overflow-y-auto rounded-lg bg-[var(--surface-low)] p-3">
      <div className="space-y-1 font-mono text-xs">
        {entries.map((entry, i) => (
          <div key={i} className="flex gap-2">
            <span className="shrink-0 text-muted-foreground">
              {new Date(entry.timestamp).toLocaleTimeString()}
            </span>
            <span
              className={
                entry.level === "error"
                  ? "text-destructive"
                  : entry.level === "warn"
                    ? "text-yellow-600 dark:text-yellow-400"
                    : "text-foreground"
              }
            >
              [{entry.level}] {entry.message}
            </span>
          </div>
        ))}
        <div ref={endRef} />
      </div>
    </div>
  );
}
