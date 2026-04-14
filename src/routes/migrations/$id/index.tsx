import { createFileRoute, Link } from "@tanstack/react-router";
import { useState, useEffect, useCallback } from "react";
import {
  getMigration,
  swapMigrationDirection,
} from "../../../server/migrations";
import {
  createTask,
  deleteTask,
  exportTask,
  importTask,
  pauseTask,
  getManifestSummary,
  runTemplateExtraction,
  retryFailedMediaDownloads,
} from "../../../server/tasks";
import { Badge } from "../../../components/ui/badge";
import { Button } from "../../../components/ui/button";
import { TaskCard } from "../../../components/TaskCard";
import { CreateTaskModal } from "../../../components/CreateTaskModal";
import { ManifestBrowser } from "../../../components/ManifestBrowser";
import { CsvDataBrowser } from "../../../components/CsvDataBrowser";
import { WarningsPanel } from "../../../components/WarningsPanel";
import { CtaMappingModal } from "../../../components/CtaMappingModal";
import { TagMappingModal } from "../../../components/TagMappingModal";
import { ArrowLeft, ArrowLeftRight, Plus } from "lucide-react";
import type { Task } from "../../../db/schema";

export const Route = createFileRoute("/migrations/$id/")({
  loader: ({ params }) => getMigration({ data: Number(params.id) }),
  component: MigrationDetail,
});

const statusVariant: Record<string, "default" | "secondary" | "outline"> = {
  draft: "secondary",
  active: "default",
  completed: "outline",
};

function MigrationDetail() {
  const loaderData = Route.useLoaderData();
  const [migration, setMigration] = useState(loaderData);
  const [modalOpen, setModalOpen] = useState(false);
  const [browseTaskId, setBrowseTaskId] = useState<number | null>(null);
  const [csvBrowseTaskId, setCsvBrowseTaskId] = useState<number | null>(null);
  const [warningsTaskId, setWarningsTaskId] = useState<number | null>(null);
  const [ctaMapTaskId, setCtaMapTaskId] = useState<number | null>(null);
  const [tagMapTaskId, setTagMapTaskId] = useState<number | null>(null);
  const [taskWarnings, setTaskWarnings] = useState<Record<number, string[]>>(
    {}
  );

  useEffect(() => {
    setMigration(loaderData);
  }, [loaderData]);

  const hasActiveTask = migration.tasks.some(
    (t: Task) =>
      t.status === "exporting" || t.status === "importing"
  );

  const refreshTasks = useCallback(async () => {
    try {
      const updated = await getMigration({ data: migration.id });
      setMigration(updated);

      // Fetch warnings for exported tasks
      const allWarnings: Record<number, string[]> = {};
      for (const task of updated.tasks) {
        if (task.manifestPath) {
          try {
            const summary = await getManifestSummary({ data: task.id });
            if (summary && summary.warnings.length > 0) {
              allWarnings[task.id] = summary.warnings;
            }
          } catch {
            /* ignore */
          }
        }
      }
      setTaskWarnings(allWarnings);
    } catch {
      /* ignore */
    }
  }, [migration.id]);

  useEffect(() => {
    if (!hasActiveTask) return;
    const interval = setInterval(refreshTasks, 2000);
    return () => clearInterval(interval);
  }, [hasActiveTask, refreshTasks]);

  // Load warning counts on mount
  useEffect(() => {
    refreshTasks();
  }, []);

  async function handleSwapDirection() {
    await swapMigrationDirection({ data: migration.id });
    await refreshTasks();
  }

  async function handleCreateTask(task: {
    type: string;
    label: string;
    outputType: string;
    config?: string;
    csvFileContent?: string;
    csvFileName?: string;
  }) {
    await createTask({
      data: {
        migrationId: migration.id,
        type: task.type as Task["type"],
        label: task.label,
        outputType: task.outputType as "same_as_source" | "hubdb" | "csv",
        config: task.config,
        csvFileContent: task.csvFileContent,
        csvFileName: task.csvFileName,
      },
    });
    setModalOpen(false);
    await refreshTasks();
  }

  async function handleExportTask(taskId: number) {
    await exportTask({ data: { taskId } });
    setTimeout(refreshTasks, 500);
  }

  async function handleImportTask(taskId: number, dryRun: boolean) {
    await importTask({ data: { taskId, dryRun } });
    setTimeout(refreshTasks, 500);
  }

  async function handlePauseTask(taskId: number) {
    await pauseTask({ data: taskId });
    setTimeout(refreshTasks, 500);
  }

  async function handleDeleteTask(taskId: number) {
    await deleteTask({ data: taskId });
    await refreshTasks();
  }

  async function handleRetryMedia(taskId: number) {
    await retryFailedMediaDownloads({ data: taskId });
    await refreshTasks();
  }

  return (
    <div className="space-y-10">
      <div className="flex items-center gap-2 text-sm text-muted-foreground">
        <Link to="/" className="hover:text-foreground">
          <ArrowLeft className="inline h-3.5 w-3.5" /> Migrations
        </Link>
      </div>

      <div className="flex items-start justify-between">
        <div className="space-y-1">
          <h1 className="text-3xl font-bold tracking-tight">
            {migration.name}
          </h1>
          <div className="flex items-center gap-2 text-sm text-muted-foreground">
            <span>
              {migration.sourceKey?.name || "—"}
              <span className="ml-1 font-mono">
                {migration.sourceKey?.portalId || ""}
              </span>
            </span>
            <button
              type="button"
              onClick={handleSwapDirection}
              className="inline-flex items-center justify-center rounded-full p-1.5 text-muted-foreground transition-colors hover:bg-[var(--surface-low)] hover:text-foreground"
              title="Swap source and target"
            >
              <ArrowLeftRight className="h-3.5 w-3.5" />
            </button>
            <span>
              {migration.targetKey?.name || "—"}
              <span className="ml-1 font-mono">
                {migration.targetKey?.portalId || ""}
              </span>
            </span>
            <Badge
              variant={statusVariant[migration.status] || "secondary"}
              className="ml-2"
            >
              {migration.status}
            </Badge>
          </div>
        </div>

        <Button onClick={() => setModalOpen(true)}>
          <Plus className="mr-1.5 h-4 w-4" />
          Add Task
        </Button>
      </div>

      {migration.tasks.length === 0 ? (
        <div className="flex flex-col items-center justify-center rounded-lg bg-[var(--surface-low)] py-16">
          <p className="text-sm text-muted-foreground">
            No tasks yet. Add a task to begin migrating content.
          </p>
        </div>
      ) : (
        <div className="space-y-3">
          {migration.tasks.map((task: Task) => (
            <TaskCard
              key={task.id}
              task={task}
              onExport={handleExportTask}
              onImport={handleImportTask}
              onPause={handlePauseTask}
              onDelete={handleDeleteTask}
              onBrowse={(id) => {
                const t = migration.tasks.find((t: Task) => t.id === id);
                if (t?.type === "csv_import") {
                  setCsvBrowseTaskId(id);
                } else {
                  setBrowseTaskId(id);
                }
              }}
              onTagMapping={(id) => setTagMapTaskId(id)}
              onRetryMedia={handleRetryMedia}
              warningCount={taskWarnings[task.id]?.length}
              onWarningsClick={() => setWarningsTaskId(task.id)}
              isRunning={hasActiveTask}
            />
          ))}
        </div>
      )}

      <CreateTaskModal
        open={modalOpen}
        onClose={() => setModalOpen(false)}
        onCreate={handleCreateTask}
        migrationId={migration.id}
        sourceKey={migration.sourceKey}
        targetKey={migration.targetKey}
        onSwap={handleSwapDirection}
      />

      {browseTaskId && (
        <ManifestBrowser
          open={!!browseTaskId}
          onClose={() => setBrowseTaskId(null)}
          taskId={browseTaskId}
        />
      )}

      {csvBrowseTaskId && (
        <CsvDataBrowser
          open={!!csvBrowseTaskId}
          onClose={() => setCsvBrowseTaskId(null)}
          taskId={csvBrowseTaskId}
        />
      )}

      {ctaMapTaskId && (
        <CtaMappingModal
          open={!!ctaMapTaskId}
          onClose={() => setCtaMapTaskId(null)}
          taskId={ctaMapTaskId}
        />
      )}

      {tagMapTaskId && (
        <TagMappingModal
          open={!!tagMapTaskId}
          onClose={() => setTagMapTaskId(null)}
          taskId={tagMapTaskId}
        />
      )}

      {warningsTaskId && (
        <WarningsPanel
          open={!!warningsTaskId}
          onClose={() => setWarningsTaskId(null)}
          warnings={taskWarnings[warningsTaskId] || []}
          onMapCtas={() => {
            setCtaMapTaskId(warningsTaskId);
            setWarningsTaskId(null);
          }}
          onExtractTemplates={async () => {
            await runTemplateExtraction({ data: warningsTaskId });
            // Refresh after a delay to let the extraction run
            setTimeout(refreshTasks, 2000);
          }}
        />
      )}
    </div>
  );
}
