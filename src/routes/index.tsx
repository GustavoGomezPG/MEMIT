import { createFileRoute, Link } from "@tanstack/react-router";
import { useState, useEffect } from "react";
import { getMigrations } from "../server/migrations";
import { getOrphanedStorage, cleanupOrphanedStorage } from "../server/tasks";
import { MigrationCard } from "../components/MigrationCard";
import { Button } from "../components/ui/button";
import { Plus, Trash2, HardDrive, Loader2 } from "lucide-react";

export const Route = createFileRoute("/")({
  loader: () => getMigrations(),
  component: Dashboard,
});

function fmtBytes(bytes: number): string {
  if (bytes === 0) return "0 B";
  const k = 1024;
  const sizes = ["B", "KB", "MB", "GB"];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return `${(bytes / Math.pow(k, i)).toFixed(1)} ${sizes[i]}`;
}

function Dashboard() {
  const migrations = Route.useLoaderData();
  const [orphanCount, setOrphanCount] = useState(0);
  const [orphanBytes, setOrphanBytes] = useState(0);
  const [cleaning, setCleaning] = useState(false);
  const [cleaned, setCleaned] = useState(false);

  useEffect(() => {
    getOrphanedStorage().then((result) => {
      setOrphanCount(result.orphaned.length);
      setOrphanBytes(result.totalBytes);
    });
  }, []);

  async function handleCleanup() {
    setCleaning(true);
    try {
      await cleanupOrphanedStorage();
      setOrphanCount(0);
      setOrphanBytes(0);
      setCleaned(true);
    } finally {
      setCleaning(false);
    }
  }

  return (
    <div className="space-y-10">
      <div className="flex items-end justify-between">
        <div>
          <h1 className="text-3xl font-bold tracking-tight">Migrations</h1>
          <p className="mt-1 text-sm text-muted-foreground">
            Manage content migrations between HubSpot portals
          </p>
        </div>
        <Link to="/migrations/new">
          <Button>
            <Plus className="mr-1.5 h-4 w-4" />
            New Migration
          </Button>
        </Link>
      </div>

      {orphanCount > 0 && !cleaned && (
        <div className="flex items-center justify-between rounded-xl bg-[var(--surface-low)] px-5 py-4">
          <div className="flex items-center gap-3">
            <HardDrive className="h-5 w-5 text-muted-foreground" />
            <div>
              <p className="text-sm font-semibold">
                {orphanCount} orphaned folder{orphanCount !== 1 ? "s" : ""} found
              </p>
              <p className="text-xs text-muted-foreground">
                {fmtBytes(orphanBytes)} from deleted tasks still on disk
              </p>
            </div>
          </div>
          <Button
            size="sm"
            variant="outline"
            onClick={handleCleanup}
            disabled={cleaning}
          >
            {cleaning ? (
              <Loader2 className="mr-1.5 h-3.5 w-3.5 animate-spin" />
            ) : (
              <Trash2 className="mr-1.5 h-3.5 w-3.5" />
            )}
            {cleaning ? "Cleaning..." : "Clean Up"}
          </Button>
        </div>
      )}

      {migrations.length === 0 ? (
        <div className="flex flex-col items-center justify-center rounded-lg bg-[var(--surface-low)] py-16">
          <p className="text-sm text-muted-foreground">
            No migration projects yet
          </p>
          <Link to="/migrations/new" className="mt-2">
            <Button variant="outline" size="sm">
              <Plus className="mr-1.5 h-3.5 w-3.5" />
              Create your first migration
            </Button>
          </Link>
        </div>
      ) : (
        <div className="grid gap-4 sm:grid-cols-2">
          {migrations.map((m) => (
            <MigrationCard
              key={m.id}
              id={m.id}
              name={m.name}
              sourceKeyName={m.sourceKeyName}
              sourcePortalId={m.sourcePortalId}
              targetKeyName={m.targetKeyName}
              targetPortalId={m.targetPortalId}
              status={m.status}
              taskCount={m.taskCount}
              completedTaskCount={m.completedTaskCount}
            />
          ))}
        </div>
      )}
    </div>
  );
}
