import { createFileRoute, Link } from "@tanstack/react-router";
import { getMigrations } from "../server/migrations";
import { MigrationCard } from "../components/MigrationCard";
import { Button } from "../components/ui/button";
import { Plus } from "lucide-react";

export const Route = createFileRoute("/")({
  loader: () => getMigrations(),
  component: Dashboard,
});

function Dashboard() {
  const migrations = Route.useLoaderData();

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
