import { Link } from "@tanstack/react-router";
import { Badge } from "./ui/badge";
import { ArrowRight } from "lucide-react";

interface MigrationCardProps {
  id: number;
  name: string;
  sourceKeyName: string;
  sourcePortalId: string | null;
  targetKeyName: string;
  targetPortalId: string | null;
  status: string;
  taskCount: number;
  completedTaskCount: number;
}

const statusVariant: Record<string, "default" | "secondary" | "outline"> = {
  draft: "secondary",
  active: "default",
  completed: "outline",
};

export function MigrationCard({
  id,
  name,
  sourceKeyName,
  sourcePortalId,
  targetKeyName,
  targetPortalId,
  status,
  taskCount,
  completedTaskCount,
}: MigrationCardProps) {
  return (
    <Link
      to="/migrations/$id"
      params={{ id: String(id) }}
      className="group block no-underline"
    >
      <div className="overflow-hidden rounded-lg bg-card transition-all hover:shadow-[0_12px_32px_rgba(0,29,53,0.06)]">
        {/* Split Surface: header band */}
        <div className="bg-[var(--surface-low)] px-5 py-3">
          <div className="flex items-start justify-between">
            <h3 className="text-sm font-semibold text-foreground">{name}</h3>
            <Badge variant={statusVariant[status] || "secondary"}>
              {status}
            </Badge>
          </div>
        </div>
        {/* Split Surface: body */}
        <div className="px-5 py-4">
          <div className="flex items-center gap-1.5 text-xs text-muted-foreground">
            <span>
              {sourceKeyName}
              {sourcePortalId && (
                <span className="ml-1 font-mono">{sourcePortalId}</span>
              )}
            </span>
            <ArrowRight className="h-3 w-3 text-accent-foreground" />
            <span>
              {targetKeyName}
              {targetPortalId && (
                <span className="ml-1 font-mono">{targetPortalId}</span>
              )}
            </span>
          </div>
          <p className="mt-2 text-sm text-muted-foreground">
            {taskCount === 0
              ? "No tasks yet"
              : `${completedTaskCount} of ${taskCount} tasks complete`}
          </p>
        </div>
      </div>
    </Link>
  );
}
