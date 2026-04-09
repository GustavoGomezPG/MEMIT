import { useState } from "react";
import {
  X,
  AlertTriangle,
  Code,
  FormInput,
  MousePointerClick,
  ImageOff,
  ChevronDown,
  ChevronUp,
  Download,
  Loader2,
} from "lucide-react";

interface WarningsPanelProps {
  open: boolean;
  onClose: () => void;
  warnings: string[];
  onExtractTemplates?: () => Promise<void>;
  onMapCtas?: () => void;
}

interface ParsedWarning {
  type: string;
  postName: string;
  message: string;
  snippet: string;
}

const typeConfig: Record<
  string,
  {
    label: string;
    icon: React.ElementType;
    color: string;
    description: string;
    resolution: string;
  }
> = {
  hubl: {
    label: "HubL Template Tokens",
    icon: Code,
    color: "text-yellow-600 dark:text-yellow-400",
    description:
      "These posts contain HubL template tokens ({% %} or {{ }}). Custom modules and template includes have been automatically extracted from the source portal and will be uploaded to the target during import.",
    resolution:
      "Modules and includes are handled automatically. If any tokens reference portal-specific data (like custom properties or HubDB queries), review those posts after import. Built-in variables (content.*, request.*, etc.) work in any portal.",
  },
  form_embed: {
    label: "Form Embeds",
    icon: FormInput,
    color: "text-blue-600 dark:text-blue-400",
    description:
      "These posts contain embedded HubSpot forms with portal-specific IDs (portalId and formId). Forms are not migrated automatically — the embed codes will point to forms in the source portal.",
    resolution:
      "Recreate the forms in the target portal, then update the embed codes in the imported posts with the new form IDs. The old forms will still render from the source portal until changed.",
  },
  cta_embed: {
    label: "CTA Embeds",
    icon: MousePointerClick,
    color: "text-purple-600 dark:text-purple-400",
    description:
      "These posts contain Call-to-Action buttons with portal-specific GUIDs. CTAs cannot be migrated via API — they must be recreated manually.",
    resolution:
      "Create equivalent CTAs in the target portal, then find and replace the old CTA embed codes in the imported posts with the new ones.",
  },
  broken_media: {
    label: "Broken Media References",
    icon: ImageOff,
    color: "text-red-600 dark:text-red-400",
    description:
      "These media URLs returned 404 errors during export. The referenced images or files no longer exist in the source portal.",
    resolution:
      "These images are already missing in the source. You can either find replacement images to upload manually, or remove the broken references from the post content after import.",
  },
};

function parseWarning(raw: string): ParsedWarning {
  // Format: [type] Post "name": message — snippet
  const match = raw.match(
    /^\[(\w+)\]\s*Post "([^"]+)":\s*(.+?)(?:\s*—\s*(.+))?$/
  );
  if (match) {
    return {
      type: match[1]!,
      postName: match[2]!,
      message: match[3]!,
      snippet: match[4] || "",
    };
  }
  return { type: "unknown", postName: "", message: raw, snippet: "" };
}

export function WarningsPanel({
  open,
  onClose,
  warnings,
  onExtractTemplates,
  onMapCtas,
}: WarningsPanelProps) {
  const [expandedType, setExpandedType] = useState<string | null>(null);
  const [extracting, setExtracting] = useState(false);

  if (!open) return null;

  const parsed = warnings.map(parseWarning);

  // Group by type
  const grouped: Record<string, ParsedWarning[]> = {};
  for (const w of parsed) {
    if (!grouped[w.type]) grouped[w.type] = [];
    grouped[w.type]!.push(w);
  }

  const types = Object.keys(grouped);

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-foreground/40 backdrop-blur-sm">
      <div className="glass-modal shadow-ambient flex max-h-[85vh] w-full max-w-3xl flex-col overflow-hidden rounded-2xl">
        {/* Header */}
        <div className="flex items-center justify-between bg-[var(--surface-high)] px-8 py-6">
          <div>
            <h2 className="text-2xl font-extrabold tracking-tight">
              Content Warnings
            </h2>
            <p className="mt-1 text-sm text-muted-foreground">
              {warnings.length} issues found that may need attention after
              import
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
        <div className="flex-1 overflow-y-auto p-8">
          <div className="space-y-4">
            {types.map((type) => {
              const config = typeConfig[type] || {
                label: type,
                icon: AlertTriangle,
                color: "text-muted-foreground",
                description: "Unknown warning type.",
                resolution: "Review the affected content manually.",
              };
              const items = grouped[type]!;
              const Icon = config.icon;
              const isExpanded = expandedType === type;

              // Group by post name
              const byPost: Record<string, ParsedWarning[]> = {};
              for (const item of items) {
                const key = item.postName || "Unknown post";
                if (!byPost[key]) byPost[key] = [];
                byPost[key]!.push(item);
              }

              return (
                <div
                  key={type}
                  className="overflow-hidden rounded-xl bg-card"
                >
                  {/* Type header */}
                  <button
                    type="button"
                    onClick={() =>
                      setExpandedType(isExpanded ? null : type)
                    }
                    className="flex w-full items-center gap-3 px-5 py-4 text-left hover:bg-[var(--surface-low)]"
                  >
                    <Icon className={`h-5 w-5 shrink-0 ${config.color}`} />
                    <div className="min-w-0 flex-1">
                      <div className="flex items-center gap-2">
                        <span className="font-semibold">{config.label}</span>
                        <span className="rounded bg-secondary px-1.5 py-0.5 text-[10px] font-bold text-secondary-foreground">
                          {items.length}
                        </span>
                      </div>
                      <p className="mt-0.5 text-xs text-muted-foreground">
                        {config.description}
                      </p>
                    </div>
                    {isExpanded ? (
                      <ChevronUp className="h-4 w-4 shrink-0 text-muted-foreground" />
                    ) : (
                      <ChevronDown className="h-4 w-4 shrink-0 text-muted-foreground" />
                    )}
                  </button>

                  {isExpanded && (
                    <div className="border-t border-border">
                      {/* Resolution */}
                      <div className="bg-[var(--surface-low)] px-5 py-3">
                        <p className="text-xs font-semibold uppercase tracking-wider text-accent-foreground">
                          How to resolve
                        </p>
                        <p className="mt-1 text-xs text-muted-foreground">
                          {config.resolution}
                        </p>
                        {type === "cta_embed" && onMapCtas && (
                          <button
                            type="button"
                            onClick={onMapCtas}
                            className="mt-3 inline-flex items-center gap-1.5 rounded-lg bg-accent-foreground px-4 py-2 text-xs font-semibold text-white transition-all hover:scale-[1.02] active:scale-[0.98]"
                          >
                            <MousePointerClick className="h-3.5 w-3.5" />
                            Map CTAs to Target Portal
                          </button>
                        )}
                        {type === "hubl" && onExtractTemplates && (
                          <button
                            type="button"
                            disabled={extracting}
                            onClick={async () => {
                              setExtracting(true);
                              try {
                                await onExtractTemplates();
                              } finally {
                                setExtracting(false);
                              }
                            }}
                            className="mt-3 inline-flex items-center gap-1.5 rounded-lg bg-accent-foreground px-4 py-2 text-xs font-semibold text-white transition-all hover:scale-[1.02] active:scale-[0.98] disabled:opacity-50"
                          >
                            {extracting ? (
                              <Loader2 className="h-3.5 w-3.5 animate-spin" />
                            ) : (
                              <Download className="h-3.5 w-3.5" />
                            )}
                            {extracting
                              ? "Extracting Templates..."
                              : "Extract Templates from Source"}
                          </button>
                        )}
                      </div>

                      {/* Affected posts */}
                      <div className="max-h-64 overflow-y-auto px-5 py-3">
                        <p className="mb-2 text-xs font-semibold uppercase tracking-wider text-muted-foreground">
                          Affected posts ({Object.keys(byPost).length})
                        </p>
                        <div className="space-y-2">
                          {Object.entries(byPost).map(
                            ([postName, postWarnings]) => (
                              <div
                                key={postName}
                                className="rounded-lg bg-[var(--surface-low)] p-3"
                              >
                                <p className="text-sm font-medium">
                                  {postName}
                                </p>
                                <div className="mt-1 space-y-1">
                                  {postWarnings.map((w, i) => (
                                    <p
                                      key={i}
                                      className="font-mono text-[11px] text-muted-foreground"
                                    >
                                      {w.snippet || w.message}
                                    </p>
                                  ))}
                                </div>
                              </div>
                            )
                          )}
                        </div>
                      </div>
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        </div>
      </div>
    </div>
  );
}
