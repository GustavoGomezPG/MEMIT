import { useState, useEffect } from "react";
import { getCtaMappings, saveCtaMappings } from "../server/tasks";
import {
  X,
  Save,
  Loader2,
  CheckCircle,
  ArrowRight,
  MousePointerClick,
} from "lucide-react";

interface CtaMappingModalProps {
  open: boolean;
  onClose: () => void;
  taskId: number;
}

interface CtaEntry {
  sourceGuid: string;
  targetGuid: string | null;
  postIds: string[];
  postCount: number;
  postNames: string[];
}

export function CtaMappingModal({
  open,
  onClose,
  taskId,
}: CtaMappingModalProps) {
  const [ctas, setCtas] = useState<CtaEntry[]>([]);
  const [mapping, setMapping] = useState<Record<string, string>>({});
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [saved, setSaved] = useState(false);

  useEffect(() => {
    if (!open) return;
    loadCtas();
  }, [open, taskId]);

  async function loadCtas() {
    setLoading(true);
    setSaved(false);
    try {
      const result = await getCtaMappings({ data: taskId });
      setCtas(result.ctas);
      setMapping(result.mapping);
    } finally {
      setLoading(false);
    }
  }

  async function handleSave() {
    setSaving(true);
    try {
      await saveCtaMappings({ data: { taskId, mapping } });
      setSaved(true);
    } finally {
      setSaving(false);
    }
  }

  function updateMapping(sourceGuid: string, targetGuid: string) {
    setSaved(false);
    setMapping((prev) => ({
      ...prev,
      [sourceGuid]: targetGuid,
    }));
  }

  if (!open) return null;

  const mappedCount = ctas.filter(
    (c) => mapping[c.sourceGuid]?.trim()
  ).length;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-foreground/40 backdrop-blur-sm">
      <div className="glass-modal shadow-ambient flex max-h-[90vh] w-full max-w-3xl flex-col overflow-hidden rounded-2xl">
        {/* Header */}
        <div className="flex items-center justify-between bg-[var(--surface-high)] px-8 py-6">
          <div>
            <h2 className="text-2xl font-extrabold tracking-tight">
              Map CTAs to Target Portal
            </h2>
            <p className="mt-1 text-sm text-muted-foreground">
              {ctas.length} CTAs found &middot; {mappedCount} mapped &middot;{" "}
              {ctas.length - mappedCount} remaining
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

        {/* Instructions */}
        <div className="bg-[var(--surface-low)] px-8 py-4">
          <p className="text-xs text-muted-foreground">
            For each CTA below, recreate it in the target HubSpot portal
            (Marketing &rarr; Lead Capture &rarr; CTAs), then paste the new
            CTA's GUID here. During import, all{" "}
            <code className="rounded bg-card px-1 py-0.5 text-[11px]">
              {"{{cta('...')}}"}
            </code>{" "}
            references will be automatically rewritten.
          </p>
        </div>

        {/* CTA list */}
        <div className="flex-1 overflow-y-auto px-8 py-4">
          {loading ? (
            <div className="flex items-center justify-center py-16">
              <Loader2 className="h-6 w-6 animate-spin text-primary" />
            </div>
          ) : ctas.length === 0 ? (
            <p className="py-8 text-center text-sm text-muted-foreground">
              No CTAs found in exported posts.
            </p>
          ) : (
            <div className="space-y-4">
              {ctas.map((cta) => {
                const isMapped = !!mapping[cta.sourceGuid]?.trim();
                return (
                  <div
                    key={cta.sourceGuid}
                    className={`overflow-hidden rounded-xl ${isMapped ? "bg-card" : "bg-card ring-1 ring-primary/20"}`}
                  >
                    {/* CTA header */}
                    <div className="flex items-start gap-3 px-5 py-4">
                      <MousePointerClick
                        className={`mt-0.5 h-5 w-5 shrink-0 ${isMapped ? "text-accent-foreground" : "text-primary"}`}
                      />
                      <div className="min-w-0 flex-1">
                        <div className="flex items-center gap-2">
                          <p className="text-xs text-muted-foreground">
                            Used in {cta.postCount} post
                            {cta.postCount > 1 ? "s" : ""}
                          </p>
                          {isMapped && (
                            <CheckCircle className="h-3.5 w-3.5 text-accent-foreground" />
                          )}
                        </div>

                        {/* Mapping row */}
                        <div className="mt-2 flex items-center gap-2">
                          <div className="min-w-0 flex-1">
                            <label className="mb-1 block text-[10px] font-bold uppercase tracking-widest text-muted-foreground">
                              Source GUID
                            </label>
                            <div className="rounded-lg bg-[var(--surface-low)] px-3 py-2 font-mono text-xs">
                              {cta.sourceGuid}
                            </div>
                          </div>
                          <ArrowRight className="mt-4 h-4 w-4 shrink-0 text-muted-foreground" />
                          <div className="min-w-0 flex-1">
                            <label className="mb-1 block text-[10px] font-bold uppercase tracking-widest text-muted-foreground">
                              Target GUID
                            </label>
                            <input
                              type="text"
                              value={mapping[cta.sourceGuid] || ""}
                              onChange={(e) =>
                                updateMapping(
                                  cta.sourceGuid,
                                  e.target.value
                                )
                              }
                              placeholder="Paste new CTA GUID..."
                              className="w-full rounded-lg bg-[var(--surface-low)] px-3 py-2 font-mono text-xs outline-none placeholder:text-muted-foreground focus:ring-2 focus:ring-primary/30"
                            />
                          </div>
                        </div>

                        {/* Affected posts */}
                        <div className="mt-2 flex flex-wrap gap-1">
                          {cta.postNames.slice(0, 3).map((name) => (
                            <span
                              key={name}
                              className="rounded bg-[var(--surface-low)] px-1.5 py-0.5 text-[10px] text-muted-foreground"
                            >
                              {name}
                            </span>
                          ))}
                          {cta.postNames.length > 3 && (
                            <span className="text-[10px] text-muted-foreground">
                              +{cta.postNames.length - 3} more
                            </span>
                          )}
                        </div>
                      </div>
                    </div>
                  </div>
                );
              })}
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="flex items-center justify-between bg-card px-8 py-5">
          <p className="text-xs text-muted-foreground">
            {mappedCount === ctas.length
              ? "All CTAs mapped — ready to import"
              : `${ctas.length - mappedCount} unmapped CTAs will keep their source GUIDs`}
          </p>
          <button
            type="button"
            onClick={handleSave}
            disabled={saving}
            className="signature-gradient flex items-center gap-2 rounded-xl px-8 py-3 font-bold text-white shadow-lg shadow-primary/20 transition-all hover:scale-[1.02] active:scale-[0.98] disabled:opacity-50"
          >
            {saving ? (
              <Loader2 className="h-4 w-4 animate-spin" />
            ) : saved ? (
              <CheckCircle className="h-4 w-4" />
            ) : (
              <Save className="h-4 w-4" />
            )}
            {saved ? "Saved" : "Save Mapping"}
          </button>
        </div>
      </div>
    </div>
  );
}
