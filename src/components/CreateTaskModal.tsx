import { useState, useMemo, useRef } from "react";
import { Doughnut } from "react-chartjs-2";
import {
  Chart as ChartJS,
  ArcElement,
  Tooltip as ChartTooltip,
  Legend,
} from "chart.js";
import {
  ArrowLeftRight,
  BookOpen,
  Database,
  FileText,
  Image,
  Layout,
  ArrowRightLeft,
  Table2,
  FileSpreadsheet,
  Rocket,
  X,
  Check,
  Search,
  Loader2,
  ArrowRight,
  ChevronLeft,
  Upload,
} from "lucide-react";
ChartJS.register(ArcElement, ChartTooltip, Legend);

import { MultiSelect } from "./MultiSelect";
import type { ServiceKey } from "../db/schema";
import { fetchSourceBlogPosts, fetchMediaSummary } from "../server/tasks";

interface CreateTaskModalProps {
  open: boolean;
  onClose: () => void;
  onCreate: (task: {
    type: string;
    label: string;
    outputType: string;
    config?: string;
    csvFileContent?: string;
    csvFileName?: string;
  }) => void;
  migrationId: number;
  sourceKey: ServiceKey | null;
  targetKey: ServiceKey | null;
  onSwap: () => void;
}

interface CsvFileState {
  name: string;
  size: number;
  content: string;
  rowCount: number;
}

const inputTypes = [
  { id: "media", label: "Media Files", icon: Image },
  { id: "blog_posts", label: "Blog Posts", icon: BookOpen },
  { id: "hubdb", label: "HubDB", icon: Database },
  { id: "page", label: "Pages", icon: Layout },
];

const allOutputTypes = [
  { id: "same_as_source", label: "Same as source", icon: ArrowRightLeft },
  { id: "hubdb", label: "HubDB", icon: Table2 },
  { id: "csv", label: "CSV", icon: FileSpreadsheet },
];

const allowedOutputs: Record<string, string[]> = {
  media: ["same_as_source", "csv"],
  blog_posts: ["same_as_source", "hubdb", "csv"],
  hubdb: ["same_as_source", "hubdb", "csv"],
  page: ["same_as_source", "hubdb", "csv"],
};

interface BlogPostSummary {
  id: string;
  name: string;
  slug: string;
  state: string;
  featuredImage: string;
  publishDate: string;
  contentGroupId: string;
  contentGroupName: string;
  tagIds: string[];
  tagNames: string[];
  url: string;
}

interface NamedItem {
  id: string;
  name: string;
}

export function CreateTaskModal({
  open,
  onClose,
  onCreate,
  migrationId,
  sourceKey,
  targetKey,
  onSwap,
}: CreateTaskModalProps) {
  const [step, setStep] = useState<
    "config" | "select-posts" | "media-summary"
  >("config");
  const [sourceType, setSourceType] = useState<"hubspot" | "csv">("hubspot");
  const [csvFile, setCsvFile] = useState<CsvFileState | null>(null);
  const csvInputRef = useRef<HTMLInputElement>(null);
  const [selectedInput, setSelectedInput] = useState<string | null>(null);
  const [selectedOutput, setSelectedOutput] = useState("same_as_source");

  // Media summary state
  const [mediaSummary, setMediaSummary] = useState<{
    totalFiles: number;
    totalBytes: number;
    byType: { ext: string; count: number; bytes: number }[];
    targetStorage: { bytesUsed: number; bytesLimit: number } | null;
    spaceWarning: string | null;
  } | null>(null);
  const [loadingMedia, setLoadingMedia] = useState(false);

  // Blog post selector state
  const [posts, setPosts] = useState<BlogPostSummary[]>([]);
  const [contentGroups, setContentGroups] = useState<NamedItem[]>([]);
  const [tags, setTags] = useState<NamedItem[]>([]);
  const [loadingPosts, setLoadingPosts] = useState(false);
  const [selectedPostIds, setSelectedPostIds] = useState<Set<string>>(
    new Set()
  );
  const [searchQuery, setSearchQuery] = useState("");
  const [filterGroups, setFilterGroups] = useState<string[]>([]);
  const [filterTags, setFilterTags] = useState<string[]>([]);
  const [filterStates, setFilterStates] = useState<string[]>([]);

  const csvOutputIds = ["hubdb", "csv"];
  const outputTypes =
    sourceType === "csv"
      ? allOutputTypes.filter((o) => csvOutputIds.includes(o.id))
      : selectedInput
        ? allOutputTypes.filter((o) =>
            (
              allowedOutputs[selectedInput] || ["same_as_source", "csv"]
            ).includes(o.id)
          )
        : allOutputTypes;

  function handleSelectInput(id: string) {
    setSelectedInput(id);
    const allowed = allowedOutputs[id] || ["same_as_source", "csv"];
    if (!allowed.includes(selectedOutput)) {
      setSelectedOutput(allowed[0]!);
    }
  }

  function handleSourceTypeChange(type: "hubspot" | "csv") {
    setSourceType(type);
    if (type === "csv") {
      setSelectedInput(null);
      if (selectedOutput === "same_as_source") {
        setSelectedOutput("hubdb");
      }
    } else {
      setCsvFile(null);
    }
  }

  function readCsvFile(file: File) {
    const reader = new FileReader();
    reader.onload = () => {
      const content = reader.result as string;
      const lines = content.split("\n").filter(Boolean);
      setCsvFile({
        name: file.name,
        size: file.size,
        content,
        rowCount: Math.max(0, lines.length - 1),
      });
    };
    reader.readAsText(file);
  }

  function handleCsvFileSelect(e: React.ChangeEvent<HTMLInputElement>) {
    const file = e.target.files?.[0];
    if (file) readCsvFile(file);
  }

  function handleCsvDrop(e: React.DragEvent) {
    e.preventDefault();
    const file = e.dataTransfer.files[0];
    if (file && file.name.toLowerCase().endsWith(".csv")) {
      readCsvFile(file);
    }
  }

  function handleClose() {
    setStep("config");
    setSourceType("hubspot");
    setCsvFile(null);
    setSelectedInput(null);
    setSelectedOutput("same_as_source");
    setMediaSummary(null);
    setPosts([]);
    setTags([]);
    setContentGroups([]);
    setSelectedPostIds(new Set());
    setSearchQuery("");
    setFilterGroups([]);
    setFilterTags([]);
    setFilterStates([]);
    onClose();
  }

  async function handleNext() {
    if (!selectedInput) return;

    if (selectedInput === "media") {
      // Show media summary step
      setLoadingMedia(true);
      setStep("media-summary");
      try {
        const result = await fetchMediaSummary({ data: migrationId });
        setMediaSummary(result);
      } catch {
        // If summary fails, still allow creating the task
        setMediaSummary(null);
      } finally {
        setLoadingMedia(false);
      }
    } else if (selectedInput === "blog_posts") {
      // Go to post selector step
      setLoadingPosts(true);
      setStep("select-posts");
      try {
        const result = await fetchSourceBlogPosts({ data: migrationId });
        setPosts(result.posts);
        setContentGroups(result.contentGroups);
        setTags(result.tags);
        setSelectedPostIds(new Set(result.posts.map((p) => p.id)));
      } catch {
        handleFinalCreate();
        return;
      } finally {
        setLoadingPosts(false);
      }
    } else {
      handleFinalCreate();
    }
  }

  function handleFinalCreate(postIds?: string[]) {
    if (sourceType === "csv") {
      if (!csvFile) return;
      onCreate({
        type: "csv_import",
        label: `CSV Import — ${csvFile.name}`,
        outputType: selectedOutput,
        config: JSON.stringify({ sourceType: "csv" }),
        csvFileContent: csvFile.content,
        csvFileName: csvFile.name,
      });
      handleClose();
      return;
    }

    if (!selectedInput) return;
    const inputDef = inputTypes.find((t) => t.id === selectedInput);
    const config =
      postIds && postIds.length > 0
        ? JSON.stringify({ selectedPostIds: postIds })
        : undefined;
    onCreate({
      type: selectedInput,
      label: `${inputDef?.label || selectedInput} Migration`,
      outputType: selectedOutput,
      config,
    });
    handleClose();
  }

  if (!open) return null;

  // Tags scoped to selected blogs
  const availableTags = (() => {
    const postsInGroup =
      filterGroups.length === 0
        ? posts
        : posts.filter((p) => filterGroups.includes(p.contentGroupId));
    const tagIdsInGroup = new Set(postsInGroup.flatMap((p) => p.tagIds));
    return tags.filter((t) => tagIdsInGroup.has(t.id));
  })();

  // Blog post filtering
  const filteredPosts = posts.filter((p) => {
    if (searchQuery) {
      const q = searchQuery.toLowerCase();
      if (
        !p.name.toLowerCase().includes(q) &&
        !p.slug.toLowerCase().includes(q)
      )
        return false;
    }
    if (filterGroups.length > 0 && !filterGroups.includes(p.contentGroupId))
      return false;
    if (filterTags.length > 0 && !filterTags.some((t) => p.tagIds.includes(t)))
      return false;
    if (filterStates.length > 0 && !filterStates.includes(p.state))
      return false;
    return true;
  });

  // Auto-sync selection: only keep IDs that match current filters
  const filteredIds = new Set(filteredPosts.map((p) => p.id));
  const activeSelection = new Set(
    [...selectedPostIds].filter((id) => filteredIds.has(id))
  );

  const allFilteredSelected =
    filteredPosts.length > 0 &&
    filteredPosts.every((p) => selectedPostIds.has(p.id));

  function toggleAll() {
    const next = new Set(selectedPostIds);
    if (allFilteredSelected) {
      filteredPosts.forEach((p) => next.delete(p.id));
    } else {
      filteredPosts.forEach((p) => next.add(p.id));
    }
    setSelectedPostIds(next);
  }

  function togglePost(id: string) {
    const next = new Set(selectedPostIds);
    if (next.has(id)) next.delete(id);
    else next.add(id);
    setSelectedPostIds(next);
  }

  const uniqueStates = Array.from(new Set(posts.map((p) => p.state)));

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-foreground/40 backdrop-blur-sm">
      <div className="glass-modal shadow-ambient flex max-h-[90vh] w-full max-w-3xl flex-col overflow-hidden rounded-2xl">
        {/* Header */}
        <div className="flex items-center justify-between bg-[var(--surface-high)] px-8 py-6">
          <div>
            {step === "media-summary" ? (
              <>
                <h2 className="text-2xl font-extrabold tracking-tight">
                  Media Migration Summary
                </h2>
                <p className="mt-1 text-sm font-medium italic text-muted-foreground">
                  Review before you migrate.
                </p>
              </>
            ) : step === "select-posts" ? (
              <>
                <h2 className="text-2xl font-extrabold tracking-tight">
                  Select Blog Posts
                </h2>
                <p className="mt-1 text-sm font-medium italic text-muted-foreground">
                  Choose which posts to include in this migration.
                </p>
              </>
            ) : (
              <>
                <h2 className="text-2xl font-extrabold tracking-tight">
                  Create New Migration Task
                </h2>
                <p className="mt-1 text-sm font-medium italic text-muted-foreground">
                  Export to local, review, then import to target.
                </p>
              </>
            )}
          </div>
          <button
            type="button"
            onClick={handleClose}
            className="flex h-10 w-10 items-center justify-center rounded-full transition-colors hover:bg-white/50 dark:hover:bg-white/10"
          >
            <X className="h-5 w-5" />
          </button>
        </div>

        {/* Body */}
        <div className="overflow-y-auto p-8">
          {step === "config" ? (
            <div className="space-y-10">
              {/* Source / Target */}
              <section>
                <div className="flex items-center gap-6">
                  <div className="flex-1">
                    <label className="mb-3 block text-xs font-bold uppercase tracking-widest text-muted-foreground">
                      Origin Source
                    </label>
                    {/* Source type toggle */}
                    <div className="mb-3 flex items-center gap-1 rounded-lg bg-[var(--surface-low)] p-1">
                      <button
                        type="button"
                        onClick={() => handleSourceTypeChange("hubspot")}
                        className={`flex flex-1 items-center justify-center gap-1.5 rounded-md px-3 py-1.5 text-xs font-bold transition-colors ${
                          sourceType === "hubspot"
                            ? "bg-card shadow-sm"
                            : "text-muted-foreground hover:text-foreground"
                        }`}
                      >
                        <Database className="h-3 w-3" />
                        HubSpot
                      </button>
                      <button
                        type="button"
                        onClick={() => handleSourceTypeChange("csv")}
                        className={`flex flex-1 items-center justify-center gap-1.5 rounded-md px-3 py-1.5 text-xs font-bold transition-colors ${
                          sourceType === "csv"
                            ? "bg-card shadow-sm"
                            : "text-muted-foreground hover:text-foreground"
                        }`}
                      >
                        <FileSpreadsheet className="h-3 w-3" />
                        CSV File
                      </button>
                    </div>
                    {/* Source card */}
                    {sourceType === "hubspot" ? (
                      <div className="flex items-center gap-4 rounded-xl bg-[var(--surface-low)] p-5">
                        <div className="flex h-12 w-12 items-center justify-center rounded-lg bg-card shadow-sm">
                          <FileText className="h-5 w-5 text-accent-foreground" />
                        </div>
                        <div>
                          <p className="font-bold">
                            {sourceKey?.name || "—"}
                          </p>
                          <p className="text-xs text-muted-foreground">
                            Portal #{sourceKey?.portalId || "—"}
                          </p>
                        </div>
                      </div>
                    ) : (
                      <div className="flex items-center gap-4 rounded-xl bg-[var(--surface-low)] p-5">
                        <div className="flex h-12 w-12 items-center justify-center rounded-lg bg-card shadow-sm">
                          <FileSpreadsheet className="h-5 w-5 text-primary" />
                        </div>
                        <div>
                          <p className="font-bold">CSV Source</p>
                          <p className="text-xs text-muted-foreground">
                            Local file upload
                          </p>
                        </div>
                      </div>
                    )}
                  </div>
                  <div className="flex flex-col items-center justify-center pt-6">
                    <button
                      type="button"
                      onClick={onSwap}
                      disabled={sourceType === "csv"}
                      className={`flex h-12 w-12 items-center justify-center rounded-full text-white shadow-lg transition-all ${
                        sourceType === "csv"
                          ? "cursor-not-allowed bg-muted opacity-40"
                          : "signature-gradient hover:scale-110 active:scale-95"
                      }`}
                    >
                      <ArrowLeftRight className="h-5 w-5" />
                    </button>
                  </div>
                  <div className="flex-1">
                    <label className="mb-3 block text-xs font-bold uppercase tracking-widest text-muted-foreground">
                      Destination Target
                    </label>
                    {/* Target type indicator */}
                    <div className="mb-3 flex items-center gap-1 rounded-lg bg-[var(--surface-low)] p-1">
                      <div className="flex flex-1 items-center justify-center gap-1.5 rounded-md bg-card px-3 py-1.5 text-xs font-bold shadow-sm">
                        <Database className="h-3 w-3" />
                        HubSpot
                      </div>
                    </div>
                    <div className="flex items-center gap-4 rounded-xl bg-[var(--surface-low)] p-5">
                      <div className="flex h-12 w-12 items-center justify-center rounded-lg bg-card shadow-sm">
                        <FileText className="h-5 w-5 text-accent-foreground" />
                      </div>
                      <div>
                        <p className="font-bold">
                          {targetKey?.name || "—"}
                        </p>
                        <p className="text-xs text-muted-foreground">
                          Portal #{targetKey?.portalId || "—"}
                        </p>
                      </div>
                    </div>
                  </div>
                </div>
              </section>

              {/* Input / Output */}
              <section className="grid grid-cols-2 gap-8">
                <div>
                  {sourceType === "csv" ? (
                    <>
                      <div className="mb-4 flex items-center justify-between">
                        <label className="text-sm font-bold uppercase tracking-widest text-muted-foreground">
                          CSV File
                        </label>
                        <span className="rounded bg-secondary px-2 py-0.5 text-[10px] font-bold text-secondary-foreground">
                          Upload
                        </span>
                      </div>
                      {csvFile ? (
                        <div className="rounded-xl border-2 border-primary bg-card p-5">
                          <div className="flex items-center justify-between">
                            <div className="flex items-center gap-3">
                              <FileSpreadsheet className="h-5 w-5 text-primary" />
                              <div>
                                <p className="font-semibold">{csvFile.name}</p>
                                <p className="text-xs text-muted-foreground">
                                  {fmtBytes(csvFile.size)} &middot;{" "}
                                  {csvFile.rowCount.toLocaleString()} rows
                                </p>
                              </div>
                            </div>
                            <button
                              type="button"
                              onClick={() => csvInputRef.current?.click()}
                              className="text-xs font-bold text-primary hover:underline"
                            >
                              Change
                            </button>
                          </div>
                        </div>
                      ) : (
                        <button
                          type="button"
                          onClick={() => csvInputRef.current?.click()}
                          onDrop={handleCsvDrop}
                          onDragOver={(e) => e.preventDefault()}
                          className="flex w-full flex-col items-center justify-center gap-3 rounded-xl border-2 border-dashed border-border bg-card p-8 transition-colors hover:border-primary hover:bg-[var(--surface-low)]"
                        >
                          <Upload className="h-8 w-8 text-muted-foreground" />
                          <div className="text-center">
                            <p className="font-semibold">
                              Click to select CSV file
                            </p>
                            <p className="mt-0.5 text-xs text-muted-foreground">
                              or drag and drop
                            </p>
                          </div>
                        </button>
                      )}
                      <input
                        ref={csvInputRef}
                        type="file"
                        accept=".csv"
                        onChange={handleCsvFileSelect}
                        className="hidden"
                      />
                    </>
                  ) : (
                    <>
                      <div className="mb-4 flex items-center justify-between">
                        <label className="text-sm font-bold uppercase tracking-widest text-muted-foreground">
                          Payload Inputs
                        </label>
                        <span className="rounded bg-secondary px-2 py-0.5 text-[10px] font-bold text-secondary-foreground">
                          Select Items
                        </span>
                      </div>
                      <div className="space-y-3">
                        {inputTypes.map((item) => {
                          const selected = selectedInput === item.id;
                          const Icon = item.icon;
                          return (
                            <button
                              key={item.id}
                              type="button"
                              onClick={() => handleSelectInput(item.id)}
                              className={`flex w-full cursor-pointer items-center justify-between rounded-xl p-4 transition-colors ${
                                selected
                                  ? "border-2 border-primary bg-card"
                                  : "border border-border bg-card hover:bg-[var(--surface-low)]"
                              }`}
                            >
                              <div className="flex items-center gap-3">
                                <Icon
                                  className={`h-5 w-5 ${selected ? "text-primary" : "text-muted-foreground"}`}
                                />
                                <span className="font-semibold">
                                  {item.label}
                                </span>
                              </div>
                              <RadioDot selected={selected} />
                            </button>
                          );
                        })}
                      </div>
                    </>
                  )}
                </div>

                <div>
                  <div className="mb-4 flex items-center justify-between">
                    <label className="text-sm font-bold uppercase tracking-widest text-muted-foreground">
                      Protocol Outputs
                    </label>
                    <span className="rounded bg-secondary px-2 py-0.5 text-[10px] font-bold text-secondary-foreground">
                      Protocol Type
                    </span>
                  </div>
                  <div className="space-y-3">
                    {outputTypes.map((item) => {
                      const selected = selectedOutput === item.id;
                      const Icon = item.icon;
                      return (
                        <button
                          key={item.id}
                          type="button"
                          onClick={() => setSelectedOutput(item.id)}
                          className={`flex w-full cursor-pointer items-center justify-between rounded-xl p-4 transition-colors ${
                            selected
                              ? "border-2 border-primary bg-card"
                              : "border border-border bg-card hover:bg-[var(--surface-low)]"
                          }`}
                        >
                          <div className="flex items-center gap-3">
                            <Icon
                              className={`h-5 w-5 ${selected ? "text-primary" : "text-muted-foreground"}`}
                            />
                            <span className="font-semibold">{item.label}</span>
                          </div>
                          <RadioDot selected={selected} />
                        </button>
                      );
                    })}
                  </div>
                </div>
              </section>

            </div>
          ) : step === "media-summary" ? (
            /* Step 2a: Media Summary */
            <div className="space-y-6">
              {loadingMedia ? (
                <div className="flex flex-col items-center justify-center py-16">
                  <Loader2 className="h-8 w-8 animate-spin text-primary" />
                  <p className="mt-3 text-sm text-muted-foreground">
                    Scanning source portal files...
                  </p>
                </div>
              ) : mediaSummary ? (
                <>
                  {/* Headline stats */}
                  <div className="grid grid-cols-2 gap-4">
                    <div className="rounded-xl bg-[var(--surface-low)] p-5">
                      <p className="text-xs font-bold uppercase tracking-widest text-muted-foreground">
                        Total Files
                      </p>
                      <p className="mt-1 text-3xl font-extrabold">
                        {mediaSummary.totalFiles.toLocaleString()}
                      </p>
                    </div>
                    <div className="rounded-xl bg-[var(--surface-low)] p-5">
                      <p className="text-xs font-bold uppercase tracking-widest text-muted-foreground">
                        Total Size
                      </p>
                      <p className="mt-1 text-3xl font-extrabold">
                        {fmtBytes(mediaSummary.totalBytes)}
                      </p>
                    </div>
                  </div>

                  {/* Target storage */}
                  {mediaSummary.targetStorage &&
                    mediaSummary.targetStorage.bytesLimit > 0 && (
                      <div className="rounded-xl bg-[var(--surface-low)] p-5">
                        <p className="text-xs font-bold uppercase tracking-widest text-muted-foreground">
                          Target Portal Storage
                        </p>
                        <div className="mt-2 flex items-end justify-between">
                          <p className="text-sm text-muted-foreground">
                            {fmtBytes(
                              mediaSummary.targetStorage.bytesUsed
                            )}{" "}
                            used of{" "}
                            {fmtBytes(
                              mediaSummary.targetStorage.bytesLimit
                            )}
                          </p>
                          <p className="text-sm font-semibold">
                            {fmtBytes(
                              mediaSummary.targetStorage.bytesLimit -
                                mediaSummary.targetStorage.bytesUsed
                            )}{" "}
                            free
                          </p>
                        </div>
                        <div className="mt-2 h-2 overflow-hidden rounded-full bg-secondary">
                          <div
                            className="h-full rounded-full bg-accent-foreground"
                            style={{
                              width: `${Math.min(100, (mediaSummary.targetStorage.bytesUsed / mediaSummary.targetStorage.bytesLimit) * 100)}%`,
                            }}
                          />
                        </div>
                      </div>
                    )}

                  {/* Space warning */}
                  {mediaSummary.spaceWarning && (
                    <div className="rounded-xl bg-[var(--primary-fixed)] px-5 py-4 text-sm font-medium text-foreground">
                      {mediaSummary.spaceWarning}
                    </div>
                  )}

                  {/* Breakdown by type — Doughnut chart */}
                  <FileTypesChart
                    byType={mediaSummary.byType}
                    totalBytes={mediaSummary.totalBytes}
                  />
                </>
              ) : (
                <div className="flex flex-col items-center justify-center py-16">
                  <p className="text-sm text-muted-foreground">
                    Could not fetch file summary. You can still create the
                    task.
                  </p>
                </div>
              )}
            </div>
          ) : (
            /* Step 2b: Blog Post Selector */
            <div className="space-y-4">
              {loadingPosts ? (
                <div className="flex flex-col items-center justify-center py-16">
                  <Loader2 className="h-8 w-8 animate-spin text-primary" />
                  <p className="mt-3 text-sm text-muted-foreground">
                    Fetching blog posts from source portal...
                  </p>
                </div>
              ) : (
                <>
                  {/* Filters */}
                  <div className="space-y-3">
                    <div className="relative">
                      <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
                      <input
                        type="text"
                        placeholder="Search posts..."
                        value={searchQuery}
                        onChange={(e) => setSearchQuery(e.target.value)}
                        className="w-full rounded-lg bg-[var(--surface-low)] py-2.5 pl-10 pr-4 text-sm outline-none placeholder:text-muted-foreground focus:ring-2 focus:ring-primary/30"
                      />
                    </div>
                    <div className="grid grid-cols-3 gap-2">
                      <MultiSelect
                        options={contentGroups.map((g) => ({
                          value: g.id,
                          label: g.name,
                        }))}
                        selected={filterGroups}
                        onChange={(v) => {
                          setFilterGroups(v);
                          setFilterTags([]);
                        }}
                        placeholder="All Blogs"
                      />
                      <MultiSelect
                        options={availableTags.map((t) => ({
                          value: t.id,
                          label: t.name,
                        }))}
                        selected={filterTags}
                        onChange={setFilterTags}
                        placeholder="All Tags"
                      />
                      <MultiSelect
                        options={uniqueStates.map((s) => ({
                          value: s,
                          label: s,
                        }))}
                        selected={filterStates}
                        onChange={setFilterStates}
                        placeholder="All States"
                      />
                    </div>
                  </div>

                  {/* Select all / count */}
                  <div className="flex items-center justify-between">
                    <button
                      type="button"
                      onClick={toggleAll}
                      className="text-xs font-bold uppercase tracking-widest text-primary hover:underline"
                    >
                      {allFilteredSelected ? "Deselect all" : "Select all"}
                    </button>
                    <span className="text-xs text-muted-foreground">
                      {activeSelection.size} of {filteredPosts.length} selected to import
                    </span>
                  </div>

                  {/* Post list */}
                  <div className="max-h-[400px] space-y-2 overflow-y-auto">
                    {filteredPosts.map((post) => {
                      const selected = selectedPostIds.has(post.id);
                      return (
                        <button
                          key={post.id}
                          type="button"
                          onClick={() => togglePost(post.id)}
                          className={`flex w-full items-center justify-between rounded-xl p-4 text-left transition-colors ${
                            selected
                              ? "border-2 border-primary bg-card"
                              : "border border-border bg-card hover:bg-[var(--surface-low)]"
                          }`}
                        >
                          <div className="min-w-0 flex-1">
                            <p className="truncate text-sm font-semibold">
                              {post.name}
                            </p>
                            <div className="mt-0.5 flex flex-wrap items-center gap-1.5 text-xs text-muted-foreground">
                              <span className="truncate font-mono">
                                /{post.slug}
                              </span>
                              <span className="shrink-0 rounded bg-secondary px-1.5 py-0.5 text-[10px] font-bold text-secondary-foreground">
                                {post.state}
                              </span>
                              {post.tagNames.slice(0, 3).map((tag) => (
                                <span
                                  key={tag}
                                  className="shrink-0 rounded bg-[var(--surface-low)] px-1.5 py-0.5 text-[10px] text-muted-foreground"
                                >
                                  {tag}
                                </span>
                              ))}
                              {post.tagNames.length > 3 && (
                                <span className="text-[10px] text-muted-foreground">
                                  +{post.tagNames.length - 3}
                                </span>
                              )}
                            </div>
                          </div>
                          <div className="ml-3 shrink-0">
                            <RadioDot selected={selected} checkbox />
                          </div>
                        </button>
                      );
                    })}
                    {filteredPosts.length === 0 && (
                      <p className="py-8 text-center text-sm text-muted-foreground">
                        No posts match your filters.
                      </p>
                    )}
                  </div>
                </>
              )}
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="flex items-center justify-between bg-card px-8 py-6">
          <div>
            {(step === "select-posts" || step === "media-summary") && (
              <button
                type="button"
                onClick={() => setStep("config")}
                className="flex items-center gap-1.5 text-sm font-bold text-secondary-foreground transition-colors hover:text-foreground"
              >
                <ChevronLeft className="h-4 w-4" />
                Back
              </button>
            )}
          </div>
          <div className="flex items-center gap-4">
            <button
              type="button"
              onClick={handleClose}
              className="px-6 py-3 font-bold text-secondary-foreground transition-colors hover:text-foreground"
            >
              Cancel
            </button>
            {step === "config" ? (
              <button
                type="button"
                onClick={
                  sourceType === "csv"
                    ? () => handleFinalCreate()
                    : handleNext
                }
                disabled={
                  sourceType === "csv" ? !csvFile : !selectedInput
                }
                className="signature-gradient flex items-center gap-2 rounded-xl px-10 py-3 font-bold text-white shadow-lg shadow-primary/20 transition-all hover:scale-[1.02] active:scale-[0.98] disabled:opacity-50 disabled:hover:scale-100"
              >
                {sourceType === "csv" ? (
                  <>
                    <Rocket className="h-4 w-4" />
                    Create Task
                  </>
                ) : selectedInput === "blog_posts" ? (
                  <>
                    <ArrowRight className="h-4 w-4" />
                    Select Posts
                  </>
                ) : selectedInput === "media" ? (
                  <>
                    <ArrowRight className="h-4 w-4" />
                    Review Files
                  </>
                ) : (
                  <>
                    <Rocket className="h-4 w-4" />
                    Create Task
                  </>
                )}
              </button>
            ) : step === "media-summary" ? (
              <button
                type="button"
                onClick={() => handleFinalCreate()}
                disabled={loadingMedia}
                className="signature-gradient flex items-center gap-2 rounded-xl px-10 py-3 font-bold text-white shadow-lg shadow-primary/20 transition-all hover:scale-[1.02] active:scale-[0.98] disabled:opacity-50 disabled:hover:scale-100"
              >
                <Rocket className="h-4 w-4" />
                Create Task
              </button>
            ) : (
              <button
                type="button"
                onClick={() =>
                  handleFinalCreate(Array.from(activeSelection))
                }
                disabled={activeSelection.size === 0 || loadingPosts}
                className="signature-gradient flex items-center gap-2 rounded-xl px-10 py-3 font-bold text-white shadow-lg shadow-primary/20 transition-all hover:scale-[1.02] active:scale-[0.98] disabled:opacity-50 disabled:hover:scale-100"
              >
                <Rocket className="h-4 w-4" />
                Create Task ({activeSelection.size})
              </button>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

const FILE_TYPE_LABELS: Record<string, string> = {
  png: "PNG Image",
  jpg: "JPEG Image",
  jpeg: "JPEG Image",
  gif: "GIF Animation",
  svg: "SVG Vector",
  webp: "WebP Image",
  ico: "Icon",
  bmp: "Bitmap",
  pdf: "PDF Document",
  doc: "Word Doc",
  docx: "Word Doc",
  xls: "Excel Sheet",
  xlsx: "Excel Sheet",
  ppt: "PowerPoint",
  pptx: "PowerPoint",
  csv: "CSV Data",
  txt: "Text File",
  mp4: "MP4 Video",
  mov: "MOV Video",
  avi: "AVI Video",
  webm: "WebM Video",
  mp3: "MP3 Audio",
  wav: "WAV Audio",
  zip: "ZIP Archive",
  rar: "RAR Archive",
  js: "JavaScript",
  css: "Stylesheet",
  html: "HTML",
  json: "JSON",
  xml: "XML",
  woff: "Web Font",
  woff2: "Web Font",
  ttf: "Font",
  eot: "Font",
  other: "Other Files",
};

const CHART_COLORS = [
  "#a7391e", "#006878", "#536478", "#ff7a59",
  "#cfe1f8", "#58423c", "#d0e4ff", "#ffdad2",
  "#1a3050", "#4db8a4",
];

function FileTypesChart({
  byType,
}: {
  byType: { ext: string; count: number; bytes: number }[];
  totalBytes: number;
}) {
  const top = byType.slice(0, 8);
  const rest = byType.slice(8);
  const segments =
    rest.length > 0
      ? [
          ...top,
          {
            ext: "other",
            count: rest.reduce((s, t) => s + t.count, 0),
            bytes: rest.reduce((s, t) => s + t.bytes, 0),
          },
        ]
      : top;

  const data = useMemo(
    () => ({
      labels: segments.map((s) => `.${s.ext}`),
      datasets: [
        {
          data: segments.map((s) => s.bytes),
          backgroundColor: segments.map(
            (_, i) => CHART_COLORS[i % CHART_COLORS.length]
          ),
          borderWidth: 0,
          hoverOffset: 6,
        },
      ],
    }),
    [segments]
  );

  const options = useMemo(
    () => ({
      responsive: true,
      cutout: "65%",
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: (ctx: { parsed: number; label: string }) =>
              `${ctx.label}  ${fmtBytes(ctx.parsed)}`,
          },
        },
      },
    }),
    []
  );

  return (
    <div className="flex items-start gap-6">
      <div className="w-44 shrink-0">
        <Doughnut data={data} options={options} />
      </div>
      <div className="flex-1 space-y-1.5 pt-2">
        <p className="mb-2 text-xs font-bold uppercase tracking-widest text-muted-foreground">
          File Types
        </p>
        {segments.map((s, i) => (
          <div key={s.ext} className="flex items-center gap-2.5">
            <span
              className="h-2.5 w-2.5 shrink-0 rounded-full"
              style={{
                backgroundColor: CHART_COLORS[i % CHART_COLORS.length],
              }}
            />
            <div className="min-w-0 flex-1">
              <div className="flex items-baseline gap-1.5">
                <span className="font-mono text-xs font-semibold uppercase">
                  .{s.ext}
                </span>
                <span className="truncate text-[10px] text-muted-foreground">
                  {FILE_TYPE_LABELS[s.ext] || "File"}
                </span>
              </div>
            </div>
            <span className="shrink-0 text-xs tabular-nums text-muted-foreground">
              {s.count}
            </span>
            <span className="w-16 shrink-0 text-right text-xs font-medium tabular-nums">
              {fmtBytes(s.bytes)}
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}

function fmtBytes(bytes: number): string {
  if (bytes === 0) return "0 B";
  const k = 1024;
  const sizes = ["B", "KB", "MB", "GB"];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return `${(bytes / Math.pow(k, i)).toFixed(1)} ${sizes[i]}`;
}

function RadioDot({
  selected,
  checkbox,
}: {
  selected: boolean;
  checkbox?: boolean;
}) {
  return (
    <div
      className={`flex h-5 w-5 items-center justify-center ${
        checkbox ? "rounded" : "rounded-full"
      } ${selected ? "bg-primary" : "border border-border"}`}
    >
      {selected && <Check className="h-3 w-3 text-primary-foreground" />}
    </div>
  );
}
