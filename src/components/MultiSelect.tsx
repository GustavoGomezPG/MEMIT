import { useState, useRef, useEffect } from "react";
import { Check, ChevronDown, X, Search } from "lucide-react";

export interface MultiSelectOption {
  value: string;
  label: string;
}

interface MultiSelectProps {
  options: MultiSelectOption[];
  selected: string[];
  onChange: (selected: string[]) => void;
  placeholder?: string;
  className?: string;
}

export function MultiSelect({
  options,
  selected,
  onChange,
  placeholder = "Select...",
  className = "",
}: MultiSelectProps) {
  const [open, setOpen] = useState(false);
  const [search, setSearch] = useState("");
  const containerRef = useRef<HTMLDivElement>(null);

  // Close on click outside
  useEffect(() => {
    if (!open) return;
    function handleClick(e: MouseEvent) {
      if (
        containerRef.current &&
        !containerRef.current.contains(e.target as Node)
      ) {
        setOpen(false);
        setSearch("");
      }
    }
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, [open]);

  const filtered = search
    ? options.filter((o) =>
        o.label.toLowerCase().includes(search.toLowerCase())
      )
    : options;

  const allFilteredSelected =
    filtered.length > 0 && filtered.every((o) => selected.includes(o.value));

  function toggleOption(value: string) {
    if (selected.includes(value)) {
      onChange(selected.filter((v) => v !== value));
    } else {
      onChange([...selected, value]);
    }
  }

  function toggleAll() {
    if (allFilteredSelected) {
      const filteredValues = new Set(filtered.map((o) => o.value));
      onChange(selected.filter((v) => !filteredValues.has(v)));
    } else {
      const next = new Set(selected);
      filtered.forEach((o) => next.add(o.value));
      onChange(Array.from(next));
    }
  }

  const selectedLabels = selected
    .map((v) => options.find((o) => o.value === v)?.label)
    .filter(Boolean);

  return (
    <div ref={containerRef} className={`relative ${className}`}>
      {/* Trigger */}
      <button
        type="button"
        onClick={() => setOpen(!open)}
        className="flex w-full items-center justify-between gap-1 rounded-lg bg-[var(--surface-low)] px-3 py-2.5 text-left text-sm outline-none"
      >
        <div className="flex min-w-0 flex-1 flex-wrap items-center gap-1">
          {selectedLabels.length === 0 ? (
            <span className="text-muted-foreground">{placeholder}</span>
          ) : selectedLabels.length <= 2 ? (
            selectedLabels.map((label) => (
              <span
                key={label}
                className="inline-flex items-center gap-0.5 rounded bg-secondary px-1.5 py-0.5 text-xs font-medium text-secondary-foreground"
              >
                {label}
                <button
                  type="button"
                  onClick={(e) => {
                    e.stopPropagation();
                    const opt = options.find((o) => o.label === label);
                    if (opt) toggleOption(opt.value);
                  }}
                  className="ml-0.5 hover:text-foreground"
                >
                  <X className="h-3 w-3" />
                </button>
              </span>
            ))
          ) : (
            <span className="text-xs font-medium">
              {selectedLabels.length} selected
            </span>
          )}
        </div>
        <div className="flex shrink-0 items-center gap-1">
          {selected.length > 0 && (
            <button
              type="button"
              onClick={(e) => {
                e.stopPropagation();
                onChange([]);
              }}
              className="text-muted-foreground hover:text-foreground"
            >
              <X className="h-3.5 w-3.5" />
            </button>
          )}
          <ChevronDown
            className={`h-3.5 w-3.5 text-muted-foreground transition-transform ${open ? "rotate-180" : ""}`}
          />
        </div>
      </button>

      {/* Dropdown */}
      {open && (
        <div className="absolute left-0 right-0 top-full z-50 mt-1 overflow-hidden rounded-lg bg-popover shadow-lg ring-1 ring-foreground/10">
          {/* Search */}
          <div className="flex items-center gap-2 border-b border-border px-3 py-2">
            <Search className="h-3.5 w-3.5 shrink-0 text-muted-foreground" />
            <input
              type="text"
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              placeholder="Search..."
              className="w-full bg-transparent text-sm outline-none placeholder:text-muted-foreground"
              autoFocus
            />
          </div>

          {/* Options */}
          <div className="max-h-56 overflow-y-auto py-1">
            {/* Select all */}
            <button
              type="button"
              onClick={toggleAll}
              className="flex w-full items-center gap-2.5 px-3 py-1.5 text-sm hover:bg-[var(--surface-low)]"
            >
              <div
                className={`flex h-4 w-4 items-center justify-center rounded border ${
                  allFilteredSelected
                    ? "border-primary bg-primary"
                    : "border-border"
                }`}
              >
                {allFilteredSelected && (
                  <Check className="h-3 w-3 text-primary-foreground" />
                )}
              </div>
              <span className="font-medium text-muted-foreground">
                Select All
              </span>
            </button>

            {filtered.map((option) => {
              const isSelected = selected.includes(option.value);
              return (
                <button
                  key={option.value}
                  type="button"
                  onClick={() => toggleOption(option.value)}
                  className="flex w-full items-center gap-2.5 px-3 py-1.5 text-sm hover:bg-[var(--surface-low)]"
                >
                  <div
                    className={`flex h-4 w-4 items-center justify-center rounded border ${
                      isSelected
                        ? "border-primary bg-primary"
                        : "border-border"
                    }`}
                  >
                    {isSelected && (
                      <Check className="h-3 w-3 text-primary-foreground" />
                    )}
                  </div>
                  <span>{option.label}</span>
                </button>
              );
            })}

            {filtered.length === 0 && (
              <p className="px-3 py-3 text-center text-xs text-muted-foreground">
                No results
              </p>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
