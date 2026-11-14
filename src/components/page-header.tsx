"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { CalendarRange, RefreshCw } from "lucide-react";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { useSalesData } from "@/hooks/use-sales-data";
import { PRESET_LABELS, syncRangeToUrl, type RangePreset } from "@/lib/range-store";
import { daysBetween } from "@/lib/dates";
import { formatDateShort } from "@/lib/format";
import { cn } from "@/lib/utils";

/** "Data through Nov 11": how fresh the numbers are, with a way to update them. */
export function DataFreshness() {
  const { through, today, syncing, hasData } = useSalesData();
  if (!hasData || !through) return null;
  const age = daysBetween(through, today);
  return (
    <Link
      href="/import"
      title="Latest sale or shipment in your data. Click to upload a newer export."
      className={cn(
        "inline-flex h-8 items-center gap-1.5 rounded-md border px-2.5 pointer-coarse:h-10 text-xs font-medium transition-colors hover:bg-accent",
        age >= 14
          ? "border-amber-500/40 text-amber-700 dark:text-amber-400"
          : "text-muted-foreground"
      )}
    >
      <RefreshCw className={cn("size-3.5", syncing && "animate-spin")} />
      <span className="hidden sm:inline">Data through</span> {formatDateShort(through)}
    </Link>
  );
}

function RangePicker() {
  const { range, setRange, bounds, through, today } = useSalesData();
  const pathname = usePathname();
  const [customOpen, setCustomOpen] = useState(false);
  const [from, setFrom] = useState("");
  const [to, setTo] = useState("");

  // Keep ?range=… in the address bar as the user moves between pages.
  useEffect(() => {
    syncRangeToUrl();
  }, [pathname, range.preset, range.from, range.to]);

  const maxDate = through ?? today;

  return (
    <Popover open={customOpen} onOpenChange={setCustomOpen}>
      <PopoverTrigger asChild>
        {/* Anchor for the custom-range popover; the Select below opens it. */}
        <span>
          <Select
            value={range.preset}
            onValueChange={(v) => {
              if (v === "custom") {
                setFrom(range.start ?? bounds?.min ?? maxDate);
                setTo(range.end ?? maxDate);
                setCustomOpen(true);
              } else {
                setRange({ preset: v as RangePreset });
              }
            }}
          >
            <SelectTrigger size="sm" className="gap-2" aria-label="Date range">
              <CalendarRange className="size-4 text-muted-foreground" />
              <SelectValue>
                {range.preset === "custom" && range.start && range.end
                  ? `${formatDateShort(range.start)} – ${formatDateShort(range.end)}`
                  : range.label}
              </SelectValue>
            </SelectTrigger>
            <SelectContent align="end">
              {Object.entries(PRESET_LABELS).map(([value, label]) => (
                <SelectItem key={value} value={value}>
                  {label}
                </SelectItem>
              ))}
              <SelectItem value="custom">Custom range…</SelectItem>
            </SelectContent>
          </Select>
        </span>
      </PopoverTrigger>
      <PopoverContent align="end" className="w-64">
        <form
          className="grid gap-3"
          onSubmit={(e) => {
            e.preventDefault();
            if (!from || !to || from > to) return;
            setRange({ preset: "custom", from, to });
            setCustomOpen(false);
          }}
        >
          <div className="grid gap-1.5">
            <Label htmlFor="range-from">From</Label>
            <Input
              id="range-from"
              type="date"
              value={from}
              min={bounds?.min}
              max={to || maxDate}
              onChange={(e) => setFrom(e.target.value)}
            />
          </div>
          <div className="grid gap-1.5">
            <Label htmlFor="range-to">To</Label>
            <Input
              id="range-to"
              type="date"
              value={to}
              min={from || bounds?.min}
              max={maxDate}
              onChange={(e) => setTo(e.target.value)}
            />
          </div>
          <Button type="submit" size="sm" disabled={!from || !to || from > to}>
            Apply
          </Button>
        </form>
      </PopoverContent>
    </Popover>
  );
}

export function PageHeader({
  title,
  description,
  showRange = true,
  actions,
}: {
  title: string;
  description?: string;
  showRange?: boolean;
  actions?: React.ReactNode;
}) {
  return (
    <div className="mb-6 flex flex-wrap items-start justify-between gap-3">
      <div className="min-w-0">
        <h1 className="text-xl font-semibold tracking-tight sm:text-2xl">{title}</h1>
        {description && <p className="mt-1 text-sm text-muted-foreground">{description}</p>}
      </div>
      <div className="flex flex-wrap items-center justify-end gap-2">
        {actions}
        <DataFreshness />
        {showRange && <RangePicker />}
      </div>
    </div>
  );
}

/** Shown in place of page content when the selected range has no orders. */
export function EmptyRange() {
  const { range, setRange } = useSalesData();
  return (
    <div className="flex flex-col items-center gap-3 rounded-xl border border-dashed px-6 py-14 text-center">
      <p className="text-sm font-medium">No orders in {range.label.toLowerCase()}</p>
      <p className="max-w-sm text-sm text-muted-foreground">
        Pick a longer range to see your history.
      </p>
      <Button variant="outline" size="sm" onClick={() => setRange({ preset: "all" })}>
        Show all time
      </Button>
    </div>
  );
}
