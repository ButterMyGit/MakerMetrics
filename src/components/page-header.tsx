"use client";

import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { useSalesData, type RangePreset } from "@/hooks/use-sales-data";
import { CalendarRange } from "lucide-react";

const PRESET_LABELS: Record<RangePreset, string> = {
  all: "All time",
  "30d": "Last 30 days",
  "90d": "Last 90 days",
  "12m": "Last 12 months",
  ytd: "This year",
  lastyear: "Last year",
};

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
  const { range, setPreset } = useSalesData();

  return (
    <div className="mb-6 flex flex-wrap items-start justify-between gap-3">
      <div className="min-w-0">
        <h1 className="text-xl font-semibold tracking-tight sm:text-2xl">{title}</h1>
        {description && (
          <p className="mt-1 text-sm text-muted-foreground">{description}</p>
        )}
      </div>
      <div className="flex items-center gap-2">
        {actions}
        {showRange && (
          <Select value={range.preset} onValueChange={(v) => setPreset(v as RangePreset)}>
            <SelectTrigger size="sm" className="gap-2">
              <CalendarRange className="size-4 text-muted-foreground" />
              <SelectValue />
            </SelectTrigger>
            <SelectContent align="end">
              {Object.entries(PRESET_LABELS).map(([value, label]) => (
                <SelectItem key={value} value={value}>
                  {label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        )}
      </div>
    </div>
  );
}
