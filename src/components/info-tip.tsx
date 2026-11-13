"use client";

import { Info } from "lucide-react";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { METRICS, type MetricId } from "@/lib/analytics/metrics";

/** A small "i" that explains how a number is calculated. Works on tap and click. */
export function InfoTip({ metric, text }: { metric?: MetricId; text?: string }) {
  const body = text ?? (metric ? METRICS[metric].formula : null);
  if (!body) return null;
  const label = metric ? METRICS[metric].label : "this number";
  return (
    <Popover>
      <PopoverTrigger asChild>
        <button
          type="button"
          aria-label={`How ${label} is calculated`}
          className="inline-flex size-4 shrink-0 items-center justify-center rounded-full text-muted-foreground/70 transition-colors hover:text-foreground focus-visible:outline-2 focus-visible:outline-ring"
        >
          <Info className="size-3.5" />
        </button>
      </PopoverTrigger>
      <PopoverContent align="start" className="w-72 text-xs leading-relaxed text-muted-foreground">
        {metric && <p className="mb-1 text-sm font-medium text-foreground">{METRICS[metric].label}</p>}
        {body}
      </PopoverContent>
    </Popover>
  );
}
