import { Card } from "@/components/ui/card";
import { cn } from "@/lib/utils";
import { formatDelta } from "@/lib/format";
import { ArrowDownRight, ArrowUpRight } from "lucide-react";
import { InfoTip } from "@/components/info-tip";
import type { MetricId } from "@/lib/analytics/metrics";

export function KpiCard({
  label,
  value,
  delta,
  deltaPoints = false,
  hint,
  metric,
  invertDelta = false,
}: {
  label: string;
  value: string;
  /** change vs the comparison period: a fraction (0.12 = +12%) or, with deltaPoints, percentage points */
  delta?: number | null;
  deltaPoints?: boolean;
  hint?: string;
  /** shows an "i" with this metric's formula */
  metric?: MetricId;
  /** set when a decrease is good (e.g. fees) */
  invertDelta?: boolean;
}) {
  const showDelta = delta != null && Number.isFinite(delta) && Math.abs(delta) >= 0.0005;
  const positive = showDelta && (invertDelta ? delta < 0 : delta >= 0);
  const deltaText = showDelta
    ? deltaPoints
      ? `${delta >= 0 ? "+" : ""}${(delta * 100).toFixed(1)} pts`
      : formatDelta(delta)
    : null;

  return (
    <Card className="gap-1.5 p-4">
      <div className="flex items-center gap-1.5">
        <p className="text-xs font-medium text-muted-foreground">{label}</p>
        {metric && <InfoTip metric={metric} />}
      </div>
      <div className="flex flex-wrap items-baseline justify-between gap-x-2">
        <p className="text-xl font-semibold tabular-nums sm:text-2xl">{value}</p>
        {deltaText && (
          <span
            className={cn(
              "flex shrink-0 items-center gap-0.5 text-xs font-medium tabular-nums",
              positive ? "text-emerald-600 dark:text-emerald-400" : "text-red-600 dark:text-red-400"
            )}
          >
            {delta! >= 0 ? (
              <ArrowUpRight className="size-3.5" />
            ) : (
              <ArrowDownRight className="size-3.5" />
            )}
            {deltaText}
          </span>
        )}
      </div>
      {hint && <p className="text-[11px] text-muted-foreground">{hint}</p>}
    </Card>
  );
}
