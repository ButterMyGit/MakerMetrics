import { Card } from "@/components/ui/card";
import { cn } from "@/lib/utils";
import { formatDelta } from "@/lib/format";
import { ArrowDownRight, ArrowUpRight } from "lucide-react";

export function KpiCard({
  label,
  value,
  delta,
  hint,
  invertDelta = false,
}: {
  label: string;
  value: string;
  /** fractional change vs previous period, e.g. 0.12 = +12% */
  delta?: number | null;
  hint?: string;
  /** set when a decrease is good (e.g. fees) */
  invertDelta?: boolean;
}) {
  const showDelta = delta != null && Number.isFinite(delta);
  const positive = showDelta && (invertDelta ? delta < 0 : delta >= 0);

  return (
    <Card className="gap-1.5 p-4">
      <p className="text-xs font-medium text-muted-foreground">{label}</p>
      <div className="flex items-baseline justify-between gap-2">
        <p className="truncate text-xl font-semibold tabular-nums sm:text-2xl">{value}</p>
        {showDelta && (
          <span
            className={cn(
              "flex shrink-0 items-center gap-0.5 text-xs font-medium tabular-nums",
              positive ? "text-emerald-600 dark:text-emerald-400" : "text-red-600 dark:text-red-400"
            )}
          >
            {delta >= 0 ? (
              <ArrowUpRight className="size-3.5" />
            ) : (
              <ArrowDownRight className="size-3.5" />
            )}
            {formatDelta(delta)}
          </span>
        )}
      </div>
      {hint && <p className="text-[11px] text-muted-foreground">{hint}</p>}
    </Card>
  );
}
