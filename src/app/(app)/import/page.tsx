"use client";

import { useCallback, useMemo, useRef, useState } from "react";
import { useRouter } from "next/navigation";
import { useSalesData } from "@/hooks/use-sales-data";
import { buildSaleItems, parseCsvFile, type ParsedCsv } from "@/lib/etsy/parse";
import { createClient } from "@/lib/supabase/client";
import { PageHeader } from "@/components/page-header";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Progress } from "@/components/ui/progress";
import { toast } from "sonner";
import { AlertTriangle, FileSpreadsheet, Loader2, Trash2, UploadCloud, X } from "lucide-react";
import { formatNumber } from "@/lib/format";

const BATCH_SIZE = 500;

const FORMAT_LABELS: Record<ParsedCsv["format"], { label: string; tone: "ok" | "warn" }> = {
  items: { label: "Order items", tone: "ok" },
  orders: { label: "Orders", tone: "ok" },
  combined: { label: "Combined export", tone: "ok" },
  payments: { label: "Payments (refunds)", tone: "ok" },
  unknown: { label: "Unrecognized", tone: "warn" },
};

export default function ImportPage() {
  const router = useRouter();
  const { refresh } = useSalesData();
  const [files, setFiles] = useState<ParsedCsv[]>([]);
  const [dragging, setDragging] = useState(false);
  const [importing, setImporting] = useState(false);
  const [progress, setProgress] = useState(0);
  const inputRef = useRef<HTMLInputElement>(null);

  const addFiles = useCallback(async (list: FileList | File[]) => {
    const csvs = [...list].filter((f) => f.name.toLowerCase().endsWith(".csv"));
    if (csvs.length === 0) {
      toast.error("Please choose .csv files exported from Etsy.");
      return;
    }
    try {
      const parsed = await Promise.all(csvs.map(parseCsvFile));
      setFiles((prev) => [
        ...prev.filter((p) => !parsed.some((n) => n.fileName === p.fileName)),
        ...parsed,
      ]);
    } catch {
      toast.error("Failed to parse one of the files.");
    }
  }, []);

  const result = useMemo(() => (files.length > 0 ? buildSaleItems(files) : null), [files]);

  async function runImport() {
    if (!result || result.rows.length === 0) return;
    setImporting(true);
    setProgress(0);
    try {
      const supabase = createClient();
      const {
        data: { user },
      } = await supabase.auth.getUser();
      if (!user) throw new Error("Not signed in");

      // Without a payments file in this batch, don't send refund_amount at all,
      // so a prior refund import isn't overwritten with null.
      const rows = result.rows.map((r) => {
        const row: Record<string, unknown> = { ...r, user_id: user.id };
        if (!result.includesRefunds) delete row.refund_amount;
        return row;
      });
      for (let i = 0; i < rows.length; i += BATCH_SIZE) {
        const batch = rows.slice(i, i + BATCH_SIZE);
        const { error } = await supabase
          .from("sale_items")
          .upsert(batch, { onConflict: "user_id,transaction_id" });
        if (error) throw new Error(error.message);
        setProgress(Math.round(((i + batch.length) / rows.length) * 100));
      }

      toast.success(`Imported ${formatNumber(rows.length)} transactions.`);
      setFiles([]);
      await refresh();
      router.push("/dashboard");
    } catch (e) {
      toast.error(e instanceof Error ? e.message : "Import failed");
    } finally {
      setImporting(false);
    }
  }

  return (
    <div>
      <PageHeader
        title="Import data"
        description="Upload the CSVs Etsy gives you — no API access needed."
        showRange={false}
      />

      <Card>
        <CardHeader>
          <CardTitle>How to export from Etsy</CardTitle>
          <CardDescription>
            Shop Manager → Settings → Options → Download Data. Download{" "}
            <span className="font-medium text-foreground">Sold Order Items</span> (required)
            and <span className="font-medium text-foreground">Sold Orders</span> (recommended
            — adds exact order totals and fees) for each year. Re-importing the same files is
            always safe: rows are deduplicated by transaction.
          </CardDescription>
        </CardHeader>
        <CardContent className="grid gap-4">
          <button
            type="button"
            onClick={() => inputRef.current?.click()}
            onDragOver={(e) => {
              e.preventDefault();
              setDragging(true);
            }}
            onDragLeave={() => setDragging(false)}
            onDrop={(e) => {
              e.preventDefault();
              setDragging(false);
              void addFiles(e.dataTransfer.files);
            }}
            className={`flex flex-col items-center justify-center gap-3 rounded-xl border-2 border-dashed px-6 py-12 text-center transition-colors ${
              dragging ? "border-primary bg-primary/5" : "border-border hover:bg-accent/40"
            }`}
          >
            <UploadCloud className="size-8 text-muted-foreground" />
            <div>
              <p className="text-sm font-medium">Drop CSV files here or tap to browse</p>
              <p className="mt-1 text-xs text-muted-foreground">
                EtsySoldOrderItemsYYYY.csv · EtsySoldOrdersYYYY.csv
              </p>
            </div>
          </button>
          <input
            ref={inputRef}
            type="file"
            accept=".csv,text/csv"
            multiple
            hidden
            onChange={(e) => {
              if (e.target.files) void addFiles(e.target.files);
              e.target.value = "";
            }}
          />

          {files.length > 0 && (
            <div className="grid gap-2">
              {files.map((f) => {
                const fmt = FORMAT_LABELS[f.format];
                return (
                  <div
                    key={f.fileName}
                    className="flex items-center justify-between gap-3 rounded-lg border px-3 py-2.5"
                  >
                    <div className="flex min-w-0 items-center gap-2.5">
                      <FileSpreadsheet className="size-4 shrink-0 text-muted-foreground" />
                      <span className="truncate text-sm font-medium">{f.fileName}</span>
                    </div>
                    <div className="flex shrink-0 items-center gap-2">
                      <span className="hidden text-xs text-muted-foreground sm:inline">
                        {formatNumber(f.rows.length)} rows
                      </span>
                      <Badge variant={fmt.tone === "ok" ? "secondary" : "destructive"}>
                        {fmt.label}
                      </Badge>
                      <Button
                        variant="ghost"
                        size="icon"
                        className="size-7"
                        onClick={() =>
                          setFiles((prev) => prev.filter((p) => p.fileName !== f.fileName))
                        }
                      >
                        <X className="size-4" />
                      </Button>
                    </div>
                  </div>
                );
              })}
            </div>
          )}

          {result?.warnings.map((w) => (
            <Alert key={w} variant="destructive">
              <AlertTriangle className="size-4" />
              <AlertTitle>Check your files</AlertTitle>
              <AlertDescription>{w}</AlertDescription>
            </Alert>
          ))}

          {result && result.rows.length > 0 && (
            <div className="flex flex-col gap-3 rounded-lg bg-muted/50 p-4 sm:flex-row sm:items-center sm:justify-between">
              <p className="text-sm">
                Ready to import{" "}
                <span className="font-semibold">{formatNumber(result.rows.length)}</span>{" "}
                transactions.
              </p>
              <Button onClick={runImport} disabled={importing}>
                {importing && <Loader2 className="size-4 animate-spin" />}
                {importing ? `Importing… ${progress}%` : "Import"}
              </Button>
            </div>
          )}
          {importing && <Progress value={progress} />}
        </CardContent>
      </Card>

      <DangerZone />
    </div>
  );
}

function DangerZone() {
  const { refresh, hasData } = useSalesData();
  const [confirming, setConfirming] = useState(false);
  const [deleting, setDeleting] = useState(false);

  if (!hasData) return null;

  async function deleteAll() {
    setDeleting(true);
    try {
      const supabase = createClient();
      const {
        data: { user },
      } = await supabase.auth.getUser();
      if (!user) throw new Error("Not signed in");
      const { error } = await supabase.from("sale_items").delete().eq("user_id", user.id);
      if (error) throw new Error(error.message);
      toast.success("All sales data deleted.");
      await refresh();
    } catch (e) {
      toast.error(e instanceof Error ? e.message : "Delete failed");
    } finally {
      setDeleting(false);
      setConfirming(false);
    }
  }

  return (
    <Card className="mt-4 border-destructive/40">
      <CardHeader>
        <CardTitle className="text-destructive">Danger zone</CardTitle>
        <CardDescription>
          Remove every imported transaction from your account. You can re-import your CSVs at
          any time.
        </CardDescription>
      </CardHeader>
      <CardContent>
        {confirming ? (
          <div className="flex flex-wrap items-center gap-2">
            <Button variant="destructive" onClick={deleteAll} disabled={deleting}>
              {deleting && <Loader2 className="size-4 animate-spin" />}
              Yes, delete everything
            </Button>
            <Button variant="outline" onClick={() => setConfirming(false)} disabled={deleting}>
              Cancel
            </Button>
          </div>
        ) : (
          <Button variant="outline" onClick={() => setConfirming(true)}>
            <Trash2 className="size-4" />
            Delete all data
          </Button>
        )}
      </CardContent>
    </Card>
  );
}
