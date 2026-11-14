"use client";

import { useCallback, useEffect, useState } from "react";
import { Check, Copy, Loader2, RefreshCw } from "lucide-react";
import { toast } from "sonner";
import type { EtsyConnectionInfo } from "@/lib/etsy/connection";
import { useProfile } from "@/hooks/use-profile";
import { useSalesData } from "@/hooks/use-sales-data";
import { initialParam } from "@/hooks/use-media-query";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";

interface Status {
  connection: EtsyConnectionInfo;
  callbackUrl: string;
  unavailable?: string;
}

const RESULT_MESSAGES: Record<string, { ok: boolean; text: string }> = {
  connected: { ok: true, text: "Etsy shop connected. Run a sync to bring in your orders." },
  declined: { ok: false, text: "Etsy access was declined, so nothing was connected." },
  failed: { ok: false, text: "Etsy authorization failed. Check that the callback URL on your Etsy app matches exactly." },
  "missing-key": { ok: false, text: "Save your Etsy app key first." },
};

/**
 * Connect a shop through the seller's own Etsy app (the solo "Seller App"
 * access tier). Read-only: orders, fees and listings are pulled in; nothing
 * on Etsy is ever changed.
 */
export function EtsyConnectionCard() {
  const { isDemo } = useProfile();
  const { refresh } = useSalesData();
  const [status, setStatus] = useState<Status | null>(null);
  const [keystring, setKeystring] = useState("");
  const [sharedSecret, setSharedSecret] = useState("");
  const [saving, setSaving] = useState(false);
  const [syncing, setSyncing] = useState(false);
  const [copied, setCopied] = useState(false);

  const load = useCallback(async () => {
    const res = await fetch("/api/etsy/connection");
    if (res.ok) setStatus((await res.json()) as Status);
  }, []);

  useEffect(() => {
    let cancelled = false;
    fetch("/api/etsy/connection")
      .then((res) => (res.ok ? (res.json() as Promise<Status>) : null))
      .then((next) => {
        if (next && !cancelled) setStatus(next);
      })
      .catch(() => undefined);
    const result = initialParam("etsy");
    const message = result ? RESULT_MESSAGES[result] : null;
    if (message) (message.ok ? toast.success : toast.error)(message.text);
    return () => {
      cancelled = true;
    };
  }, []);

  async function saveKey(e: React.FormEvent) {
    e.preventDefault();
    setSaving(true);
    try {
      const res = await fetch("/api/etsy/connection", {
        method: "PUT",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ keystring, sharedSecret }),
      });
      const body = (await res.json()) as { error?: string };
      if (!res.ok) throw new Error(body.error ?? "Could not save the key.");
      // Straight on to Etsy to authorize the shop.
      window.location.href = "/api/etsy/connect";
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "Could not save the key.");
      setSaving(false);
    }
  }

  async function sync() {
    setSyncing(true);
    try {
      let orders = 0;
      // A long first sync comes back in parts; keep going until it is done.
      for (let round = 0; round < 20; round++) {
        const res = await fetch("/api/etsy/sync", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ timeZone: Intl.DateTimeFormat().resolvedOptions().timeZone }),
        });
        const body = (await res.json()) as { orders?: number; more?: boolean; error?: string };
        if (!res.ok) throw new Error(body.error ?? "Sync failed.");
        orders += body.orders ?? 0;
        if (!body.more) break;
      }
      toast.success(orders > 0 ? `Synced ${orders} new or changed orders from Etsy.` : "Already up to date.");
      await Promise.all([refresh(), load()]);
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "Sync failed.");
      await load();
    } finally {
      setSyncing(false);
    }
  }

  async function disconnect() {
    await fetch("/api/etsy/connection", { method: "DELETE" });
    setKeystring("");
    setSharedSecret("");
    await load();
    toast.success("Etsy disconnected. Your imported orders are kept.");
  }

  const connection = status?.connection;
  const connected = connection?.status === "connected";

  return (
    <Card id="etsy" className="scroll-mt-6">
      <CardHeader>
        <CardTitle>
          Etsy connection <span className="text-sm font-normal text-muted-foreground">(Coming Soon!)</span>
        </CardTitle>
        <CardDescription>
          Optional. Pull orders, fees and listings straight from Etsy instead of uploading CSVs.
          It uses an API key from your own Etsy developer account and can only read your shop;
          it never changes anything on Etsy.
        </CardDescription>
      </CardHeader>
      <CardContent className="grid max-w-xl grid-cols-[minmax(0,1fr)] gap-4">
        {status?.unavailable ? (
          <p className="text-sm text-muted-foreground">
            The Etsy connection isn&apos;t available on this installation yet (the database
            update for it hasn&apos;t been applied).
          </p>
        ) : !status ? (
          <Loader2 className="size-4 animate-spin text-muted-foreground" />
        ) : connected ? (
          <>
            <div className="text-sm">
              <p className="flex items-center gap-1.5 font-medium">
                <Check className="size-4 text-emerald-600" />
                Connected{connection.shopName ? ` to ${connection.shopName}` : ""}
              </p>
              <p className="mt-1 text-muted-foreground">
                {connection.lastSyncedAt
                  ? `Last synced ${new Date(connection.lastSyncedAt).toLocaleString()}.`
                  : "Not synced yet."}{" "}
                Coupon codes aren&apos;t available from Etsy&apos;s API; they still come from the
                Sold Order Items CSV.
              </p>
              {connection.lastError && (
                <p className="mt-1 text-destructive">Last sync problem: {connection.lastError}</p>
              )}
            </div>
            <div className="flex flex-wrap gap-2">
              <Button onClick={sync} disabled={syncing || isDemo}>
                {syncing ? <Loader2 className="size-4 animate-spin" /> : <RefreshCw className="size-4" />}
                {syncing ? "Syncing…" : "Sync now"}
              </Button>
              <Button variant="outline" onClick={disconnect} disabled={syncing}>
                Disconnect
              </Button>
            </div>
          </>
        ) : (
          <>
            <ol className="ml-5 list-decimal space-y-2 text-sm text-muted-foreground">
              <li>
                In{" "}
                <a
                  href="https://www.etsy.com/developers/your-apps"
                  target="_blank"
                  rel="noopener noreferrer"
                  className="underline"
                >
                  Etsy → Your Apps
                </a>
                , create an app for your own shop (Seller App access) and add this callback URL to
                it, exactly:
                <span className="mt-1.5 flex items-center gap-2">
                  <code className="min-w-0 flex-1 break-all rounded bg-muted px-2 py-1.5 text-xs text-foreground">
                    {status.callbackUrl}
                  </code>
                  <Button
                    type="button"
                    variant="outline"
                    size="sm"
                    onClick={() => {
                      void navigator.clipboard.writeText(status.callbackUrl);
                      setCopied(true);
                    }}
                  >
                    {copied ? <Check className="size-3.5" /> : <Copy className="size-3.5" />}
                    {copied ? "Copied" : "Copy"}
                  </Button>
                </span>
              </li>
              <li>Paste the app&apos;s keystring and shared secret below.</li>
              <li>Approve read access on Etsy when it asks.</li>
            </ol>
            <form onSubmit={saveKey} className="grid grid-cols-[minmax(0,1fr)] gap-3">
              <div className="grid gap-1.5">
                <Label htmlFor="etsy-keystring">Keystring</Label>
                <Input
                  id="etsy-keystring"
                  autoComplete="off"
                  disabled={isDemo}
                  value={keystring}
                  onChange={(e) => setKeystring(e.target.value)}
                  placeholder={connection?.keyHint ? `Saved (ends in ${connection.keyHint})` : ""}
                />
              </div>
              <div className="grid gap-1.5">
                <Label htmlFor="etsy-secret">Shared secret</Label>
                <Input
                  id="etsy-secret"
                  type="password"
                  autoComplete="off"
                  disabled={isDemo}
                  value={sharedSecret}
                  onChange={(e) => setSharedSecret(e.target.value)}
                />
              </div>
              <div className="flex flex-wrap gap-2">
                <Button type="submit" disabled={isDemo || saving || !keystring.trim() || !sharedSecret.trim()}>
                  {saving && <Loader2 className="size-4 animate-spin" />}
                  Save and connect
                </Button>
                {connection?.status !== "none" && (
                  <>
                    <Button type="button" variant="outline" asChild>
                      <a href="/api/etsy/connect">Authorize with saved key</a>
                    </Button>
                    <Button type="button" variant="ghost" onClick={disconnect}>
                      Remove key
                    </Button>
                  </>
                )}
              </div>
              {connection?.lastError && (
                <p className="text-sm text-destructive">{connection.lastError}</p>
              )}
            </form>
          </>
        )}
      </CardContent>
    </Card>
  );
}
