"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";
import Link from "next/link";
import { createClient } from "@/lib/supabase/client";
import { useSalesData } from "@/hooks/use-sales-data";
import { useProfile } from "@/hooks/use-profile";
import { PageHeader } from "@/components/page-header";
import { AccentPicker } from "@/components/accent-picker";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { toast } from "sonner";
import { formatNumber } from "@/lib/format";
import { Loader2, LogOut, Trash2, TriangleAlert } from "lucide-react";

export default function SettingsPage() {
  const router = useRouter();
  const { allRows, bounds } = useSalesData();
  const { shopName, setShopName, email } = useProfile();
  const [draftName, setDraftName] = useState(shopName ?? "");
  const [saving, setSaving] = useState(false);

  async function saveProfile(e: React.FormEvent) {
    e.preventDefault();
    setSaving(true);
    try {
      const supabase = createClient();
      const {
        data: { user },
      } = await supabase.auth.getUser();
      if (!user) throw new Error("Not signed in");
      const trimmed = draftName.trim() || null;
      const { error } = await supabase
        .from("profiles")
        .upsert({ user_id: user.id, shop_name: trimmed });
      if (error) throw new Error(error.message);
      setShopName(trimmed);
      toast.success("Settings saved.");
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "Failed to save");
    } finally {
      setSaving(false);
    }
  }

  async function signOut() {
    const supabase = createClient();
    await supabase.auth.signOut();
    router.push("/login");
    router.refresh();
  }

  return (
    <div>
      <PageHeader title="Settings" showRange={false} />

      <div className="grid gap-4">
        <Card>
          <CardHeader>
            <CardTitle>Shop</CardTitle>
            <CardDescription>Shown in the sidebar and across the app.</CardDescription>
          </CardHeader>
          <CardContent>
            <form onSubmit={saveProfile} className="grid max-w-md gap-4">
              <div className="grid gap-2">
                <Label htmlFor="shop-name">Shop name</Label>
                <Input
                  id="shop-name"
                  value={draftName}
                  onChange={(e) => setDraftName(e.target.value)}
                  placeholder="My Etsy Shop"
                />
              </div>
              <Button
                type="submit"
                className="w-fit"
                disabled={saving || draftName.trim() === (shopName ?? "")}
              >
                {saving && <Loader2 className="size-4 animate-spin" />}
                Save
              </Button>
            </form>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Appearance</CardTitle>
            <CardDescription>
              Pick the accent color used across charts and highlights.
            </CardDescription>
          </CardHeader>
          <CardContent>
            <AccentPicker />
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Data</CardTitle>
            <CardDescription>
              {formatNumber(allRows.length)} transactions imported
              {bounds ? ` (${bounds.min} → ${bounds.max})` : ""}.
            </CardDescription>
          </CardHeader>
          <CardContent>
            <Button variant="outline" asChild>
              <Link href="/import">Manage data</Link>
            </Button>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Account</CardTitle>
            <CardDescription>{email ?? "…"}</CardDescription>
          </CardHeader>
          <CardContent className="flex flex-wrap gap-2">
            <Button variant="outline" onClick={signOut}>
              <LogOut className="size-4" />
              Sign out
            </Button>
            <DeleteAccountDialog />
          </CardContent>
        </Card>
      </div>
    </div>
  );
}

function DeleteAccountDialog() {
  const router = useRouter();
  const [open, setOpen] = useState(false);
  const [confirmText, setConfirmText] = useState("");
  const [deleting, setDeleting] = useState(false);

  async function deleteAccount() {
    setDeleting(true);
    try {
      const res = await fetch("/api/account/delete", { method: "POST" });
      const body = (await res.json()) as { ok?: boolean; error?: string };
      if (!res.ok || !body.ok) {
        throw new Error(body.error ?? "Failed to delete account");
      }
      // Ensure local session is cleared, then leave.
      const supabase = createClient();
      await supabase.auth.signOut();
      toast.success("Your account and all data were deleted.");
      router.push("/login");
      router.refresh();
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "Failed to delete account");
      setDeleting(false);
    }
  }

  return (
    <Dialog
      open={open}
      onOpenChange={(o) => {
        setOpen(o);
        if (!o) setConfirmText("");
      }}
    >
      <Button variant="destructive" onClick={() => setOpen(true)}>
        <Trash2 className="size-4" />
        Delete account
      </Button>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2 text-destructive">
            <TriangleAlert className="size-5" />
            Delete account
          </DialogTitle>
          <DialogDescription>
            This permanently deletes your account and{" "}
            <span className="font-medium text-foreground">all imported sales data</span>. This
            cannot be undone. Type <span className="font-mono font-semibold">DELETE</span> to
            confirm.
          </DialogDescription>
        </DialogHeader>
        <Input
          value={confirmText}
          onChange={(e) => setConfirmText(e.target.value)}
          placeholder="DELETE"
          autoComplete="off"
        />
        <DialogFooter>
          <Button variant="outline" onClick={() => setOpen(false)} disabled={deleting}>
            Cancel
          </Button>
          <Button
            variant="destructive"
            disabled={confirmText !== "DELETE" || deleting}
            onClick={deleteAccount}
          >
            {deleting && <Loader2 className="size-4 animate-spin" />}
            Delete everything
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
