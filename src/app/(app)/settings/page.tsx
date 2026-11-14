"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";
import Link from "next/link";
import { Loader2, LogOut, Trash2, TriangleAlert, Upload } from "lucide-react";
import { toast } from "sonner";
import { createClient } from "@/lib/supabase/client";
import { clearSalesCache } from "@/lib/data/sales-cache";
import { useSalesData } from "@/hooks/use-sales-data";
import { useProfile } from "@/hooks/use-profile";
import { usePrivacy } from "@/hooks/use-privacy";
import { PageHeader } from "@/components/page-header";
import { AccentPicker } from "@/components/accent-picker";
import { AiSettingsCard } from "@/components/ai-settings-card";
import { EtsyConnectionCard } from "@/components/etsy-connection-card";
import { EmailForm, PasswordForm } from "@/components/settings/login-settings";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Switch } from "@/components/ui/switch";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { formatDate, formatNumber } from "@/lib/format";

const SECTIONS = [
  { id: "shop", label: "Shop" },
  { id: "login", label: "Sign-in" },
  { id: "data", label: "Data" },
  { id: "analyst", label: "AI Analyst" },
  { id: "display", label: "Display" },
  { id: "account", label: "Account" },
] as const;

function Section({
  id,
  title,
  children,
}: {
  id: string;
  title: string;
  children: React.ReactNode;
}) {
  return (
    <section id={id} className="scroll-mt-6">
      <h2 className="mb-3 text-xs font-medium uppercase tracking-wide text-muted-foreground">
        {title}
      </h2>
      <div className="grid gap-4">{children}</div>
    </section>
  );
}

function ShopCard() {
  const { shopName, setShopName, isDemo, prefs, updatePrefs } = useProfile();
  const [draftName, setDraftName] = useState(shopName ?? "");
  const [draftDays, setDraftDays] = useState(
    prefs.processingDays != null ? String(prefs.processingDays) : ""
  );
  const [saving, setSaving] = useState(false);

  const nameChanged = draftName.trim() !== (shopName ?? "");
  const daysChanged = draftDays.trim() !== (prefs.processingDays != null ? String(prefs.processingDays) : "");

  async function save(e: React.FormEvent) {
    e.preventDefault();
    setSaving(true);
    try {
      if (nameChanged) {
        const supabase = createClient();
        const {
          data: { user },
        } = await supabase.auth.getUser();
        if (!user) throw new Error("Not signed in");
        const trimmed = draftName.trim() || null;
        const { error } = await supabase.from("profiles").upsert({ user_id: user.id, shop_name: trimmed });
        if (error) throw new Error(error.message);
        setShopName(trimmed);
      }
      if (daysChanged) {
        const n = Number(draftDays);
        updatePrefs({
          processingDays: draftDays.trim() === "" || !Number.isFinite(n) ? null : n,
        });
      }
      toast.success("Saved.");
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "Failed to save");
    } finally {
      setSaving(false);
    }
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle>Your shop</CardTitle>
        <CardDescription>
          The name appears in the sidebar. Processing time is what on-time shipping is measured
          against.
        </CardDescription>
      </CardHeader>
      <CardContent>
        <form onSubmit={save} className="grid max-w-md gap-4">
          <div className="grid gap-1.5">
            <Label htmlFor="shop-name">Shop name</Label>
            <Input
              id="shop-name"
              value={draftName}
              disabled={isDemo}
              onChange={(e) => setDraftName(e.target.value)}
              placeholder="My Etsy Shop"
            />
          </div>
          <div className="grid gap-1.5">
            <Label htmlFor="processing-days">Processing time</Label>
            <div className="flex items-center gap-2">
              <Input
                id="processing-days"
                type="number"
                min={0}
                max={90}
                value={draftDays}
                onChange={(e) => setDraftDays(e.target.value)}
                placeholder="e.g. 3"
                className="w-24"
              />
              <span className="text-sm text-muted-foreground">business days to ship an order</span>
            </div>
          </div>
          <Button type="submit" className="w-fit" disabled={saving || (!nameChanged && !daysChanged)}>
            {saving && <Loader2 className="size-4 animate-spin" />}
            Save
          </Button>
        </form>
      </CardContent>
    </Card>
  );
}

export default function SettingsPage() {
  const router = useRouter();
  const { allOrders, lineItems, bounds, through } = useSalesData();
  const { email, isDemo } = useProfile();
  const privacy = usePrivacy();
  const units = allOrders.reduce((s, o) => s + o.units, 0);

  async function signOut() {
    const supabase = createClient();
    await supabase.auth.signOut();
    await clearSalesCache();
    router.push("/login");
    router.refresh();
  }

  return (
    <div>
      <PageHeader title="Settings" showRange={false} />

      <div className="gap-10 lg:grid lg:grid-cols-[160px_minmax(0,1fr)]">
        <nav aria-label="Settings sections" className="hidden lg:block">
          <ul className="sticky top-6 grid gap-0.5 text-sm">
            {SECTIONS.map((s) => (
              <li key={s.id}>
                <a
                  href={`#${s.id}`}
                  className="block rounded-md px-3 py-1.5 text-muted-foreground transition-colors hover:bg-accent hover:text-foreground"
                >
                  {s.label}
                </a>
              </li>
            ))}
          </ul>
        </nav>

        <div className="stagger grid max-w-3xl gap-8">
          <Section id="shop" title="Shop">
            <ShopCard />
          </Section>

          <Section id="login" title="Sign-in">
            <Card>
              <CardHeader>
                <CardTitle>Email</CardTitle>
                <CardDescription>
                  {isDemo
                    ? "The demo account's sign-in details can't be changed."
                    : "The address you sign in with. A change takes effect after you confirm it from the new inbox."}
                </CardDescription>
              </CardHeader>
              <CardContent>
                <EmailForm />
              </CardContent>
            </Card>
            <Card>
              <CardHeader>
                <CardTitle>Password</CardTitle>
                <CardDescription>You&apos;ll stay signed in on this device after changing it.</CardDescription>
              </CardHeader>
              <CardContent>
                <PasswordForm />
              </CardContent>
            </Card>
          </Section>

          <Section id="data" title="Data">
            <Card>
              <CardHeader>
                <CardTitle>Imported sales</CardTitle>
                <CardDescription>
                  {allOrders.length === 0
                    ? "Nothing imported yet."
                    : `${formatNumber(allOrders.length)} orders containing ${formatNumber(lineItems)} line items and ${formatNumber(units)} units${
                        bounds ? `, from ${formatDate(bounds.min)} to ${formatDate(through ?? bounds.max)}` : ""
                      }. An order can hold several line items (different products), and a line item can be more than one unit.`}
                </CardDescription>
              </CardHeader>
              <CardContent>
                <Button variant="outline" asChild>
                  <Link href="/import">
                    <Upload className="size-4" />
                    Import or manage data
                  </Link>
                </Button>
              </CardContent>
            </Card>
            <EtsyConnectionCard />
          </Section>

          <Section id="analyst" title="AI Analyst">
            <AiSettingsCard />
          </Section>

          <Section id="display" title="Display">
            <Card>
              <CardHeader>
                <CardTitle>Accent color</CardTitle>
                <CardDescription>
                  {isDemo ? "The demo account uses the saved demo theme." : "Used for charts and highlights."}
                </CardDescription>
              </CardHeader>
              <CardContent>
                <AccentPicker disabled={isDemo} />
              </CardContent>
            </Card>
            <Card>
              <CardHeader>
                <CardTitle>Privacy mode</CardTitle>
                <CardDescription>
                  Shows buyers as initials and locations as a state or country, and shortens order
                  numbers. Use it for screenshots or when sharing your screen.
                </CardDescription>
              </CardHeader>
              <CardContent>
                <div className="flex items-center gap-3">
                  <Switch id="privacy-mode" checked={privacy.enabled} onCheckedChange={privacy.toggle} />
                  <Label htmlFor="privacy-mode">{privacy.enabled ? "On" : "Off"}</Label>
                </div>
              </CardContent>
            </Card>
          </Section>

          <Section id="account" title="Account">
            <Card>
              <CardHeader>
                <CardTitle>Signed in as {email ?? "…"}</CardTitle>
                <CardDescription>
                  Deleting your account removes it and everything you imported, permanently.
                </CardDescription>
              </CardHeader>
              <CardContent className="flex flex-wrap gap-2">
                <Button variant="outline" onClick={signOut}>
                  <LogOut className="size-4" />
                  Sign out
                </Button>
                <DeleteAccountDialog isDemo={isDemo} />
              </CardContent>
            </Card>
          </Section>
        </div>
      </div>
    </div>
  );
}

function DeleteAccountDialog({ isDemo }: { isDemo: boolean }) {
  const router = useRouter();
  const [open, setOpen] = useState(false);
  const [confirmText, setConfirmText] = useState("");
  const [deleting, setDeleting] = useState(false);

  async function deleteAccount() {
    if (isDemo) {
      toast.error("The demo account is read-only.");
      return;
    }
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
      await clearSalesCache();
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
      <Button variant="destructive" onClick={() => setOpen(true)} disabled={isDemo}>
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
