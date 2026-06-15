"use client";

import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import Link from "next/link";
import { createClient } from "@/lib/supabase/client";
import { useSalesData } from "@/hooks/use-sales-data";
import { PageHeader } from "@/components/page-header";
import { AccentPicker } from "@/components/accent-picker";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { toast } from "sonner";
import { formatNumber } from "@/lib/format";
import { Loader2, LogOut } from "lucide-react";

export default function SettingsPage() {
  const router = useRouter();
  const { allRows, bounds } = useSalesData();
  const [email, setEmail] = useState<string | null>(null);
  const [shopName, setShopName] = useState("");
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    const supabase = createClient();
    void (async () => {
      const {
        data: { user },
      } = await supabase.auth.getUser();
      if (!user) return;
      setEmail(user.email ?? null);
      const { data } = await supabase
        .from("profiles")
        .select("shop_name")
        .eq("user_id", user.id)
        .maybeSingle();
      if (data?.shop_name) setShopName(data.shop_name);
    })();
  }, []);

  async function saveProfile(e: React.FormEvent) {
    e.preventDefault();
    setSaving(true);
    try {
      const supabase = createClient();
      const {
        data: { user },
      } = await supabase.auth.getUser();
      if (!user) throw new Error("Not signed in");
      const { error } = await supabase
        .from("profiles")
        .upsert({ user_id: user.id, shop_name: shopName.trim() || null });
      if (error) throw new Error(error.message);
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
          </CardHeader>
          <CardContent>
            <form onSubmit={saveProfile} className="grid max-w-md gap-4">
              <div className="grid gap-2">
                <Label htmlFor="shop-name">Shop name</Label>
                <Input
                  id="shop-name"
                  value={shopName}
                  onChange={(e) => setShopName(e.target.value)}
                  placeholder="My Etsy Shop"
                />
              </div>
              <Button type="submit" className="w-fit" disabled={saving}>
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
          <CardContent>
            <Button variant="outline" onClick={signOut}>
              <LogOut className="size-4" />
              Sign out
            </Button>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
