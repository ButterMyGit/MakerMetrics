"use client";

import { useState } from "react";
import { Loader2 } from "lucide-react";
import { toast } from "sonner";
import { createClient } from "@/lib/supabase/client";
import { useProfile } from "@/hooks/use-profile";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";

const MIN_PASSWORD = 8;

/** Change the email address used to sign in. Supabase confirms it by email first. */
export function EmailForm() {
  const { email, isDemo } = useProfile();
  const [next, setNext] = useState("");
  const [saving, setSaving] = useState(false);
  const [sentTo, setSentTo] = useState<string | null>(null);

  async function submit(e: React.FormEvent) {
    e.preventDefault();
    const address = next.trim().toLowerCase();
    if (!address || address === email?.toLowerCase()) return;
    setSaving(true);
    try {
      const supabase = createClient();
      const { error } = await supabase.auth.updateUser(
        { email: address },
        { emailRedirectTo: `${window.location.origin}/auth/callback` }
      );
      if (error) throw new Error(error.message);
      setSentTo(address);
      setNext("");
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "Could not change the email.");
    } finally {
      setSaving(false);
    }
  }

  return (
    <form onSubmit={submit} className="grid max-w-md gap-3">
      <div className="grid gap-1.5">
        <Label htmlFor="current-email">Current email</Label>
        <Input id="current-email" value={email ?? ""} readOnly disabled />
      </div>
      <div className="grid gap-1.5">
        <Label htmlFor="new-email">New email</Label>
        <Input
          id="new-email"
          type="email"
          autoComplete="email"
          disabled={isDemo}
          value={next}
          onChange={(e) => setNext(e.target.value)}
          placeholder="you@example.com"
        />
      </div>
      {sentTo && (
        <p className="rounded-md bg-muted px-3 py-2 text-sm text-muted-foreground animate-in fade-in duration-200">
          Check your inbox: a confirmation link was sent to{" "}
          <span className="font-medium text-foreground">{sentTo}</span>. Your sign-in email changes
          once you open it. Until then, keep using {email}.
        </p>
      )}
      <Button type="submit" className="w-fit" disabled={isDemo || saving || !next.trim()}>
        {saving && <Loader2 className="size-4 animate-spin" />}
        Change email
      </Button>
    </form>
  );
}

/** Change the password. The current one is checked first, so a borrowed session can't change it. */
export function PasswordForm() {
  const { email, isDemo } = useProfile();
  const [current, setCurrent] = useState("");
  const [next, setNext] = useState("");
  const [confirm, setConfirm] = useState("");
  const [saving, setSaving] = useState(false);

  const tooShort = next.length > 0 && next.length < MIN_PASSWORD;
  const mismatch = confirm.length > 0 && confirm !== next;
  const ready = current.length > 0 && next.length >= MIN_PASSWORD && confirm === next;

  async function submit(e: React.FormEvent) {
    e.preventDefault();
    if (!ready || !email) return;
    setSaving(true);
    try {
      const supabase = createClient();
      const check = await supabase.auth.signInWithPassword({ email, password: current });
      if (check.error) throw new Error("Your current password isn't right.");
      const { error } = await supabase.auth.updateUser({ password: next });
      if (error) throw new Error(error.message);
      setCurrent("");
      setNext("");
      setConfirm("");
      toast.success("Password changed.");
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "Could not change the password.");
    } finally {
      setSaving(false);
    }
  }

  return (
    <form onSubmit={submit} className="grid max-w-md gap-3">
      <div className="grid gap-1.5">
        <Label htmlFor="current-password">Current password</Label>
        <Input
          id="current-password"
          type="password"
          autoComplete="current-password"
          disabled={isDemo}
          value={current}
          onChange={(e) => setCurrent(e.target.value)}
        />
      </div>
      <div className="grid gap-1.5">
        <Label htmlFor="new-password">New password</Label>
        <Input
          id="new-password"
          type="password"
          autoComplete="new-password"
          disabled={isDemo}
          value={next}
          aria-invalid={tooShort}
          onChange={(e) => setNext(e.target.value)}
        />
        <p className={tooShort ? "text-xs text-destructive" : "text-xs text-muted-foreground"}>
          At least {MIN_PASSWORD} characters.
        </p>
      </div>
      <div className="grid gap-1.5">
        <Label htmlFor="confirm-password">Repeat new password</Label>
        <Input
          id="confirm-password"
          type="password"
          autoComplete="new-password"
          disabled={isDemo}
          value={confirm}
          aria-invalid={mismatch}
          onChange={(e) => setConfirm(e.target.value)}
        />
        {mismatch && <p className="text-xs text-destructive">The two passwords don&apos;t match.</p>}
      </div>
      <Button type="submit" className="w-fit" disabled={isDemo || saving || !ready}>
        {saving && <Loader2 className="size-4 animate-spin" />}
        Change password
      </Button>
    </form>
  );
}
