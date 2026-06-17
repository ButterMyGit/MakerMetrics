"use client";

import { useEffect, useSyncExternalStore } from "react";
import {
  applyAccent,
  DEFAULT_ACCENT,
  getAccent,
  getServerAccent,
  seedAccent,
  subscribeAccent,
  setAccentUser,
} from "@/lib/theme/accent-store";
import { createClient } from "@/lib/supabase/client";

/**
 * Keeps the document accent CSS variables in sync with the stored color.
 *
 * `initialAccent` is seeded from `profiles.settings.accent` server-side so
 * the color persists across devices and browsers. On first load it seeds
 * localStorage too, so subsequent navigation is instant without a DB round
 * trip. When the user changes the color via AccentPicker, it saves back to
 * Supabase via the `setAccentPersisted` helper exported below.
 */
export function AccentProvider({
  userId,
  initialAccent,
  children,
}: {
  userId?: string;
  initialAccent?: string | null;
  children: React.ReactNode;
}) {
  const accent = useSyncExternalStore(subscribeAccent, getAccent, getServerAccent);

  // Seed localStorage from the DB value the first time (no local value yet).
  useEffect(() => {
    if (!initialAccent) return;
    const local = localStorage.getItem("makermetrics-accent");
    if (!local || local === DEFAULT_ACCENT) {
      localStorage.setItem("makermetrics-accent", initialAccent);
    }
  }, [initialAccent]);

  useEffect(() => {
    if (!userId) return;
    setAccentUser(userId);
    seedAccent(initialAccent ?? getAccent());
  }, [userId, initialAccent]);

  useEffect(() => {
    applyAccent(accent);
  }, [accent]);

  return <>{children}</>;
}

/** Save accent to both localStorage and `profiles.settings.accent` in Supabase. */
export async function setAccentPersisted(hex: string): Promise<void> {
  const { setAccent } = await import("@/lib/theme/accent-store");
  setAccent(hex);
  try {
    const supabase = createClient();
    const {
      data: { user },
    } = await supabase.auth.getUser();
    if (!user) return;
    // Use a raw RPC approach to merge into the JSONB without overwriting
    // other keys in settings.
    await supabase.rpc("set_profile_setting", { key: "accent", value: hex });
  } catch {
    // Saving to DB is best-effort; local change already applied.
  }
}
