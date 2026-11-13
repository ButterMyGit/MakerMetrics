"use client";

import { useMemo } from "react";
import { useProfile } from "@/hooks/use-profile";
import { maskName } from "@/lib/format";

/**
 * Privacy mode hides who your buyers are (initials only, state instead of
 * city) so the app can be shown in screenshots or on a shared screen.
 */
export function usePrivacy() {
  const { prefs, updatePrefs } = useProfile();
  const enabled = prefs.privacy === true;
  return useMemo(
    () => ({
      enabled,
      toggle: () => updatePrefs({ privacy: enabled ? null : true }),
      name: (name: string) => (enabled ? maskName(name) : name),
      location: (location: string | null) => {
        if (!enabled || !location) return location;
        const parts = location.split(", ");
        return parts.length > 1 ? parts.slice(1).join(", ") : "Hidden";
      },
      /** order numbers are shortened so a screenshot can't be matched to an order */
      orderId: (id: string) => (enabled ? `…${id.slice(-3)}` : id),
    }),
    [enabled, updatePrefs]
  );
}
