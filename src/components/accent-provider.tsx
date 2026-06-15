"use client";

import { useEffect, useSyncExternalStore } from "react";
import {
  applyAccent,
  getAccent,
  getServerAccent,
  subscribeAccent,
} from "@/lib/theme/accent-store";

/**
 * Keeps the document's accent CSS variables in sync with the saved color
 * (and across tabs). The initial, flash-free application happens via the
 * blocking inline script in the root layout.
 */
export function AccentProvider({ children }: { children: React.ReactNode }) {
  const accent = useSyncExternalStore(subscribeAccent, getAccent, getServerAccent);

  useEffect(() => {
    applyAccent(accent);
  }, [accent]);

  return <>{children}</>;
}
