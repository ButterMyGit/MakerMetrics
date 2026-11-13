/**
 * Per-account preferences stored in profiles.settings.prefs. Everything is
 * optional; absent means "use the default".
 */
export interface Prefs {
  /** mask buyer names and exact locations (for screenshots and screen sharing) */
  privacy?: boolean;
  /** the shop's processing time in business days, for on-time shipping */
  processingDays?: number;
  /** let the AI analyst see buyer names (off by default) */
  shareBuyerNames?: boolean;
}

export function parsePrefs(value: unknown): Prefs {
  if (!value || typeof value !== "object") return {};
  const v = value as Record<string, unknown>;
  const prefs: Prefs = {};
  if (typeof v.privacy === "boolean") prefs.privacy = v.privacy;
  if (typeof v.shareBuyerNames === "boolean") prefs.shareBuyerNames = v.shareBuyerNames;
  if (
    typeof v.processingDays === "number" &&
    Number.isFinite(v.processingDays) &&
    v.processingDays >= 0 &&
    v.processingDays <= 90
  ) {
    prefs.processingDays = Math.round(v.processingDays);
  }
  return prefs;
}

/** Read prefs out of a profiles.settings JSON value. */
export function prefsFromSettings(settings: unknown): Prefs {
  if (!settings || typeof settings !== "object") return {};
  return parsePrefs((settings as Record<string, unknown>).prefs);
}
