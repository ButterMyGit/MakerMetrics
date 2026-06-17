export const DEMO_EMAIL = process.env.NEXT_PUBLIC_DEMO_EMAIL?.trim().toLowerCase() ?? "";

export function isDemoEmail(email?: string | null): boolean {
  return Boolean(DEMO_EMAIL && email?.trim().toLowerCase() === DEMO_EMAIL);
}
