import { redirect } from "next/navigation";
import { createClient } from "@/lib/supabase/server";
import { SalesDataProvider } from "@/hooks/use-sales-data";
import { ProfileProvider } from "@/hooks/use-profile";
import { AccentProvider } from "@/components/accent-provider";
import { DesktopSidebar, MobileTabBar } from "@/components/nav";
import { CommandPalette } from "@/components/command-palette";
import { PageTransition } from "@/components/page-transition";
import { isDemoEmail } from "@/lib/demo-account";
import { prefsFromSettings } from "@/lib/prefs";

export default async function AppLayout({
  children,
}: Readonly<{ children: React.ReactNode }>) {
  const supabase = await createClient();
  // The proxy has already refreshed the session; this only verifies the token.
  const { data } = await supabase.auth.getClaims();
  const claims = data?.claims;

  if (!claims?.sub) redirect("/login");

  const user = {
    id: claims.sub,
    email: typeof claims.email === "string" ? claims.email : null,
  };
  const appMetadata = (claims.app_metadata ?? {}) as Record<string, unknown>;
  const isDemo = appMetadata.is_demo === true || isDemoEmail(user.email);

  const { data: profile } = await supabase
    .from("profiles")
    .select("shop_name, settings")
    .eq("user_id", user.id)
    .maybeSingle();

  const savedAccent =
    typeof profile?.settings === "object" &&
    profile.settings !== null &&
    typeof (profile.settings as Record<string, unknown>).accent === "string"
      ? (profile.settings as Record<string, string>).accent
      : null;

  return (
    <ProfileProvider
      userId={user.id}
      initialShopName={profile?.shop_name ?? null}
      initialPrefs={prefsFromSettings(profile?.settings)}
      email={user.email}
      isDemo={isDemo}
    >
    <AccentProvider userId={user.id} initialAccent={savedAccent}>
      <SalesDataProvider>
        <div className="flex min-h-dvh w-full">
          <DesktopSidebar />
          <main className="min-w-0 flex-1 pb-20 lg:pb-0">
            <div className="mx-auto w-full max-w-[1400px] px-4 py-6 sm:px-6 lg:px-8">
              <PageTransition>{children}</PageTransition>
            </div>
          </main>
          <MobileTabBar />
          <CommandPalette />
        </div>
      </SalesDataProvider>
    </AccentProvider>
    </ProfileProvider>
  );
}
