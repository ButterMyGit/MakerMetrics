import { redirect } from "next/navigation";
import { createClient } from "@/lib/supabase/server";
import { SalesDataProvider } from "@/hooks/use-sales-data";
import { ProfileProvider } from "@/hooks/use-profile";
import { AccentProvider } from "@/components/accent-provider";
import { DesktopSidebar, MobileTabBar } from "@/components/nav";

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
      email={user.email}
    >
    <AccentProvider userId={user.id} initialAccent={savedAccent}>
      <SalesDataProvider>
        <div className="flex min-h-dvh w-full">
          <DesktopSidebar />
          <main className="min-w-0 flex-1 pb-20 lg:pb-0">
            <div className="mx-auto w-full max-w-6xl px-4 py-6 sm:px-6 lg:px-8">
              {children}
            </div>
          </main>
          <MobileTabBar />
        </div>
      </SalesDataProvider>
    </AccentProvider>
    </ProfileProvider>
  );
}
