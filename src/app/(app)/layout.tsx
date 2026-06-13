import { redirect } from "next/navigation";
import { createClient } from "@/lib/supabase/server";
import { SalesDataProvider } from "@/hooks/use-sales-data";
import { DesktopSidebar, MobileTabBar } from "@/components/nav";

export default async function AppLayout({
  children,
}: Readonly<{ children: React.ReactNode }>) {
  const supabase = await createClient();
  const {
    data: { user },
  } = await supabase.auth.getUser();

  if (!user) redirect("/login");

  return (
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
  );
}
