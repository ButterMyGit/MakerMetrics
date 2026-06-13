"use client";

import Link from "next/link";
import { usePathname, useRouter } from "next/navigation";
import {
  BarChart3,
  LayoutDashboard,
  Package,
  Receipt,
  Sparkles,
  TrendingUp,
  Upload,
  Users,
  Settings,
  MoreHorizontal,
  LogOut,
} from "lucide-react";
import { cn } from "@/lib/utils";
import { Sheet, SheetContent, SheetHeader, SheetTitle, SheetTrigger } from "@/components/ui/sheet";
import { createClient } from "@/lib/supabase/client";
import { useState } from "react";

const NAV_ITEMS = [
  { href: "/dashboard", label: "Dashboard", icon: LayoutDashboard },
  { href: "/orders", label: "Orders", icon: Receipt },
  { href: "/products", label: "Products", icon: Package },
  { href: "/customers", label: "Customers", icon: Users },
  { href: "/forecast", label: "Forecast", icon: TrendingUp },
  { href: "/assistant", label: "AI Analyst", icon: Sparkles },
  { href: "/import", label: "Import data", icon: Upload },
  { href: "/settings", label: "Settings", icon: Settings },
];

function useSignOut() {
  const router = useRouter();
  return async () => {
    const supabase = createClient();
    await supabase.auth.signOut();
    router.push("/login");
    router.refresh();
  };
}

export function DesktopSidebar() {
  const pathname = usePathname();
  const signOut = useSignOut();

  return (
    <aside className="hidden lg:flex w-60 shrink-0 flex-col border-r bg-sidebar text-sidebar-foreground">
      <div className="flex items-center gap-2.5 px-5 py-5">
        <div className="flex size-8 items-center justify-center rounded-lg bg-primary text-primary-foreground">
          <BarChart3 className="size-4" />
        </div>
        <span className="text-base font-semibold tracking-tight">MakerMetrics</span>
      </div>
      <nav className="flex flex-1 flex-col gap-1 px-3">
        {NAV_ITEMS.map((item) => {
          const active = pathname.startsWith(item.href);
          return (
            <Link
              key={item.href}
              href={item.href}
              className={cn(
                "flex items-center gap-3 rounded-lg px-3 py-2 text-sm font-medium transition-colors",
                active
                  ? "bg-sidebar-accent text-sidebar-accent-foreground"
                  : "text-muted-foreground hover:bg-sidebar-accent/60 hover:text-sidebar-accent-foreground"
              )}
            >
              <item.icon className="size-4" />
              {item.label}
            </Link>
          );
        })}
      </nav>
      <div className="px-3 pb-4">
        <button
          onClick={signOut}
          className="flex w-full items-center gap-3 rounded-lg px-3 py-2 text-sm font-medium text-muted-foreground transition-colors hover:bg-sidebar-accent/60 hover:text-sidebar-accent-foreground"
        >
          <LogOut className="size-4" />
          Sign out
        </button>
      </div>
    </aside>
  );
}

const MOBILE_TABS = NAV_ITEMS.slice(0, 3); // Dashboard, Orders, Products
const MOBILE_ASSISTANT = NAV_ITEMS[5];
const MOBILE_MORE = [NAV_ITEMS[3], NAV_ITEMS[4], NAV_ITEMS[6], NAV_ITEMS[7]];

export function MobileTabBar() {
  const pathname = usePathname();
  const signOut = useSignOut();
  const [moreOpen, setMoreOpen] = useState(false);

  const tabClass = (active: boolean) =>
    cn(
      "flex flex-1 flex-col items-center justify-center gap-0.5 py-2 text-[11px] font-medium",
      active ? "text-primary" : "text-muted-foreground"
    );

  return (
    <nav className="fixed inset-x-0 bottom-0 z-40 flex border-t bg-background/95 backdrop-blur pb-[env(safe-area-inset-bottom)] lg:hidden">
      {[...MOBILE_TABS, MOBILE_ASSISTANT].map((item) => (
        <Link
          key={item.href}
          href={item.href}
          className={tabClass(pathname.startsWith(item.href))}
        >
          <item.icon className="size-5" />
          {item.label === "AI Analyst" ? "AI" : item.label}
        </Link>
      ))}
      <Sheet open={moreOpen} onOpenChange={setMoreOpen}>
        <SheetTrigger asChild>
          <button
            className={tabClass(MOBILE_MORE.some((i) => pathname.startsWith(i.href)))}
          >
            <MoreHorizontal className="size-5" />
            More
          </button>
        </SheetTrigger>
        <SheetContent side="bottom" className="rounded-t-2xl pb-[env(safe-area-inset-bottom)]">
          <SheetHeader>
            <SheetTitle>More</SheetTitle>
          </SheetHeader>
          <div className="grid gap-1 px-4 pb-6">
            {MOBILE_MORE.map((item) => (
              <Link
                key={item.href}
                href={item.href}
                onClick={() => setMoreOpen(false)}
                className="flex items-center gap-3 rounded-lg px-3 py-3 text-sm font-medium hover:bg-accent"
              >
                <item.icon className="size-4.5" />
                {item.label}
              </Link>
            ))}
            <button
              onClick={() => {
                setMoreOpen(false);
                void signOut();
              }}
              className="flex items-center gap-3 rounded-lg px-3 py-3 text-left text-sm font-medium text-muted-foreground hover:bg-accent"
            >
              <LogOut className="size-4.5" />
              Sign out
            </button>
          </div>
        </SheetContent>
      </Sheet>
    </nav>
  );
}
