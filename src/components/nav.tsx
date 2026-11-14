"use client";

import Link from "next/link";
import Image from "next/image";
import { usePathname, useRouter } from "next/navigation";
import {
  LayoutDashboard,
  Package,
  Receipt,
  Sparkles,
  TrendingUp,
  EyeOff,
  CircleHelp,
  Eye,
  Search,
  Users,
  Settings,
  MoreHorizontal,
  LogOut,
} from "lucide-react";
import { cn } from "@/lib/utils";
import { Sheet, SheetContent, SheetHeader, SheetTitle, SheetTrigger } from "@/components/ui/sheet";
import { createClient } from "@/lib/supabase/client";
import { useProfile } from "@/hooks/use-profile";
import { clearSalesCache } from "@/lib/data/sales-cache";
import { useState } from "react";
import { usePrivacy } from "@/hooks/use-privacy";
import { HelpDialog } from "@/components/help-dialog";

const NAV_ITEMS = [
  { href: "/dashboard", label: "Dashboard", icon: LayoutDashboard },
  { href: "/orders", label: "Orders", icon: Receipt },
  { href: "/products", label: "Products", icon: Package },
  { href: "/customers", label: "Customers", icon: Users },
  { href: "/forecast", label: "Outlook", icon: TrendingUp },
  { href: "/assistant", label: "AI Analyst", icon: Sparkles },
  { href: "/settings", label: "Settings", icon: Settings },
];

function useSignOut() {
  const router = useRouter();
  return async () => {
    const supabase = createClient();
    await supabase.auth.signOut();
    await clearSalesCache();
    router.push("/login");
    router.refresh();
  };
}

export function DesktopSidebar() {
  const pathname = usePathname();
  const signOut = useSignOut();
  const { shopName } = useProfile();
  const privacy = usePrivacy();

  return (
    <aside className="sticky top-0 hidden h-dvh w-60 shrink-0 flex-col border-r bg-sidebar text-sidebar-foreground lg:flex">
      <div className="flex items-center gap-2.5 px-5 py-5">
        <div className="flex size-8 shrink-0 items-center justify-center overflow-hidden rounded-lg bg-background ring-1 ring-border">
          <Image
            src="/makermetrics-logo.png"
            alt="MakerMetrics"
            width={32}
            height={32}
            className="size-full object-cover"
            priority
          />
        </div>
        <div className="flex min-w-0 flex-col">
          <span className="truncate text-base font-semibold tracking-tight">
            {shopName || "MakerMetrics"}
          </span>
          {shopName && (
            <span className="truncate text-xs text-muted-foreground">MakerMetrics</span>
          )}
        </div>
      </div>
      <nav className="flex flex-1 flex-col gap-1 overflow-y-auto px-3">
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
      <div className="grid gap-1 px-3 pb-4">
        <button
          onClick={() =>
            window.dispatchEvent(new KeyboardEvent("keydown", { key: "k", ctrlKey: true }))
          }
          className="flex w-full items-center gap-3 rounded-lg px-3 py-2 text-sm font-medium text-muted-foreground transition-colors hover:bg-sidebar-accent/60 hover:text-sidebar-accent-foreground"
        >
          <Search className="size-4" />
          Search
          <kbd className="ml-auto rounded border px-1.5 text-[10px] font-normal">Ctrl K</kbd>
        </button>
        <button
          onClick={privacy.toggle}
          aria-pressed={privacy.enabled}
          title="Hide buyer names and cities, for screenshots and screen sharing"
          className="flex w-full items-center gap-3 rounded-lg px-3 py-2 text-sm font-medium text-muted-foreground transition-colors hover:bg-sidebar-accent/60 hover:text-sidebar-accent-foreground"
        >
          {privacy.enabled ? <EyeOff className="size-4" /> : <Eye className="size-4" />}
          Privacy mode
          <span className="ml-auto text-xs font-normal">{privacy.enabled ? "On" : "Off"}</span>
        </button>
        <HelpDialog>
          <button className="flex w-full items-center gap-3 rounded-lg px-3 py-2 text-sm font-medium text-muted-foreground transition-colors hover:bg-sidebar-accent/60 hover:text-sidebar-accent-foreground">
            <CircleHelp className="size-4" />
            Help
          </button>
        </HelpDialog>
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
const MOBILE_MORE = [NAV_ITEMS[3], NAV_ITEMS[4], NAV_ITEMS[6]];

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
            <HelpDialog>
              <button className="flex items-center gap-3 rounded-lg px-3 py-3 text-left text-sm font-medium hover:bg-accent">
                <CircleHelp className="size-4.5" />
                Help
              </button>
            </HelpDialog>
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
