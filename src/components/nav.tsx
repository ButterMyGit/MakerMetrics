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
import { useEffect, useState } from "react";
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

/**
 * True while the tab bar should be tucked away: the page is being scrolled
 * down, or a text field has focus (the on-screen keyboard is up). Scrolling
 * up, reaching the top, or moving to another page brings it back.
 */
function useTabBarHidden(pathname: string): boolean {
  const [scrolledAway, setScrolledAway] = useState<string | null>(null);
  const [typing, setTyping] = useState(false);

  useEffect(() => {
    let last = window.scrollY;
    const onScroll = () => {
      const max = document.documentElement.scrollHeight - window.innerHeight;
      // clamp so the rubber-band bounce at either end doesn't count as scrolling
      const y = Math.min(Math.max(window.scrollY, 0), Math.max(max, 0));
      const delta = y - last;
      if (Math.abs(delta) < 8) return;
      last = y;
      setScrolledAway(delta > 0 && y > 64 ? pathname : null);
    };
    const isField = (el: EventTarget | null) =>
      el instanceof HTMLElement &&
      (el.tagName === "TEXTAREA" ||
        el.isContentEditable ||
        (el instanceof HTMLInputElement &&
          !["checkbox", "radio", "button", "submit", "range", "file"].includes(el.type)));
    const onFocusIn = (e: FocusEvent) => setTyping(isField(e.target));
    const onFocusOut = () => setTyping(false);

    window.addEventListener("scroll", onScroll, { passive: true });
    document.addEventListener("focusin", onFocusIn);
    document.addEventListener("focusout", onFocusOut);
    return () => {
      window.removeEventListener("scroll", onScroll);
      document.removeEventListener("focusin", onFocusIn);
      document.removeEventListener("focusout", onFocusOut);
    };
  }, [pathname]);

  return typing || scrolledAway === pathname;
}

export function MobileTabBar() {
  const pathname = usePathname();
  const signOut = useSignOut();
  const privacy = usePrivacy();
  const [moreOpen, setMoreOpen] = useState(false);
  const hidden = useTabBarHidden(pathname) && !moreOpen;

  const tabClass = (active: boolean) =>
    cn(
      "flex h-full min-w-0 flex-1 flex-col items-center justify-center gap-0.5 rounded-full text-[10.5px] font-medium transition-colors active:scale-95",
      active ? "bg-primary/12 text-primary" : "text-muted-foreground"
    );
  const moreRow =
    "flex min-h-12 items-center gap-3 rounded-xl px-3 text-left text-[15px] font-medium active:bg-accent hover:bg-accent";

  return (
    <>
      {/* keeps page content from showing through the status bar on full-screen phones */}
      <div
        aria-hidden
        className="pointer-events-none fixed inset-x-0 top-0 z-30 h-[var(--safe-top)] bg-background/85 backdrop-blur lg:hidden"
      />
      <nav
        aria-label="Main"
        className={cn(
          "fixed bottom-[var(--tabbar-gap)] left-[max(0.75rem,var(--safe-left))] right-[max(0.75rem,var(--safe-right))] z-40 mx-auto flex h-[var(--tabbar-h)] max-w-md items-center gap-0.5 rounded-full border border-border/70 bg-background/75 p-1.5 shadow-lg shadow-black/25 backdrop-blur-xl backdrop-saturate-150 transition-transform duration-300 ease-out focus-within:translate-y-0 lg:hidden",
          hidden && "translate-y-[calc(100%+var(--tabbar-gap)+0.5rem)]"
        )}
      >
        {[...MOBILE_TABS, MOBILE_ASSISTANT].map((item) => {
          const active = pathname.startsWith(item.href);
          return (
            <Link
              key={item.href}
              href={item.href}
              aria-current={active ? "page" : undefined}
              className={tabClass(active)}
            >
              <item.icon className="size-5" />
              <span className="max-w-full truncate px-0.5">
                {item.label === "AI Analyst" ? "AI" : item.label === "Dashboard" ? "Home" : item.label}
              </span>
            </Link>
          );
        })}
        <Sheet open={moreOpen} onOpenChange={setMoreOpen}>
          <SheetTrigger asChild>
            <button className={tabClass(MOBILE_MORE.some((i) => pathname.startsWith(i.href)))}>
              <MoreHorizontal className="size-5" />
              More
            </button>
          </SheetTrigger>
          <SheetContent side="bottom" showCloseButton={false} className="gap-0 rounded-t-3xl">
            <div aria-hidden className="mx-auto mt-2.5 h-1 w-9 rounded-full bg-muted-foreground/30" />
            <SheetHeader className="sr-only">
              <SheetTitle>More</SheetTitle>
            </SheetHeader>
            <div className="grid gap-0.5 px-[max(0.75rem,var(--safe-left))] pb-4 pt-3">
              {MOBILE_MORE.map((item) => (
                <Link
                  key={item.href}
                  href={item.href}
                  onClick={() => setMoreOpen(false)}
                  className={cn(moreRow, pathname.startsWith(item.href) && "text-primary")}
                >
                  <item.icon className="size-5" />
                  {item.label}
                </Link>
              ))}
              <button onClick={privacy.toggle} aria-pressed={privacy.enabled} className={moreRow}>
                {privacy.enabled ? <EyeOff className="size-5" /> : <Eye className="size-5" />}
                Privacy mode
                <span className="ml-auto text-sm font-normal text-muted-foreground">
                  {privacy.enabled ? "On" : "Off"}
                </span>
              </button>
              <HelpDialog>
                <button className={moreRow}>
                  <CircleHelp className="size-5" />
                  Help
                </button>
              </HelpDialog>
              <button
                onClick={() => {
                  setMoreOpen(false);
                  void signOut();
                }}
                className={cn(moreRow, "text-muted-foreground")}
              >
                <LogOut className="size-5" />
                Sign out
              </button>
            </div>
          </SheetContent>
        </Sheet>
      </nav>
    </>
  );
}
