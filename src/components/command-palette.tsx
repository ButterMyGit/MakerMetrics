"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { useRouter } from "next/navigation";
import { Search } from "lucide-react";
import { Dialog, DialogContent, DialogDescription, DialogTitle } from "@/components/ui/dialog";
import { useSalesData } from "@/hooks/use-sales-data";
import { usePrivacy } from "@/hooks/use-privacy";
import { productStats } from "@/lib/analytics/core";
import { PRESET_LABELS, type RangePreset } from "@/lib/range-store";
import { cn } from "@/lib/utils";

interface Command {
  id: string;
  group: string;
  label: string;
  hint?: string;
  run: () => void;
}

const PAGES: { href: string; label: string }[] = [
  { href: "/dashboard", label: "Dashboard" },
  { href: "/orders", label: "Orders" },
  { href: "/orders?tab=fulfillment", label: "Fulfillment" },
  { href: "/orders?tab=coupons", label: "Coupons" },
  { href: "/products", label: "Products" },
  { href: "/customers", label: "Customers" },
  { href: "/forecast", label: "Outlook" },
  { href: "/assistant", label: "AI Analyst" },
  { href: "/import", label: "Import data" },
  { href: "/settings", label: "Settings" },
];

/** Cmd/Ctrl+K: jump to a page, product or order, or change the date range. */
export function CommandPalette() {
  const router = useRouter();
  const { allOrders, setRange } = useSalesData();
  const privacy = usePrivacy();
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const [active, setActive] = useState(0);
  const listRef = useRef<HTMLUListElement>(null);

  useEffect(() => {
    function onKey(e: KeyboardEvent) {
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "k") {
        e.preventDefault();
        setOpen((v) => !v);
      }
    }
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);

  const commands = useMemo<Command[]>(() => {
    if (!open) return [];
    const go = (href: string) => () => router.push(href);
    const list: Command[] = PAGES.map((p) => ({
      id: `page:${p.href}`,
      group: "Go to",
      label: p.label,
      run: go(p.href),
    }));
    for (const [preset, label] of Object.entries(PRESET_LABELS)) {
      list.push({
        id: `range:${preset}`,
        group: "Date range",
        label,
        run: () => setRange({ preset: preset as RangePreset }),
      });
    }
    list.push({
      id: "privacy",
      group: "Actions",
      label: privacy.enabled ? "Turn off privacy mode" : "Turn on privacy mode",
      hint: "Hide buyer names",
      run: privacy.toggle,
    });
    for (const p of productStats(allOrders)) {
      list.push({
        id: `product:${p.key}`,
        group: "Products",
        label: p.name,
        hint: `${p.units} sold`,
        run: go(`/products?product=${encodeURIComponent(p.key)}`),
      });
    }
    for (let i = allOrders.length - 1; i >= 0; i--) {
      const o = allOrders[i];
      if (!o.orderId) continue;
      list.push({
        id: `order:${o.key}`,
        group: "Orders",
        label: `#${privacy.orderId(o.orderId)} · ${privacy.name(o.buyerName)}`,
        hint: o.itemsLabel,
        run: go(`/orders?order=${encodeURIComponent(o.key)}`),
      });
    }
    return list;
  }, [open, allOrders, privacy, router, setRange]);

  const results = useMemo(() => {
    const q = query.trim().toLowerCase();
    if (!q) return commands.filter((c) => c.group !== "Products" && c.group !== "Orders");
    return commands
      .filter((c) => `${c.label} ${c.hint ?? ""}`.toLowerCase().includes(q))
      .slice(0, 30);
  }, [commands, query]);

  function choose(command: Command | undefined) {
    if (!command) return;
    setOpen(false);
    setQuery("");
    setActive(0);
    command.run();
  }

  return (
    <Dialog
      open={open}
      onOpenChange={(o) => {
        setOpen(o);
        if (!o) {
          setQuery("");
          setActive(0);
        }
      }}
    >
      <DialogContent showCloseButton={false} className="top-[20%] translate-y-0 gap-0 overflow-hidden p-0 sm:max-w-lg">
        <DialogTitle className="sr-only">Search and commands</DialogTitle>
        <DialogDescription className="sr-only">
          Jump to a page, product or order, or change the date range.
        </DialogDescription>
        <div className="flex items-center gap-2 border-b px-3">
          <Search className="size-4 shrink-0 text-muted-foreground" />
          <input
            autoFocus
            value={query}
            onChange={(e) => {
              setQuery(e.target.value);
              setActive(0);
            }}
            onKeyDown={(e) => {
              if (e.key === "ArrowDown") {
                e.preventDefault();
                setActive((i) => Math.min(i + 1, results.length - 1));
              } else if (e.key === "ArrowUp") {
                e.preventDefault();
                setActive((i) => Math.max(i - 1, 0));
              } else if (e.key === "Enter") {
                e.preventDefault();
                choose(results[active]);
              }
            }}
            placeholder="Search pages, products, orders…"
            className="h-11 w-full bg-transparent text-sm outline-none placeholder:text-muted-foreground"
          />
        </div>
        <ul ref={listRef} className="max-h-80 overflow-y-auto p-1.5">
          {results.length === 0 && (
            <li className="px-3 py-6 text-center text-sm text-muted-foreground">No matches.</li>
          )}
          {results.map((c, i) => (
            <li key={c.id}>
              {(i === 0 || results[i - 1].group !== c.group) && (
                <p className="px-2.5 pb-1 pt-2 text-[11px] font-medium uppercase tracking-wide text-muted-foreground">
                  {c.group}
                </p>
              )}
              <button
                type="button"
                onMouseEnter={() => setActive(i)}
                onClick={() => choose(c)}
                className={cn(
                  "flex w-full items-center justify-between gap-3 rounded-md px-2.5 py-2 text-left text-sm",
                  i === active && "bg-accent"
                )}
              >
                <span className="min-w-0 truncate">{c.label}</span>
                {c.hint && (
                  <span className="max-w-[45%] shrink-0 truncate text-xs text-muted-foreground">
                    {c.hint}
                  </span>
                )}
              </button>
            </li>
          ))}
        </ul>
      </DialogContent>
    </Dialog>
  );
}
