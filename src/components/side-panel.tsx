"use client";

import { Card } from "@/components/ui/card";
import { Sheet, SheetContent, SheetDescription, SheetHeader, SheetTitle } from "@/components/ui/sheet";
import { useMediaQuery } from "@/hooks/use-media-query";

/** Screens wide enough to show a detail panel beside the list instead of over it. */
export function useWidePanel(): boolean {
  return useMediaQuery("(min-width: 1280px)");
}

/**
 * Detail panel for a selected row. On wide screens it sits beside the list,
 * which stays usable; on smaller ones it slides over the page.
 * `panelKey` changes when a different row is selected, replaying the entrance.
 */
export function SidePanel({
  open,
  onClose,
  title,
  panelKey,
  children,
}: {
  open: boolean;
  onClose: () => void;
  /** read by screen readers on small screens */
  title: string;
  panelKey?: string | null;
  children: React.ReactNode;
}) {
  const wide = useWidePanel();

  if (wide) {
    if (!open) return null;
    return (
      <Card
        key={panelKey ?? undefined}
        className="sticky top-6 max-h-[calc(100dvh-3rem)] w-[380px] shrink-0 overflow-y-auto p-4 animate-in fade-in slide-in-from-right-4 duration-200"
      >
        {children}
      </Card>
    );
  }

  return (
    <Sheet open={open} onOpenChange={(next) => !next && onClose()}>
      <SheetContent side="right" className="w-full overflow-y-auto sm:max-w-md">
        <SheetHeader className="sr-only">
          <SheetTitle>{title}</SheetTitle>
          <SheetDescription>Details for the selected row.</SheetDescription>
        </SheetHeader>
        <div className="px-4 pb-8 pt-10">{children}</div>
      </SheetContent>
    </Sheet>
  );
}

/** Title row for a side panel: heading, optional subtitle, and a close button on wide screens. */
export function PanelHeader({
  title,
  subtitle,
  onClose,
  actions,
}: {
  title: string;
  subtitle?: React.ReactNode;
  onClose: () => void;
  actions?: React.ReactNode;
}) {
  const wide = useWidePanel();
  return (
    <div className="flex items-start justify-between gap-2">
      <div className="min-w-0">
        <h2 className="truncate text-base font-semibold">{title}</h2>
        {subtitle && <p className="text-sm text-muted-foreground">{subtitle}</p>}
      </div>
      <div className="flex shrink-0 items-center gap-1">
        {actions}
        {wide && (
          <button
            type="button"
            onClick={onClose}
            aria-label="Close"
            className="inline-flex size-8 items-center justify-center rounded-md text-muted-foreground hover:bg-accent hover:text-foreground"
          >
            <svg viewBox="0 0 24 24" className="size-4" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round">
              <path d="M18 6 6 18M6 6l12 12" />
            </svg>
          </button>
        )}
      </div>
    </div>
  );
}
