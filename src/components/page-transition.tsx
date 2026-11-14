"use client";

import { usePathname } from "next/navigation";

/** Replays a short fade-and-rise whenever the route changes. */
export function PageTransition({ children }: { children: React.ReactNode }) {
  const pathname = usePathname();
  return (
    <div key={pathname} className="page-enter">
      {children}
    </div>
  );
}
