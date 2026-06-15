"use client";

import { createContext, useContext, useMemo, useState, type ReactNode } from "react";

interface ProfileContextValue {
  shopName: string | null;
  setShopName: (name: string | null) => void;
  email: string | null;
}

const ProfileContext = createContext<ProfileContextValue | null>(null);

export function ProfileProvider({
  initialShopName,
  email,
  children,
}: {
  initialShopName: string | null;
  email: string | null;
  children: ReactNode;
}) {
  const [shopName, setShopName] = useState<string | null>(initialShopName);

  const value = useMemo(
    () => ({ shopName, setShopName, email }),
    [shopName, email]
  );

  return <ProfileContext.Provider value={value}>{children}</ProfileContext.Provider>;
}

export function useProfile(): ProfileContextValue {
  const ctx = useContext(ProfileContext);
  if (!ctx) throw new Error("useProfile must be used within ProfileProvider");
  return ctx;
}
