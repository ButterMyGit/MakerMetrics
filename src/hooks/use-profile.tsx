"use client";

import { createContext, useContext, useMemo, useState, type ReactNode } from "react";

interface ProfileContextValue {
  userId: string;
  shopName: string | null;
  setShopName: (name: string | null) => void;
  email: string | null;
}

const ProfileContext = createContext<ProfileContextValue | null>(null);

export function ProfileProvider({
  userId,
  initialShopName,
  email,
  children,
}: {
  userId: string;
  initialShopName: string | null;
  email: string | null;
  children: ReactNode;
}) {
  const [shopName, setShopName] = useState<string | null>(initialShopName);

  const value = useMemo(
    () => ({ userId, shopName, setShopName, email }),
    [userId, shopName, email]
  );

  return <ProfileContext.Provider value={value}>{children}</ProfileContext.Provider>;
}

export function useProfile(): ProfileContextValue {
  const ctx = useContext(ProfileContext);
  if (!ctx) throw new Error("useProfile must be used within ProfileProvider");
  return ctx;
}
