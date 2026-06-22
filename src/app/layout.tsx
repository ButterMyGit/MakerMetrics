import type { Metadata, Viewport } from "next";
import { Geist, Geist_Mono } from "next/font/google";
import { Toaster } from "@/components/ui/sonner";
import { AccentProvider } from "@/components/accent-provider";
import "./globals.css";

const geistSans = Geist({
  variable: "--font-sans",
  subsets: ["latin"],
});

const geistMono = Geist_Mono({
  variable: "--font-geist-mono",
  subsets: ["latin"],
});

export const metadata: Metadata = {
  title: {
    default: "MakerMetrics",
    template: "%s · MakerMetrics",
  },
  description:
    "Analytics for Etsy sellers. Import your Etsy CSV exports and get actionable insights, forecasts, and an AI analyst.",
  icons: {
    icon: "/makermetrics-logo.png",
    apple: "/makermetrics-logo.png",
  },
};

export const viewport: Viewport = {
  width: "device-width",
  initialScale: 1,
  themeColor: "#0a0a0a",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html
      lang="en"
      className={`${geistSans.variable} ${geistMono.variable} h-full antialiased`}
      suppressHydrationWarning
    >
      <body className="min-h-full flex flex-col bg-background text-foreground">
        <AccentProvider>{children}</AccentProvider>
        <Toaster position="top-center" richColors />
      </body>
    </html>
  );
}
