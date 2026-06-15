import type { Metadata, Viewport } from "next";
import { Geist, Geist_Mono } from "next/font/google";
import { Toaster } from "@/components/ui/sonner";
import { AccentProvider } from "@/components/accent-provider";
import "./globals.css";

// Applies the saved accent before first paint to avoid a color flash.
const ACCENT_INIT = `(function(){try{var h=localStorage.getItem('makermetrics-accent');if(!/^#[0-9a-fA-F]{6}$/.test(h||''))h='#2563eb';var r=document.documentElement.style;r.setProperty('--chart-1',h);r.setProperty('--chart-2','color-mix(in oklab, '+h+', white 24%)');r.setProperty('--chart-3','color-mix(in oklab, '+h+', white 46%)');r.setProperty('--chart-4','color-mix(in oklab, '+h+', black 18%)');r.setProperty('--chart-5','color-mix(in oklab, '+h+', black 38%)');}catch(e){}})();`;

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
    >
      <head>
        <script dangerouslySetInnerHTML={{ __html: ACCENT_INIT }} />
      </head>
      <body className="min-h-full flex flex-col bg-background text-foreground">
        <AccentProvider>{children}</AccentProvider>
        <Toaster position="top-center" richColors />
      </body>
    </html>
  );
}
