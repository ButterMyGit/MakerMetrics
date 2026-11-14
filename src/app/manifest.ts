import type { MetadataRoute } from "next";

/** Lets phones add MakerMetrics to the home screen and open it full screen. */
export default function manifest(): MetadataRoute.Manifest {
  return {
    name: "MakerMetrics",
    short_name: "MakerMetrics",
    description: "Analytics for Etsy sellers.",
    start_url: "/dashboard",
    display: "standalone",
    background_color: "#0a0a0a",
    theme_color: "#0a0a0a",
    icons: [{ src: "/makermetrics-logo.png", sizes: "any", type: "image/png" }],
  };
}
