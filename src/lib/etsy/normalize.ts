/**
 * Pure normalizers for values that come out of Etsy exports. They run when
 * rows are read (not only at import), so older imports get the same cleanup
 * without re-uploading.
 */

export interface Variation {
  name: string;
  value: string;
}

const ENTITIES: Record<string, string> = {
  "&quot;": '"',
  "&amp;": "&",
  "&#39;": "'",
  "&apos;": "'",
  "&lt;": "<",
  "&gt;": ">",
};

/** Etsy exports HTML-escape quotes and ampersands in titles and variations. */
export function decodeEntities(s: string): string {
  return s.replace(/&(quot|amp|#39|apos|lt|gt);/g, (m) => ENTITIES[m] ?? m);
}

/**
 * Split an Etsy coupon field into individual codes. Multi-item orders export
 * the same code once per line joined with ";" (e.g. "OCT20OFF;OCT20OFF"), and
 * an order can carry more than one code.
 */
export function parseCoupons(raw: string | null | undefined): string[] {
  if (!raw) return [];
  const seen = new Set<string>();
  for (const part of raw.split(/[;,]/)) {
    const code = part.trim().toUpperCase();
    if (code) seen.add(code);
  }
  return [...seen];
}

/**
 * Parse Etsy's "Variations" column ("Size: 8x10, Color: Black") into
 * name/value pairs. A comma only starts a new pair when what follows looks
 * like "Name: value", so values containing commas stay intact.
 */
export function parseVariations(raw: string | null | undefined): Variation[] {
  if (!raw) return [];
  const text = decodeEntities(raw).trim();
  if (!text) return [];

  const out: Variation[] = [];
  for (const segment of text.split(",")) {
    const m = segment.match(/^\s*([^:,]{1,40}?)\s*:\s*(.*)$/);
    if (m && m[2].trim() !== "") {
      out.push({ name: m[1].trim(), value: m[2].trim() });
    } else if (out.length > 0) {
      out[out.length - 1].value += `,${segment}`.trimEnd();
    } else if (segment.trim()) {
      out.push({ name: "Option", value: segment.trim() });
    }
  }
  return out;
}

/** Short label for a line item's chosen options, e.g. "8x10 / Black". */
export function variationLabel(list: Variation[]): string | null {
  if (list.length === 0) return null;
  return list.map((v) => v.value).join(" / ");
}

/**
 * Listing titles are often "Product name | Type | keywords". The first
 * segment is the display name; the second, when present, is kept as a type.
 */
export function deriveItemParts(itemName: string | null): {
  cardName: string | null;
  productType: string | null;
} {
  if (!itemName) return { cardName: null, productType: null };
  const title = decodeEntities(itemName);
  const idx = title.indexOf("|");
  if (idx === -1) return { cardName: title.trim() || null, productType: null };
  return {
    cardName: title.slice(0, idx).trim() || null,
    productType: title.slice(idx + 1).trim() || null,
  };
}

/** Stored `style` column: the option values of the line item. */
export function deriveStyle(variations: string | null): string | null {
  return variationLabel(parseVariations(variations));
}
