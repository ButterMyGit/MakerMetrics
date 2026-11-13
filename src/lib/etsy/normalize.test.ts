import { describe, expect, it } from "vitest";
import { dbRowToSaleRow, type SaleItemDbRow } from "@/lib/types";
import { decodeEntities, parseCoupons, parseVariations } from "./normalize";

describe("parseCoupons", () => {
  it("splits, trims, upper-cases and de-duplicates", () => {
    expect(parseCoupons("OCT20OFF;OCT20OFF")).toEqual(["OCT20OFF"]);
    expect(parseCoupons(" save10 , FREESHIP;save10")).toEqual(["SAVE10", "FREESHIP"]);
    expect(parseCoupons("")).toEqual([]);
    expect(parseCoupons(null)).toEqual([]);
  });
});

describe("parseVariations", () => {
  it("reads any option name, not just Style", () => {
    expect(parseVariations("Size: 8x10, Color: Black")).toEqual([
      { name: "Size", value: "8x10" },
      { name: "Color", value: "Black" },
    ]);
    expect(parseVariations("Set:Base Set")).toEqual([{ name: "Set", value: "Base Set" }]);
  });

  it("keeps commas and colons that belong to a value", () => {
    expect(parseVariations("Finish: Matte, no frame, Size: A4")).toEqual([
      { name: "Finish", value: "Matte, no frame" },
      { name: "Size", value: "A4" },
    ]);
    expect(parseVariations("Pickup time: 10:30")).toEqual([{ name: "Pickup time", value: "10:30" }]);
  });

  it("decodes the HTML entities Etsy exports", () => {
    expect(parseVariations("Size: 8&quot; x 10&quot;")).toEqual([{ name: "Size", value: '8" x 10"' }]);
    expect(decodeEntities("Salt &amp; Pepper")).toBe("Salt & Pepper");
  });

  it("handles empty input and a bare value", () => {
    expect(parseVariations(null)).toEqual([]);
    expect(parseVariations("  ")).toEqual([]);
    expect(parseVariations("Large")).toEqual([{ name: "Option", value: "Large" }]);
  });
});

describe("dbRowToSaleRow", () => {
  const db = (overrides: Partial<SaleItemDbRow>): SaleItemDbRow =>
    ({ transaction_id: "t1", quantity: 1, ...overrides }) as SaleItemDbRow;

  it("normalizes rows that were imported before the parser was fixed", () => {
    const r = dbRowToSaleRow(
      db({
        listing_id: "123",
        item_name: "Fox &amp; Hare | Print",
        card_name: "Fox &amp; Hare",
        variations: "Size: 8x10",
        style: "Size: 8x10", // old parser left the prefix in
        coupon_code: "OCT20OFF;OCT20OFF",
      })
    );
    expect(r.cardName).toBe("Fox & Hare");
    expect(r.productType).toBe("Print");
    expect(r.style).toBe("8x10");
    expect(r.coupons).toEqual(["OCT20OFF"]);
    expect(r.couponCode).toBe("OCT20OFF");
    expect(r.productKey).toBe("l:123");
  });

  it("falls back to the title as product identity without a listing id", () => {
    expect(dbRowToSaleRow(db({ item_name: "Plain Item" })).productKey).toBe("n:Plain Item");
    expect(dbRowToSaleRow(db({})).productKey).toBeNull();
  });
});
