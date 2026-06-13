import { describe, expect, it } from "vitest";
import Papa from "papaparse";
import { buildSaleItems, detectFormat, deriveItemParts, deriveStyle, type ParsedCsv } from "./parse";

const ITEMS_CSV = `Sale Date,Item Name,Buyer,Quantity,Price,Coupon Code,Coupon Details,Discount Amount,Shipping Discount,Order Shipping,Order Sales Tax,Item Total,Currency,Transaction ID,Listing ID,Date Paid,Date Shipped,Ship Name,Ship Address1,Ship City,Ship State,Ship Zipcode,Ship Country,Order ID,Variations,Order Type,Listings Type,Payment Type,InPerson Discount,InPerson Location,VAT Paid by Buyer,SKU
08/14/25,Sunset Fox | Greeting Card,coolbuyer42,2,$5.00,,,,,$4.50,$0.62,$10.00,USD,4001,9001,08/14/25,08/15/25,Jane Doe,1 Main St,Austin,TX,78701,United States,#3001,Style: Matte,online,listing,online_cc,,,,FOX-01
08/14/25,Sunset Fox | Sticker,coolbuyer42,1,$3.00,,,,,$0.00,$0.00,$3.00,USD,4002,9002,08/14/25,08/15/25,Jane Doe,1 Main St,Austin,TX,78701,United States,#3001,Custom Property: Glossy,online,listing,online_cc,,,,FOX-ST
09/02/25,Moon Bear | Greeting Card,otherbuyer,1,$5.00,WELCOME10,10% off,$0.50,,$4.50,$0.00,$5.00,USD,4003,9003,09/02/25,09/04/25,Bob Roe,2 Oak Ave,Denver,CO,80202,United States,#3002,Style: Matte,online,listing,paypal,,,,BEAR-01`;

const ORDERS_CSV = `Sale Date,Order ID,Buyer User ID,Buyer,Full Name,First Name,Last Name,Number of Items,Payment Method,Date Shipped,Street 1,Street 2,Ship City,Ship State,Ship Zipcode,Ship Country,Currency,Order Value,Coupon Code,Coupon Details,Discount Amount,Shipping Discount,Shipping,Sales Tax,Order Total,Status,Card Processing Fees,Order Net,Adjusted Order Total,Adjusted Card Processing Fees,Adjusted Net Order Amount,Buyer Receipt Message,SKU,Order Type,Payment Type,InPerson Discount,InPerson Location,VAT Paid by Buyer,Date Paid
08/14/25,3001,777,coolbuyer42,Jane Doe,Jane,Doe,2,credit_card,08/15/25,1 Main St,,Austin,TX,78701,United States,USD,$13.00,,,,,$4.50,$0.62,$18.12,Paid,$1.20,$16.30,$18.12,$1.20,$16.30,,FOX-01,online,online_cc,,,,08/14/25
09/02/25,3002,888,otherbuyer,Bob Roe,Bob,Roe,1,paypal,09/04/25,2 Oak Ave,,Denver,CO,80202,United States,USD,$5.00,WELCOME10,10% off,$0.50,,$4.50,$0.00,$9.00,Paid,$0.75,$8.25,$9.00,$0.75,$8.25,,BEAR-01,online,paypal,,,,09/02/25`;

function parse(fileName: string, csv: string): ParsedCsv {
  const result = Papa.parse<Record<string, string>>(csv, {
    header: true,
    skipEmptyLines: "greedy",
    transformHeader: (h) => h.trim(),
  });
  return { fileName, format: detectFormat(result.meta.fields ?? []), rows: result.data };
}

describe("detectFormat", () => {
  it("detects items / orders / combined / unknown", () => {
    expect(detectFormat(["Transaction ID", "Item Name", "Price"])).toBe("items");
    expect(detectFormat(["Order ID", "Full Name", "Order Net"])).toBe("orders");
    expect(detectFormat(["Transaction ID", "Item Name", "Full Name"])).toBe("combined");
    expect(detectFormat(["foo", "bar"])).toBe("unknown");
  });
});

describe("derivations", () => {
  it("splits item name into card name and product type", () => {
    expect(deriveItemParts("Sunset Fox | Greeting Card")).toEqual({
      cardName: "Sunset Fox",
      productType: "Greeting Card",
    });
    expect(deriveItemParts("Plain Item")).toEqual({
      cardName: "Plain Item",
      productType: null,
    });
  });

  it("strips style prefixes from variations", () => {
    expect(deriveStyle("Style: Matte")).toBe("Matte");
    expect(deriveStyle("Custom Property: Glossy")).toBe("Glossy");
    expect(deriveStyle(null)).toBeNull();
  });
});

describe("buildSaleItems", () => {
  it("imports items alone", () => {
    const { rows, warnings } = buildSaleItems([parse("items.csv", ITEMS_CSV)]);
    expect(warnings).toEqual([]);
    expect(rows).toHaveLength(3);

    const first = rows.find((r) => r.transaction_id === "4001")!;
    expect(first.order_id).toBe("3001"); // "#3001" cleaned
    expect(first.sale_date).toBe("2025-08-14");
    expect(first.card_name).toBe("Sunset Fox");
    expect(first.product_type).toBe("Greeting Card");
    expect(first.style).toBe("Matte");
    expect(first.quantity).toBe(2);
    expect(first.price).toBe(5);
    expect(first.item_total).toBe(10);
    expect(first.shipping).toBe(4.5);
    expect(first.buyer_username).toBe("coolbuyer42");
    expect(first.ship_state).toBe("TX");
    // order-level financials absent without an orders file
    expect(first.order_net).toBeNull();
  });

  it("merges order financials onto items by Order ID", () => {
    const { rows, warnings, unmatchedItems } = buildSaleItems([
      parse("items.csv", ITEMS_CSV),
      parse("orders.csv", ORDERS_CSV),
    ]);
    expect(warnings).toEqual([]);
    expect(unmatchedItems).toBe(0);

    const first = rows.find((r) => r.transaction_id === "4001")!;
    expect(first.order_net).toBe(16.3);
    expect(first.order_total).toBe(18.12);
    expect(first.card_processing_fees).toBe(1.2);
    expect(first.full_name).toBe("Jane Doe");

    const third = rows.find((r) => r.transaction_id === "4003")!;
    expect(third.order_net).toBe(8.25);
    expect(third.coupon_code).toBe("WELCOME10");
  });

  it("warns when only an orders file is provided", () => {
    const { rows, warnings } = buildSaleItems([parse("orders.csv", ORDERS_CSV)]);
    expect(rows).toHaveLength(0);
    expect(warnings.some((w) => w.includes("can't be imported"))).toBe(true);
  });

  it("deduplicates by transaction id on re-import", () => {
    const file = parse("items.csv", ITEMS_CSV);
    const { rows } = buildSaleItems([file, { ...file, fileName: "items-copy.csv" }]);
    expect(rows).toHaveLength(3);
  });
});
