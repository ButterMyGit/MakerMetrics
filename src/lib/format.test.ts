import { describe, expect, it } from "vitest";
import {
  displayBuyerName,
  displayLocation,
  formatMoneyApprox,
  humanizeCode,
  maskName,
  roundApprox,
  titleCase,
} from "./format";

describe("display cleanup", () => {
  it("re-cases names typed in all caps or all lowercase, and leaves mixed case alone", () => {
    expect(titleCase("john vitiello")).toBe("John Vitiello");
    expect(titleCase("MARY-ANN O'BRIEN")).toBe("Mary-Ann O'Brien");
    expect(titleCase("Ronald McDonald")).toBe("Ronald McDonald");
  });

  it("never shows an email as a buyer name", () => {
    expect(displayBuyerName("abc@mail.etsy.com", "craftfan")).toBe("craftfan");
    expect(displayBuyerName("abc@mail.etsy.com", null)).toBe("Etsy buyer");
    expect(displayBuyerName("jane doe")).toBe("Jane Doe");
    expect(displayBuyerName(null, null)).toBe("Etsy buyer");
  });

  it("formats locations consistently", () => {
    expect(displayLocation("PAINESVILLE", "oh", "United States")).toBe("Painesville, OH");
    expect(displayLocation("Warrenton", "VA", null)).toBe("Warrenton, VA");
    expect(displayLocation("toronto", "ON", "Canada")).toBe("Toronto, ON, Canada");
    expect(displayLocation(null, null, null)).toBeNull();
  });

  it("turns export codes into words", () => {
    expect(humanizeCode("online_cc")).toBe("Credit card (online)");
    expect(humanizeCode("online_paypal")).toBe("PayPal (online)");
    expect(humanizeCode("some_new_method")).toBe("Some New Method");
    expect(humanizeCode(null)).toBeNull();
  });

  it("masks names for privacy mode", () => {
    expect(maskName("John Vitiello")).toBe("J. V.");
    expect(maskName("craftfan")).toBe("C.");
  });
});

describe("approximate money", () => {
  it("rounds to two significant figures so estimates don't look exact", () => {
    expect(roundApprox(1906.35)).toBe(1900);
    expect(roundApprox(250.46)).toBe(250);
    expect(roundApprox(43.2)).toBe(43);
    expect(roundApprox(0)).toBe(0);
    expect(formatMoneyApprox(1906.35)).toBe("$1,900");
  });
});
