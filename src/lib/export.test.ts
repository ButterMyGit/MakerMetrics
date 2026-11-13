import { describe, expect, it } from "vitest";
import { toCsv } from "./export";

describe("toCsv", () => {
  it("quotes cells that need it and leaves numbers bare", () => {
    expect(toCsv(["Name", "Total"], [["Smith, Jo", 12.5], ['Say "hi"', null]])).toBe(
      'Name,Total\r\n"Smith, Jo",12.5\r\n"Say ""hi""",'
    );
  });

  it("neutralizes text that a spreadsheet would run as a formula", () => {
    expect(toCsv(["Buyer"], [["=HYPERLINK(1)"], [-5]])).toBe("Buyer\r\n'=HYPERLINK(1)\r\n-5");
  });
});
