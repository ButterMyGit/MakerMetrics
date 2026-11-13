/** Build a CSV string. Values are quoted when they contain commas, quotes or newlines. */
export function toCsv(headers: string[], rows: (string | number | null | undefined)[][]): string {
  const cell = (v: string | number | null | undefined): string => {
    if (v == null) return "";
    let s = String(v);
    // Keep spreadsheets from treating buyer-entered text as a formula.
    if (typeof v === "string" && /^[=+\-@\t\r]/.test(s)) s = `'${s}`;
    return /[",\n\r]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
  };
  return [headers, ...rows].map((r) => r.map(cell).join(",")).join("\r\n");
}

/** Save a CSV file from the browser. */
export function downloadCsv(
  filename: string,
  headers: string[],
  rows: (string | number | null | undefined)[][]
): void {
  const blob = new Blob(["﻿", toCsv(headers, rows)], { type: "text/csv;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}
