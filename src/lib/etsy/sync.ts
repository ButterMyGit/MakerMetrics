import type { SupabaseClient } from "@supabase/supabase-js";
import type { EtsyClient } from "./client";
import { EtsyError } from "./client";
import type { EtsyLedgerEntry, EtsyListing, EtsyPage, EtsyPayment, EtsyReceipt } from "./api-types";
import { ledgerToRow, listingToRow, receiptToRows, type ApiSaleRow } from "./map";

const PAGE = 100;
const BATCH = 500;
/** Receipts changed since a little before the last sync are fetched again. */
const OVERLAP_SECONDS = 2 * 86400;

export interface SyncResult {
  orders: number;
  lineItems: number;
  ledgerEntries: number;
  listings: number;
  /**
   * Set when the time budget ran out while fetching orders: everything
   * modified up to this moment is stored, so the next call resumes from here.
   */
  resumeFrom: Date | null;
}

async function upsert(
  supabase: SupabaseClient,
  table: string,
  rows: Record<string, unknown>[],
  onConflict: string
): Promise<void> {
  for (let i = 0; i < rows.length; i += BATCH) {
    const { error } = await supabase.from(table).upsert(rows.slice(i, i + BATCH), { onConflict });
    if (error) throw new Error(error.message);
  }
}

/**
 * Pull orders (with their payment fees), ledger entries and listings from
 * Etsy into the same tables the CSV import fills. Incremental after the
 * first run. Orders are fetched oldest-change first; if `deadline` passes
 * the sync returns `resumeFrom` so a follow-up call continues from there
 * instead of hitting the function time limit.
 */
export async function syncShop(options: {
  etsy: EtsyClient;
  supabase: SupabaseClient;
  userId: string;
  shopId: string;
  /** previous successful sync, if any */
  since: Date | null;
  timeZone: string;
  deadline: number;
}): Promise<SyncResult> {
  const { etsy, supabase, userId, shopId, since, timeZone, deadline } = options;
  const result: SyncResult = { orders: 0, lineItems: 0, ledgerEntries: 0, listings: 0, resumeFrom: null };
  const sinceSeconds = since ? Math.floor(since.getTime() / 1000) - OVERLAP_SECONDS : undefined;
  const outOfTime = () => Date.now() > deadline;

  // --- Orders ---------------------------------------------------------------
  let lastUpdated = 0;
  for (let offset = 0; ; offset += PAGE) {
    const page = await etsy.get<EtsyPage<EtsyReceipt>>(`/shops/${shopId}/receipts`, {
      limit: PAGE,
      offset,
      min_last_modified: sinceSeconds,
      sort_on: "updated",
      sort_order: "asc",
    });
    const rows: (ApiSaleRow & { user_id: string })[] = [];
    for (const receipt of page.results) {
      lastUpdated = Math.max(lastUpdated, receipt.updated_timestamp ?? 0);
      let payment: EtsyPayment | null = null;
      try {
        const payments = await etsy.get<EtsyPage<EtsyPayment>>(
          `/shops/${shopId}/receipts/${receipt.receipt_id}/payments`
        );
        payment = payments.results[0] ?? null;
      } catch (error) {
        // Orders paid outside Etsy Payments have no payment record.
        if (!(error instanceof EtsyError) || error.status !== 404) throw error;
      }
      for (const row of receiptToRows(receipt, payment, timeZone)) rows.push({ ...row, user_id: userId });
    }
    await upsert(supabase, "sale_items", rows, "user_id,transaction_id");
    result.orders += page.results.length;
    result.lineItems += rows.length;
    if (page.results.length < PAGE) break;
    if (outOfTime() && lastUpdated > 0) {
      result.resumeFrom = new Date((lastUpdated + OVERLAP_SECONDS) * 1000);
      return result;
    }
  }

  // --- Ledger (fees, ads, labels, deposits) ---------------------------------
  const now = Math.floor(Date.now() / 1000);
  for (let offset = 0; ; offset += PAGE) {
    const page = await etsy.get<EtsyPage<EtsyLedgerEntry>>(
      `/shops/${shopId}/payment-account/ledger-entries`,
      // Etsy requires both bounds; the first sync goes back three years.
      { limit: PAGE, offset, min_created: sinceSeconds ?? now - 3 * 365 * 86400, max_created: now }
    );
    await upsert(
      supabase,
      "ledger_entries",
      page.results.map((e) => ({ ...ledgerToRow(e, timeZone), user_id: userId })),
      "user_id,entry_id"
    );
    result.ledgerEntries += page.results.length;
    if (page.results.length < PAGE) break;
  }

  // --- Listings -------------------------------------------------------------
  for (const state of ["active", "inactive", "sold_out", "expired", "draft"]) {
    for (let offset = 0; ; offset += PAGE) {
      const page = await etsy.get<EtsyPage<EtsyListing>>(`/shops/${shopId}/listings`, {
        state,
        limit: PAGE,
        offset,
      });
      await upsert(
        supabase,
        "listings",
        page.results.map((l) => ({ ...listingToRow(l), user_id: userId })),
        "user_id,listing_id"
      );
      result.listings += page.results.length;
      if (page.results.length < PAGE) break;
    }
  }

  return result;
}
