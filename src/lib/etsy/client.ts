import { createHash, randomBytes } from "node:crypto";

/**
 * Minimal Etsy Open API v3 client: OAuth 2.0 with PKCE, token refresh, and
 * GET requests that respect Etsy's rate limits. Read-only scopes only.
 */

const API = "https://api.etsy.com/v3/application";
const TOKEN_URL = "https://api.etsy.com/v3/public/oauth/token";
const AUTHORIZE_URL = "https://www.etsy.com/oauth/connect";

/** Orders and fees, listings (including inactive), shop details. Nothing is ever written. */
export const ETSY_SCOPES = ["transactions_r", "listings_r", "shops_r"];

export interface EtsySecrets {
  keystring: string;
  sharedSecret: string;
  accessToken?: string;
  refreshToken?: string;
  /** epoch milliseconds */
  expiresAt?: number;
}

export class EtsyError extends Error {
  constructor(
    message: string,
    readonly status: number
  ) {
    super(message);
  }
}

export function createPkce(): { verifier: string; challenge: string; state: string } {
  const verifier = randomBytes(48).toString("base64url");
  return {
    verifier,
    challenge: createHash("sha256").update(verifier).digest("base64url"),
    state: randomBytes(16).toString("base64url"),
  };
}

export function authorizeUrl(keystring: string, redirectUri: string, challenge: string, state: string): string {
  const params = new URLSearchParams({
    response_type: "code",
    client_id: keystring,
    redirect_uri: redirectUri,
    scope: ETSY_SCOPES.join(" "),
    state,
    code_challenge: challenge,
    code_challenge_method: "S256",
  });
  return `${AUTHORIZE_URL}?${params.toString()}`;
}

async function tokenRequest(body: Record<string, string>): Promise<Pick<EtsySecrets, "accessToken" | "refreshToken" | "expiresAt">> {
  const res = await fetch(TOKEN_URL, {
    method: "POST",
    headers: { "content-type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams(body).toString(),
    signal: AbortSignal.timeout(20000),
  });
  const json = (await res.json().catch(() => ({}))) as {
    access_token?: string;
    refresh_token?: string;
    expires_in?: number;
    error?: string;
    error_description?: string;
  };
  if (!res.ok || !json.access_token || !json.refresh_token) {
    throw new EtsyError(json.error_description ?? json.error ?? "Etsy did not issue a token.", res.status);
  }
  return {
    accessToken: json.access_token,
    refreshToken: json.refresh_token,
    expiresAt: Date.now() + (json.expires_in ?? 3600) * 1000,
  };
}

export function exchangeCode(keystring: string, code: string, verifier: string, redirectUri: string) {
  return tokenRequest({
    grant_type: "authorization_code",
    client_id: keystring,
    redirect_uri: redirectUri,
    code,
    code_verifier: verifier,
  });
}

export function refreshTokens(keystring: string, refreshToken: string) {
  return tokenRequest({ grant_type: "refresh_token", client_id: keystring, refresh_token: refreshToken });
}

const wait = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * A client bound to one seller's credentials. `onTokens` is called whenever
 * the access token is refreshed so the caller can store the new pair.
 */
export function etsyClient(
  secrets: EtsySecrets,
  onTokens: (tokens: Pick<EtsySecrets, "accessToken" | "refreshToken" | "expiresAt">) => Promise<void>
) {
  let current = { ...secrets };

  async function token(): Promise<string> {
    if (!current.refreshToken) throw new EtsyError("Etsy is not authorized yet.", 401);
    if (!current.accessToken || !current.expiresAt || current.expiresAt - Date.now() < 60000) {
      const fresh = await refreshTokens(current.keystring, current.refreshToken);
      current = { ...current, ...fresh };
      await onTokens(fresh);
    }
    return current.accessToken as string;
  }

  async function get<T>(path: string, params: Record<string, string | number | boolean | undefined> = {}): Promise<T> {
    const query = new URLSearchParams();
    for (const [k, v] of Object.entries(params)) if (v !== undefined) query.set(k, String(v));
    const url = `${API}${path}${query.size > 0 ? `?${query.toString()}` : ""}`;

    for (let attempt = 0; ; attempt++) {
      const res = await fetch(url, {
        headers: {
          "x-api-key": `${current.keystring}:${current.sharedSecret}`,
          authorization: `Bearer ${await token()}`,
        },
        signal: AbortSignal.timeout(30000),
      });
      if (res.status === 429 && attempt < 4) {
        // Etsy says how long to wait; back off further on repeats.
        const retryAfter = Number(res.headers.get("retry-after")) || 1;
        await wait(Math.min(30, retryAfter * (attempt + 1)) * 1000);
        continue;
      }
      if (res.status >= 500 && attempt < 2) {
        await wait(1500 * (attempt + 1));
        continue;
      }
      const json = (await res.json().catch(() => null)) as (T & { error?: string }) | null;
      if (!res.ok || json == null) {
        throw new EtsyError(json?.error ?? `Etsy request failed (${res.status}).`, res.status);
      }
      // Stay under the per-second limit when close to it.
      if (res.headers.get("x-remaining-this-second") === "0") await wait(1000);
      return json;
    }
  }

  return { get };
}

export type EtsyClient = ReturnType<typeof etsyClient>;
