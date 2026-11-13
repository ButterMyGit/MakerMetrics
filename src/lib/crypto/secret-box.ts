import { createCipheriv, createDecipheriv, createHash, randomBytes } from "node:crypto";

/**
 * AES-256-GCM encryption for secrets stored in the database (API keys,
 * OAuth tokens). The key comes from AI_SETTINGS_ENCRYPTION_KEY, which exists
 * only on the server.
 */

export interface SealedBox {
  v: 1;
  iv: string;
  tag: string;
  data: string;
}

function key(): Buffer {
  const secret = process.env.AI_SETTINGS_ENCRYPTION_KEY;
  if (!secret || secret.length < 32) {
    throw new Error("Set AI_SETTINGS_ENCRYPTION_KEY (32+ characters) to store secrets.");
  }
  return createHash("sha256").update(secret).digest();
}

export function seal(value: unknown): SealedBox {
  const iv = randomBytes(12);
  const cipher = createCipheriv("aes-256-gcm", key(), iv);
  const data = Buffer.concat([cipher.update(JSON.stringify(value), "utf8"), cipher.final()]);
  return {
    v: 1,
    iv: iv.toString("base64"),
    tag: cipher.getAuthTag().toString("base64"),
    data: data.toString("base64"),
  };
}

export function open<T>(box: unknown): T | null {
  if (!box || typeof box !== "object") return null;
  const b = box as Partial<SealedBox>;
  if (b.v !== 1 || typeof b.iv !== "string" || typeof b.tag !== "string" || typeof b.data !== "string") {
    return null;
  }
  const decipher = createDecipheriv("aes-256-gcm", key(), Buffer.from(b.iv, "base64"));
  decipher.setAuthTag(Buffer.from(b.tag, "base64"));
  const text = Buffer.concat([
    decipher.update(Buffer.from(b.data, "base64")),
    decipher.final(),
  ]).toString("utf8");
  return JSON.parse(text) as T;
}
