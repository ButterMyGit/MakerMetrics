import { createCipheriv, createDecipheriv, createHash, randomBytes } from "node:crypto";

const PROVIDERS = new Set(["openai", "anthropic", "google"]);

export interface SavedAiSettings {
  provider: "openai" | "anthropic" | "google";
  model: string;
  apiKey: string;
}

interface EncryptedPayload {
  version: 1;
  provider: SavedAiSettings["provider"];
  model: string;
  iv: string;
  tag: string;
  ciphertext: string;
}

function encryptionKey(): Buffer {
  const secret = process.env.AI_SETTINGS_ENCRYPTION_KEY;
  if (!secret || secret.length < 32) {
    throw new Error("Set AI_SETTINGS_ENCRYPTION_KEY to save AI provider keys.");
  }
  return createHash("sha256").update(secret).digest();
}

export function isAiProvider(value: unknown): value is SavedAiSettings["provider"] {
  return typeof value === "string" && PROVIDERS.has(value);
}

export function encryptedAiSettingsFromProfileSettings(
  settings: unknown
): EncryptedPayload | null {
  if (!settings || typeof settings !== "object") return null;
  const value = (settings as Record<string, unknown>).ai;
  if (!value || typeof value !== "object") return null;
  const candidate = value as Partial<EncryptedPayload>;
  if (
    candidate.version !== 1 ||
    !isAiProvider(candidate.provider) ||
    typeof candidate.model !== "string" ||
    typeof candidate.iv !== "string" ||
    typeof candidate.tag !== "string" ||
    typeof candidate.ciphertext !== "string"
  ) {
    return null;
  }
  return candidate as EncryptedPayload;
}

export function publicAiSettingsFromProfileSettings(settings: unknown) {
  const saved = encryptedAiSettingsFromProfileSettings(settings);
  if (!saved) return null;
  return {
    provider: saved.provider,
    model: saved.model,
    hasApiKey: true,
  };
}

export function encryptAiSettings(settings: SavedAiSettings): EncryptedPayload {
  const iv = randomBytes(12);
  const cipher = createCipheriv("aes-256-gcm", encryptionKey(), iv);
  const ciphertext = Buffer.concat([
    cipher.update(settings.apiKey, "utf8"),
    cipher.final(),
  ]);
  return {
    version: 1,
    provider: settings.provider,
    model: settings.model,
    iv: iv.toString("base64"),
    tag: cipher.getAuthTag().toString("base64"),
    ciphertext: ciphertext.toString("base64"),
  };
}

export function decryptAiSettings(settings: unknown): SavedAiSettings | null {
  const saved = encryptedAiSettingsFromProfileSettings(settings);
  if (!saved) return null;
  const decipher = createDecipheriv(
    "aes-256-gcm",
    encryptionKey(),
    Buffer.from(saved.iv, "base64")
  );
  decipher.setAuthTag(Buffer.from(saved.tag, "base64"));
  const apiKey = Buffer.concat([
    decipher.update(Buffer.from(saved.ciphertext, "base64")),
    decipher.final(),
  ]).toString("utf8");
  return {
    provider: saved.provider,
    model: saved.model,
    apiKey,
  };
}
