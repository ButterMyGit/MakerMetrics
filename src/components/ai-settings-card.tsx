"use client";

import { useEffect, useState, useSyncExternalStore } from "react";
import { CheckCircle2, Loader2 } from "lucide-react";
import { toast } from "sonner";
import {
  deleteAiSettings,
  getAiSettings,
  getServerAiSettings,
  loadAiSettings,
  saveAiSettings,
  setAiSettingsUser,
  subscribeAiSettings,
  type AiSettings,
} from "@/lib/ai/settings-store";
import type { ModelOption } from "@/lib/ai/models";
import { useProfile } from "@/hooks/use-profile";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Switch } from "@/components/ui/switch";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";

const PROVIDERS: { id: AiSettings["provider"]; label: string; keyHint: string; keysUrl: string }[] = [
  { id: "anthropic", label: "Anthropic (Claude)", keyHint: "sk-ant-…", keysUrl: "https://console.anthropic.com/settings/keys" },
  { id: "openai", label: "OpenAI", keyHint: "sk-…", keysUrl: "https://platform.openai.com/api-keys" },
  { id: "google", label: "Google (Gemini)", keyHint: "AIza…", keysUrl: "https://aistudio.google.com/apikey" },
];

const CUSTOM = "__custom__";

/** Provider, key and model for the AI Analyst. Models come from the provider, not a fixed list. */
export function AiSettingsCard() {
  const { userId, isDemo, prefs, updatePrefs } = useProfile();
  const settings = useSyncExternalStore(subscribeAiSettings, getAiSettings, getServerAiSettings);

  const [provider, setProvider] = useState<AiSettings["provider"] | null>(null);
  const [apiKey, setApiKey] = useState("");
  const [models, setModels] = useState<ModelOption[] | null>(null);
  const [model, setModel] = useState<string | null>(null);
  const [customModel, setCustomModel] = useState("");
  const [checking, setChecking] = useState(false);
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    setAiSettingsUser(userId);
    void loadAiSettings().catch(() => undefined);
  }, [userId]);

  const activeProvider = provider ?? settings?.provider ?? "anthropic";
  const providerInfo = PROVIDERS.find((p) => p.id === activeProvider)!;
  const hasSavedKey = settings?.hasApiKey === true && settings.provider === activeProvider;
  const selected = model ?? (hasSavedKey ? settings!.model : null);
  const selectedListed = selected != null && models?.some((m) => m.id === selected);
  const finalModel = selected === CUSTOM ? customModel.trim() : selected;

  async function check() {
    setChecking(true);
    try {
      const res = await fetch("/api/ai/models", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ provider: activeProvider, apiKey: apiKey.trim() || undefined }),
      });
      const body = (await res.json()) as { models?: ModelOption[]; error?: string };
      if (!res.ok || !body.models) throw new Error(body.error ?? "Could not check the key.");
      setModels(body.models);
      if (!selected && body.models[0]) setModel(body.models[0].id);
      toast.success(`Key works. ${body.models.length} models available.`);
    } catch (err) {
      setModels(null);
      toast.error(err instanceof Error ? err.message : "Could not check the key.");
    } finally {
      setChecking(false);
    }
  }

  async function save() {
    if (!finalModel) {
      toast.error("Choose a model first.");
      return;
    }
    setSaving(true);
    try {
      await saveAiSettings({ provider: activeProvider, apiKey: apiKey.trim() || undefined, model: finalModel });
      setApiKey("");
      toast.success("AI settings saved.");
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "Failed to save AI settings.");
    } finally {
      setSaving(false);
    }
  }

  return (
    <Card id="ai">
      <CardHeader>
        <CardTitle>AI Analyst</CardTitle>
        <CardDescription>
          {isDemo
            ? "The demo account is read-only. Create your own account to connect an AI provider."
            : "Bring your own API key. It is encrypted before it is saved, used only to answer your questions, and you pay the provider directly for what you use."}
        </CardDescription>
      </CardHeader>
      <CardContent className="grid max-w-md gap-4">
        <div className="grid gap-2">
          <Label>Provider</Label>
          <Select
            value={activeProvider}
            disabled={isDemo}
            onValueChange={(v) => {
              setProvider(v as AiSettings["provider"]);
              setModels(null);
              setModel(null);
              setApiKey("");
            }}
          >
            <SelectTrigger>
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {PROVIDERS.map((p) => (
                <SelectItem key={p.id} value={p.id}>
                  {p.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>

        <div className="grid gap-2">
          <Label htmlFor="ai-key">API key</Label>
          <div className="flex gap-2">
            <Input
              id="ai-key"
              type="password"
              autoComplete="off"
              disabled={isDemo}
              placeholder={hasSavedKey ? "Saved. Enter a new key to replace it" : providerInfo.keyHint}
              value={apiKey}
              onChange={(e) => setApiKey(e.target.value)}
            />
            <Button
              type="button"
              variant="outline"
              disabled={isDemo || checking || (!apiKey.trim() && !hasSavedKey)}
              onClick={check}
            >
              {checking ? <Loader2 className="size-4 animate-spin" /> : <CheckCircle2 className="size-4" />}
              Check
            </Button>
          </div>
          <p className="text-xs text-muted-foreground">
            Get a key from{" "}
            <a href={providerInfo.keysUrl} target="_blank" rel="noopener noreferrer" className="underline">
              {providerInfo.label.split(" ")[0]}
            </a>
            . Check confirms the key works and loads the models it can use.
          </p>
        </div>

        <div className="grid gap-2">
          <Label>Model</Label>
          {models ? (
            <Select value={selected && (selectedListed || selected === CUSTOM) ? selected : undefined} onValueChange={setModel}>
              <SelectTrigger>
                <SelectValue placeholder="Choose a model" />
              </SelectTrigger>
              <SelectContent>
                {models.map((m) => (
                  <SelectItem key={m.id} value={m.id}>
                    {m.label}
                  </SelectItem>
                ))}
                <SelectItem value={CUSTOM}>Other (type a model ID)…</SelectItem>
              </SelectContent>
            </Select>
          ) : (
            <p className="rounded-md border border-dashed px-3 py-2 text-sm text-muted-foreground">
              {hasSavedKey && settings ? (
                <>
                  Using <span className="font-medium text-foreground">{settings.model}</span>. Press
                  Check to pick a different one.
                </>
              ) : (
                "Enter a key and press Check to see the available models."
              )}
            </p>
          )}
          {selected === CUSTOM && (
            <Input
              value={customModel}
              onChange={(e) => setCustomModel(e.target.value)}
              placeholder="Exact model ID from the provider"
              aria-label="Model ID"
            />
          )}
        </div>

        <div className="flex flex-wrap gap-2">
          <Button
            disabled={isDemo || saving || !finalModel || (!apiKey.trim() && !hasSavedKey)}
            onClick={save}
          >
            {saving && <Loader2 className="size-4 animate-spin" />}
            Save
          </Button>
          {settings?.hasApiKey && (
            <Button
              variant="outline"
              disabled={isDemo || saving}
              onClick={async () => {
                try {
                  await deleteAiSettings();
                  setModels(null);
                  setModel(null);
                  toast.success("AI key removed.");
                } catch (err) {
                  toast.error(err instanceof Error ? err.message : "Failed to remove the key.");
                }
              }}
            >
              Remove key
            </Button>
          )}
        </div>

        <div className="flex items-start justify-between gap-4 border-t pt-4">
          <div>
            <Label htmlFor="share-names">Share buyer names with the AI</Label>
            <p className="mt-1 text-xs text-muted-foreground">
              Off by default: the analyst sees buyers as numbered labels (Buyer 12) and locations
              as a state or country, so personal details stay inside MakerMetrics.
            </p>
          </div>
          <Switch
            id="share-names"
            checked={prefs.shareBuyerNames === true}
            onCheckedChange={(on) => updatePrefs({ shareBuyerNames: on ? true : null })}
          />
        </div>
      </CardContent>
    </Card>
  );
}
