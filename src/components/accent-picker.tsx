"use client";

import { useRef, useSyncExternalStore } from "react";
import { Check, Pipette } from "lucide-react";
import {
  ACCENT_PRESETS,
  getAccent,
  getServerAccent,
  subscribeAccent,
} from "@/lib/theme/accent-store";
import { setAccentPersisted } from "@/components/accent-provider";
import { cn } from "@/lib/utils";

export function AccentPicker({ disabled = false }: { disabled?: boolean }) {
  const accent = useSyncExternalStore(subscribeAccent, getAccent, getServerAccent);
  const colorInputRef = useRef<HTMLInputElement>(null);
  const isPreset = ACCENT_PRESETS.some((p) => p.value.toLowerCase() === accent.toLowerCase());

  return (
    <div className="flex flex-wrap items-center gap-3">
      {ACCENT_PRESETS.map((preset) => {
        const selected = preset.value.toLowerCase() === accent.toLowerCase();
        return (
          <button
            key={preset.value}
            type="button"
            title={preset.name}
            aria-label={preset.name}
            disabled={disabled}
            onClick={() => void setAccentPersisted(preset.value)}
            className={cn(
              "flex size-9 items-center justify-center rounded-full transition-transform hover:scale-110",
              disabled && "cursor-not-allowed opacity-60 hover:scale-100",
              selected && "ring-2 ring-offset-2 ring-offset-background"
            )}
            style={{ backgroundColor: preset.value, ["--tw-ring-color" as string]: preset.value }}
          >
            {selected && <Check className="size-4 text-white" strokeWidth={3} />}
          </button>
        );
      })}

      {/* Custom color wheel via the native picker */}
      <button
        type="button"
        title="Custom color"
        aria-label="Pick a custom color"
        disabled={disabled}
        onClick={() => colorInputRef.current?.click()}
        className={cn(
          "relative flex size-9 items-center justify-center rounded-full transition-transform hover:scale-110",
          disabled && "cursor-not-allowed opacity-60 hover:scale-100",
          !isPreset && "ring-2 ring-offset-2 ring-offset-background"
        )}
        style={{
          background: !isPreset
            ? accent
            : "conic-gradient(from 0deg, #ef4444, #eab308, #22c55e, #06b6d4, #3b82f6, #a855f7, #ef4444)",
          ["--tw-ring-color" as string]: !isPreset ? accent : "var(--ring)",
        }}
      >
        {!isPreset ? (
          <Check className="size-4 text-white" strokeWidth={3} />
        ) : (
          <Pipette className="size-4 text-white drop-shadow" />
        )}
        <input
          ref={colorInputRef}
          type="color"
          value={accent}
          disabled={disabled}
          onChange={(e) => void setAccentPersisted(e.target.value)}
          className="absolute inset-0 size-full cursor-pointer opacity-0"
          aria-hidden
          tabIndex={-1}
        />
      </button>
    </div>
  );
}
