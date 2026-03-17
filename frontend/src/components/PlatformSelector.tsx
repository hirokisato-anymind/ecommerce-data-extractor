"use client";

import Image from "next/image";
import type { Platform } from "@/lib/api";

interface PlatformSelectorProps {
  platforms: Platform[];
  selected: string | null;
  onSelect: (id: string) => void;
  onConfigure: (platform: Platform) => void;
}

const PLATFORM_LOGOS: Record<string, { src: string; w: number; h: number }> = {
  shopify: { src: "/shopify_logo.webp", w: 24, h: 24 },
  amazon: { src: "/Amazon_logo.png", w: 24, h: 24 },
  rakuten: { src: "/rakuten_logo.png", w: 24, h: 24 },
  yahoo: { src: "/yahoo_logo.png", w: 24, h: 24 },
};

export function PlatformSelector({
  platforms,
  selected,
  onSelect,
  onConfigure,
}: PlatformSelectorProps) {
  return (
    <div className="space-y-1">
      {platforms.map((p) => {
        const logo = PLATFORM_LOGOS[p.id];
        return (
          <div
            key={p.id}
            className={`sidebar-item flex items-center justify-between px-3 py-2.5 cursor-pointer ${
              selected === p.id ? "active" : ""
            }`}
            onClick={() => onSelect(p.id)}
          >
            <div className="flex items-center gap-3 min-w-0">
              {logo ? (
                <Image
                  src={logo.src}
                  alt={p.name}
                  width={logo.w}
                  height={logo.h}
                  className="rounded object-contain shrink-0"
                />
              ) : (
                <span
                  className={`status-dot ${p.configured ? "connected" : "disconnected"}`}
                />
              )}
              <span className="text-sm font-medium truncate text-slate-800">{p.name}</span>
              <span
                className={`status-dot ${p.configured ? "connected" : "disconnected"}`}
              />
            </div>
            <button
              className="opacity-0 p-1 rounded hover:bg-slate-200 transition-opacity shrink-0"
              style={{ opacity: selected === p.id ? 0.7 : 0 }}
              onClick={(e) => {
                e.stopPropagation();
                onConfigure(p);
              }}
              title="API設定"
            >
              <svg
                width="14"
                height="14"
                viewBox="0 0 24 24"
                fill="none"
                stroke="currentColor"
                strokeWidth="2"
                strokeLinecap="round"
                strokeLinejoin="round"
                className="text-slate-500"
              >
                <path d="M12.22 2h-.44a2 2 0 0 0-2 2v.18a2 2 0 0 1-1 1.73l-.43.25a2 2 0 0 1-2 0l-.15-.08a2 2 0 0 0-2.73.73l-.22.38a2 2 0 0 0 .73 2.73l.15.1a2 2 0 0 1 1 1.72v.51a2 2 0 0 1-1 1.74l-.15.09a2 2 0 0 0-.73 2.73l.22.38a2 2 0 0 0 2.73.73l.15-.08a2 2 0 0 1 2 0l.43.25a2 2 0 0 1 1 1.73V20a2 2 0 0 0 2 2h.44a2 2 0 0 0 2-2v-.18a2 2 0 0 1 1-1.73l.43-.25a2 2 0 0 1 2 0l.15.08a2 2 0 0 0 2.73-.73l.22-.39a2 2 0 0 0-.73-2.73l-.15-.08a2 2 0 0 1-1-1.74v-.5a2 2 0 0 1 1-1.74l.15-.09a2 2 0 0 0 .73-2.73l-.22-.38a2 2 0 0 0-2.73-.73l-.15.08a2 2 0 0 1-2 0l-.43-.25a2 2 0 0 1-1-1.73V4a2 2 0 0 0-2-2z" />
                <circle cx="12" cy="12" r="3" />
              </svg>
            </button>
          </div>
        );
      })}
    </div>
  );
}
