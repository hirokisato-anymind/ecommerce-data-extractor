"use client";

import type { Platform } from "@/lib/api";

interface StatusBarProps {
  platforms: Platform[];
}

export function StatusBar({ platforms }: StatusBarProps) {
  const configured = platforms.filter((p) => p.configured).length;

  return (
    <div className="px-3 py-3 border-t border-slate-200">
      <div className="flex items-center gap-2 text-xs">
        <span
          className={`status-dot ${configured > 0 ? "connected" : "disconnected"}`}
        />
        <span className="text-slate-500">
          {configured}/{platforms.length} 接続済
        </span>
      </div>
    </div>
  );
}
