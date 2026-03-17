"use client";

import type { Endpoint } from "@/lib/api";

interface EndpointPickerProps {
  endpoints: Endpoint[];
  selected: string | null;
  onSelect: (id: string) => void;
  disabled?: boolean;
}

export function EndpointPicker({
  endpoints,
  selected,
  onSelect,
  disabled,
}: EndpointPickerProps) {
  if (endpoints.length === 0) {
    return (
      <div className="text-sm text-muted-foreground py-2">
        {disabled
          ? "プラットフォームを選択してください"
          : "利用可能なエンドポイントがありません"}
      </div>
    );
  }

  return (
    <div className="space-y-2">
      <label className="text-sm font-medium text-slate-700">エンドポイント</label>
      <div className="flex flex-wrap gap-2">
        {endpoints.map((ep) => (
          <button
            key={ep.id}
            type="button"
            disabled={disabled}
            onClick={() => onSelect(ep.id)}
            className={`pill-button ${selected === ep.id ? "active" : ""}`}
            title={ep.description}
          >
            {ep.name}
          </button>
        ))}
      </div>
    </div>
  );
}
