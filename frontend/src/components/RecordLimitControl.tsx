"use client";

import { Input } from "@/components/ui/input";

interface RecordLimitControlProps {
  value: number;
  onChange: (value: number) => void;
}

export function RecordLimitControl({
  value,
  onChange,
}: RecordLimitControlProps) {
  const isUnlimited = value === 0;

  return (
    <div className="space-y-2">
      <label className="text-sm font-medium">取得件数上限</label>
      <div className="flex items-center gap-3">
        <label className="flex items-center gap-1.5 text-sm cursor-pointer">
          <input
            type="checkbox"
            checked={isUnlimited}
            onChange={(e) => onChange(e.target.checked ? 0 : 10000)}
            className="rounded"
          />
          全件取得
        </label>
        {!isUnlimited && (
          <Input
            type="number"
            min={1}
            value={value}
            onChange={(e) => {
              const n = parseInt(e.target.value, 10);
              if (!isNaN(n) && n >= 1) onChange(n);
            }}
            className="w-32"
          />
        )}
        {isUnlimited && (
          <span className="text-sm text-muted-foreground">無制限</span>
        )}
      </div>
    </div>
  );
}
