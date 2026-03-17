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
  return (
    <div className="space-y-2">
      <label className="text-sm font-medium">取得件数</label>
      <Input
        type="number"
        min={1}
        max={10000}
        value={value}
        onChange={(e) => {
          const n = parseInt(e.target.value, 10);
          if (!isNaN(n) && n >= 1 && n <= 10000) onChange(n);
        }}
        className="w-32"
      />
    </div>
  );
}
