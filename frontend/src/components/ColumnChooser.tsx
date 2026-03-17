"use client";

import { Checkbox } from "@/components/ui/checkbox";
import type { SchemaField } from "@/lib/api";

interface ColumnChooserProps {
  fields: SchemaField[];
  selected: Set<string>;
  onToggle: (key: string) => void;
  onSelectAll: () => void;
  onDeselectAll: () => void;
}

export function ColumnChooser({
  fields,
  selected,
  onToggle,
  onSelectAll,
  onDeselectAll,
}: ColumnChooserProps) {
  const allSelected = fields.length > 0 && selected.size === fields.length;

  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between">
        <label className="text-sm font-medium">カラム選択</label>
        <span className="text-xs text-muted-foreground">
          {selected.size} / {fields.length} 選択中
        </span>
      </div>
      <div className="rounded-md border max-h-80 overflow-y-auto">
        <table className="w-full text-sm">
          <thead className="sticky top-0 bg-muted/80 backdrop-blur-sm">
            <tr className="border-b">
              <th className="w-10 px-3 py-2 text-left">
                <Checkbox
                  checked={allSelected}
                  onCheckedChange={() =>
                    allSelected ? onDeselectAll() : onSelectAll()
                  }
                />
              </th>
              <th className="px-3 py-2 text-left font-medium">フィールド名</th>
              <th className="px-3 py-2 text-left font-medium">説明</th>
              <th className="px-3 py-2 text-left font-medium w-20">型</th>
            </tr>
          </thead>
          <tbody>
            {fields.map((f) => (
              <tr
                key={f.key}
                className="border-b last:border-0 hover:bg-accent/50 cursor-pointer transition-colors"
                onClick={() => onToggle(f.key)}
              >
                <td className="px-3 py-1.5">
                  <Checkbox
                    checked={selected.has(f.key)}
                    onCheckedChange={() => onToggle(f.key)}
                  />
                </td>
                <td className="px-3 py-1.5 font-mono text-xs">{f.key}</td>
                <td className="px-3 py-1.5 text-muted-foreground">
                  {f.description || f.label}
                </td>
                <td className="px-3 py-1.5 text-xs text-muted-foreground">
                  {f.type}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
