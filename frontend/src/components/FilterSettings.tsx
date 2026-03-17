"use client";

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import type { SchemaField, FilterConfig } from "@/lib/api";

interface FilterSettingsProps {
  fields: SchemaField[];
  filters: FilterConfig[];
  onChange: (filters: FilterConfig[]) => void;
}

const STRING_OPERATORS = [
  { value: "eq", label: "等しい" },
  { value: "contains", label: "含む" },
  { value: "starts_with", label: "前方一致" },
];

const NUMBER_OPERATORS = [
  { value: "eq", label: "等しい" },
  { value: "gte", label: "以上" },
  { value: "lte", label: "以下" },
  { value: "range", label: "範囲" },
];

const DATE_OPERATORS = [
  { value: "last_n_days", label: "直近N日" },
  { value: "last_n_hours", label: "直近N時間" },
  { value: "date_range", label: "期間指定" },
];

function getOperators(fieldType: string) {
  if (fieldType === "date" || fieldType === "datetime") return DATE_OPERATORS;
  if (fieldType === "number" || fieldType === "integer" || fieldType === "float") return NUMBER_OPERATORS;
  return STRING_OPERATORS;
}

function getValuePlaceholder(operator: string): string {
  switch (operator) {
    case "last_n_days": return "例: 7";
    case "last_n_hours": return "例: 24";
    case "date_range": return "開始日,終了日 (例: 2024-01-01,2024-12-31)";
    case "range": return "最小値,最大値 (例: 100,500)";
    default: return "値を入力";
  }
}

export function FilterSettings({ fields, filters, onChange }: FilterSettingsProps) {
  const addFilter = () => {
    onChange([...filters, { column: "", operator: "", value: "" }]);
  };

  const removeFilter = (index: number) => {
    onChange(filters.filter((_, i) => i !== index));
  };

  const updateFilter = (index: number, patch: Partial<FilterConfig>) => {
    const updated = filters.map((f, i) => {
      if (i !== index) return f;
      const newFilter = { ...f, ...patch };
      // Reset operator and value when column changes
      if (patch.column && patch.column !== f.column) {
        newFilter.operator = "";
        newFilter.value = "";
      }
      // Reset value when operator changes
      if (patch.operator && patch.operator !== f.operator) {
        newFilter.value = "";
      }
      return newFilter;
    });
    onChange(updated);
  };

  const getFieldType = (columnKey: string): string => {
    const field = fields.find((f) => f.key === columnKey);
    return field?.type ?? "string";
  };

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between">
        <label className="text-sm font-medium text-slate-700">フィルター設定</label>
        <span className="text-xs text-muted-foreground">
          {filters.length > 0 ? `${filters.length} 件のフィルター` : "フィルターなし"}
        </span>
      </div>

      {filters.length > 0 && (
        <div className="space-y-2">
          {filters.map((filter, index) => {
            const fieldType = getFieldType(filter.column);
            const operators = filter.column ? getOperators(fieldType) : [];
            const isDateField = fieldType === "date" || fieldType === "datetime";

            return (
              <div
                key={index}
                className="flex items-start gap-2 p-3 rounded-lg border bg-slate-50/50"
              >
                {/* Column selector */}
                <div className="flex-1 min-w-0">
                  <Select
                    value={filter.column}
                    onValueChange={(v) => { if (v) updateFilter(index, { column: v }); }}
                  >
                    <SelectTrigger className="w-full text-xs h-8">
                      <SelectValue placeholder="カラムを選択" />
                    </SelectTrigger>
                    <SelectContent>
                      {fields.map((f) => (
                        <SelectItem key={f.key} value={f.key}>
                          <span className="font-mono text-xs">{f.key}</span>
                          {f.description && (
                            <span className="ml-1 text-muted-foreground text-xs">
                              ({f.description})
                            </span>
                          )}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>

                {/* Operator selector */}
                <div className="w-32 shrink-0">
                  <Select
                    value={filter.operator}
                    onValueChange={(v) => { if (v) updateFilter(index, { operator: v }); }}
                    disabled={!filter.column}
                  >
                    <SelectTrigger className="w-full text-xs h-8">
                      <SelectValue placeholder="条件" />
                    </SelectTrigger>
                    <SelectContent>
                      {operators.map((op) => (
                        <SelectItem key={op.value} value={op.value}>
                          {op.label}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>

                {/* Value input */}
                <div className="flex-1 min-w-0">
                  {isDateField && filter.operator === "last_n_days" ? (
                    <div className="flex items-center gap-1">
                      <Input
                        type="number"
                        min={1}
                        value={filter.value}
                        onChange={(e) => updateFilter(index, { value: e.target.value })}
                        placeholder="N"
                        className="w-20 text-xs h-8"
                      />
                      <span className="text-xs text-muted-foreground whitespace-nowrap">日前まで</span>
                    </div>
                  ) : isDateField && filter.operator === "last_n_hours" ? (
                    <div className="flex items-center gap-1">
                      <Input
                        type="number"
                        min={1}
                        value={filter.value}
                        onChange={(e) => updateFilter(index, { value: e.target.value })}
                        placeholder="N"
                        className="w-20 text-xs h-8"
                      />
                      <span className="text-xs text-muted-foreground whitespace-nowrap">時間前まで</span>
                    </div>
                  ) : isDateField && filter.operator === "date_range" ? (
                    <div className="flex flex-col gap-1">
                      <div className="flex items-center gap-1">
                        <span className="text-xs text-muted-foreground whitespace-nowrap">開始日</span>
                        <Input
                          type={fieldType === "datetime" ? "datetime-local" : "date"}
                          value={filter.value.split(",")[0] || ""}
                          onChange={(e) => {
                            const end = filter.value.split(",")[1] || "";
                            updateFilter(index, { value: `${e.target.value},${end}` });
                          }}
                          className="text-xs h-8"
                        />
                      </div>
                      <div className="flex items-center gap-1">
                        <span className="text-xs text-muted-foreground whitespace-nowrap">終了日</span>
                        <Input
                          type={fieldType === "datetime" ? "datetime-local" : "date"}
                          value={filter.value.split(",")[1] || ""}
                          onChange={(e) => {
                            const start = filter.value.split(",")[0] || "";
                            updateFilter(index, { value: `${start},${e.target.value}` });
                          }}
                          className="text-xs h-8"
                        />
                      </div>
                    </div>
                  ) : (
                    <Input
                      value={filter.value}
                      onChange={(e) => updateFilter(index, { value: e.target.value })}
                      placeholder={getValuePlaceholder(filter.operator)}
                      className="text-xs h-8"
                      disabled={!filter.operator}
                    />
                  )}
                </div>

                {/* Delete button */}
                <Button
                  variant="ghost"
                  size="sm"
                  className="h-8 w-8 p-0 text-slate-400 hover:text-red-500 shrink-0"
                  onClick={() => removeFilter(index)}
                >
                  <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                    <path d="M18 6L6 18M6 6l12 12" />
                  </svg>
                </Button>
              </div>
            );
          })}
        </div>
      )}

      <Button
        variant="outline"
        size="sm"
        onClick={addFilter}
        className="text-xs"
      >
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" className="mr-1">
          <path d="M12 5v14M5 12h14" />
        </svg>
        フィルター追加
      </Button>
    </div>
  );
}
