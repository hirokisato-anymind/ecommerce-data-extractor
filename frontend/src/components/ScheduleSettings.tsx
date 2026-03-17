"use client";

import { Badge } from "@/components/ui/badge";
import { Label } from "@/components/ui/label";
import type { ScheduleSlotConfig } from "@/lib/api";

interface ScheduleSettingsProps {
  config: ScheduleSlotConfig;
  enabled: boolean;
  onConfigChange: (config: ScheduleSlotConfig) => void;
  onEnabledChange: (enabled: boolean) => void;
}

const FREQUENCIES = [
  { value: "hourly" as const, label: "毎時" },
  { value: "daily" as const, label: "毎日" },
  { value: "weekly" as const, label: "毎週" },
  { value: "monthly" as const, label: "毎月" },
];

const DAY_OF_WEEK_LABELS = ["月", "火", "水", "木", "金", "土", "日"];

const MINUTE_OPTIONS = [0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55];

// Hours ordered: 6-23, then 0-5
const HOUR_ROWS = [...Array.from({ length: 18 }, (_, i) => i + 6), ...Array.from({ length: 6 }, (_, i) => i)];

function getNextExecution(config: ScheduleSlotConfig, enabled: boolean): string {
  if (!enabled) return "-";
  const now = new Date();
  const next = new Date(now);

  if (config.frequency === "hourly") {
    next.setMinutes(config.minute, 0, 0);
    if (next <= now) next.setHours(next.getHours() + 1);
  } else if (config.frequency === "daily") {
    next.setHours(config.hour, config.minute, 0, 0);
    if (next <= now) next.setDate(next.getDate() + 1);
  } else if (config.frequency === "weekly") {
    const dow = config.day_of_week ?? 0;
    next.setHours(config.hour, config.minute, 0, 0);
    // JS: 0=Sun, 1=Mon, ... 6=Sat. Our config: 0=Mon, 6=Sun.
    const jsDow = dow === 6 ? 0 : dow + 1;
    const currentDow = next.getDay();
    let daysUntil = jsDow - currentDow;
    if (daysUntil < 0) daysUntil += 7;
    if (daysUntil === 0 && next <= now) daysUntil = 7;
    next.setDate(next.getDate() + daysUntil);
  } else if (config.frequency === "monthly") {
    const dom = config.day_of_month ?? 1;
    next.setDate(dom);
    next.setHours(config.hour, config.minute, 0, 0);
    if (next <= now) next.setMonth(next.getMonth() + 1);
  }

  return next.toLocaleString("ja-JP", {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    weekday: "short",
  });
}

export function ScheduleSettings({ config, enabled, onConfigChange, onEnabledChange }: ScheduleSettingsProps) {
  const showTimePicker = config.frequency !== "hourly";
  const showDayOfWeek = config.frequency === "weekly";
  const showDayOfMonth = config.frequency === "monthly";

  const handleFrequencyChange = (freq: ScheduleSlotConfig["frequency"]) => {
    const updated: ScheduleSlotConfig = { ...config, frequency: freq };
    if (freq === "weekly" && updated.day_of_week === undefined) {
      updated.day_of_week = 0; // Monday
    }
    if (freq === "monthly" && updated.day_of_month === undefined) {
      updated.day_of_month = 1;
    }
    onConfigChange(updated);
  };

  const isSlotSelected = (hour: number, minute: number) => {
    return config.hour === hour && config.minute === minute;
  };

  const handleSlotClick = (hour: number, minute: number) => {
    onConfigChange({ ...config, hour, minute });
  };

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <label className="text-sm font-medium text-slate-700">スケジュール設定</label>
        <Badge variant={enabled ? "default" : "secondary"} className="text-[10px]">
          {enabled ? "有効" : "無効"}
        </Badge>
      </div>

      {/* Toggle */}
      <div className="flex items-center gap-3">
        <button
          type="button"
          role="switch"
          aria-checked={enabled}
          onClick={() => onEnabledChange(!enabled)}
          className={`relative inline-flex h-6 w-11 items-center rounded-full transition-colors ${
            enabled ? "bg-[#4f63d2]" : "bg-slate-300"
          }`}
        >
          <span
            className={`inline-block h-4 w-4 transform rounded-full bg-white transition-transform ${
              enabled ? "translate-x-6" : "translate-x-1"
            }`}
          />
        </button>
        <span className="text-sm text-slate-600">
          {enabled ? "定期実行を有効化中" : "定期実行を無効化中"}
        </span>
      </div>

      {enabled && (
        <div className="space-y-4">
          {/* Frequency */}
          <div className="space-y-2">
            <Label className="text-xs text-muted-foreground">実行頻度</Label>
            <div className="flex flex-wrap gap-2">
              {FREQUENCIES.map((freq) => (
                <button
                  key={freq.value}
                  type="button"
                  onClick={() => handleFrequencyChange(freq.value)}
                  className={`pill-button ${config.frequency === freq.value ? "active" : ""}`}
                >
                  {freq.label}
                </button>
              ))}
            </div>
          </div>

          {/* Time Slot Grid */}
          {showTimePicker && (
            <div className="space-y-2">
              <Label className="text-xs text-muted-foreground">
                実行時刻（クリックして選択）
              </Label>
              <div className="time-slot-grid rounded-md border overflow-hidden">
                {/* Header */}
                <div className="grid grid-cols-[3rem_repeat(12,1fr)] bg-slate-100 border-b">
                  <div className="text-[10px] text-muted-foreground font-medium p-1.5 text-center">時</div>
                  {MINUTE_OPTIONS.map((m) => (
                    <div key={m} className="text-[10px] text-muted-foreground font-medium p-1.5 text-center border-l">
                      :{String(m).padStart(2, "0")}
                    </div>
                  ))}
                </div>
                {/* Rows */}
                <div className="max-h-[240px] overflow-y-auto">
                  {HOUR_ROWS.map((hour) => (
                    <div key={hour} className="grid grid-cols-[3rem_repeat(12,1fr)] border-b last:border-b-0">
                      <div className="text-xs text-slate-600 font-medium p-1.5 text-center bg-slate-50">
                        {String(hour).padStart(2, "0")}
                      </div>
                      {MINUTE_OPTIONS.map((minute) => (
                        <button
                          key={`${hour}-${minute}`}
                          type="button"
                          onClick={() => handleSlotClick(hour, minute)}
                          className={`p-1.5 text-xs border-l transition-colors ${
                            isSlotSelected(hour, minute)
                              ? "bg-[#4f63d2] text-white font-semibold"
                              : "hover:bg-blue-50 text-slate-400"
                          }`}
                        >
                          {isSlotSelected(hour, minute) ? `${String(hour).padStart(2, "0")}:${String(minute).padStart(2, "0")}` : ""}
                        </button>
                      ))}
                    </div>
                  ))}
                </div>
              </div>
              <p className="text-xs text-muted-foreground">
                選択中: {String(config.hour).padStart(2, "0")}:{String(config.minute).padStart(2, "0")}
              </p>
            </div>
          )}

          {/* Minute selector for hourly */}
          {config.frequency === "hourly" && (
            <div className="space-y-2">
              <Label className="text-xs text-muted-foreground">実行分</Label>
              <div className="flex flex-wrap gap-2">
                {MINUTE_OPTIONS.map((m) => (
                  <button
                    key={m}
                    type="button"
                    onClick={() => onConfigChange({ ...config, minute: m })}
                    className={`pill-button ${config.minute === m ? "active" : ""}`}
                  >
                    {String(m).padStart(2, "0")}分
                  </button>
                ))}
              </div>
            </div>
          )}

          {/* Day of Week */}
          {showDayOfWeek && (
            <div className="space-y-2">
              <Label className="text-xs text-muted-foreground">曜日</Label>
              <div className="flex flex-wrap gap-2">
                {DAY_OF_WEEK_LABELS.map((label, i) => (
                  <button
                    key={i}
                    type="button"
                    onClick={() => onConfigChange({ ...config, day_of_week: i })}
                    className={`pill-button ${config.day_of_week === i ? "active" : ""}`}
                  >
                    {label}
                  </button>
                ))}
              </div>
            </div>
          )}

          {/* Day of Month */}
          {showDayOfMonth && (
            <div className="space-y-2">
              <Label className="text-xs text-muted-foreground">日</Label>
              <div className="grid grid-cols-7 gap-1.5 max-w-xs">
                {Array.from({ length: 31 }, (_, i) => i + 1).map((day) => (
                  <button
                    key={day}
                    type="button"
                    onClick={() => onConfigChange({ ...config, day_of_month: day })}
                    className={`w-9 h-9 rounded-md text-xs font-medium transition-colors ${
                      config.day_of_month === day
                        ? "bg-[#4f63d2] text-white"
                        : "bg-slate-100 text-slate-600 hover:bg-blue-50 hover:text-[#4f63d2]"
                    }`}
                  >
                    {day}
                  </button>
                ))}
              </div>
            </div>
          )}

          {/* Next Execution */}
          <div className="rounded-md bg-slate-50 border p-3">
            <div className="text-xs text-muted-foreground mb-1">次回実行予定</div>
            <div className="text-sm font-medium text-slate-700">
              {getNextExecution(config, enabled)}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
