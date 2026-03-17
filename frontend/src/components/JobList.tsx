"use client";

import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import type { FilterConfig, DestinationConfig, ScheduleSlotConfig } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogFooter,
  DialogClose,
} from "@/components/ui/dialog";

export interface ScheduleJob {
  id: string;
  name: string;
  platform_id: string;
  endpoint_id: string;
  columns: string[] | null;
  filters: FilterConfig[] | null;
  limit: number;
  destination: DestinationConfig;
  schedule_config: ScheduleSlotConfig;
  enabled: boolean;
  created_at: string;
  updated_at: string;
  last_run_at: string | null;
  last_run_status: string | null;
}

interface JobListProps {
  onEdit: (job: ScheduleJob) => void;
}

const PLATFORM_NAMES: Record<string, string> = {
  shopify: "Shopify",
  amazon: "Amazon",
  rakuten: "Rakuten",
  yahoo: "Yahoo",
};

function formatSchedule(config: ScheduleSlotConfig): string {
  const time = `${String(config.hour).padStart(2, "0")}:${String(config.minute).padStart(2, "0")}`;
  switch (config.frequency) {
    case "hourly":
      return `毎時 ${String(config.minute).padStart(2, "0")}分`;
    case "daily":
      return `毎日 ${time}`;
    case "weekly": {
      const days = ["月", "火", "水", "木", "金", "土", "日"];
      return `毎週${days[config.day_of_week ?? 0]} ${time}`;
    }
    case "monthly":
      return `毎月${config.day_of_month ?? 1}日 ${time}`;
    default:
      return "-";
  }
}

function formatDateTime(dt: string | null): string {
  if (!dt) return "-";
  try {
    return new Date(dt).toLocaleString("ja-JP", {
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
    });
  } catch {
    return dt;
  }
}

const TRANSFER_MODE_LABELS: Record<string, string> = {
  append: "APPEND",
  append_direct: "APPEND DIRECT",
  replace: "REPLACE",
  delete_in_advance: "DELETE IN ADVANCE",
  upsert: "UPSERT",
};

export function JobList({ onEdit }: JobListProps) {
  const queryClient = useQueryClient();
  const [deleteTarget, setDeleteTarget] = useState<ScheduleJob | null>(null);
  const [runningJobIds, setRunningJobIds] = useState<Set<string>>(new Set());

  const { data: jobs = [], isLoading, isError, error } = useQuery<ScheduleJob[]>({
    queryKey: ["schedules"],
    queryFn: api.listSchedules,
    refetchInterval: 30000,
  });

  const deleteMutation = useMutation({
    mutationFn: (id: string) => api.deleteSchedule(id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["schedules"] });
      setDeleteTarget(null);
    },
  });

  const toggleMutation = useMutation({
    mutationFn: ({ id, enabled }: { id: string; enabled: boolean }) =>
      api.updateSchedule(id, { enabled }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["schedules"] });
    },
  });

  const triggerMutation = useMutation({
    mutationFn: (id: string) => {
      setRunningJobIds((prev) => new Set(prev).add(id));
      return api.triggerSchedule(id);
    },
    onSuccess: (_data, id) => {
      setRunningJobIds((prev) => { const next = new Set(prev); next.delete(id); return next; });
      queryClient.invalidateQueries({ queryKey: ["schedules"] });
    },
    onError: (_error, id) => {
      setRunningJobIds((prev) => { const next = new Set(prev); next.delete(id); return next; });
    },
  });

  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="flex items-center gap-2 text-sm text-slate-500">
          <svg className="animate-spin h-4 w-4" viewBox="0 0 24 24" fill="none">
            <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
            <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
          </svg>
          読み込み中...
        </div>
      </div>
    );
  }

  if (isError) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="text-sm text-red-600 bg-red-50 p-4 rounded-md">
          ジョブ一覧の取得に失敗しました: {(error as Error).message}
        </div>
      </div>
    );
  }

  if (jobs.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center h-64 text-slate-400">
        <svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" className="mb-3">
          <rect x="2" y="7" width="20" height="14" rx="2" ry="2" />
          <path d="M16 21V5a2 2 0 0 0-2-2h-4a2 2 0 0 0-2 2v16" />
        </svg>
        <p className="text-sm">登録済みのジョブはありません</p>
      </div>
    );
  }

  return (
    <div className="space-y-4">
      <div className="rounded-lg border border-slate-200 bg-white shadow-sm overflow-hidden">
        <Table>
          <TableHeader>
            <TableRow className="bg-slate-50">
              <TableHead className="text-xs">ジョブ名</TableHead>
              <TableHead className="text-xs">プラットフォーム</TableHead>
              <TableHead className="text-xs">エンドポイント</TableHead>
              <TableHead className="text-xs">スケジュール</TableHead>
              <TableHead className="text-xs">転送モード</TableHead>
              <TableHead className="text-xs">転送先</TableHead>
              <TableHead className="text-xs">ステータス</TableHead>
              <TableHead className="text-xs">最終実行</TableHead>
              <TableHead className="text-xs text-right">操作</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {jobs.map((job) => (
              <TableRow key={job.id}>
                {/* Job name */}
                <TableCell className="text-sm font-medium text-slate-800 max-w-[160px] truncate">
                  {job.name}
                </TableCell>

                {/* Platform */}
                <TableCell className="text-xs text-slate-600">
                  {PLATFORM_NAMES[job.platform_id] || job.platform_id}
                </TableCell>

                {/* Endpoint */}
                <TableCell className="text-xs text-slate-600">
                  {job.endpoint_id}
                </TableCell>

                {/* Schedule */}
                <TableCell className="text-xs text-slate-600">
                  {formatSchedule(job.schedule_config)}
                </TableCell>

                {/* Transfer mode */}
                <TableCell>
                  <Badge variant="outline" className="text-[10px] font-normal">
                    {TRANSFER_MODE_LABELS[job.destination.transfer_mode] || job.destination.transfer_mode}
                  </Badge>
                </TableCell>

                {/* Destination */}
                <TableCell className="text-xs text-slate-600 max-w-[200px] truncate" title={`${job.destination.project_id}.${job.destination.dataset_id}.${job.destination.table_id}`}>
                  {job.destination.project_id}.{job.destination.dataset_id}.{job.destination.table_id}
                </TableCell>

                {/* Status toggle */}
                <TableCell>
                  <button
                    type="button"
                    role="switch"
                    aria-checked={job.enabled}
                    onClick={() => toggleMutation.mutate({ id: job.id, enabled: !job.enabled })}
                    disabled={toggleMutation.isPending}
                    className={`relative inline-flex h-5 w-9 items-center rounded-full transition-colors ${
                      job.enabled ? "bg-[#4f63d2]" : "bg-slate-300"
                    }`}
                  >
                    <span
                      className={`inline-block h-3.5 w-3.5 transform rounded-full bg-white transition-transform ${
                        job.enabled ? "translate-x-4.5" : "translate-x-0.5"
                      }`}
                    />
                  </button>
                </TableCell>

                {/* Last run */}
                <TableCell>
                  <div className="flex items-center gap-1.5">
                    <span className="text-xs text-slate-600">
                      {formatDateTime(job.last_run_at)}
                    </span>
                    {job.last_run_status && (() => {
                      const isSuccess = job.last_run_status.startsWith("成功") || job.last_run_status === "success";
                      const isZeroRows = isSuccess && job.last_run_status.includes("0行");
                      return (
                        <Badge
                          variant={isSuccess ? (isZeroRows ? "outline" : "default") : "destructive"}
                          className={`text-[10px] px-1.5 py-0 ${isZeroRows ? "text-amber-600 border-amber-400" : ""}`}
                          title={job.last_run_status}
                        >
                          {isSuccess ? (isZeroRows ? "0件" : job.last_run_status.match(/(\d+)行/)?.[0] || "成功") : "失敗"}
                        </Badge>
                      );
                    })()}
                  </div>
                </TableCell>

                {/* Actions */}
                <TableCell>
                  <div className="flex items-center justify-end gap-1">
                    <Button
                      variant="ghost"
                      size="sm"
                      className="text-xs h-7 px-2"
                      onClick={() => onEdit(job)}
                    >
                      編集
                    </Button>
                    <Button
                      variant="ghost"
                      size="sm"
                      className="text-xs h-7 px-2"
                      onClick={() => triggerMutation.mutate(job.id)}
                      disabled={runningJobIds.has(job.id)}
                    >
                      {runningJobIds.has(job.id) ? "実行中..." : "今すぐ実行"}
                    </Button>
                    <Button
                      variant="ghost"
                      size="sm"
                      className="text-xs h-7 px-2 text-red-600 hover:text-red-700 hover:bg-red-50"
                      onClick={() => setDeleteTarget(job)}
                    >
                      削除
                    </Button>
                  </div>
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </div>

      {/* Delete Confirmation Dialog */}
      <Dialog open={!!deleteTarget} onOpenChange={(open) => { if (!open) setDeleteTarget(null); }}>
        <DialogContent className="sm:max-w-md">
          <DialogHeader>
            <DialogTitle>ジョブの削除</DialogTitle>
            <DialogDescription>
              ジョブ「{deleteTarget?.name}」を削除してもよろしいですか？この操作は取り消せません。
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <DialogClose render={<Button variant="outline" />}>
              キャンセル
            </DialogClose>
            <Button
              variant="destructive"
              onClick={() => {
                if (deleteTarget) deleteMutation.mutate(deleteTarget.id);
              }}
              disabled={deleteMutation.isPending}
            >
              {deleteMutation.isPending ? "削除中..." : "削除する"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
