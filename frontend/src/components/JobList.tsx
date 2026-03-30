"use client";

import { useState, useEffect, useRef, Fragment } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import type { FilterConfig, DestinationConfig, ScheduleSlotConfig, JobLogEntry } from "@/lib/api";
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
  keyword?: string;
  limit: number;
  destination: DestinationConfig;
  schedule_config: ScheduleSlotConfig;
  enabled: boolean;
  created_at: string;
  updated_at: string;
  last_run_at: string | null;
  last_run_status: string | null;
  last_synced_at: string | null;
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

function getNextRunDate(config: ScheduleSlotConfig): string {
  const now = new Date();
  const target = new Date(now);

  switch (config.frequency) {
    case "hourly": {
      target.setMinutes(config.minute, 0, 0);
      if (target <= now) target.setHours(target.getHours() + 1);
      break;
    }
    case "daily": {
      target.setHours(config.hour, config.minute, 0, 0);
      if (target <= now) target.setDate(target.getDate() + 1);
      break;
    }
    case "weekly": {
      target.setHours(config.hour, config.minute, 0, 0);
      const targetDow = ((config.day_of_week ?? 0) + 1) % 7;
      let diff = targetDow - now.getDay();
      if (diff < 0 || (diff === 0 && target <= now)) diff += 7;
      target.setDate(target.getDate() + diff);
      break;
    }
    case "monthly": {
      const dom = config.day_of_month ?? 1;
      target.setDate(dom);
      target.setHours(config.hour, config.minute, 0, 0);
      if (target <= now) target.setMonth(target.getMonth() + 1);
      break;
    }
    default:
      return "-";
  }

  return target.toLocaleString("ja-JP", {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  });
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

const COL_COUNT = 8;

// -- Log Panel Component --
function JobLogPanel({ scheduleId, isRunning }: { scheduleId: string; isRunning: boolean }) {
  const [logs, setLogs] = useState<JobLogEntry[]>([]);
  const nextIndexRef = useRef(0);
  const scrollRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    // Reset when scheduleId changes
    setLogs([]);
    nextIndexRef.current = 0;
  }, [scheduleId]);

  useEffect(() => {
    let cancelled = false;
    const poll = async () => {
      try {
        const res = await api.getJobLogs(scheduleId, nextIndexRef.current);
        if (cancelled) return;
        if (res.logs.length > 0) {
          setLogs((prev) => [...prev, ...res.logs]);
          nextIndexRef.current = res.next_index;
        } else if (nextIndexRef.current > 0) {
          // Backend may have cleared logs (new job run) — check from 0
          const fresh = await api.getJobLogs(scheduleId, 0);
          if (cancelled) return;
          if (fresh.logs.length > 0 && fresh.next_index <= nextIndexRef.current) {
            // Logs were reset: replace with fresh data
            setLogs(fresh.logs);
            nextIndexRef.current = fresh.next_index;
          }
        }
      } catch {
        // ignore
      }
    };
    poll();
    const interval = setInterval(poll, isRunning ? 2000 : 10000);
    return () => { cancelled = true; clearInterval(interval); };
  }, [scheduleId, isRunning]);

  // Auto-scroll to bottom
  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [logs]);

  return (
    <div className="bg-slate-50 border-t border-slate-200">
      <div ref={scrollRef} className="max-h-64 overflow-y-auto p-3 font-mono text-xs space-y-0.5">
        {logs.length === 0 ? (
          <p className="text-slate-400 py-2 text-center">ログはまだありません</p>
        ) : (
          logs.map((entry) => {
            const time = new Date(entry.timestamp).toLocaleTimeString("ja-JP", {
              hour: "2-digit",
              minute: "2-digit",
              second: "2-digit",
            });
            const color =
              entry.level === "error" ? "text-red-600" :
              entry.level === "warning" ? "text-amber-600" :
              "text-slate-700";
            return (
              <div key={entry.index} className={`flex gap-2 ${color}`}>
                <span className="text-slate-400 shrink-0">{time}</span>
                <span>{entry.message}</span>
              </div>
            );
          })
        )}
        {isRunning && (
          <div className="flex items-center gap-1.5 text-slate-400 pt-1">
            <svg className="animate-spin h-3 w-3" viewBox="0 0 24 24" fill="none">
              <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
              <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
            </svg>
            実行中...
          </div>
        )}
      </div>
    </div>
  );
}

export function JobList({ onEdit }: JobListProps) {
  const queryClient = useQueryClient();
  const [deleteTarget, setDeleteTarget] = useState<ScheduleJob | null>(null);
  const [expandedJobId, setExpandedJobId] = useState<string | null>(null);

  const { data: jobs = [], isLoading, isError, error } = useQuery<ScheduleJob[]>({
    queryKey: ["schedules"],
    queryFn: api.listSchedules,
    refetchInterval: (query) => {
      const hasRunning = query.state.data?.some((j) => j.last_run_status === "実行中");
      return hasRunning ? 5000 : 30000;
    },
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
    onMutate: async ({ id, enabled }) => {
      await queryClient.cancelQueries({ queryKey: ["schedules"] });
      const previous = queryClient.getQueryData<ScheduleJob[]>(["schedules"]);
      queryClient.setQueryData<ScheduleJob[]>(["schedules"], (old) =>
        old?.map((job) => (job.id === id ? { ...job, enabled } : job))
      );
      return { previous };
    },
    onError: (_err, _vars, context) => {
      if (context?.previous) {
        queryClient.setQueryData(["schedules"], context.previous);
      }
    },
    onSettled: () => {
      queryClient.invalidateQueries({ queryKey: ["schedules"] });
    },
  });

  const triggerMutation = useMutation({
    mutationFn: (id: string) => api.triggerSchedule(id),
    onSuccess: (_data, id) => {
      setExpandedJobId(id);
      queryClient.invalidateQueries({ queryKey: ["schedules"] });
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
              <TableHead className="text-xs w-8"></TableHead>
              <TableHead className="text-xs">ジョブ名</TableHead>
              <TableHead className="text-xs">エンドポイント</TableHead>
              <TableHead className="text-xs">次回実行</TableHead>
              <TableHead className="text-xs">転送モード</TableHead>
              <TableHead className="text-xs">転送先</TableHead>
              <TableHead className="text-xs">ステータス</TableHead>
              <TableHead className="text-xs">最終実行</TableHead>
              <TableHead className="text-xs text-right">操作</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {jobs.map((job) => {
              const isExpanded = expandedJobId === job.id;
              const isRunning = job.last_run_status === "実行中";
              return (
                <Fragment key={job.id}>
                  <TableRow
                    className={`cursor-pointer hover:bg-slate-50 ${isExpanded ? "bg-slate-50" : ""}`}
                    onClick={() => setExpandedJobId(isExpanded ? null : job.id)}
                  >
                    {/* Chevron */}
                    <TableCell className="w-8 px-2">
                      <svg
                        width="14"
                        height="14"
                        viewBox="0 0 24 24"
                        fill="none"
                        stroke="currentColor"
                        strokeWidth="2"
                        className={`text-slate-400 transition-transform ${isExpanded ? "rotate-90" : ""}`}
                      >
                        <path d="M9 18l6-6-6-6" />
                      </svg>
                    </TableCell>

                    {/* Job name */}
                    <TableCell className="text-sm font-medium text-slate-800 max-w-[160px] truncate">
                      {job.name}
                    </TableCell>

                    {/* Endpoint */}
                    <TableCell className="text-xs text-slate-600">
                      {PLATFORM_NAMES[job.platform_id] || job.platform_id} / {job.endpoint_id}
                    </TableCell>

                    {/* Next run */}
                    <TableCell className="text-xs text-slate-600">
                      {job.enabled ? getNextRunDate(job.schedule_config) : "-"}
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
                        onClick={(e) => { e.stopPropagation(); toggleMutation.mutate({ id: job.id, enabled: !job.enabled }); }}
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
                        {isRunning ? (
                          <Badge variant="secondary" className="text-[10px] px-1.5 py-0 animate-pulse">
                            <svg className="animate-spin h-3 w-3 mr-1 inline" viewBox="0 0 24 24" fill="none">
                              <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                              <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
                            </svg>
                            実行中
                          </Badge>
                        ) : job.last_run_status ? (
                          <Badge
                            variant={job.last_run_status.startsWith("成功") || job.last_run_status === "success" ? "default" : "destructive"}
                            className="text-[10px] px-1.5 py-0"
                            title={job.last_run_status}
                          >
                            {job.last_run_status.startsWith("成功") || job.last_run_status === "success" ? "成功" : "失敗"}
                          </Badge>
                        ) : null}
                      </div>
                      {job.last_synced_at && (
                        <div className="text-[10px] text-slate-400 mt-0.5" title={`次回は ${formatDateTime(job.last_synced_at)} 以降の更新分を取得`}>
                          増分同期: {formatDateTime(job.last_synced_at)} 〜
                        </div>
                      )}
                    </TableCell>

                    {/* Actions */}
                    <TableCell>
                      <div className="flex items-center justify-end gap-1">
                        <Button
                          variant="ghost"
                          size="sm"
                          className="text-xs h-7 px-2"
                          onClick={(e) => { e.stopPropagation(); onEdit(job); }}
                        >
                          編集
                        </Button>
                        <Button
                          variant="ghost"
                          size="sm"
                          className="text-xs h-7 px-2"
                          onClick={(e) => { e.stopPropagation(); triggerMutation.mutate(job.id); }}
                          disabled={isRunning}
                        >
                          {isRunning ? "実行中..." : "今すぐ実行"}
                        </Button>
                        <Button
                          variant="ghost"
                          size="sm"
                          className="text-xs h-7 px-2 text-red-600 hover:text-red-700 hover:bg-red-50"
                          onClick={(e) => { e.stopPropagation(); setDeleteTarget(job); }}
                        >
                          削除
                        </Button>
                      </div>
                    </TableCell>
                  </TableRow>

                  {/* Expandable log panel */}
                  {isExpanded && (
                    <TableRow>
                      <TableCell colSpan={COL_COUNT + 1} className="p-0">
                        <JobLogPanel scheduleId={job.id} isRunning={isRunning} />
                      </TableCell>
                    </TableRow>
                  )}
                </Fragment>
              );
            })}
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
