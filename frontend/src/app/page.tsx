"use client";

import { useState, useCallback, useEffect, Suspense } from "react";
import { useSearchParams, useRouter } from "next/navigation";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import Image from "next/image";
import { api } from "@/lib/api";
import type { ExtractResult, FilterConfig, DestinationConfig, ScheduleSlotConfig, Platform } from "@/lib/api";
import { PlatformSelector } from "@/components/PlatformSelector";
import { EndpointPicker } from "@/components/EndpointPicker";
import { ColumnChooser } from "@/components/ColumnChooser";
import { DataPreview } from "@/components/DataPreview";
import { BigQueryDestination } from "@/components/BigQueryDestination";
import { StatusBar } from "@/components/StatusBar";
import { FilterSettings } from "@/components/FilterSettings";
import { ScheduleSettings } from "@/components/ScheduleSettings";
import { CredentialsDialog } from "@/components/CredentialsDialog";
import { JobList } from "@/components/JobList";
import type { ScheduleJob } from "@/components/JobList";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";

export default function Page() {
  return (
    <Suspense fallback={
      <div className="h-screen flex items-center justify-center bg-[#f4f6f9]">
        <p className="text-sm text-slate-500">読み込み中...</p>
      </div>
    }>
      <Dashboard />
    </Suspense>
  );
}

function Dashboard() {
  const queryClient = useQueryClient();
  const searchParams = useSearchParams();
  const router = useRouter();

  // URLパラメータから初期値を復元
  const [sidebarMode, setSidebarMode] = useState<"new" | "jobs">(
    (searchParams.get("mode") as "new" | "jobs") || "new"
  );
  const [platformId, setPlatformId] = useState<string | null>(
    searchParams.get("platform") || null
  );
  const [endpointId, setEndpointId] = useState<string | null>(
    searchParams.get("endpoint") || null
  );
  const [selectedColumns, setSelectedColumns] = useState<Set<string>>(new Set());
  const [limit, setLimit] = useState(100);
  const [data, setData] = useState<ExtractResult | null>(null);
  const [configPlatform, setConfigPlatform] = useState<Platform | null>(null);
  const [filters, setFilters] = useState<FilterConfig[]>([]);
  const [startDate, setStartDate] = useState("");
  const [endDate, setEndDate] = useState("");
  const [activeStep, setActiveStep] = useState(
    Number(searchParams.get("step")) || 1
  );
  const [editingJobId, setEditingJobId] = useState<string | null>(
    searchParams.get("edit") || null
  );
  const [showTestPreview, setShowTestPreview] = useState(false);
  const [jobName, setJobName] = useState("");

  // ナビゲーション状態をURLに同期
  useEffect(() => {
    const params = new URLSearchParams();
    if (sidebarMode !== "new") params.set("mode", sidebarMode);
    if (platformId) params.set("platform", platformId);
    if (endpointId) params.set("endpoint", endpointId);
    if (activeStep !== 1) params.set("step", String(activeStep));
    if (editingJobId) params.set("edit", editingJobId);
    const qs = params.toString();
    const newUrl = qs ? `/?${qs}` : "/";
    router.replace(newUrl, { scroll: false });
  }, [sidebarMode, platformId, endpointId, activeStep, editingJobId, router]);

  // BigQuery destination state
  const [destination, setDestination] = useState<DestinationConfig>({
    type: "bigquery",
    project_id: "",
    dataset_id: "",
    table_id: "",
    transfer_mode: "append" as const,
    key_columns: [],
    location: "US",
  });

  // Schedule state
  const [scheduleConfig, setScheduleConfig] = useState<ScheduleSlotConfig>({
    frequency: "daily",
    hour: 9,
    minute: 0,
  });
  const [scheduleEnabled, setScheduleEnabled] = useState(false);

  // Save status
  const [saveStatus, setSaveStatus] = useState<"idle" | "saving" | "success" | "error">("idle");
  const [saveError, setSaveError] = useState("");

  // Fetch platforms
  const { data: platforms = [] } = useQuery({
    queryKey: ["platforms"],
    queryFn: api.getPlatforms,
  });

  // Fetch endpoints when platform selected
  const { data: endpoints = [] } = useQuery({
    queryKey: ["endpoints", platformId],
    queryFn: () => api.getEndpoints(platformId!),
    enabled: !!platformId,
  });

  // Fetch schema when endpoint selected
  const { data: schema = [] } = useQuery({
    queryKey: ["schema", platformId, endpointId],
    queryFn: () => api.getSchema(platformId!, endpointId!),
    enabled: !!platformId && !!endpointId,
  });

  // Extract data mutation (for test fetch in step 4)
  const extractMutation = useMutation({
    mutationFn: () =>
      api.extractData({
        platform_id: platformId!,
        endpoint_id: endpointId!,
        columns:
          selectedColumns.size > 0
            ? Array.from(selectedColumns).join(",")
            : undefined,
        limit,
        filters: filters.length > 0 ? JSON.stringify(filters) : undefined,
        start_date: startDate || undefined,
        end_date: endDate || undefined,
        fetch_all: limit > 100,
      }),
    onSuccess: (result) => {
      setData(result);
      setShowTestPreview(true);
    },
  });

  const handlePlatformSelect = useCallback((id: string) => {
    setPlatformId(id);
    setEndpointId(null);
    setSelectedColumns(new Set());
    setData(null);
    setFilters([]);
    setActiveStep(1);
    setEditingJobId(null);
    setJobName("");
  }, []);

  const handleEndpointSelect = useCallback((id: string) => {
    setEndpointId(id);
    setSelectedColumns(new Set());
    setData(null);
    setFilters([]);
    setActiveStep(2);
  }, []);

  const handleColumnToggle = useCallback((key: string) => {
    setSelectedColumns((prev) => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  }, []);

  const handleSelectAll = useCallback(() => {
    setSelectedColumns(new Set(schema.map((f) => f.key)));
  }, [schema]);

  const handleDeselectAll = useCallback(() => {
    setSelectedColumns(new Set());
  }, []);

  const handleSaveSchedule = async () => {
    if (!platformId || !endpointId) return;
    setSaveStatus("saving");
    setSaveError("");
    try {
      const payload = {
        name: jobName || `${platformId}_${endpointId}_schedule`,
        platform_id: platformId,
        endpoint_id: endpointId,
        columns: selectedColumns.size > 0 ? Array.from(selectedColumns) : [],
        filters,
        limit: 1000,
        schedule_config: scheduleConfig,
        destination,
        enabled: scheduleEnabled,
      };

      let savedJobId: string | null = null;
      if (editingJobId) {
        const result = await api.updateSchedule(editingJobId, payload);
        savedJobId = result.id ?? editingJobId;
      } else {
        const result = await api.createSchedule(payload);
        savedJobId = result.id;
      }

      // スケジュール無効の場合は即時実行
      if (!scheduleEnabled && savedJobId) {
        try {
          await api.triggerSchedule(savedJobId);
        } catch {
          // 即時実行の失敗は警告のみ
          console.warn("即時実行のトリガーに失敗しました");
        }
      }

      setSaveStatus("success");
      queryClient.invalidateQueries({ queryKey: ["schedules"] });
      // 保存成功後、ジョブ一覧に自動切替
      setTimeout(() => {
        setSaveStatus("idle");
        setSidebarMode("jobs");
        setEditingJobId(null);
      }, 1500);
    } catch (e) {
      setSaveStatus("error");
      setSaveError((e as Error).message);
    }
  };

  const handleEditJob = useCallback((job: ScheduleJob) => {
    setPlatformId(job.platform_id);
    setEndpointId(job.endpoint_id);
    setSelectedColumns(new Set(job.columns ?? []));
    setFilters(job.filters ?? []);
    setLimit(job.limit);
    setDestination(job.destination);
    setScheduleConfig(job.schedule_config);
    setScheduleEnabled(job.enabled);
    setEditingJobId(job.id);
    setJobName(job.name);
    setData(null);
    setShowTestPreview(false);
    setSidebarMode("new");
    setActiveStep(1);
  }, []);

  const canFetch = !!platformId && !!endpointId;

  const selectedPlatform = platforms.find((p) => p.id === platformId);
  const selectedEndpoint = endpoints.find((e) => e.id === endpointId);

  const availableColumns = selectedColumns.size > 0
    ? Array.from(selectedColumns)
    : schema.map((f) => f.key);

  const platformLogos: Record<string, string> = {
    shopify: "/shopify_logo.webp",
    amazon: "/Amazon_logo.png",
    rakuten: "/rakuten_logo.png",
    yahoo: "/yahoo_logo.png",
  };

  const steps = [
    { num: 1, label: "接続先選択" },
    { num: 2, label: "カラム・フィルター設定" },
    { num: 3, label: "転送先設定" },
    { num: 4, label: "スケジュール・保存" },
  ];

  return (
    <div className="h-screen bg-[#f4f6f9] flex overflow-hidden">
      {/* Light Sidebar */}
      <aside className="w-64 sidebar-light flex flex-col shrink-0 h-screen">
        {/* Logo */}
        <div className="px-4 py-3 border-b border-slate-200 flex items-center justify-center">
          <Image
            src="/AnyX.png"
            alt="AnyX Data Hub"
            width={120}
            height={36}
            className="object-contain"
            priority
          />
        </div>

        {/* Mode Tabs */}
        <div className="flex border-b border-slate-200">
          <button
            onClick={() => setSidebarMode("new")}
            className={`flex-1 py-2.5 text-xs font-medium text-center transition-colors ${
              sidebarMode === "new"
                ? "text-[#4f63d2] border-b-2 border-[#4f63d2] bg-blue-50/50"
                : "text-slate-500 hover:text-slate-700 hover:bg-slate-50"
            }`}
          >
            新規設定
          </button>
          <button
            onClick={() => setSidebarMode("jobs")}
            className={`flex-1 py-2.5 text-xs font-medium text-center transition-colors ${
              sidebarMode === "jobs"
                ? "text-[#4f63d2] border-b-2 border-[#4f63d2] bg-blue-50/50"
                : "text-slate-500 hover:text-slate-700 hover:bg-slate-50"
            }`}
          >
            ジョブ一覧
          </button>
        </div>

        {/* Mode: new - Platform list */}
        {sidebarMode === "new" && (
          <>
            <div className="px-4 pt-5 pb-2">
              <p className="text-[10px] uppercase tracking-wider text-slate-400 font-semibold">
                プラットフォーム
              </p>
            </div>
            <div className="flex-1 px-2 overflow-y-auto">
              <PlatformSelector
                platforms={platforms}
                selected={platformId}
                onSelect={handlePlatformSelect}
                onConfigure={setConfigPlatform}
              />
            </div>
            <StatusBar platforms={platforms} />
          </>
        )}

        {/* Mode: jobs - sidebar info */}
        {sidebarMode === "jobs" && (
          <div className="p-4 text-sm text-slate-500">
            登録済みジョブの管理
          </div>
        )}
      </aside>

      {/* Main Content */}
      <div className="flex-1 flex flex-col h-screen overflow-hidden">
        {/* Top Bar - Fixed */}
        <header className="bg-white border-b border-slate-200 px-6 py-3 flex items-center justify-between shrink-0 z-50">
          {sidebarMode === "new" ? (
            <>
              <div className="flex items-center gap-3">
                <h2 className="text-sm font-semibold text-slate-800">
                  {editingJobId ? "ジョブ編集" : "データ抽出設定"}
                </h2>
                {selectedPlatform && (
                  <Badge variant="outline" className="text-xs font-normal">
                    {selectedPlatform.name}
                    {selectedEndpoint && ` / ${selectedEndpoint.name}`}
                  </Badge>
                )}
                {editingJobId && (
                  <Button
                    variant="outline"
                    size="sm"
                    className="text-xs ml-2"
                    onClick={() => {
                      setEditingJobId(null);
                      setJobName("");
                      setSidebarMode("jobs");
                    }}
                  >
                    キャンセル
                  </Button>
                )}
              </div>

              {/* Step Indicator - hidden in edit mode */}
              {!editingJobId && (
                <div className="step-indicator">
                  {steps.map((step, i) => (
                    <div key={step.num} className="flex items-center">
                      <button
                        onClick={() => {
                          if (step.num === 1) setActiveStep(1);
                          else if (step.num === 2 && endpointId) setActiveStep(2);
                          else if (step.num === 3 && endpointId) setActiveStep(3);
                          else if (step.num === 4 && endpointId) setActiveStep(4);
                        }}
                        className="flex items-center gap-1.5 cursor-pointer"
                        title={step.label}
                      >
                        <span
                          className={`step-dot ${
                            activeStep === step.num
                              ? "active"
                              : activeStep > step.num
                              ? "completed"
                              : "inactive"
                          }`}
                        >
                          {activeStep > step.num ? (
                            <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="3">
                              <path d="M20 6L9 17l-5-5" />
                            </svg>
                          ) : (
                            step.num
                          )}
                        </span>
                        <span
                          className={`text-xs font-medium hidden sm:inline ${
                            activeStep === step.num
                              ? "text-[#4f63d2]"
                              : activeStep > step.num
                              ? "text-green-600"
                              : "text-slate-400"
                          }`}
                        >
                          {step.label}
                        </span>
                      </button>
                      {i < steps.length - 1 && (
                        <span
                          className={`step-line mx-2 ${
                            activeStep > step.num ? "completed" : ""
                          }`}
                        />
                      )}
                    </div>
                  ))}
                </div>
              )}
            </>
          ) : (
            <div className="flex items-center gap-3">
              <h2 className="text-sm font-semibold text-slate-800">
                ジョブ管理
              </h2>
            </div>
          )}
        </header>

        {/* Main Scrollable Area */}
        <main className="flex-1 overflow-y-auto p-6">
          {sidebarMode === "jobs" ? (
            <div className="max-w-6xl mx-auto">
              <JobList onEdit={handleEditJob} />
            </div>
          ) : editingJobId ? (
            /* ===== Edit Mode: All sections on one scrollable page ===== */
            <div className="max-w-5xl mx-auto space-y-6">

              {/* Section 1: Platform & Endpoint */}
              <Card className="shadow-sm border-slate-200">
                <CardContent className="p-6 space-y-6">
                  <div className="section-header">
                    <span className="section-number">1</span>
                    <h3>接続先選択</h3>
                  </div>
                  <div className="space-y-4">
                    <div className="flex items-center gap-3">
                      {platformId && platformLogos[platformId] && (
                        <Image
                          src={platformLogos[platformId]}
                          alt={selectedPlatform?.name || ""}
                          width={32}
                          height={32}
                          className="rounded object-contain"
                        />
                      )}
                      <span className="text-sm font-medium text-slate-700">
                        {selectedPlatform?.name}
                      </span>
                      <span className={`status-dot ${selectedPlatform?.configured ? "connected" : "disconnected"}`} />
                    </div>
                    <EndpointPicker
                      endpoints={endpoints}
                      selected={endpointId}
                      onSelect={(id) => {
                        setEndpointId(id);
                        setSelectedColumns(new Set());
                        setFilters([]);
                      }}
                      disabled={!platformId}
                    />
                  </div>
                </CardContent>
              </Card>

              {/* Section 2: Column & Filter */}
              <Card className="shadow-sm border-slate-200">
                <CardContent className="p-6 space-y-4">
                  <div className="section-header">
                    <span className="section-number">2</span>
                    <h3>カラム選択</h3>
                  </div>
                  {schema.length > 0 ? (
                    <ColumnChooser
                      fields={schema}
                      selected={selectedColumns}
                      onToggle={handleColumnToggle}
                      onSelectAll={handleSelectAll}
                      onDeselectAll={handleDeselectAll}
                    />
                  ) : (
                    <div className="text-sm text-muted-foreground py-4 text-center">
                      スキーマを読み込み中...
                    </div>
                  )}
                </CardContent>
              </Card>

              <Card className="shadow-sm border-slate-200">
                <CardContent className="p-6">
                  <div className="section-header">
                    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#4f63d2" strokeWidth="2" className="shrink-0">
                      <path d="M22 3H2l8 9.46V19l4 2v-8.54L22 3z" />
                    </svg>
                    <h3>フィルター設定</h3>
                  </div>
                  <FilterSettings
                    fields={schema}
                    filters={filters}
                    onChange={setFilters}
                  />
                </CardContent>
              </Card>

              {/* Section 3: BigQuery Destination */}
              <Card className="shadow-sm border-slate-200">
                <CardContent className="p-6">
                  <div className="section-header">
                    <span className="section-number">3</span>
                    <h3>転送先設定 (BigQuery)</h3>
                  </div>
                  <BigQueryDestination
                    config={destination}
                    onChange={setDestination}
                    availableColumns={availableColumns}
                    isEditing
                  />
                </CardContent>
              </Card>

              {/* Section 4: Schedule */}
              <Card className="shadow-sm border-slate-200">
                <CardContent className="p-6">
                  <div className="section-header">
                    <span className="section-number">4</span>
                    <h3>スケジュール設定</h3>
                  </div>
                  <ScheduleSettings
                    config={scheduleConfig}
                    enabled={scheduleEnabled}
                    onConfigChange={setScheduleConfig}
                    onEnabledChange={setScheduleEnabled}
                  />
                </CardContent>
              </Card>

              {/* Section 5: Job Name */}
              <Card className="shadow-sm border-slate-200">
                <CardContent className="p-6">
                  <div className="section-header">
                    <span className="section-number">5</span>
                    <h3>ジョブ名</h3>
                  </div>
                  <div className="mt-3">
                    <Label htmlFor="edit-job-name" className="text-sm text-slate-700">ジョブ名</Label>
                    <Input
                      id="edit-job-name"
                      value={jobName}
                      onChange={(e) => setJobName(e.target.value)}
                      placeholder="ジョブ名を入力"
                      className="mt-1 max-w-md"
                    />
                  </div>
                </CardContent>
              </Card>

              {/* Save / Update Button */}
              <Card className="shadow-sm border-slate-200">
                <CardContent className="p-6">
                  <div className="flex items-center justify-between">
                    <div>
                      <h3 className="text-sm font-semibold text-slate-800 mb-1">
                        {scheduleEnabled ? "設定を更新" : "更新して即時実行"}
                      </h3>
                      <p className="text-xs text-muted-foreground">
                        {scheduleEnabled
                          ? "転送先・スケジュール設定を更新します"
                          : "設定を更新し、データ転送を即座に実行します"}
                      </p>
                    </div>
                    <Button
                      onClick={handleSaveSchedule}
                      disabled={saveStatus === "saving" || !destination.project_id || !destination.dataset_id || !destination.table_id}
                      className="bg-[#4f63d2] hover:bg-[#3d4fb8]"
                    >
                      {saveStatus === "saving" ? (
                        <>
                          <svg className="animate-spin h-4 w-4 mr-2" viewBox="0 0 24 24" fill="none">
                            <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                            <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
                          </svg>
                          更新中...
                        </>
                      ) : (
                        scheduleEnabled ? "更新してスケジュール登録" : "更新して今すぐ実行"
                      )}
                    </Button>
                  </div>

                  {saveStatus === "success" && (
                    <div className="mt-3 text-sm text-green-600 bg-green-50 p-3 rounded-md flex items-center gap-2">
                      <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                        <path d="M20 6L9 17l-5-5" />
                      </svg>
                      {scheduleEnabled ? "スケジュールを更新しました" : "更新して実行を開始しました"}
                    </div>
                  )}

                  {saveStatus === "error" && (
                    <div className="mt-3 text-sm text-red-600 bg-red-50 p-3 rounded-md">
                      更新に失敗しました: {saveError}
                    </div>
                  )}
                </CardContent>
              </Card>
            </div>
          ) : (
            /* ===== New Job: Step-by-step wizard ===== */
            <div className="max-w-5xl mx-auto space-y-6">

              {/* Step 1: Platform & Endpoint Selection */}
              {activeStep === 1 && (
                <Card className="shadow-sm border-slate-200">
                  <CardContent className="p-6 space-y-6">
                    <div className="section-header">
                      <span className="section-number">1</span>
                      <h3>接続先とエンドポイントを選択</h3>
                    </div>

                    {!platformId ? (
                      <div className="text-center py-12">
                        <Image
                          src="/select_platform.svg"
                          alt="接続先とエンドポイントを選択"
                          width={120}
                          height={120}
                          className="mx-auto mb-3 object-contain"
                        />
                        <p className="text-sm text-slate-500">
                          左のサイドバーからプラットフォームを選択してください
                        </p>
                      </div>
                    ) : (
                      <div className="space-y-4">
                        <div className="flex items-center gap-3 mb-4">
                          {platformId && platformLogos[platformId] && (
                            <Image
                              src={platformLogos[platformId]}
                              alt={selectedPlatform?.name || ""}
                              width={32}
                              height={32}
                              className="rounded object-contain"
                            />
                          )}
                          <span className="text-sm font-medium text-slate-700">
                            {selectedPlatform?.name} を選択中
                          </span>
                          <span className={`status-dot ${selectedPlatform?.configured ? "connected" : "disconnected"}`} />
                          {!selectedPlatform?.configured && (
                            <Button
                              variant="outline"
                              size="sm"
                              className="text-xs ml-2"
                              onClick={() => selectedPlatform && setConfigPlatform(selectedPlatform)}
                            >
                              API設定を行う
                            </Button>
                          )}
                        </div>

                        <EndpointPicker
                          endpoints={endpoints}
                          selected={endpointId}
                          onSelect={handleEndpointSelect}
                          disabled={!platformId}
                        />

                        {endpointId && (
                          <div className="flex justify-end pt-4">
                            <Button
                              onClick={() => setActiveStep(2)}
                              className="bg-[#4f63d2] hover:bg-[#3d4fb8]"
                            >
                              次へ: カラム設定
                              <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" className="ml-1">
                                <path d="M5 12h14M12 5l7 7-7 7" />
                              </svg>
                            </Button>
                          </div>
                        )}
                      </div>
                    )}
                  </CardContent>
                </Card>
              )}

              {/* Step 2: Column & Filter Configuration */}
              {activeStep === 2 && (
                <div className="space-y-6">
                  {/* Column Chooser */}
                  <Card className="shadow-sm border-slate-200">
                    <CardContent className="p-6 space-y-4">
                      <div className="section-header">
                        <span className="section-number">2</span>
                        <h3>カラム選択</h3>
                      </div>

                      {schema.length > 0 ? (
                        <ColumnChooser
                          fields={schema}
                          selected={selectedColumns}
                          onToggle={handleColumnToggle}
                          onSelectAll={handleSelectAll}
                          onDeselectAll={handleDeselectAll}
                        />
                      ) : (
                        <div className="text-sm text-muted-foreground py-4 text-center">
                          スキーマを読み込み中...
                        </div>
                      )}
                    </CardContent>
                  </Card>

                  {/* Filter Settings */}
                  <Card className="shadow-sm border-slate-200">
                    <CardContent className="p-6">
                      <div className="section-header">
                        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#4f63d2" strokeWidth="2" className="shrink-0">
                          <path d="M22 3H2l8 9.46V19l4 2v-8.54L22 3z" />
                        </svg>
                        <h3>フィルター設定</h3>
                      </div>
                      <FilterSettings
                        fields={schema}
                        filters={filters}
                        onChange={setFilters}
                      />
                    </CardContent>
                  </Card>

                  {/* Date Range Filter */}
                  <Card className="shadow-sm border-slate-200">
                    <CardContent className="p-6">
                      <div className="section-header">
                        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#4f63d2" strokeWidth="2" className="shrink-0">
                          <rect x="3" y="4" width="18" height="18" rx="2" ry="2" />
                          <line x1="16" y1="2" x2="16" y2="6" />
                          <line x1="8" y1="2" x2="8" y2="6" />
                          <line x1="3" y1="10" x2="21" y2="10" />
                        </svg>
                        <h3>日付範囲（API レベル）</h3>
                      </div>
                      <p className="text-xs text-muted-foreground mt-1 mb-3">
                        プラットフォームAPIに直接渡す日付範囲です。空欄の場合はデフォルト期間が使用されます。
                      </p>
                      <div className="flex items-center gap-3">
                        <div>
                          <Label className="text-xs text-slate-600">開始日</Label>
                          <Input
                            type="date"
                            value={startDate}
                            onChange={(e) => setStartDate(e.target.value)}
                            className="text-xs h-8 w-40"
                          />
                        </div>
                        <span className="text-xs text-muted-foreground mt-4">〜</span>
                        <div>
                          <Label className="text-xs text-slate-600">終了日</Label>
                          <Input
                            type="date"
                            value={endDate}
                            onChange={(e) => setEndDate(e.target.value)}
                            className="text-xs h-8 w-40"
                          />
                        </div>
                        {(startDate || endDate) && (
                          <Button
                            variant="ghost"
                            size="sm"
                            className="text-xs mt-4 text-slate-400 hover:text-red-500"
                            onClick={() => { setStartDate(""); setEndDate(""); }}
                          >
                            クリア
                          </Button>
                        )}
                      </div>
                    </CardContent>
                  </Card>

                  {/* Navigation */}
                  <div className="flex justify-between">
                    <Button
                      variant="outline"
                      onClick={() => setActiveStep(1)}
                    >
                      <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" className="mr-1">
                        <path d="M19 12H5M12 19l-7-7 7-7" />
                      </svg>
                      戻る: 接続先選択
                    </Button>
                    <Button
                      onClick={() => setActiveStep(3)}
                      className="bg-[#4f63d2] hover:bg-[#3d4fb8]"
                    >
                      次へ: 転送先設定
                      <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" className="ml-1">
                        <path d="M5 12h14M12 5l7 7-7 7" />
                      </svg>
                    </Button>
                  </div>
                </div>
              )}

              {/* Step 3: BigQuery Destination */}
              {activeStep === 3 && (
                <div className="space-y-6">
                  <Card className="shadow-sm border-slate-200">
                    <CardContent className="p-6">
                      <div className="section-header">
                        <span className="section-number">3</span>
                        <h3>転送先設定 (BigQuery)</h3>
                      </div>
                      <BigQueryDestination
                        config={destination}
                        onChange={setDestination}
                        availableColumns={availableColumns}
                      />
                    </CardContent>
                  </Card>

                  {/* Navigation */}
                  <div className="flex justify-between">
                    <Button
                      variant="outline"
                      onClick={() => setActiveStep(2)}
                    >
                      <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" className="mr-1">
                        <path d="M19 12H5M12 19l-7-7 7-7" />
                      </svg>
                      戻る: カラム・フィルター設定
                    </Button>
                    <Button
                      onClick={() => setActiveStep(4)}
                      className="bg-[#4f63d2] hover:bg-[#3d4fb8]"
                    >
                      次へ: スケジュール・保存
                      <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" className="ml-1">
                        <path d="M5 12h14M12 5l7 7-7 7" />
                      </svg>
                    </Button>
                  </div>
                </div>
              )}

              {/* Step 4: Schedule, Job Name & Save */}
              {activeStep === 4 && (
                <div className="space-y-6">
                  {/* Schedule Settings */}
                  <Card className="shadow-sm border-slate-200">
                    <CardContent className="p-6">
                      <div className="section-header">
                        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#4f63d2" strokeWidth="2" className="shrink-0">
                          <circle cx="12" cy="12" r="10" />
                          <path d="M12 6v6l4 2" />
                        </svg>
                        <h3>スケジュール設定</h3>
                      </div>
                      <ScheduleSettings
                        config={scheduleConfig}
                        enabled={scheduleEnabled}
                        onConfigChange={setScheduleConfig}
                        onEnabledChange={setScheduleEnabled}
                      />
                    </CardContent>
                  </Card>

                  {/* Job Name */}
                  <Card className="shadow-sm border-slate-200">
                    <CardContent className="p-6">
                      <div className="section-header">
                        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#4f63d2" strokeWidth="2" className="shrink-0">
                          <path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7" />
                          <path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z" />
                        </svg>
                        <h3>ジョブ名</h3>
                      </div>
                      <div className="mt-3">
                        <Label htmlFor="new-job-name" className="text-sm text-slate-700">ジョブ名</Label>
                        <Input
                          id="new-job-name"
                          value={jobName || (platformId && endpointId ? `${platformId}_${endpointId}_schedule` : "")}
                          onChange={(e) => setJobName(e.target.value)}
                          placeholder="ジョブ名を入力"
                          className="mt-1 max-w-md"
                        />
                        <p className="text-xs text-muted-foreground mt-1">
                          空欄の場合は自動生成されます
                        </p>
                      </div>
                    </CardContent>
                  </Card>

                  {/* Optional: Test Data Fetch & Preview */}
                  <Card className="shadow-sm border-slate-200">
                    <CardContent className="p-6">
                      <div className="flex items-center justify-between mb-4">
                        <div className="section-header mb-0 pb-0 border-b-0">
                          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#4f63d2" strokeWidth="2" className="shrink-0">
                            <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4M7 10l5 5 5-5M12 15V3" />
                          </svg>
                          <h3>テストデータ取得（任意）</h3>
                        </div>
                        <Button
                          onClick={() => extractMutation.mutate()}
                          disabled={!canFetch || extractMutation.isPending}
                          variant="outline"
                          size="sm"
                        >
                          {extractMutation.isPending ? (
                            <>
                              <svg className="animate-spin h-4 w-4 mr-1.5" viewBox="0 0 24 24" fill="none">
                                <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                                <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
                              </svg>
                              取得中...
                            </>
                          ) : (
                            "テストデータ取得"
                          )}
                        </Button>
                      </div>

                      {extractMutation.isError && (
                        <div className="text-sm text-red-600 bg-red-50 p-3 rounded-md mb-4">
                          エラー: {(extractMutation.error as Error).message}
                        </div>
                      )}

                      {showTestPreview && data && (
                        <div className="space-y-2">
                          <div className="flex items-center gap-2 mb-2">
                            <Badge variant="secondary" className="text-xs">
                              {data.total !== null ? `全 ${data.total} 件中 ` : ""}
                              {data.items.length} 件表示
                            </Badge>
                          </div>
                          <DataPreview
                            columns={data.columns}
                            items={data.items}
                            total={data.total}
                          />
                        </div>
                      )}

                      {!showTestPreview && !extractMutation.isPending && (
                        <p className="text-xs text-muted-foreground">
                          保存前にデータの取得テストを行えます。テストは任意です。
                        </p>
                      )}
                    </CardContent>
                  </Card>

                  {/* Save Button & Status */}
                  <Card className="shadow-sm border-slate-200">
                    <CardContent className="p-6">
                      <div className="flex items-center justify-between">
                        <div>
                          <h3 className="text-sm font-semibold text-slate-800 mb-1">
                            {scheduleEnabled ? "設定を保存" : "保存して即時実行"}
                          </h3>
                          <p className="text-xs text-muted-foreground">
                            {scheduleEnabled
                              ? "転送先・スケジュール設定を保存してジョブを登録します"
                              : "転送先設定を保存し、データ転送を即座に実行します"}
                          </p>
                        </div>
                        <Button
                          onClick={handleSaveSchedule}
                          disabled={saveStatus === "saving" || !destination.project_id || !destination.dataset_id || !destination.table_id}
                          className="bg-[#4f63d2] hover:bg-[#3d4fb8]"
                        >
                          {saveStatus === "saving" ? (
                            <>
                              <svg className="animate-spin h-4 w-4 mr-2" viewBox="0 0 24 24" fill="none">
                                <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                                <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
                              </svg>
                              保存中...
                            </>
                          ) : (
                            scheduleEnabled ? "保存してスケジュール登録" : "保存して今すぐ実行"
                          )}
                        </Button>
                      </div>

                      {saveStatus === "success" && (
                        <div className="mt-3 text-sm text-green-600 bg-green-50 p-3 rounded-md flex items-center gap-2">
                          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                            <path d="M20 6L9 17l-5-5" />
                          </svg>
                          {scheduleEnabled ? "スケジュールを保存しました" : "保存して実行を開始しました"}
                        </div>
                      )}

                      {saveStatus === "error" && (
                        <div className="mt-3 text-sm text-red-600 bg-red-50 p-3 rounded-md">
                          保存に失敗しました: {saveError}
                        </div>
                      )}
                    </CardContent>
                  </Card>

                  {/* Navigation */}
                  <div className="flex justify-between">
                    <Button
                      variant="outline"
                      onClick={() => setActiveStep(3)}
                    >
                      <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" className="mr-1">
                        <path d="M19 12H5M12 19l-7-7 7-7" />
                      </svg>
                      戻る: 転送先設定
                    </Button>
                  </div>
                </div>
              )}
            </div>
          )}
        </main>
      </div>

      {/* Credentials Dialog */}
      <CredentialsDialog
        platformId={configPlatform?.id ?? null}
        platformName={configPlatform?.name ?? ""}
        open={!!configPlatform}
        onOpenChange={(open) => {
          if (!open) setConfigPlatform(null);
        }}
      />
    </div>
  );
}
