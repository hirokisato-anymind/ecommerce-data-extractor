"use client";

import { useState, useEffect, useCallback } from "react";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { api } from "@/lib/api";
import type { DestinationConfig } from "@/lib/api";

interface BigQueryDestinationProps {
  config: DestinationConfig;
  onChange: (config: DestinationConfig) => void;
  availableColumns: string[];
  isEditing?: boolean;
}

const BIGQUERY_LOCATIONS = [
  { value: "US", label: "US (マルチリージョン)" },
  { value: "EU", label: "EU (マルチリージョン)" },
  { value: "asia-northeast1", label: "asia-northeast1 (東京)" },
  { value: "asia-northeast2", label: "asia-northeast2 (大阪)" },
  { value: "asia-northeast3", label: "asia-northeast3 (ソウル)" },
  { value: "asia-east1", label: "asia-east1 (台湾)" },
  { value: "asia-east2", label: "asia-east2 (香港)" },
  { value: "asia-southeast1", label: "asia-southeast1 (シンガポール)" },
  { value: "us-central1", label: "us-central1 (アイオワ)" },
  { value: "us-east1", label: "us-east1 (サウスカロライナ)" },
  { value: "europe-west1", label: "europe-west1 (ベルギー)" },
  { value: "europe-west2", label: "europe-west2 (ロンドン)" },
] as const;

const TRANSFER_MODES = [
  { value: "append" as const, label: "APPEND", desc: "既存データに追加（重複チェックあり）" },
  { value: "append_direct" as const, label: "APPEND DIRECT", desc: "既存データに直接追加（高速・重複チェックなし）" },
  { value: "replace" as const, label: "REPLACE", desc: "テーブルを全件洗い替え" },
  { value: "delete_in_advance" as const, label: "DELETE IN ADVANCE", desc: "指定キーで既存データを削除後に挿入" },
  { value: "upsert" as const, label: "UPSERT", desc: "指定キーで既存データを更新、なければ挿入" },
] as const;

export function BigQueryDestination({ config, onChange, availableColumns, isEditing = false }: BigQueryDestinationProps) {
  const [testStatus, setTestStatus] = useState<"idle" | "testing" | "success" | "error">("idle");
  const [testError, setTestError] = useState("");
  const [authStatus, setAuthStatus] = useState<{ authenticated: boolean; email?: string } | null>(null);
  const [authLoading, setAuthLoading] = useState(false);
  const [authError, setAuthError] = useState("");
  const [authFallbackUrl, setAuthFallbackUrl] = useState("");

  // OAuth config state
  const [oauthConfigured, setOauthConfigured] = useState<boolean | null>(null);
  const [showOAuthSetup, setShowOAuthSetup] = useState(false);
  const [oauthClientId, setOauthClientId] = useState("");
  const [oauthClientSecret, setOauthClientSecret] = useState("");
  const [oauthSaveStatus, setOauthSaveStatus] = useState<"idle" | "saving" | "success" | "error">("idle");
  const [oauthSaveError, setOauthSaveError] = useState("");

  const [customLocation, setCustomLocation] = useState(false);

  // Track "new" input mode for dataset/table
  const [newDataset, setNewDataset] = useState(false);
  const [newTable, setNewTable] = useState(false);

  // Suggestions for project/dataset/table
  const [projects, setProjects] = useState<{ project_id: string; name: string }[]>([]);
  const [datasets, setDatasets] = useState<string[]>([]);
  const [tables, setTables] = useState<string[]>([]);
  const [loadingProjects, setLoadingProjects] = useState(false);
  const [loadingDatasets, setLoadingDatasets] = useState(false);
  const [loadingTables, setLoadingTables] = useState(false);

  // Fetch projects when authenticated
  useEffect(() => {
    if (!authStatus?.authenticated) return;
    setLoadingProjects(true);
    api.listBigQueryProjects().then((r) => setProjects(r.projects)).catch(() => {}).finally(() => setLoadingProjects(false));
  }, [authStatus?.authenticated]);

  // Fetch datasets when project changes
  useEffect(() => {
    if (!config.project_id || !authStatus?.authenticated) { setDatasets([]); return; }
    setLoadingDatasets(true);
    api.listBigQueryDatasets(config.project_id).then((r) => setDatasets(r.datasets)).catch(() => {}).finally(() => setLoadingDatasets(false));
  }, [config.project_id, authStatus?.authenticated]);

  // Fetch tables when dataset changes
  useEffect(() => {
    if (!config.project_id || !config.dataset_id || !authStatus?.authenticated) { setTables([]); return; }
    setLoadingTables(true);
    api.listBigQueryTablesSimple(config.project_id, config.dataset_id).then((r) => setTables(r.tables)).catch(() => {}).finally(() => setLoadingTables(false));
  }, [config.project_id, config.dataset_id, authStatus?.authenticated]);

  const needsKeyColumns = config.transfer_mode === "append" || config.transfer_mode === "upsert" || config.transfer_mode === "delete_in_advance";

  // Check if current location is a preset or custom
  const isPresetLocation = BIGQUERY_LOCATIONS.some((l) => l.value === config.location);

  // Check OAuth config status
  const checkOAuthConfig = useCallback(async () => {
    try {
      const status = await api.getBigQueryOAuthConfigStatus();
      setOauthConfigured(status.configured);
      if (!status.configured) {
        setShowOAuthSetup(true);
      }
    } catch {
      setOauthConfigured(false);
      setShowOAuthSetup(true);
    }
  }, []);

  // Check Google auth status
  const checkAuthStatus = useCallback(async () => {
    try {
      const status = await api.getBigQueryAuthStatus();
      setAuthStatus(status);
    } catch {
      setAuthStatus({ authenticated: false });
    }
  }, []);

  useEffect(() => {
    checkOAuthConfig();
    checkAuthStatus();
  }, [checkOAuthConfig, checkAuthStatus]);

  // Listen for OAuth callback completion
  useEffect(() => {
    const handleMessage = (event: MessageEvent) => {
      if (event.data?.type === "bigquery-auth-success") {
        checkAuthStatus();
      }
    };
    window.addEventListener("message", handleMessage);
    return () => window.removeEventListener("message", handleMessage);
  }, [checkAuthStatus]);

  const handleSaveOAuthConfig = async () => {
    if (!oauthClientId || !oauthClientSecret) return;
    setOauthSaveStatus("saving");
    setOauthSaveError("");
    try {
      await api.saveBigQueryOAuthConfig({
        client_id: oauthClientId,
        client_secret: oauthClientSecret,
      });
      setOauthSaveStatus("success");
      setOauthConfigured(true);
      setShowOAuthSetup(false);
      setTimeout(() => setOauthSaveStatus("idle"), 2000);
    } catch (e) {
      setOauthSaveStatus("error");
      setOauthSaveError((e as Error).message);
    }
  };

  const handleGoogleAuth = async () => {
    setAuthLoading(true);
    setAuthError("");
    setAuthFallbackUrl("");
    try {
      const { authorize_url } = await api.getBigQueryAuthUrl();
      const popup = window.open(authorize_url, "google-auth", "width=500,height=600,popup=yes");
      if (!popup || popup.closed) {
        setAuthFallbackUrl(authorize_url);
        setAuthLoading(false);
        return;
      }
      const interval = setInterval(() => {
        if (popup.closed) {
          clearInterval(interval);
          setAuthLoading(false);
          checkAuthStatus();
        }
      }, 500);
    } catch (e) {
      setAuthLoading(false);
      const msg = (e as Error).message;
      if (msg.includes("500")) {
        setAuthError("OAuth設定が必要です。下のOAuth設定フォームからClient IDとSecretを設定してください。");
        setShowOAuthSetup(true);
      } else {
        setAuthError(`認証URLの取得に失敗: ${msg}`);
      }
    }
  };

  const handleTestConnection = async () => {
    if (!config.project_id || !config.dataset_id) return;
    setTestStatus("testing");
    setTestError("");
    try {
      const result = await api.testBigQueryConnection({
        project_id: config.project_id,
        dataset_id: config.dataset_id,
      });
      setTestStatus(result.ok ? "success" : "error");
      if (!result.ok) setTestError(result.error || "接続に失敗しました");
    } catch (e) {
      setTestStatus("error");
      setTestError((e as Error).message);
    }
  };

  const toggleKeyColumn = (col: string) => {
    const current = config.key_columns;
    const next = current.includes(col)
      ? current.filter((c) => c !== col)
      : [...current, col];
    onChange({ ...config, key_columns: next });
  };

  return (
    <div className="space-y-5">
      {/* Step A: OAuth Config (if not configured) */}
      {oauthConfigured === false || showOAuthSetup ? (
        <div className="rounded-lg border border-amber-200 bg-amber-50 p-4 space-y-3">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-2">
              <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="#d97706" strokeWidth="2" className="shrink-0">
                <path d="M10.29 3.86L1.82 18a2 2 0 001.71 3h16.94a2 2 0 001.71-3L13.71 3.86a2 2 0 00-3.42 0z" />
                <line x1="12" y1="9" x2="12" y2="13" />
                <line x1="12" y1="17" x2="12.01" y2="17" />
              </svg>
              <span className="text-sm font-medium text-amber-800">
                {oauthConfigured ? "Google OAuth設定" : "Google OAuth設定が必要です"}
              </span>
            </div>
            {oauthConfigured && (
              <Button variant="ghost" size="sm" className="text-xs" onClick={() => setShowOAuthSetup(false)}>
                閉じる
              </Button>
            )}
          </div>

          <div className="text-xs text-amber-700 space-y-1">
            <p>GCPコンソールでOAuthクライアントIDを作成し、以下に入力してください:</p>
            <ol className="list-decimal ml-4 space-y-0.5">
              <li><a href="https://console.cloud.google.com/apis/credentials" target="_blank" rel="noopener noreferrer" className="text-[#4f63d2] underline">GCPコンソール &gt; APIとサービス &gt; 認証情報</a> を開く</li>
              <li>「認証情報を作成」→「OAuthクライアントID」を選択</li>
              <li>アプリケーションの種類: 「ウェブアプリケーション」</li>
              <li>承認済みリダイレクトURI に <code className="bg-amber-100 px-1 rounded">http://localhost:8000/api/bigquery/callback</code> を追加</li>
              <li>作成後、クライアントIDとシークレットをコピー</li>
            </ol>
          </div>

          <div className="space-y-2">
            <div>
              <Label className="text-xs text-amber-800">Client ID</Label>
              <Input
                value={oauthClientId}
                onChange={(e) => setOauthClientId(e.target.value)}
                placeholder="xxxx.apps.googleusercontent.com"
                className="text-sm h-8 bg-white"
              />
            </div>
            <div>
              <Label className="text-xs text-amber-800">Client Secret</Label>
              <Input
                type="password"
                value={oauthClientSecret}
                onChange={(e) => setOauthClientSecret(e.target.value)}
                placeholder="GOCSPX-xxxx"
                className="text-sm h-8 bg-white"
              />
            </div>
            <div className="flex items-center gap-2">
              <Button
                size="sm"
                onClick={handleSaveOAuthConfig}
                disabled={!oauthClientId || !oauthClientSecret || oauthSaveStatus === "saving"}
                className="bg-amber-600 hover:bg-amber-700 text-white text-xs"
              >
                {oauthSaveStatus === "saving" ? "保存中..." : "OAuth設定を保存"}
              </Button>
              {oauthSaveStatus === "success" && (
                <span className="text-xs text-green-600">保存しました</span>
              )}
              {oauthSaveStatus === "error" && (
                <span className="text-xs text-red-600">{oauthSaveError}</span>
              )}
            </div>
          </div>
        </div>
      ) : null}

      {/* Step B: Google Account Authentication */}
      <div className="space-y-2">
        <div className="flex items-center justify-between">
          <Label className="text-sm font-medium text-slate-700">Google認証</Label>
          {oauthConfigured && (
            <button
              type="button"
              onClick={() => setShowOAuthSetup(!showOAuthSetup)}
              className="text-[10px] text-slate-400 hover:text-slate-600"
            >
              OAuth設定を変更
            </button>
          )}
        </div>
        <div className="flex items-center gap-3">
          {authStatus?.authenticated ? (
            <div className="flex items-center gap-2">
              <span className="flex items-center gap-1.5 text-sm text-green-600">
                <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                  <path d="M20 6L9 17l-5-5" />
                </svg>
                認証済み
              </span>
              {authStatus.email && (
                <Badge variant="secondary" className="text-xs font-normal">
                  {authStatus.email}
                </Badge>
              )}
              <Button variant="outline" size="sm" onClick={handleGoogleAuth} className="text-xs ml-2">
                再認証
              </Button>
            </div>
          ) : (
            <Button
              onClick={handleGoogleAuth}
              disabled={authLoading || !oauthConfigured}
              variant="outline"
              className="text-sm"
            >
              {authLoading ? (
                <>
                  <svg className="animate-spin h-4 w-4 mr-1.5" viewBox="0 0 24 24" fill="none">
                    <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                    <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
                  </svg>
                  認証中...
                </>
              ) : (
                <>
                  <svg width="16" height="16" viewBox="0 0 24 24" className="mr-1.5">
                    <path fill="#4285F4" d="M22.56 12.25c0-.78-.07-1.53-.2-2.25H12v4.26h5.92a5.06 5.06 0 0 1-2.2 3.32v2.77h3.57c2.08-1.92 3.28-4.74 3.28-8.1z"/>
                    <path fill="#34A853" d="M12 23c2.97 0 5.46-.98 7.28-2.66l-3.57-2.77c-.98.66-2.23 1.06-3.71 1.06-2.86 0-5.29-1.93-6.16-4.53H2.18v2.84C3.99 20.53 7.7 23 12 23z"/>
                    <path fill="#FBBC05" d="M5.84 14.09c-.22-.66-.35-1.36-.35-2.09s.13-1.43.35-2.09V7.07H2.18C1.43 8.55 1 10.22 1 12s.43 3.45 1.18 4.93l2.85-2.22.81-.62z"/>
                    <path fill="#EA4335" d="M12 5.38c1.62 0 3.06.56 4.21 1.64l3.15-3.15C17.45 2.09 14.97 1 12 1 7.7 1 3.99 3.47 2.18 7.07l3.66 2.84c.87-2.6 3.3-4.53 6.16-4.53z"/>
                  </svg>
                  Googleアカウントで認証
                </>
              )}
            </Button>
          )}
        </div>
        {authError && (
          <div className="text-sm text-red-600 bg-red-50 p-3 rounded-md">
            {authError}
          </div>
        )}
        {authFallbackUrl && (
          <div className="text-sm bg-amber-50 border border-amber-200 p-3 rounded-md">
            <p className="text-amber-800 mb-1">ポップアップがブロックされました。以下のリンクから認証してください:</p>
            <a href={authFallbackUrl} target="_blank" rel="noopener noreferrer" className="text-[#4f63d2] underline break-all">
              Google認証ページを開く
            </a>
          </div>
        )}
        {!oauthConfigured && !showOAuthSetup && (
          <p className="text-xs text-amber-600">
            先にOAuth設定を行ってください
          </p>
        )}
      </div>

      {/* Project ID */}
      <div className="space-y-1.5">
        <Label className="text-sm font-medium text-slate-700">GCPプロジェクト</Label>
        {projects.length > 0 ? (
          <select
            value={config.project_id || ""}
            onChange={(e) => onChange({ ...config, project_id: e.target.value, dataset_id: "", table_id: "" })}
            className="flex h-9 w-full rounded-md border border-input bg-background px-3 py-1 text-sm shadow-sm focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring"
          >
            <option value="">プロジェクトを選択...</option>
            {projects.map((p) => (
              <option key={p.project_id} value={p.project_id}>
                {p.name} ({p.project_id})
              </option>
            ))}
          </select>
        ) : (
          <Input
            value={config.project_id}
            onChange={(e) => onChange({ ...config, project_id: e.target.value })}
            placeholder={loadingProjects ? "読み込み中..." : "my-gcp-project"}
            className="text-sm h-9"
          />
        )}
      </div>

      {/* Location / Region */}
      <div className="space-y-1.5">
        <Label className="text-sm font-medium text-slate-700">ロケーション（リージョン）</Label>
        {isEditing ? (
          <div className="space-y-1.5">
            <div className="flex h-9 w-full items-center rounded-md border border-input bg-slate-100 px-3 py-1 text-sm text-slate-600 cursor-not-allowed">
              {BIGQUERY_LOCATIONS.find((l) => l.value === config.location)?.label || config.location || "US"}
            </div>
            <p className="text-xs text-muted-foreground">
              ロケーションはジョブ作成後に変更できません
            </p>
          </div>
        ) : !customLocation && isPresetLocation ? (
          <div className="space-y-1.5">
            <select
              value={config.location || "US"}
              onChange={(e) => {
                if (e.target.value === "__custom__") {
                  setCustomLocation(true);
                  onChange({ ...config, location: "" });
                } else {
                  onChange({ ...config, location: e.target.value });
                }
              }}
              className="flex h-9 w-full rounded-md border border-input bg-background px-3 py-1 text-sm shadow-sm transition-colors focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring"
            >
              {BIGQUERY_LOCATIONS.map((loc) => (
                <option key={loc.value} value={loc.value}>
                  {loc.label}
                </option>
              ))}
              <option value="__custom__">その他（手動入力）</option>
            </select>
          </div>
        ) : (
          <div className="flex items-center gap-2">
            <Input
              value={config.location || ""}
              onChange={(e) => onChange({ ...config, location: e.target.value })}
              placeholder="例: australia-southeast1"
              className="text-sm h-9"
            />
            <Button
              variant="outline"
              size="sm"
              type="button"
              className="text-xs shrink-0"
              onClick={() => {
                setCustomLocation(false);
                onChange({ ...config, location: "US" });
              }}
            >
              一覧から選択
            </Button>
          </div>
        )}
        {!isEditing && (
          <p className="text-xs text-muted-foreground">
            BigQueryデータセットのリージョンを指定します
          </p>
        )}
      </div>

      {/* Dataset ID */}
      <div className="space-y-1.5">
        <Label className="text-sm font-medium text-slate-700">データセット</Label>
        <div className="flex gap-2">
          {datasets.length > 0 && !newDataset ? (
            <select
              value={config.dataset_id || ""}
              onChange={(e) => {
                if (e.target.value === "__new__") {
                  setNewDataset(true);
                  setNewTable(true);
                  onChange({ ...config, dataset_id: "", table_id: "" });
                } else {
                  setNewTable(false);
                  onChange({ ...config, dataset_id: e.target.value, table_id: "" });
                }
              }}
              className="flex h-9 w-full rounded-md border border-input bg-background px-3 py-1 text-sm shadow-sm focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring"
            >
              <option value="">データセットを選択...</option>
              {datasets.map((d) => (
                <option key={d} value={d}>{d}</option>
              ))}
              <option value="__new__">+ 新規データセット</option>
            </select>
          ) : (
            <div className="flex gap-2 w-full">
              <Input
                value={config.dataset_id}
                onChange={(e) => onChange({ ...config, dataset_id: e.target.value })}
                placeholder={loadingDatasets ? "読み込み中..." : "新しいデータセット名を入力"}
                className="text-sm h-9"
                autoFocus={newDataset}
              />
              {newDataset && datasets.length > 0 && (
                <Button
                  variant="outline"
                  size="sm"
                  type="button"
                  className="text-xs shrink-0"
                  onClick={() => { setNewDataset(false); onChange({ ...config, dataset_id: "", table_id: "" }); }}
                >
                  一覧から選択
                </Button>
              )}
            </div>
          )}
        </div>
      </div>

      {/* Table ID */}
      <div className="space-y-1.5">
        <Label className="text-sm font-medium text-slate-700">テーブル名</Label>
        <div className="flex gap-2">
          {tables.length > 0 && !newTable ? (
            <select
              value={config.table_id || ""}
              onChange={(e) => {
                if (e.target.value === "__new__") {
                  setNewTable(true);
                  onChange({ ...config, table_id: "" });
                } else {
                  onChange({ ...config, table_id: e.target.value });
                }
              }}
              className="flex h-9 w-full rounded-md border border-input bg-background px-3 py-1 text-sm shadow-sm focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring"
            >
              <option value="">テーブルを選択...</option>
              {tables.map((t) => (
                <option key={t} value={t}>{t}</option>
              ))}
              <option value="__new__">+ 新規テーブル</option>
            </select>
          ) : (
            <div className="flex gap-2 w-full">
              <Input
                value={config.table_id}
                onChange={(e) => onChange({ ...config, table_id: e.target.value })}
                placeholder={loadingTables ? "読み込み中..." : "新しいテーブル名を入力"}
                className="text-sm h-9"
                autoFocus={newTable}
              />
              {newTable && tables.length > 0 && (
                <Button
                  variant="outline"
                  size="sm"
                  type="button"
                  className="text-xs shrink-0"
                  onClick={() => { setNewTable(false); onChange({ ...config, table_id: "" }); }}
                >
                  一覧から選択
                </Button>
              )}
            </div>
          )}
        </div>
        <p className="text-xs text-muted-foreground">
          データセットやテーブルが存在しない場合は、初回実行時に自動的に作成されます
        </p>
      </div>

      {/* Connection Test */}
      <div className="flex items-center gap-3">
        <Button
          variant="outline"
          size="sm"
          onClick={handleTestConnection}
          disabled={!config.project_id || !config.dataset_id || !authStatus?.authenticated || testStatus === "testing"}
          className="text-sm"
        >
          {testStatus === "testing" ? (
            <>
              <svg className="animate-spin h-4 w-4 mr-1.5" viewBox="0 0 24 24" fill="none">
                <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
              </svg>
              テスト中...
            </>
          ) : (
            "接続テスト"
          )}
        </Button>
        {testStatus === "success" && (
          <span className="flex items-center gap-1.5 text-sm text-green-600">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
              <path d="M20 6L9 17l-5-5" />
            </svg>
            接続成功
          </span>
        )}
        {testStatus === "error" && (
          <span className="text-sm text-red-600">接続失敗: {testError}</span>
        )}
      </div>

      {/* Transfer Mode */}
      <div className="space-y-2">
        <Label className="text-sm font-medium text-slate-700">転送モード</Label>
        <div className="flex flex-wrap gap-2">
          {TRANSFER_MODES.map((mode) => (
            <button
              key={mode.value}
              type="button"
              onClick={() => onChange({ ...config, transfer_mode: mode.value, key_columns: mode.value === "append" || mode.value === "upsert" || mode.value === "delete_in_advance" ? config.key_columns : [] })}
              className={`pill-button ${config.transfer_mode === mode.value ? "active" : ""}`}
            >
              {mode.label}
            </button>
          ))}
        </div>
        <p className="text-xs text-muted-foreground mt-1">
          {TRANSFER_MODES.find((m) => m.value === config.transfer_mode)?.desc}
        </p>
      </div>

      {/* Key Columns (for APPEND / UPSERT / DELETE_IN_ADVANCE) */}
      {needsKeyColumns && (
        <div className="space-y-2">
          <Label className="text-sm font-medium text-slate-700">キーカラム</Label>
          <p className="text-xs text-muted-foreground">
            {config.transfer_mode === "append"
              ? "重複チェック用のキーカラムを選択してください（未選択の場合は重複チェックなしで追加）"
              : "重複判定やデータ更新に使用するカラムを選択してください"}
          </p>
          <div className="flex flex-wrap gap-1.5">
            {availableColumns.length > 0 ? (
              availableColumns.map((col) => (
                <button
                  key={col}
                  type="button"
                  onClick={() => toggleKeyColumn(col)}
                  className={`pill-button text-xs ${config.key_columns.includes(col) ? "active" : ""}`}
                >
                  {col}
                </button>
              ))
            ) : (
              <span className="text-xs text-muted-foreground">カラムが選択されていません（ステップ2でカラムを選択してください）</span>
            )}
          </div>
          {config.key_columns.length > 0 && (
            <div className="flex gap-1.5 flex-wrap mt-1">
              <span className="text-xs text-muted-foreground">選択中:</span>
              {config.key_columns.map((col) => (
                <Badge key={col} variant="secondary" className="text-xs">
                  {col}
                </Badge>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
