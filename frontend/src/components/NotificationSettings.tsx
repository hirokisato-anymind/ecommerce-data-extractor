"use client";

import { useState, useEffect } from "react";
import { api } from "@/lib/api";
import type { NotificationSettingsResponse, ExpiryRecord } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";

// ---------------------------------------------------------------------------
// Slack Settings Section
// ---------------------------------------------------------------------------

function SlackSettings() {
  const [settings, setSettings] = useState<NotificationSettingsResponse | null>(null);
  const [webhookUrl, setWebhookUrl] = useState("");
  const [channel, setChannel] = useState("");
  const [enabled, setEnabled] = useState(true);
  const [saving, setSaving] = useState(false);
  const [testing, setTesting] = useState(false);
  const [testResult, setTestResult] = useState<string | null>(null);

  useEffect(() => {
    api.getNotificationSettings().then((s) => {
      setSettings(s);
      setChannel(s.slack_channel);
      setEnabled(s.enabled);
    }).catch(() => {});
  }, []);

  const handleSave = async () => {
    setSaving(true);
    try {
      const payload: Record<string, unknown> = { slack_channel: channel, enabled };
      if (webhookUrl) payload.slack_webhook_url = webhookUrl;
      await api.saveNotificationSettings(payload as { slack_webhook_url?: string; slack_channel?: string; enabled?: boolean });
      // Refresh
      const s = await api.getNotificationSettings();
      setSettings(s);
      setWebhookUrl("");
    } catch {
      // ignore
    } finally {
      setSaving(false);
    }
  };

  const handleTest = async () => {
    setTesting(true);
    setTestResult(null);
    try {
      const res = await api.testSlackNotification();
      setTestResult(res.ok ? "送信成功" : (res.error || "送信失敗"));
    } catch {
      setTestResult("送信失敗");
    } finally {
      setTesting(false);
    }
  };

  return (
    <Card className="shadow-sm border-slate-200">
      <CardContent className="p-6 space-y-4">
        <div className="flex items-center gap-2">
          <h3 className="text-sm font-semibold text-slate-800">Slack通知設定</h3>
          {settings?.has_webhook && (
            <Badge variant={settings.enabled ? "default" : "secondary"} className="text-[10px]">
              {settings.enabled ? "有効" : "無効"}
            </Badge>
          )}
        </div>

        <div className="space-y-3">
          <div>
            <Label className="text-xs text-slate-600">Webhook URL</Label>
            <Input
              type="url"
              placeholder={settings?.has_webhook ? `設定済み (${settings.slack_webhook_url_preview})` : "https://hooks.slack.com/services/..."}
              value={webhookUrl}
              onChange={(e) => setWebhookUrl(e.target.value)}
              className="text-sm mt-1"
            />
          </div>

          <div>
            <Label className="text-xs text-slate-600">チャンネル（任意）</Label>
            <Input
              placeholder="#alerts"
              value={channel}
              onChange={(e) => setChannel(e.target.value)}
              className="text-sm mt-1"
            />
          </div>

          <div className="flex items-center gap-2">
            <button
              type="button"
              role="switch"
              aria-checked={enabled}
              onClick={() => setEnabled(!enabled)}
              className={`relative inline-flex h-5 w-9 items-center rounded-full transition-colors ${
                enabled ? "bg-[#4f63d2]" : "bg-slate-300"
              }`}
            >
              <span
                className={`inline-block h-3.5 w-3.5 transform rounded-full bg-white transition-transform ${
                  enabled ? "translate-x-4.5" : "translate-x-0.5"
                }`}
              />
            </button>
            <span className="text-xs text-slate-600">通知を有効にする</span>
          </div>
        </div>

        <div className="flex items-center gap-2 pt-2">
          <Button size="sm" className="text-xs" onClick={handleSave} disabled={saving}>
            {saving ? "保存中..." : "保存"}
          </Button>
          {settings?.has_webhook && (
            <Button size="sm" variant="outline" className="text-xs" onClick={handleTest} disabled={testing}>
              {testing ? "送信中..." : "テスト送信"}
            </Button>
          )}
          {testResult && (
            <span className={`text-xs ${testResult === "送信成功" ? "text-green-600" : "text-red-600"}`}>
              {testResult}
            </span>
          )}
        </div>
      </CardContent>
    </Card>
  );
}

// ---------------------------------------------------------------------------
// Credential Expiry Section
// ---------------------------------------------------------------------------

function formatRemaining(hours: number | null | undefined): string {
  if (hours === null || hours === undefined) return "-";
  if (hours <= 0) return "期限切れ";
  const days = Math.floor(hours / 24);
  const h = hours % 24;
  if (days > 0) return `${days}日${h}時間`;
  return `${h}時間`;
}

function expiryColor(hours: number | null | undefined): string {
  if (hours === null || hours === undefined) return "text-slate-500";
  if (hours <= 0) return "text-red-600";
  if (hours <= 72) return "text-red-500";    // 3 days
  if (hours <= 168) return "text-amber-500";  // 7 days
  return "text-green-600";
}

function notifBadge(label: string, sent: string | null | undefined) {
  return (
    <span
      key={label}
      className={`inline-flex items-center text-[10px] px-1.5 py-0.5 rounded ${
        sent ? "bg-green-100 text-green-700" : "bg-slate-100 text-slate-400"
      }`}
    >
      {label === "7d" ? "1週間前" : label === "3d" ? "3日前" : label === "1d" ? "前日" : "6時間前"}
      {sent && " ✓"}
    </span>
  );
}

function CredentialExpiryPanel() {
  const [records, setRecords] = useState<ExpiryRecord[]>([]);
  const [rakutenDate, setRakutenDate] = useState("");
  const [saving, setSaving] = useState(false);

  const loadRecords = () => {
    api.getExpiryRecords().then(setRecords).catch(() => {});
  };

  useEffect(() => { loadRecords(); }, []);

  const rakutenRecord = records.find(
    (r) => r.platform_id === "rakuten" && r.credential_type === "license_key"
  );
  const yahooRecord = records.find(
    (r) => r.platform_id === "yahoo" && r.credential_type === "oauth_token"
  );

  // Init rakutenDate from existing record
  useEffect(() => {
    if (rakutenRecord?.expires_at && !rakutenDate) {
      try {
        const d = new Date(rakutenRecord.expires_at);
        setRakutenDate(d.toISOString().split("T")[0]);
      } catch { /* ignore */ }
    }
  }, [rakutenRecord, rakutenDate]);

  const handleSaveRakuten = async () => {
    if (!rakutenDate) return;
    setSaving(true);
    try {
      await api.setRakutenLicenseExpiry(rakutenDate);
      loadRecords();
    } catch { /* ignore */ }
    finally { setSaving(false); }
  };

  return (
    <Card className="shadow-sm border-slate-200">
      <CardContent className="p-6 space-y-5">
        <h3 className="text-sm font-semibold text-slate-800">認証情報の有効期限</h3>

        {/* Rakuten */}
        <div className="space-y-2">
          <div className="flex items-center gap-2">
            <span className="text-xs font-medium text-slate-700">楽天 ライセンスキー</span>
            <Badge variant="outline" className="text-[10px]">手動入力</Badge>
          </div>
          <div className="flex items-center gap-2">
            <Input
              type="date"
              value={rakutenDate}
              onChange={(e) => setRakutenDate(e.target.value)}
              className="text-sm w-48"
            />
            <Button size="sm" className="text-xs" onClick={handleSaveRakuten} disabled={saving || !rakutenDate}>
              {saving ? "保存中..." : "保存"}
            </Button>
          </div>
          {rakutenRecord && (
            <div className="flex items-center gap-3 text-xs">
              <span className={expiryColor(rakutenRecord.remaining_hours)}>
                残り: {formatRemaining(rakutenRecord.remaining_hours)}
              </span>
              <div className="flex gap-1">
                {(["7d", "3d", "1d", "6h"] as const).map((l) =>
                  notifBadge(l, rakutenRecord.sent_notifications?.[l])
                )}
              </div>
            </div>
          )}
        </div>

        <hr className="border-slate-200" />

        {/* Yahoo */}
        <div className="space-y-2">
          <div className="flex items-center gap-2">
            <span className="text-xs font-medium text-slate-700">Yahoo OAuth トークン</span>
            <Badge variant="outline" className="text-[10px]">自動追跡</Badge>
          </div>
          {yahooRecord?.expires_at ? (
            <div className="space-y-1">
              <div className="text-xs text-slate-600">
                有効期限: {new Date(yahooRecord.expires_at).toLocaleString("ja-JP", {
                  year: "numeric", month: "2-digit", day: "2-digit",
                  hour: "2-digit", minute: "2-digit",
                })}
              </div>
              <div className="flex items-center gap-3 text-xs">
                <span className={expiryColor(yahooRecord.remaining_hours)}>
                  残り: {formatRemaining(yahooRecord.remaining_hours)}
                </span>
                <div className="flex gap-1">
                  {(["7d", "3d", "1d", "6h"] as const).map((l) =>
                    notifBadge(l, yahooRecord.sent_notifications?.[l])
                  )}
                </div>
              </div>
            </div>
          ) : (
            <p className="text-xs text-slate-400">
              Yahoo OAuth認証を実行すると自動的に追跡が開始されます
            </p>
          )}
        </div>
      </CardContent>
    </Card>
  );
}

// ---------------------------------------------------------------------------
// Main Export
// ---------------------------------------------------------------------------

export function NotificationSettings() {
  return (
    <div className="space-y-6 max-w-2xl mx-auto">
      <CredentialExpiryPanel />
      <SlackSettings />
    </div>
  );
}
