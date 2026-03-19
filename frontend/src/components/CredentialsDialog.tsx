"use client";

import { useState, useEffect } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogFooter,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Badge } from "@/components/ui/badge";
import { Separator } from "@/components/ui/separator";

interface CredentialsDialogProps {
  platformId: string | null;
  platformName: string;
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

export function CredentialsDialog({
  platformId,
  platformName,
  open,
  onOpenChange,
}: CredentialsDialogProps) {
  const queryClient = useQueryClient();
  const [formValues, setFormValues] = useState<Record<string, string>>({});
  const [saveError, setSaveError] = useState("");

  const { data: creds, isLoading } = useQuery({
    queryKey: ["credentials", platformId],
    queryFn: () => api.getCredentials(platformId!),
    enabled: !!platformId && open,
  });

  // Initialize form values when credentials are loaded
  useEffect(() => {
    if (creds) {
      const vals: Record<string, string> = {};
      for (const f of creds.fields) {
        vals[f.key] = f.value;
      }
      setFormValues(vals);
    }
  }, [creds]);

  // Reset on close
  useEffect(() => {
    if (!open) {
      setFormValues({});
      setSaveError("");
    }
  }, [open]);

  const saveMutation = useMutation({
    mutationFn: () => {
      // Secret fields that are still empty (not changed by user) should not be sent,
      // otherwise they would clear existing values in storage.
      const secretKeys = new Set(
        (creds?.fields ?? []).filter((f) => f.secret && f.hasValue).map((f) => f.key)
      );
      const filtered: Record<string, string> = {};
      for (const [k, v] of Object.entries(formValues)) {
        if (secretKeys.has(k) && !v) continue; // skip unchanged secret
        filtered[k] = v;
      }
      return api.saveCredentials(platformId!, filtered);
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["platforms"] });
      queryClient.invalidateQueries({ queryKey: ["credentials", platformId] });
      onOpenChange(false);
    },
    onError: (e) => {
      setSaveError((e as Error).message);
    },
  });

  const handleOAuth = async () => {
    if (!platformId) return;
    try {
      const { authorize_url } = await api.getOAuthUrl(platformId);
      window.open(authorize_url, "_blank", "width=600,height=700");
    } catch {
      setSaveError(
        "OAuth URLの取得に失敗しました。Client IDとストアドメインを先に保存してください。"
      );
    }
  };

  const editableFields = creds?.fields.filter((f) => !f.readonly) ?? [];
  const readonlyFields = creds?.fields.filter((f) => f.readonly) ?? [];

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-lg max-h-[85vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle>{platformName} - API設定</DialogTitle>
        </DialogHeader>

        {isLoading ? (
          <div className="py-8 text-center text-sm text-muted-foreground">
            読み込み中...
          </div>
        ) : (
          <div className="space-y-4 py-2">
            {/* Editable fields */}
            {editableFields.map((field) => (
              <div key={field.key} className="space-y-1">
                <div className="flex items-center gap-2">
                  <Label htmlFor={field.key} className="text-sm font-medium">
                    {field.label}
                  </Label>
                  {field.hasValue && (
                    <Badge variant="default" className="text-[10px] py-0">
                      設定済
                    </Badge>
                  )}
                </div>
                <Input
                  id={field.key}
                  type={field.secret ? "password" : "text"}
                  placeholder={
                    field.secret && field.hasValue
                      ? "変更する場合のみ入力"
                      : field.hint
                  }
                  value={formValues[field.key] ?? ""}
                  onChange={(e) =>
                    setFormValues((prev) => ({
                      ...prev,
                      [field.key]: e.target.value,
                    }))
                  }
                />
                <p className="text-xs text-muted-foreground">{field.hint}</p>
              </div>
            ))}

            {/* OAuth section */}
            {creds?.oauth && (
              <>
                <Separator />
                <div className="space-y-3">
                  <h3 className="text-sm font-semibold">OAuth認証</h3>
                  <p className="text-xs text-muted-foreground">
                    上の Client ID / Secret
                    を保存した後、OAuth認証ボタンを押すとアクセストークンを自動取得します。
                  </p>

                  {/* Show readonly token fields status */}
                  {readonlyFields.map((field) => (
                    <div
                      key={field.key}
                      className="flex items-center justify-between rounded-md border p-3"
                    >
                      <div>
                        <span className="text-sm">{field.label}</span>
                        <p className="text-xs text-muted-foreground">
                          {field.hint}
                        </p>
                      </div>
                      {field.hasValue ? (
                        <Badge variant="default" className="text-[10px]">
                          取得済
                        </Badge>
                      ) : (
                        <Badge variant="secondary" className="text-[10px]">
                          未取得
                        </Badge>
                      )}
                    </div>
                  ))}

                  <Button
                    variant="secondary"
                    className="w-full"
                    onClick={handleOAuth}
                  >
                    OAuth認証を開始
                  </Button>
                </div>
              </>
            )}

            {saveError && (
              <p className="text-sm text-red-600">{saveError}</p>
            )}

            <DialogFooter>
              <Button variant="outline" onClick={() => onOpenChange(false)}>
                キャンセル
              </Button>
              <Button
                onClick={() => saveMutation.mutate()}
                disabled={saveMutation.isPending}
              >
                {saveMutation.isPending ? "保存中..." : "保存"}
              </Button>
            </DialogFooter>
          </div>
        )}
      </DialogContent>
    </Dialog>
  );
}
