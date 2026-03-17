"use client";

import { Button } from "@/components/ui/button";
import { api } from "@/lib/api";

interface ExportPanelProps {
  platformId: string | null;
  endpointId: string | null;
  columns: string;
  limit: number;
  disabled: boolean;
}

export function ExportPanel({
  platformId,
  endpointId,
  columns,
  limit,
  disabled,
}: ExportPanelProps) {
  const handleExport = (format: "csv" | "json") => {
    if (!platformId || !endpointId) return;
    const url = api.getExportUrl(format, {
      platform_id: platformId,
      endpoint_id: endpointId,
      columns: columns || undefined,
      limit,
    });
    window.open(url, "_blank");
  };

  return (
    <div className="flex gap-2">
      <Button
        variant="outline"
        size="sm"
        disabled={disabled}
        onClick={() => handleExport("csv")}
      >
        CSV ダウンロード
      </Button>
      <Button
        variant="outline"
        size="sm"
        disabled={disabled}
        onClick={() => handleExport("json")}
      >
        JSON ダウンロード
      </Button>
    </div>
  );
}
