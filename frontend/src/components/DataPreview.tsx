"use client";

import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { ScrollArea } from "@/components/ui/scroll-area";

interface DataPreviewProps {
  columns: string[];
  items: Record<string, unknown>[];
  total: number | null;
}

export function DataPreview({ columns, items, total }: DataPreviewProps) {
  if (items.length === 0) {
    return (
      <div className="flex items-center justify-center h-48 text-muted-foreground text-sm">
        データがありません。Fetch Data をクリックしてデータを取得してください。
      </div>
    );
  }

  return (
    <div className="space-y-2">
      <div className="text-sm text-muted-foreground">
        {total !== null && `全 ${total} 件中 `}
        {items.length} 件表示
      </div>
      <ScrollArea className="h-[400px] rounded border">
        <Table>
          <TableHeader>
            <TableRow>
              {columns.map((col) => (
                <TableHead key={col} className="whitespace-nowrap">
                  {col}
                </TableHead>
              ))}
            </TableRow>
          </TableHeader>
          <TableBody>
            {items.map((item, i) => (
              <TableRow key={i}>
                {columns.map((col) => (
                  <TableCell key={col} className="max-w-[300px] truncate">
                    {formatCell(item[col])}
                  </TableCell>
                ))}
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </ScrollArea>
    </div>
  );
}

function formatCell(value: unknown): string {
  if (value === null || value === undefined) return "-";
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}
