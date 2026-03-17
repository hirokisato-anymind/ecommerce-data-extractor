const API_BASE = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000/api";

async function fetchAPI<T>(path: string, params?: Record<string, string>): Promise<T> {
  const url = new URL(`${API_BASE}${path}`);
  if (params) {
    Object.entries(params).forEach(([k, v]) => {
      if (v !== undefined && v !== "") url.searchParams.set(k, v);
    });
  }
  const res = await fetch(url.toString());
  if (!res.ok) throw new Error(`API error: ${res.status} ${res.statusText}`);
  return res.json();
}

export interface Platform {
  id: string;
  name: string;
  configured: boolean;
}

export interface Endpoint {
  id: string;
  name: string;
  description: string;
}

export interface SchemaField {
  key: string;
  label: string;
  type: string;
  description: string;
}

export interface ExtractResult {
  items: Record<string, unknown>[];
  columns: string[];
  next_cursor: string | null;
  total: number | null;
}

export interface FilterConfig {
  column: string;
  operator: string;
  value: string;
}

export interface ScheduleConfig {
  enabled: boolean;
  cronExpression: string;
  description: string;
}

export interface DestinationConfig {
  type: "bigquery";
  project_id: string;
  dataset_id: string;
  table_id: string;
  transfer_mode: "append" | "append_direct" | "replace" | "delete_in_advance" | "upsert";
  key_columns: string[];
  location: string;
}

export interface ScheduleSlotConfig {
  frequency: "hourly" | "daily" | "weekly" | "monthly";
  hour: number;
  minute: number;
  day_of_week?: number; // 0=Mon, 6=Sun
  day_of_month?: number; // 1-31
}

export interface SchedulePayload {
  name: string;
  platform_id: string;
  endpoint_id: string;
  columns: string[];
  filters: FilterConfig[];
  limit: number;
  schedule_config: ScheduleSlotConfig;
  destination: DestinationConfig;
  enabled: boolean;
}

export interface BigQueryTable {
  table_id: string;
  row_count: number;
  created: string;
}

export interface CredentialField {
  key: string;
  label: string;
  hint: string;
  secret: boolean;
  readonly: boolean;
  value: string;
  hasValue: boolean;
}

export interface CredentialsResponse {
  platform_id: string;
  fields: CredentialField[];
  oauth: boolean;
}

export const api = {
  getPlatforms: () => fetchAPI<Platform[]>("/platforms"),
  getEndpoints: (platformId: string) =>
    fetchAPI<Endpoint[]>(`/platforms/${platformId}/endpoints`),
  getSchema: (platformId: string, endpointId: string) =>
    fetchAPI<SchemaField[]>(
      `/platforms/${platformId}/endpoints/${endpointId}/schema`
    ),
  extractData: (params: {
    platform_id: string;
    endpoint_id: string;
    columns?: string;
    limit?: number;
    cursor?: string;
    filters?: string;
    start_date?: string;
    end_date?: string;
    fetch_all?: boolean;
  }) => {
    const searchParams: Record<string, string> = {
      platform_id: params.platform_id,
      endpoint_id: params.endpoint_id,
    };
    if (params.columns) searchParams.columns = params.columns;
    if (params.limit) searchParams.limit = String(params.limit);
    if (params.cursor) searchParams.cursor = params.cursor;
    if (params.filters) searchParams.filters = params.filters;
    if (params.start_date) searchParams.start_date = params.start_date;
    if (params.end_date) searchParams.end_date = params.end_date;
    if (params.fetch_all) searchParams.fetch_all = "true";
    return fetchAPI<ExtractResult>("/extract", searchParams);
  },
  getCredentials: async (platformId: string): Promise<CredentialsResponse> => {
    const res = await fetch(`${API_BASE}/credentials/${platformId}`);
    if (!res.ok) throw new Error(`API error: ${res.status} ${res.statusText}`);
    return res.json();
  },
  saveCredentials: async (
    platformId: string,
    values: Record<string, string>,
  ): Promise<{ ok: boolean }> => {
    const res = await fetch(`${API_BASE}/credentials/${platformId}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ values }),
    });
    if (!res.ok) throw new Error(`API error: ${res.status} ${res.statusText}`);
    return res.json();
  },
  getOAuthUrl: (platformId: string) =>
    fetchAPI<{ authorize_url: string }>(`/oauth/${platformId}/authorize`),
  getExportUrl: (
    format: "csv" | "json",
    params: {
      platform_id: string;
      endpoint_id: string;
      columns?: string;
      limit?: number;
    }
  ) => {
    const url = new URL(`${API_BASE}/export/${format}`);
    url.searchParams.set("platform_id", params.platform_id);
    url.searchParams.set("endpoint_id", params.endpoint_id);
    if (params.columns) url.searchParams.set("columns", params.columns);
    if (params.limit) url.searchParams.set("limit", String(params.limit));
    return url.toString();
  },

  // BigQuery OAuth Config
  getBigQueryOAuthConfigStatus: async () => {
    const res = await fetch(`${API_BASE}/bigquery/oauth-config-status`);
    if (!res.ok) throw new Error(`API error: ${res.status}`);
    return res.json() as Promise<{ configured: boolean; client_id_preview?: string }>;
  },

  saveBigQueryOAuthConfig: async (params: { client_id: string; client_secret: string }) => {
    const res = await fetch(`${API_BASE}/bigquery/oauth-config`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(params),
    });
    if (!res.ok) throw new Error(`API error: ${res.status}`);
    return res.json() as Promise<{ ok: boolean; message: string }>;
  },

  // BigQuery Auth
  getBigQueryAuthUrl: async () => {
    const res = await fetch(`${API_BASE}/bigquery/auth-url`);
    if (!res.ok) throw new Error(`API error: ${res.status}`);
    return res.json() as Promise<{ authorize_url: string }>;
  },

  getBigQueryAuthStatus: async () => {
    const res = await fetch(`${API_BASE}/bigquery/auth-status`);
    if (!res.ok) throw new Error(`API error: ${res.status}`);
    return res.json() as Promise<{ authenticated: boolean; email?: string; error?: string }>;
  },

  testBigQueryConnection: async (params: { project_id: string; dataset_id: string }) => {
    const res = await fetch(`${API_BASE}/bigquery/test-connection`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(params),
    });
    if (!res.ok) throw new Error(`API error: ${res.status}`);
    return res.json() as Promise<{ ok: boolean; datasets: string[]; error?: string }>;
  },

  listBigQueryTables: async (params: { project_id: string; dataset_id: string }) => {
    const res = await fetch(`${API_BASE}/bigquery/tables`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(params),
    });
    if (!res.ok) throw new Error(`API error: ${res.status}`);
    return res.json() as Promise<{ tables: BigQueryTable[] }>;
  },

  getBigQueryTableSchema: async (params: { project_id: string; dataset_id: string; table_id: string }) => {
    const res = await fetch(`${API_BASE}/bigquery/table-schema`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(params),
    });
    if (!res.ok) throw new Error(`API error: ${res.status}`);
    return res.json() as Promise<{ columns: { name: string; type: string; mode: string }[] }>;
  },

  // Schedules
  createSchedule: async (payload: SchedulePayload) => {
    const res = await fetch(`${API_BASE}/schedules/`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (!res.ok) throw new Error(`API error: ${res.status}`);
    return res.json();
  },

  listSchedules: async () => {
    const res = await fetch(`${API_BASE}/schedules/`);
    if (!res.ok) throw new Error(`API error: ${res.status}`);
    return res.json();
  },

  deleteSchedule: async (id: string) => {
    const res = await fetch(`${API_BASE}/schedules/${id}`, { method: "DELETE" });
    if (!res.ok && res.status !== 204) throw new Error(`API error: ${res.status}`);
    return { ok: true };
  },

  updateSchedule: async (id: string, payload: Partial<SchedulePayload>) => {
    const res = await fetch(`${API_BASE}/schedules/${id}`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (!res.ok) throw new Error(`API error: ${res.status}`);
    return res.json();
  },

  triggerSchedule: async (id: string) => {
    const res = await fetch(`${API_BASE}/schedules/${id}/run`, {
      method: "POST",
    });
    if (!res.ok) throw new Error(`API error: ${res.status}`);
    return res.json();
  },
};
