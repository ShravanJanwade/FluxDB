// FluxDB Studio - TypeScript Type Definitions

// ============================================================================
// Connection Types
// ============================================================================

export interface Connection {
  id: string;
  name: string;
  host: string;
  port: number;
  isConnected: boolean;
  lastConnected?: Date;
}

export interface ConnectionConfig {
  host: string;
  port: number;
  name?: string;
}

// ============================================================================
// Database Types
// ============================================================================

export interface Database {
  name: string;
  measurements: Measurement[];
}

export interface Measurement {
  name: string;
  fields: FieldInfo[];
  tags: string[];
}

export interface FieldInfo {
  name: string;
  type: 'float' | 'integer' | 'boolean' | 'string';
}

// ============================================================================
// Query Types
// ============================================================================

export interface QueryResult {
  results: QueryResultItem[];
}

export interface QueryResultItem {
  statement_id: number;
  series?: SeriesResult[];
  error?: string;
}

export interface SeriesResult {
  name: string;
  columns: string[];
  values: any[][];
}

export interface QueryExecution {
  id: string;
  query: string;
  database: string;
  executedAt: Date;
  duration: number;
  rowCount: number;
  error?: string;
  result?: QueryResult;
}

// ============================================================================
// Write Types
// ============================================================================

export interface WriteRequest {
  database: string;
  precision?: 'ns' | 'us' | 'ms' | 's';
  data: string; // Line protocol format
}

export interface DataPointInput {
  measurement: string;
  tags: Record<string, string>;
  fields: Record<string, number | string | boolean>;
  timestamp?: number;
}

// ============================================================================
// Stats Types
// ============================================================================

export interface ServerStats {
  database_count: number;
  total_entries: number;
  total_size_bytes: number;
  databases: DatabaseStats[];
}

export interface DatabaseStats {
  name: string;
  memtable_size: number;
  sstables: number;
  total_entries: number;
}

export interface HealthStatus {
  status: 'ok' | 'error';
  version: string;
  latency?: number;
}

// ============================================================================
// UI Types
// ============================================================================

export type ViewType = 'query' | 'dashboard' | 'explorer' | 'import' | 'export' | 'write' | 'update' | 'delete' | 'settings';

export type ToastType = 'success' | 'error' | 'warning' | 'info';

export interface ToastMessage {
  id: string;
  type: ToastType;
  title: string;
  message?: string;
  duration?: number;
}

export interface TabItem {
  id: string;
  label: string;
  icon?: React.ComponentType<{ size?: number }>;
  closable?: boolean;
}

// ============================================================================
// Import/Export Types
// ============================================================================

export type ImportFormat = 'csv' | 'json' | 'line_protocol';
export type ExportFormat = 'csv' | 'json' | 'line_protocol';

export interface ImportConfig {
  format: ImportFormat;
  database: string;
  measurement?: string;
  timestampField?: string;
  timestampPrecision?: 'ns' | 'us' | 'ms' | 's';
  skipHeader?: boolean;
  fieldMappings?: Record<string, string>;
  tagColumns?: string[];
}

export interface ExportConfig {
  format: ExportFormat;
  database: string;
  query: string;
  fileName: string;
  includeHeader?: boolean;
}

// ============================================================================
// API Error Types
// ============================================================================

export interface ApiError {
  error: string;
  status?: number;
}

export class FluxDBApiError extends Error {
  constructor(
    message: string,
    public readonly status?: number,
    public readonly details?: string
  ) {
    super(message);
    this.name = 'FluxDBApiError';
  }
}
