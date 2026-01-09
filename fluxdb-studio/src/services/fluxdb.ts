// FluxDB API Service
// HTTP client for communicating with the FluxDB server

import {
  QueryResult,
  ServerStats,
  HealthStatus,
  WriteRequest,
  FluxDBApiError,
} from '../types';

const DEFAULT_TIMEOUT = 30000; // 30 seconds

class FluxDBService {
  private baseUrl: string = 'http://localhost:8086';

  setBaseUrl(host: string, port: number) {
    this.baseUrl = `http://${host}:${port}`;
  }

  getBaseUrl(): string {
    return this.baseUrl;
  }

  private async request<T>(
    endpoint: string,
    options: RequestInit = {},
    timeout: number = DEFAULT_TIMEOUT
  ): Promise<T> {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), timeout);

    try {
      const response = await fetch(`${this.baseUrl}${endpoint}`, {
        ...options,
        signal: controller.signal,
        headers: {
          'Content-Type': 'application/json',
          ...options.headers,
        },
      });

      clearTimeout(timeoutId);

      if (!response.ok) {
        const errorData = await response.json().catch(() => ({}));
        throw new FluxDBApiError(
          errorData.error || `HTTP ${response.status}`,
          response.status,
          JSON.stringify(errorData)
        );
      }

      // Handle empty responses
      const text = await response.text();
      if (!text) {
        return {} as T;
      }

      return JSON.parse(text);
    } catch (error) {
      clearTimeout(timeoutId);
      
      if (error instanceof FluxDBApiError) {
        throw error;
      }
      
      if (error instanceof Error) {
        if (error.name === 'AbortError') {
          throw new FluxDBApiError('Request timeout', 408);
        }
        throw new FluxDBApiError(error.message);
      }
      
      throw new FluxDBApiError('Unknown error occurred');
    }
  }

  // ============================================================================
  // Health & Connection
  // ============================================================================

  async testConnection(host: string, port: number): Promise<HealthStatus> {
    const previousUrl = this.baseUrl;
    this.setBaseUrl(host, port);
    
    try {
      const startTime = performance.now();
      const result = await this.request<{ status: string; version: string }>('/health', {}, 5000);
      const latency = Math.round(performance.now() - startTime);
      
      return {
        status: result.status === 'ok' ? 'ok' : 'error',
        version: result.version,
        latency,
      };
    } catch (error) {
      this.baseUrl = previousUrl;
      throw error;
    }
  }

  async ping(): Promise<boolean> {
    try {
      const response = await fetch(`${this.baseUrl}/ping`, {
        method: 'GET',
      });
      const text = await response.text();
      return text === 'pong';
    } catch {
      return false;
    }
  }

  async getHealth(): Promise<HealthStatus> {
    const startTime = performance.now();
    const result = await this.request<{ status: string; version: string }>('/health');
    const latency = Math.round(performance.now() - startTime);
    
    return {
      status: result.status === 'ok' ? 'ok' : 'error',
      version: result.version,
      latency,
    };
  }

  // ============================================================================
  // Database Management
  // ============================================================================

  async listDatabases(): Promise<string[]> {
    return this.request<string[]>('/databases');
  }

  async createDatabase(name: string): Promise<void> {
    await this.request(`/databases/${encodeURIComponent(name)}`, {
      method: 'POST',
    });
  }

  async dropDatabase(name: string): Promise<void> {
    await this.request(`/databases/${encodeURIComponent(name)}`, {
      method: 'DELETE',
    });
  }

  // ============================================================================
  // Query Execution
  // ============================================================================

  async query(database: string, sql: string): Promise<QueryResult> {
    const params = new URLSearchParams({
      db: database,
      q: sql,
    });
    
    return this.request<QueryResult>(`/query?${params.toString()}`, {
      method: 'GET',
    });
  }

  async queryV2(database: string, query: string): Promise<QueryResult> {
    return this.request<QueryResult>('/api/v2/query', {
      method: 'POST',
      body: JSON.stringify({
        query,
        database,
      }),
    });
  }

  // ============================================================================
  // Write Operations
  // ============================================================================

  async write(request: WriteRequest): Promise<void> {
    const params = new URLSearchParams({
      db: request.database,
      precision: request.precision || 'ns',
    });

    await fetch(`${this.baseUrl}/write?${params.toString()}`, {
      method: 'POST',
      headers: {
        'Content-Type': 'text/plain',
      },
      body: request.data,
    }).then(async (response) => {
      if (!response.ok) {
        const errorData = await response.json().catch(() => ({}));
        throw new FluxDBApiError(
          errorData.error || `HTTP ${response.status}`,
          response.status
        );
      }
    });
  }

  async writeLineProtocol(
    database: string,
    data: string,
    precision: 'ns' | 'us' | 'ms' | 's' = 'ns'
  ): Promise<void> {
    return this.write({ database, data, precision });
  }

  // ============================================================================
  // Statistics
  // ============================================================================

  async getStats(): Promise<ServerStats> {
    return this.request<ServerStats>('/stats');
  }

  async getMetrics(): Promise<string> {
    const response = await fetch(`${this.baseUrl}/metrics`);
    if (!response.ok) {
      throw new FluxDBApiError(`HTTP ${response.status}`, response.status);
    }
    return response.text();
  }

  // ============================================================================
  // Utility Methods
  // ============================================================================

  formatLineProtocol(
    measurement: string,
    tags: Record<string, string>,
    fields: Record<string, number | string | boolean>,
    timestamp?: number
  ): string {
    // Format: measurement,tag1=val1,tag2=val2 field1=val1,field2=val2 timestamp
    let line = measurement;

    // Add tags
    const tagPairs = Object.entries(tags)
      .map(([k, v]) => `${k}=${v}`)
      .join(',');
    if (tagPairs) {
      line += `,${tagPairs}`;
    }

    // Add fields
    const fieldPairs = Object.entries(fields)
      .map(([k, v]) => {
        if (typeof v === 'string') {
          return `${k}="${v}"`;
        } else if (typeof v === 'boolean') {
          return `${k}=${v}`;
        } else if (Number.isInteger(v)) {
          return `${k}=${v}i`;
        } else {
          return `${k}=${v}`;
        }
      })
      .join(',');
    line += ` ${fieldPairs}`;

    // Add timestamp if provided
    if (timestamp !== undefined) {
      line += ` ${timestamp}`;
    }

    return line;
  }

  parseCSVToLineProtocol(
    csv: string,
    measurement: string,
    tagColumns: string[],
    timestampColumn?: string,
    skipHeader: boolean = true
  ): string[] {
    const lines = csv.trim().split('\n');
    const headers = lines[0].split(',').map((h) => h.trim());
    const dataLines = skipHeader ? lines.slice(1) : lines;

    return dataLines.map((line) => {
      const values = line.split(',').map((v) => v.trim());
      const record: Record<string, string> = {};
      headers.forEach((h, i) => {
        record[h] = values[i];
      });

      const tags: Record<string, string> = {};
      const fields: Record<string, number | string | boolean> = {};
      let timestamp: number | undefined;

      headers.forEach((h) => {
        if (tagColumns.includes(h)) {
          tags[h] = record[h];
        } else if (timestampColumn && h === timestampColumn) {
          timestamp = parseInt(record[h], 10);
        } else {
          const num = parseFloat(record[h]);
          if (!isNaN(num)) {
            fields[h] = num;
          } else if (record[h] === 'true' || record[h] === 'false') {
            fields[h] = record[h] === 'true';
          } else {
            fields[h] = record[h];
          }
        }
      });

      return this.formatLineProtocol(measurement, tags, fields, timestamp);
    });
  }
}

// Export singleton instance
export const fluxdb = new FluxDBService();
export default fluxdb;
