// useQuery hook - Query execution and management
import { useState, useCallback } from 'react';
import { fluxdb } from '../services/fluxdb';
import { QueryResult as ApiQueryResult, SeriesResult } from '../types';

export interface QueryRow {
  [key: string]: any;
}

export interface QueryResult {
  columns: string[];
  rows: QueryRow[];
  executionTime: number;
  error?: string;
}

export interface UseQueryOptions {
  database: string | null;
  onSuccess?: (result: QueryResult) => void;
  onError?: (error: Error) => void;
}

// Parse API response into flattened rows
function parseApiResponse(response: ApiQueryResult): { columns: string[]; rows: QueryRow[] } {
  if (!response.results || response.results.length === 0) {
    return { columns: [], rows: [] };
  }

  const firstResult = response.results[0];
  
  if (firstResult.error) {
    throw new Error(firstResult.error);
  }

  if (!firstResult.series || firstResult.series.length === 0) {
    return { columns: [], rows: [] };
  }

  // Get columns from first series
  const columns = firstResult.series[0].columns || [];
  
  // Flatten all series values into rows
  const rows: QueryRow[] = [];
  
  for (const series of firstResult.series) {
    for (const values of (series.values || [])) {
      const row: QueryRow = {};
      columns.forEach((col, idx) => {
        row[col] = values[idx];
      });
      rows.push(row);
    }
  }

  return { columns, rows };
}

export function useQuery(options: UseQueryOptions) {
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<Error | null>(null);
  const [result, setResult] = useState<QueryResult | null>(null);
  const [queryHistory, setQueryHistory] = useState<string[]>([]);

  const executeQuery = useCallback(async (query: string): Promise<QueryResult | null> => {
    if (!options.database) {
      const err = new Error('No database selected');
      setError(err);
      options.onError?.(err);
      return null;
    }

    setIsLoading(true);
    setError(null);

    try {
      const startTime = performance.now();
      const response = await fluxdb.query(options.database, query);
      const endTime = performance.now();

      const parsed = parseApiResponse(response);
      
      const queryResult: QueryResult = {
        columns: parsed.columns,
        rows: parsed.rows,
        executionTime: endTime - startTime,
      };

      setResult(queryResult);
      setQueryHistory(prev => [query, ...prev.slice(0, 49)]); // Keep last 50 queries
      options.onSuccess?.(queryResult);
      
      return queryResult;
    } catch (err: any) {
      const error = new Error(err.message || 'Query execution failed');
      setError(error);
      options.onError?.(error);
      return null;
    } finally {
      setIsLoading(false);
    }
  }, [options.database, options.onSuccess, options.onError]);

  const clearResult = useCallback(() => {
    setResult(null);
    setError(null);
  }, []);

  const clearHistory = useCallback(() => {
    setQueryHistory([]);
  }, []);

  return {
    executeQuery,
    isLoading,
    error,
    result,
    queryHistory,
    clearResult,
    clearHistory,
  };
}

export default useQuery;
