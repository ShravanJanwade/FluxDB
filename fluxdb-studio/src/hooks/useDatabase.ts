// useDatabase hook - Database operations and management
import { useState, useCallback, useEffect } from 'react';
import { fluxdb } from '../services/fluxdb';

export interface Database {
  name: string;
  measurements?: string[];
  size?: number;
}

export interface UseDatabaseOptions {
  autoFetch?: boolean;
  onError?: (error: Error) => void;
}

export function useDatabase(options: UseDatabaseOptions = {}) {
  const [databases, setDatabases] = useState<Database[]>([]);
  const [selectedDatabase, setSelectedDatabase] = useState<string | null>(null);
  const [measurements, setMeasurements] = useState<string[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<Error | null>(null);

  // Fetch all databases
  const fetchDatabases = useCallback(async () => {
    setIsLoading(true);
    setError(null);

    try {
      const dbs = await fluxdb.listDatabases();
      setDatabases(dbs.map((name: string) => ({ name })));
      return dbs;
    } catch (err: any) {
      const error = new Error(err.message || 'Failed to fetch databases');
      setError(error);
      options.onError?.(error);
      return [];
    } finally {
      setIsLoading(false);
    }
  }, [options.onError]);

  // Create a new database
  const createDatabase = useCallback(async (name: string) => {
    setIsLoading(true);
    setError(null);

    try {
      await fluxdb.createDatabase(name);
      await fetchDatabases(); // Refresh the list
      return true;
    } catch (err: any) {
      const error = new Error(err.message || 'Failed to create database');
      setError(error);
      options.onError?.(error);
      return false;
    } finally {
      setIsLoading(false);
    }
  }, [fetchDatabases, options.onError]);

  // Drop a database
  const dropDatabase = useCallback(async (name: string) => {
    setIsLoading(true);
    setError(null);

    try {
      await fluxdb.dropDatabase(name);
      if (selectedDatabase === name) {
        setSelectedDatabase(null);
      }
      await fetchDatabases(); // Refresh the list
      return true;
    } catch (err: any) {
      const error = new Error(err.message || 'Failed to drop database');
      setError(error);
      options.onError?.(error);
      return false;
    } finally {
      setIsLoading(false);
    }
  }, [fetchDatabases, selectedDatabase, options.onError]);

  // Fetch measurements for a database
  const fetchMeasurements = useCallback(async (dbName: string) => {
    setIsLoading(true);

    try {
      const result = await fluxdb.query(dbName, 'SHOW MEASUREMENTS');
      const measurementNames: string[] = [];
      
      // Parse the API response structure
      if (result.results && result.results[0]?.series) {
        for (const series of result.results[0].series) {
          for (const values of (series.values || [])) {
            if (values[0]) {
              measurementNames.push(values[0]);
            }
          }
        }
      }
      
      setMeasurements(measurementNames);
      return measurementNames;
    } catch (err: any) {
      console.error('Failed to fetch measurements:', err);
      setMeasurements([]);
      return [];
    } finally {
      setIsLoading(false);
    }
  }, []);

  // Select a database
  const selectDatabase = useCallback(async (name: string | null) => {
    setSelectedDatabase(name);
    if (name) {
      await fetchMeasurements(name);
    } else {
      setMeasurements([]);
    }
  }, [fetchMeasurements]);

  // Auto-fetch on mount if enabled
  useEffect(() => {
    if (options.autoFetch) {
      fetchDatabases();
    }
  }, [options.autoFetch, fetchDatabases]);

  return {
    databases,
    selectedDatabase,
    measurements,
    isLoading,
    error,
    fetchDatabases,
    createDatabase,
    dropDatabase,
    selectDatabase,
    fetchMeasurements,
  };
}

export default useDatabase;
