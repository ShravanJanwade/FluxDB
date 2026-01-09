// Query Store - Zustand state management for queries
import { create } from 'zustand';
import { persist } from 'zustand/middleware';
import { QueryResult, QueryExecution } from '../types';
import { fluxdb } from '../services/fluxdb';

interface QueryState {
  // State
  currentQuery: string;
  currentDatabase: string;
  isExecuting: boolean;
  lastResult: QueryResult | null;
  lastError: string | null;
  lastExecutionTime: number | null;
  queryHistory: QueryExecution[];
  
  // Actions
  setCurrentQuery: (query: string) => void;
  setCurrentDatabase: (database: string) => void;
  executeQuery: () => Promise<QueryResult>;
  executeCustomQuery: (database: string, query: string) => Promise<QueryResult>;
  clearResults: () => void;
  clearHistory: () => void;
  removeFromHistory: (id: string) => void;
}

const MAX_HISTORY_SIZE = 100;

const generateId = () => `query_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;

export const useQueryStore = create<QueryState>()(
  persist(
    (set, get) => ({
      // Initial state
      currentQuery: '',
      currentDatabase: 'default',
      isExecuting: false,
      lastResult: null,
      lastError: null,
      lastExecutionTime: null,
      queryHistory: [],

      // Set current query
      setCurrentQuery: (query) => {
        set({ currentQuery: query });
      },

      // Set current database
      setCurrentDatabase: (database) => {
        set({ currentDatabase: database });
      },

      // Execute the current query
      executeQuery: async () => {
        const { currentQuery, currentDatabase } = get();
        
        if (!currentQuery.trim()) {
          throw new Error('Query cannot be empty');
        }

        set({ isExecuting: true, lastError: null });
        const startTime = performance.now();

        try {
          const result = await fluxdb.query(currentDatabase, currentQuery);
          const duration = Math.round(performance.now() - startTime);
          
          // Calculate row count
          let rowCount = 0;
          if (result.results?.[0]?.series) {
            rowCount = result.results[0].series.reduce(
              (acc, s) => acc + (s.values?.length || 0),
              0
            );
          }

          // Add to history
          const execution: QueryExecution = {
            id: generateId(),
            query: currentQuery,
            database: currentDatabase,
            executedAt: new Date(),
            duration,
            rowCount,
            result,
            error: result.results?.[0]?.error,
          };

          set((state) => ({
            isExecuting: false,
            lastResult: result,
            lastExecutionTime: duration,
            lastError: result.results?.[0]?.error || null,
            queryHistory: [execution, ...state.queryHistory].slice(0, MAX_HISTORY_SIZE),
          }));

          return result;
        } catch (error) {
          const errorMessage = error instanceof Error ? error.message : 'Query failed';
          const duration = Math.round(performance.now() - startTime);

          // Add failed query to history
          const execution: QueryExecution = {
            id: generateId(),
            query: currentQuery,
            database: currentDatabase,
            executedAt: new Date(),
            duration,
            rowCount: 0,
            error: errorMessage,
          };

          set((state) => ({
            isExecuting: false,
            lastResult: null,
            lastExecutionTime: duration,
            lastError: errorMessage,
            queryHistory: [execution, ...state.queryHistory].slice(0, MAX_HISTORY_SIZE),
          }));

          throw error;
        }
      },

      // Execute a custom query
      executeCustomQuery: async (database, query) => {
        set({
          currentQuery: query,
          currentDatabase: database,
        });
        return get().executeQuery();
      },

      // Clear results
      clearResults: () => {
        set({
          lastResult: null,
          lastError: null,
          lastExecutionTime: null,
        });
      },

      // Clear history
      clearHistory: () => {
        set({ queryHistory: [] });
      },

      // Remove specific item from history
      removeFromHistory: (id) => {
        set((state) => ({
          queryHistory: state.queryHistory.filter((q) => q.id !== id),
        }));
      },
    }),
    {
      name: 'fluxdb-queries',
      partialize: (state) => ({
        queryHistory: state.queryHistory.slice(0, 50), // Only persist last 50 queries
        currentDatabase: state.currentDatabase,
      }),
    }
  )
);
