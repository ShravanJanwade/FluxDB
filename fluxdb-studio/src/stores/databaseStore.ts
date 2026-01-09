// Database Store - Zustand state management for database explorer
import { create } from 'zustand';
import { ServerStats } from '../types';
import { fluxdb } from '../services/fluxdb';

interface DatabaseState {
  // State
  databases: string[];
  selectedDatabase: string | null;
  isLoading: boolean;
  error: string | null;
  stats: ServerStats | null;
  
  // Actions
  fetchDatabases: () => Promise<void>;
  createDatabase: (name: string) => Promise<void>;
  dropDatabase: (name: string) => Promise<void>;
  selectDatabase: (name: string | null) => void;
  fetchStats: () => Promise<void>;
  refresh: () => Promise<void>;
}

export const useDatabaseStore = create<DatabaseState>((set, get) => ({
  // Initial state
  databases: [],
  selectedDatabase: null,
  isLoading: false,
  error: null,
  stats: null,

  // Fetch all databases
  fetchDatabases: async () => {
    set({ isLoading: true, error: null });
    
    try {
      const databases = await fluxdb.listDatabases();
      set({ databases, isLoading: false });
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Failed to fetch databases';
      set({ error: message, isLoading: false });
      throw error;
    }
  },

  // Create a new database
  createDatabase: async (name) => {
    set({ isLoading: true, error: null });
    
    try {
      await fluxdb.createDatabase(name);
      // Refresh the list
      const databases = await fluxdb.listDatabases();
      set({
        databases,
        isLoading: false,
        selectedDatabase: name,
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Failed to create database';
      set({ error: message, isLoading: false });
      throw error;
    }
  },

  // Drop a database
  dropDatabase: async (name) => {
    set({ isLoading: true, error: null });
    
    try {
      await fluxdb.dropDatabase(name);
      // Refresh the list
      const databases = await fluxdb.listDatabases();
      const { selectedDatabase } = get();
      
      set({
        databases,
        isLoading: false,
        selectedDatabase: selectedDatabase === name ? null : selectedDatabase,
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Failed to drop database';
      set({ error: message, isLoading: false });
      throw error;
    }
  },

  // Select a database
  selectDatabase: (name) => {
    set({ selectedDatabase: name });
  },

  // Fetch server stats
  fetchStats: async () => {
    try {
      const stats = await fluxdb.getStats();
      set({ stats });
    } catch (error) {
      console.error('Failed to fetch stats:', error);
    }
  },

  // Refresh all data
  refresh: async () => {
    const { fetchDatabases, fetchStats } = get();
    await Promise.all([fetchDatabases(), fetchStats()]);
  },
}));
