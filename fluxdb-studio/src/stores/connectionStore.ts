// Connection Store - Zustand state management for connections
import { create } from 'zustand';
import { persist } from 'zustand/middleware';
import { Connection, HealthStatus } from '../types';
import { fluxdb } from '../services/fluxdb';

interface ConnectionState {
  // State
  connections: Connection[];
  activeConnection: Connection | null;
  isConnecting: boolean;
  connectionError: string | null;
  serverVersion: string | null;
  
  // Actions
  addConnection: (connection: Omit<Connection, 'id' | 'isConnected'>) => void;
  removeConnection: (id: string) => void;
  updateConnection: (id: string, updates: Partial<Connection>) => void;
  connect: (id: string) => Promise<HealthStatus>;
  disconnect: () => void;
  testConnection: (host: string, port: number) => Promise<HealthStatus>;
  setActiveConnection: (connection: Connection | null) => void;
  clearError: () => void;
}

const generateId = () => `conn_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;

export const useConnectionStore = create<ConnectionState>()(
  persist(
    (set, get) => ({
      // Initial state
      connections: [],
      activeConnection: null,
      isConnecting: false,
      connectionError: null,
      serverVersion: null,

      // Add a new connection
      addConnection: (connection) => {
        const newConnection: Connection = {
          ...connection,
          id: generateId(),
          isConnected: false,
        };
        set((state) => ({
          connections: [...state.connections, newConnection],
        }));
      },

      // Remove a connection
      removeConnection: (id) => {
        set((state) => ({
          connections: state.connections.filter((c) => c.id !== id),
          activeConnection:
            state.activeConnection?.id === id ? null : state.activeConnection,
        }));
      },

      // Update a connection
      updateConnection: (id, updates) => {
        set((state) => ({
          connections: state.connections.map((c) =>
            c.id === id ? { ...c, ...updates } : c
          ),
          activeConnection:
            state.activeConnection?.id === id
              ? { ...state.activeConnection, ...updates }
              : state.activeConnection,
        }));
      },

      // Connect to a server
      connect: async (id) => {
        const state = get();
        const connection = state.connections.find((c) => c.id === id);
        
        if (!connection) {
          throw new Error('Connection not found');
        }

        set({ isConnecting: true, connectionError: null });

        try {
          const health = await fluxdb.testConnection(connection.host, connection.port);
          
          const updatedConnection: Connection = {
            ...connection,
            isConnected: true,
            lastConnected: new Date(),
          };

          set((state) => ({
            isConnecting: false,
            activeConnection: updatedConnection,
            serverVersion: health.version,
            connections: state.connections.map((c) =>
              c.id === id ? updatedConnection : { ...c, isConnected: false }
            ),
          }));

          return health;
        } catch (error) {
          const message = error instanceof Error ? error.message : 'Connection failed';
          set({
            isConnecting: false,
            connectionError: message,
          });
          throw error;
        }
      },

      // Disconnect from the current server
      disconnect: () => {
        const state = get();
        if (state.activeConnection) {
          set((state) => ({
            activeConnection: null,
            serverVersion: null,
            connections: state.connections.map((c) =>
              c.id === state.activeConnection?.id
                ? { ...c, isConnected: false }
                : c
            ),
          }));
        }
      },

      // Test a connection without persisting
      testConnection: async (host, port) => {
        set({ isConnecting: true, connectionError: null });
        
        try {
          const health = await fluxdb.testConnection(host, port);
          set({ isConnecting: false });
          return health;
        } catch (error) {
          const message = error instanceof Error ? error.message : 'Connection failed';
          set({
            isConnecting: false,
            connectionError: message,
          });
          throw error;
        }
      },

      // Set active connection directly
      setActiveConnection: (connection) => {
        if (connection) {
          fluxdb.setBaseUrl(connection.host, connection.port);
        }
        set({ activeConnection: connection });
      },

      // Clear error message
      clearError: () => {
        set({ connectionError: null });
      },
    }),
    {
      name: 'fluxdb-connections',
      partialize: (state) => ({
        connections: state.connections.map((c) => ({
          ...c,
          isConnected: false, // Don't persist connection status
        })),
      }),
    }
  )
);
