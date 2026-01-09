// useConnection hook - Connection state and health monitoring
import { useState, useCallback, useEffect, useRef } from 'react';
import { fluxdb } from '../services/fluxdb';
import { HealthStatus } from '../types';

export interface ConnectionConfig {
  host: string;
  port: number;
  name?: string;
}

export interface ConnectionStatus {
  isConnected: boolean;
  isConnecting: boolean;
  latency: number | null;
  lastChecked: Date | null;
  serverVersion?: string;
}

export interface UseConnectionOptions {
  healthCheckInterval?: number; // ms
  autoConnect?: boolean;
  onConnect?: () => void;
  onDisconnect?: () => void;
  onError?: (error: Error) => void;
}

export function useConnection(options: UseConnectionOptions = {}) {
  const {
    healthCheckInterval = 30000, // 30 seconds default
    autoConnect = false,
    onConnect,
    onDisconnect,
    onError,
  } = options;

  const [config, setConfig] = useState<ConnectionConfig | null>(null);
  const [status, setStatus] = useState<ConnectionStatus>({
    isConnected: false,
    isConnecting: false,
    latency: null,
    lastChecked: null,
  });
  const [error, setError] = useState<Error | null>(null);
  const healthCheckRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const wasConnectedRef = useRef(false);

  // Connect to server
  const connect = useCallback(async (newConfig: ConnectionConfig) => {
    setStatus(prev => ({ ...prev, isConnecting: true }));
    setError(null);

    try {
      const result = await fluxdb.testConnection(newConfig.host, newConfig.port);

      if (result.status === 'ok') {
        setConfig(newConfig);
        setStatus({
          isConnected: true,
          isConnecting: false,
          latency: result.latency || null,
          lastChecked: new Date(),
          serverVersion: result.version,
        });
        wasConnectedRef.current = true;
        onConnect?.();
        return true;
      } else {
        throw new Error('Connection failed');
      }
    } catch (err: any) {
      const error = new Error(err.message || 'Failed to connect');
      setError(error);
      setStatus({
        isConnected: false,
        isConnecting: false,
        latency: null,
        lastChecked: new Date(),
      });
      onError?.(error);
      return false;
    }
  }, [onConnect, onError]);

  // Disconnect
  const disconnect = useCallback(() => {
    if (healthCheckRef.current) {
      clearInterval(healthCheckRef.current);
      healthCheckRef.current = null;
    }
    setConfig(null);
    setStatus({
      isConnected: false,
      isConnecting: false,
      latency: null,
      lastChecked: new Date(),
    });
    if (wasConnectedRef.current) {
      wasConnectedRef.current = false;
      onDisconnect?.();
    }
  }, [onDisconnect]);

  // Health check
  const checkHealth = useCallback(async () => {
    if (!config) return false;

    try {
      const result = await fluxdb.getHealth();
      const isHealthy = result.status === 'ok';

      setStatus(prev => ({
        ...prev,
        isConnected: isHealthy,
        latency: result.latency || null,
        lastChecked: new Date(),
      }));

      if (!isHealthy && wasConnectedRef.current) {
        wasConnectedRef.current = false;
        onDisconnect?.();
      } else if (isHealthy && !wasConnectedRef.current) {
        wasConnectedRef.current = true;
        onConnect?.();
      }

      return isHealthy;
    } catch (err) {
      setStatus(prev => ({
        ...prev,
        isConnected: false,
        lastChecked: new Date(),
      }));

      if (wasConnectedRef.current) {
        wasConnectedRef.current = false;
        onDisconnect?.();
      }

      return false;
    }
  }, [config, onConnect, onDisconnect]);

  // Set up periodic health checks
  useEffect(() => {
    if (status.isConnected && healthCheckInterval > 0) {
      healthCheckRef.current = setInterval(checkHealth, healthCheckInterval);

      return () => {
        if (healthCheckRef.current) {
          clearInterval(healthCheckRef.current);
        }
      };
    }
  }, [status.isConnected, healthCheckInterval, checkHealth]);

  // Auto-connect on mount if enabled
  useEffect(() => {
    if (autoConnect) {
      // Try to connect to default localhost
      connect({ host: 'localhost', port: 8086, name: 'Local FluxDB' });
    }
  }, [autoConnect, connect]);

  return {
    config,
    status,
    error,
    connect,
    disconnect,
    checkHealth,
    isConnected: status.isConnected,
    isConnecting: status.isConnecting,
    latency: status.latency,
  };
}

export default useConnection;
