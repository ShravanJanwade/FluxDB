// StatusBar Component - Bottom status bar
import { useConnectionStore } from '../../stores/connectionStore';
import { useQueryStore } from '../../stores/queryStore';
import { useDatabaseStore } from '../../stores/databaseStore';
import { useEffect, useState } from 'react';

export function StatusBar() {
  const { activeConnection, serverVersion } = useConnectionStore();
  const { lastExecutionTime, lastResult, lastError } = useQueryStore();
  const { stats } = useDatabaseStore();
  const [time, setTime] = useState(new Date());

  useEffect(() => {
    const timer = setInterval(() => setTime(new Date()), 1000);
    return () => clearInterval(timer);
  }, []);

  const getRowCount = () => {
    if (!lastResult?.results?.[0]?.series) return 0;
    return lastResult.results[0].series.reduce(
      (acc, s) => acc + (s.values?.length || 0),
      0
    );
  };

  return (
    <footer className="statusbar">
      <div className="statusbar-left">
        <div className="status-indicator">
          <span 
            className={`status-dot ${activeConnection ? '' : 'disconnected'}`} 
          />
          <span>
            {activeConnection 
              ? `${activeConnection.host}:${activeConnection.port}` 
              : 'Disconnected'}
          </span>
        </div>

        {serverVersion && (
          <span className="status-version">
            FluxDB v{serverVersion}
          </span>
        )}

        {stats && (
          <span className="status-stats">
            {stats.database_count} database{stats.database_count !== 1 ? 's' : ''} • {' '}
            {formatNumber(stats.total_entries)} entries • {' '}
            {formatBytes(stats.total_size_bytes)}
          </span>
        )}
      </div>

      <div className="statusbar-center">
        {lastExecutionTime !== null && !lastError && (
          <span className="status-execution">
            Query completed in {lastExecutionTime}ms • {getRowCount()} rows
          </span>
        )}
        {lastError && (
          <span className="status-error">
            Error: {lastError}
          </span>
        )}
      </div>

      <div className="statusbar-right">
        <span className="status-time">
          {time.toLocaleTimeString()}
        </span>
      </div>
    </footer>
  );
}

function formatNumber(num: number): string {
  if (num >= 1_000_000) {
    return (num / 1_000_000).toFixed(1) + 'M';
  }
  if (num >= 1_000) {
    return (num / 1_000).toFixed(1) + 'K';
  }
  return num.toString();
}

function formatBytes(bytes: number): string {
  if (bytes >= 1_073_741_824) {
    return (bytes / 1_073_741_824).toFixed(2) + ' GB';
  }
  if (bytes >= 1_048_576) {
    return (bytes / 1_048_576).toFixed(2) + ' MB';
  }
  if (bytes >= 1_024) {
    return (bytes / 1_024).toFixed(2) + ' KB';
  }
  return bytes + ' B';
}
