// ConnectionList Component - List of saved connections
import { 
  Wifi, 
  WifiOff, 
  Trash2, 
  Check 
} from 'lucide-react';
import { useConnectionStore } from '../../stores/connectionStore';
import { useDatabaseStore } from '../../stores/databaseStore';
import { Connection } from '../../types';
import { useState } from 'react';

export function ConnectionList() {
  const { 
    connections, 
    connect, 
    disconnect, 
    removeConnection 
  } = useConnectionStore();
  const { fetchDatabases, fetchStats } = useDatabaseStore();
  const [loadingId, setLoadingId] = useState<string | null>(null);

  const handleConnect = async (connection: Connection) => {
    if (connection.isConnected) {
      disconnect();
      return;
    }

    setLoadingId(connection.id);
    try {
      await connect(connection.id);
      await Promise.all([fetchDatabases(), fetchStats()]);
    } catch (error) {
      // Error handled in store
    } finally {
      setLoadingId(null);
    }
  };

  const handleDelete = (id: string, e: React.MouseEvent) => {
    e.stopPropagation();
    if (confirm('Are you sure you want to delete this connection?')) {
      removeConnection(id);
    }
  };

  if (connections.length === 0) {
    return (
      <div className="connection-list-empty">
        <WifiOff size={32} />
        <p>No saved connections</p>
        <span>Create a new connection to get started</span>
      </div>
    );
  }

  return (
    <div className="connection-list">
      <h3 className="connection-list-title">Saved Connections</h3>
      
      {connections.map((connection) => (
        <div
          key={connection.id}
          className={`connection-item ${connection.isConnected ? 'connected' : ''}`}
          onClick={() => handleConnect(connection)}
        >
          <div className="connection-item-icon">
            {connection.isConnected ? (
              <Wifi size={18} className="icon-connected" />
            ) : (
              <WifiOff size={18} className="icon-disconnected" />
            )}
          </div>

          <div className="connection-item-info">
            <div className="connection-item-name">{connection.name}</div>
            <div className="connection-item-host">
              {connection.host}:{connection.port}
            </div>
          </div>

          <div className="connection-item-status">
            {loadingId === connection.id ? (
              <span className="loading-spinner" />
            ) : connection.isConnected ? (
              <Check size={16} className="icon-connected" />
            ) : null}
          </div>

          <div className="connection-item-actions">
            <button 
              className="btn-icon-sm"
              onClick={(e) => handleDelete(connection.id, e)}
              title="Delete"
            >
              <Trash2 size={14} />
            </button>
          </div>
        </div>
      ))}
    </div>
  );
}
