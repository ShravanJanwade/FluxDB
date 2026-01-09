// ConnectionModal Component - Modal for creating new connections
import { useState } from 'react';
import { X, Wifi, Loader2, Check, AlertCircle } from 'lucide-react';
import { useConnectionStore } from '../../stores/connectionStore';
import { useDatabaseStore } from '../../stores/databaseStore';

interface ConnectionModalProps {
  isOpen: boolean;
  onClose: () => void;
}

export function ConnectionModal({ isOpen, onClose }: ConnectionModalProps) {
  const [name, setName] = useState('');
  const [host, setHost] = useState('localhost');
  const [port, setPort] = useState('8086');
  const [testStatus, setTestStatus] = useState<'idle' | 'testing' | 'success' | 'error'>('idle');
  const [testMessage, setTestMessage] = useState('');
  
  const { addConnection, connect, isConnecting } = useConnectionStore();
  const { fetchDatabases, fetchStats } = useDatabaseStore();

  const handleTest = async () => {
    setTestStatus('testing');
    setTestMessage('');
    
    try {
      const { testConnection } = useConnectionStore.getState();
      const health = await testConnection(host, parseInt(port, 10));
      setTestStatus('success');
      setTestMessage(`Connected! FluxDB v${health.version} (${health.latency}ms)`);
    } catch (error) {
      setTestStatus('error');
      setTestMessage(error instanceof Error ? error.message : 'Connection failed');
    }
  };

  const handleConnect = async () => {
    // Add and connect
    const connectionName = name.trim() || `${host}:${port}`;
    
    addConnection({
      name: connectionName,
      host,
      port: parseInt(port, 10),
    });

    const state = useConnectionStore.getState();
    const newConnection = state.connections[state.connections.length - 1];
    
    try {
      await connect(newConnection.id);
      await Promise.all([fetchDatabases(), fetchStats()]);
      onClose();
      resetForm();
    } catch (error) {
      // Error is handled in the store
    }
  };

  const resetForm = () => {
    setName('');
    setHost('localhost');
    setPort('8086');
    setTestStatus('idle');
    setTestMessage('');
  };

  const handleClose = () => {
    onClose();
    resetForm();
  };

  if (!isOpen) return null;

  return (
    <div className="modal-overlay" onClick={handleClose}>
      <div className="modal" onClick={(e) => e.stopPropagation()}>
        <div className="modal-header">
          <h2>
            <Wifi size={20} style={{ marginRight: 8 }} />
            New Connection
          </h2>
          <button className="btn-icon" onClick={handleClose}>
            <X size={20} />
          </button>
        </div>

        <div className="modal-body">
          <div className="form-group">
            <label className="form-label">Connection Name (optional)</label>
            <input
              type="text"
              className="form-input"
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="My FluxDB Server"
            />
          </div>

          <div className="form-row">
            <div className="form-group">
              <label className="form-label">Host</label>
              <input
                type="text"
                className="form-input"
                value={host}
                onChange={(e) => setHost(e.target.value)}
                placeholder="localhost"
              />
            </div>
            <div className="form-group" style={{ maxWidth: '120px' }}>
              <label className="form-label">Port</label>
              <input
                type="number"
                className="form-input"
                value={port}
                onChange={(e) => setPort(e.target.value)}
                placeholder="8086"
              />
            </div>
          </div>

          {testStatus !== 'idle' && (
            <div className={`connection-test-result ${testStatus}`}>
              {testStatus === 'testing' && (
                <>
                  <Loader2 size={16} className="spin" />
                  <span>Testing connection...</span>
                </>
              )}
              {testStatus === 'success' && (
                <>
                  <Check size={16} />
                  <span>{testMessage}</span>
                </>
              )}
              {testStatus === 'error' && (
                <>
                  <AlertCircle size={16} />
                  <span>{testMessage}</span>
                </>
              )}
            </div>
          )}
        </div>

        <div className="modal-footer">
          <button 
            className="btn-secondary" 
            onClick={handleTest}
            disabled={testStatus === 'testing' || !host || !port}
          >
            {testStatus === 'testing' ? (
              <>
                <Loader2 size={14} className="spin" />
                Testing...
              </>
            ) : (
              'Test Connection'
            )}
          </button>
          <button 
            className="btn-primary"
            onClick={handleConnect}
            disabled={isConnecting || !host || !port}
          >
            {isConnecting ? (
              <>
                <Loader2 size={14} className="spin" />
                Connecting...
              </>
            ) : (
              'Connect'
            )}
          </button>
        </div>
      </div>
    </div>
  );
}
