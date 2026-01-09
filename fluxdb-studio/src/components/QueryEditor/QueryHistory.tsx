// QueryHistory Component - Display query history
import { useState } from 'react';
import { 
  History, 
  Trash2, 
  Play, 
  Copy, 
  Check, 
  AlertCircle,
  Clock,
  Search,
  X
} from 'lucide-react';
import { useQueryStore } from '../../stores/queryStore';
import { QueryExecution } from '../../types';

interface QueryHistoryProps {
  isOpen: boolean;
  onClose: () => void;
}

export function QueryHistory({ isOpen, onClose }: QueryHistoryProps) {
  const { queryHistory, setCurrentQuery, setCurrentDatabase, removeFromHistory, clearHistory } = useQueryStore();
  const [search, setSearch] = useState('');
  const [copiedId, setCopiedId] = useState<string | null>(null);

  if (!isOpen) return null;

  const filteredHistory = queryHistory.filter((item) =>
    item.query.toLowerCase().includes(search.toLowerCase()) ||
    item.database.toLowerCase().includes(search.toLowerCase())
  );

  const handleUseQuery = (item: QueryExecution) => {
    setCurrentQuery(item.query);
    setCurrentDatabase(item.database);
    onClose();
  };

  const handleCopy = async (item: QueryExecution) => {
    await navigator.clipboard.writeText(item.query);
    setCopiedId(item.id);
    setTimeout(() => setCopiedId(null), 2000);
  };

  const formatTime = (date: Date) => {
    const now = new Date();
    const diff = now.getTime() - new Date(date).getTime();
    
    if (diff < 60000) return 'Just now';
    if (diff < 3600000) return `${Math.floor(diff / 60000)}m ago`;
    if (diff < 86400000) return `${Math.floor(diff / 3600000)}h ago`;
    return new Date(date).toLocaleDateString();
  };

  return (
    <div className="query-history-panel">
      <div className="history-header">
        <div className="history-title">
          <History size={18} />
          <h3>Query History</h3>
        </div>
        <button className="btn-icon" onClick={onClose}>
          <X size={18} />
        </button>
      </div>

      <div className="history-search">
        <Search size={14} />
        <input
          type="text"
          placeholder="Search queries..."
          value={search}
          onChange={(e) => setSearch(e.target.value)}
        />
        {search && (
          <button className="btn-icon-sm" onClick={() => setSearch('')}>
            <X size={12} />
          </button>
        )}
      </div>

      {queryHistory.length > 0 && (
        <div className="history-actions">
          <button 
            className="btn-secondary btn-sm"
            onClick={() => {
              if (confirm('Clear all query history?')) {
                clearHistory();
              }
            }}
          >
            <Trash2 size={14} />
            Clear All
          </button>
        </div>
      )}

      <div className="history-list">
        {filteredHistory.length === 0 ? (
          <div className="history-empty">
            <History size={32} />
            <p>{queryHistory.length === 0 ? 'No query history' : 'No matching queries'}</p>
          </div>
        ) : (
          filteredHistory.map((item) => (
            <div 
              key={item.id} 
              className={`history-item ${item.error ? 'has-error' : ''}`}
            >
              <div className="history-item-header">
                <div className="history-item-meta">
                  <span className="history-database">{item.database}</span>
                  <span className="history-time">
                    <Clock size={12} />
                    {formatTime(item.executedAt)}
                  </span>
                </div>
                <div className="history-item-stats">
                  {item.error ? (
                    <span className="history-error">
                      <AlertCircle size={12} />
                      Error
                    </span>
                  ) : (
                    <>
                      <span>{item.rowCount} rows</span>
                      <span>{item.duration}ms</span>
                    </>
                  )}
                </div>
              </div>

              <pre className="history-query">{item.query}</pre>

              <div className="history-item-actions">
                <button 
                  className="btn-icon-sm"
                  onClick={() => handleUseQuery(item)}
                  title="Use this query"
                >
                  <Play size={14} />
                </button>
                <button 
                  className="btn-icon-sm"
                  onClick={() => handleCopy(item)}
                  title="Copy query"
                >
                  {copiedId === item.id ? <Check size={14} /> : <Copy size={14} />}
                </button>
                <button 
                  className="btn-icon-sm"
                  onClick={() => removeFromHistory(item.id)}
                  title="Remove from history"
                >
                  <Trash2 size={14} />
                </button>
              </div>
            </div>
          ))
        )}
      </div>
    </div>
  );
}
