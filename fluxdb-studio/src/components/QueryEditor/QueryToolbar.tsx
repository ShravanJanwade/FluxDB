// QueryToolbar Component - Toolbar for query editor
import { 
  Play, 
  Sparkles, 
  Trash2, 
  History, 
  Download,
  Clock,
  Database
} from 'lucide-react';
import { useQueryStore } from '../../stores/queryStore';
import { useDatabaseStore } from '../../stores/databaseStore';
import { useConnectionStore } from '../../stores/connectionStore';

interface QueryToolbarProps {
  onShowHistory?: () => void;
}

export function QueryToolbar({ onShowHistory }: QueryToolbarProps) {
  const { 
    currentQuery, 
    currentDatabase, 
    setCurrentDatabase, 
    executeQuery, 
    clearResults,
    isExecuting,
    lastExecutionTime 
  } = useQueryStore();
  const { databases } = useDatabaseStore();
  const { activeConnection } = useConnectionStore();

  const handleExecute = async () => {
    if (!currentQuery.trim() || isExecuting) return;
    
    try {
      await executeQuery();
    } catch (error) {
      // Error handled in store
    }
  };

  const handleClear = () => {
    clearResults();
  };

  const handleFormat = () => {
    // Basic SQL formatting
    const formatted = currentQuery
      .replace(/\s+/g, ' ')
      .replace(/\s*,\s*/g, ', ')
      .replace(/\s*(SELECT|FROM|WHERE|GROUP BY|ORDER BY|LIMIT|AND|OR)\s*/gi, '\n$1 ')
      .trim();
    
    useQueryStore.getState().setCurrentQuery(formatted);
  };

  return (
    <div className="query-toolbar">
      <div className="query-toolbar-left">
        <button 
          className="btn-primary btn-execute"
          onClick={handleExecute}
          disabled={!activeConnection || isExecuting || !currentQuery.trim()}
          title="Execute Query (Ctrl+Enter)"
        >
          <Play size={16} />
          <span>Run</span>
        </button>

        <button 
          className="btn-secondary"
          onClick={handleFormat}
          disabled={!currentQuery.trim()}
          title="Format Query"
        >
          <Sparkles size={16} />
        </button>

        <button 
          className="btn-secondary"
          onClick={handleClear}
          title="Clear Results"
        >
          <Trash2 size={16} />
        </button>

        <button 
          className="btn-secondary"
          onClick={onShowHistory}
          title="Query History"
        >
          <History size={16} />
        </button>

        <div className="toolbar-divider" />

        <div className="database-selector">
          <Database size={14} />
          <select 
            value={currentDatabase}
            onChange={(e) => setCurrentDatabase(e.target.value)}
            disabled={!activeConnection}
          >
            {databases.length === 0 ? (
              <option value="default">default</option>
            ) : (
              databases.map((db) => (
                <option key={db} value={db}>{db}</option>
              ))
            )}
          </select>
        </div>
      </div>

      <div className="query-toolbar-right">
        {lastExecutionTime !== null && (
          <div className="execution-time">
            <Clock size={14} />
            <span>{lastExecutionTime}ms</span>
          </div>
        )}
        
        <button 
          className="btn-icon"
          title="Export Results"
        >
          <Download size={16} />
        </button>
      </div>
    </div>
  );
}
