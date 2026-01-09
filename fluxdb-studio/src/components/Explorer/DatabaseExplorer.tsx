// DatabaseExplorer Component - Tree view for exploring databases
import { useState, useEffect } from 'react';
import { 
  Database, 
  ChevronRight, 
  ChevronDown, 
  Table2, 
  RefreshCw,
  Plus,
  Trash2,
  Loader2
} from 'lucide-react';
import { useDatabaseStore } from '../../stores/databaseStore';
import { useConnectionStore } from '../../stores/connectionStore';
import { useQueryStore } from '../../stores/queryStore';

interface TreeNodeState {
  [key: string]: boolean;
}

export function DatabaseExplorer() {
  const { databases, selectedDatabase, selectDatabase, fetchDatabases, createDatabase, dropDatabase, isLoading } = useDatabaseStore();
  const { activeConnection } = useConnectionStore();
  const { setCurrentDatabase, setCurrentQuery } = useQueryStore();
  const [expanded, setExpanded] = useState<TreeNodeState>({});
  const [showCreateModal, setShowCreateModal] = useState(false);
  const [newDbName, setNewDbName] = useState('');
  const [contextMenu, setContextMenu] = useState<{ x: number; y: number; db: string } | null>(null);

  useEffect(() => {
    if (activeConnection) {
      fetchDatabases();
    }
  }, [activeConnection]);

  const toggleExpand = (db: string) => {
    setExpanded((prev) => ({ ...prev, [db]: !prev[db] }));
  };

  const handleSelect = (db: string) => {
    selectDatabase(db);
    setCurrentDatabase(db);
  };

  const handleRefresh = async () => {
    await fetchDatabases();
  };

  const handleCreateDatabase = async () => {
    if (!newDbName.trim()) return;
    
    try {
      await createDatabase(newDbName.trim());
      setNewDbName('');
      setShowCreateModal(false);
    } catch (error) {
      // Error handled in store
    }
  };

  const handleDropDatabase = async (name: string) => {
    if (!confirm(`Are you sure you want to drop database "${name}"? This action cannot be undone.`)) {
      return;
    }
    
    try {
      await dropDatabase(name);
    } catch (error) {
      // Error handled in store
    }
  };

  const handleContextMenu = (e: React.MouseEvent, db: string) => {
    e.preventDefault();
    setContextMenu({ x: e.clientX, y: e.clientY, db });
  };

  const closeContextMenu = () => {
    setContextMenu(null);
  };

  const handleQueryFromExplorer = (db: string) => {
    setCurrentDatabase(db);
    setCurrentQuery(`SELECT * FROM ${db} LIMIT 100`);
  };

  if (!activeConnection) {
    return (
      <div className="database-explorer">
        <div className="explorer-empty">
          <Database size={32} />
          <p>Not connected</p>
          <span>Connect to a FluxDB server to explore databases</span>
        </div>
      </div>
    );
  }

  return (
    <div className="database-explorer" onClick={closeContextMenu}>
      <div className="explorer-header">
        <h3>
          <Database size={16} />
          Databases
        </h3>
        <div className="explorer-actions">
          <button 
            className="btn-icon"
            onClick={() => setShowCreateModal(true)}
            title="Create Database"
          >
            <Plus size={16} />
          </button>
          <button 
            className="btn-icon"
            onClick={handleRefresh}
            disabled={isLoading}
            title="Refresh"
          >
            <RefreshCw size={16} className={isLoading ? 'spin' : ''} />
          </button>
        </div>
      </div>

      <div className="explorer-tree">
        {isLoading && databases.length === 0 ? (
          <div className="explorer-loading">
            <Loader2 size={20} className="spin" />
            <span>Loading databases...</span>
          </div>
        ) : databases.length === 0 ? (
          <div className="explorer-empty-list">
            <p>No databases found</p>
            <button 
              className="btn-secondary btn-sm"
              onClick={() => setShowCreateModal(true)}
            >
              <Plus size={14} />
              Create Database
            </button>
          </div>
        ) : (
          databases.map((db) => (
            <div key={db} className="tree-node">
              <div 
                className={`tree-item ${selectedDatabase === db ? 'selected' : ''}`}
                onClick={() => handleSelect(db)}
                onDoubleClick={() => handleQueryFromExplorer(db)}
                onContextMenu={(e) => handleContextMenu(e, db)}
              >
                <button 
                  className="tree-expand"
                  onClick={(e) => {
                    e.stopPropagation();
                    toggleExpand(db);
                  }}
                >
                  {expanded[db] ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
                </button>
                <Database size={16} className="tree-icon" />
                <span className="tree-label">{db}</span>
              </div>
              
              {expanded[db] && (
                <div className="tree-children">
                  <div className="tree-item tree-child">
                    <Table2 size={14} className="tree-icon" />
                    <span className="tree-label muted">Measurements</span>
                  </div>
                </div>
              )}
            </div>
          ))
        )}
      </div>

      {/* Context Menu */}
      {contextMenu && (
        <div 
          className="context-menu"
          style={{ top: contextMenu.y, left: contextMenu.x }}
        >
          <button 
            className="context-menu-item"
            onClick={() => {
              handleQueryFromExplorer(contextMenu.db);
              closeContextMenu();
            }}
          >
            Query Database
          </button>
          <button 
            className="context-menu-item danger"
            onClick={() => {
              handleDropDatabase(contextMenu.db);
              closeContextMenu();
            }}
          >
            <Trash2 size={14} />
            Drop Database
          </button>
        </div>
      )}

      {/* Create Database Modal */}
      {showCreateModal && (
        <div className="modal-overlay" onClick={() => setShowCreateModal(false)}>
          <div className="modal modal-sm" onClick={(e) => e.stopPropagation()}>
            <div className="modal-header">
              <h2>Create Database</h2>
            </div>
            <div className="modal-body">
              <div className="form-group">
                <label className="form-label">Database Name</label>
                <input
                  type="text"
                  className="form-input"
                  value={newDbName}
                  onChange={(e) => setNewDbName(e.target.value)}
                  placeholder="my_database"
                  autoFocus
                  onKeyDown={(e) => {
                    if (e.key === 'Enter') handleCreateDatabase();
                  }}
                />
              </div>
            </div>
            <div className="modal-footer">
              <button 
                className="btn-secondary" 
                onClick={() => setShowCreateModal(false)}
              >
                Cancel
              </button>
              <button 
                className="btn-primary"
                onClick={handleCreateDatabase}
                disabled={!newDbName.trim()}
              >
                Create
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
