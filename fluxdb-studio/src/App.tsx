// FluxDB Studio - Main Application
import { useState, useEffect } from 'react';
import { Sidebar } from './components/Layout/Sidebar';
import { Navbar } from './components/Layout/Navbar';
import { StatusBar } from './components/Layout/StatusBar';
import { QueryEditor } from './components/QueryEditor/QueryEditor';
import { ResultsPanel } from './components/QueryEditor/ResultsPanel';
import { QueryHistory } from './components/QueryEditor/QueryHistory';
import { DatabaseExplorer } from './components/Explorer/DatabaseExplorer';
import { ConnectionModal } from './components/Connection/ConnectionModal';
import { ConnectionList } from './components/Connection/ConnectionList';
import { Dashboard } from './components/Dashboard/Dashboard';
import { ImportModal } from './components/DataIO/ImportModal';
import { ExportModal } from './components/DataIO/ExportModal';
import { ManualInsert } from './components/DataIO/ManualInsert';
import { WritePanel } from './components/DataIO/WritePanel';
import { UpdatePanel } from './components/DataIO/UpdatePanel';
import { DeletePanel } from './components/DataIO/DeletePanel';
import { useConnectionStore } from './stores/connectionStore';
import { useDatabaseStore } from './stores/databaseStore';
import { ViewType } from './types';

export default function App() {
  const [currentView, setCurrentView] = useState<ViewType>('query');
  const [isConnectionModalOpen, setIsConnectionModalOpen] = useState(false);
  const [isImportModalOpen, setIsImportModalOpen] = useState(false);
  const [isExportModalOpen, setIsExportModalOpen] = useState(false);
  const [isHistoryOpen, setIsHistoryOpen] = useState(false);
  
  const { activeConnection } = useConnectionStore();
  const { fetchDatabases, fetchStats } = useDatabaseStore();

  // Fetch data when connected
  useEffect(() => {
    if (activeConnection) {
      fetchDatabases();
      fetchStats();
    }
  }, [activeConnection]);

  // Handle view changes for import/export
  useEffect(() => {
    if (currentView === 'import' && activeConnection) {
      setIsImportModalOpen(true);
      setCurrentView('query'); // Reset to query view
    } else if (currentView === 'export' && activeConnection) {
      setIsExportModalOpen(true);
      setCurrentView('query');
    }
  }, [currentView, activeConnection]);

  const renderMainContent = () => {
    if (!activeConnection) {
      return (
        <div className="no-connection">
          <div className="no-connection-content">
            <div className="flux-logo-container">
              <div className="flux-logo">⚡</div>
              <div className="flux-logo-glow" />
            </div>
            <h1>FluxDB Studio</h1>
            <p>A powerful time-series database management tool</p>
            <button 
              className="btn-primary btn-lg"
              onClick={() => setIsConnectionModalOpen(true)}
            >
              New Connection
            </button>
            
            <ConnectionList />
          </div>
        </div>
      );
    }

    switch (currentView) {
      case 'query':
        return (
          <div className="query-view">
            <div className="query-panel">
              <QueryEditor />
            </div>
            <div className="results-panel">
              <ResultsPanel />
            </div>
            <QueryHistory 
              isOpen={isHistoryOpen} 
              onClose={() => setIsHistoryOpen(false)} 
            />
          </div>
        );

      case 'dashboard':
        return <Dashboard />;

      case 'explorer':
        return <DatabaseExplorer />;

      case 'import':
        return (
          <div className="import-view">
            <ManualInsert />
          </div>
        );

      case 'write':
        return (
          <div className="write-view">
            <WritePanel />
          </div>
        );

      case 'update':
        return (
          <div className="update-view">
            <UpdatePanel />
          </div>
        );

      case 'delete':
        return (
          <div className="delete-view">
            <DeletePanel />
          </div>
        );

      case 'export':
        return null; // Handled by modal

      case 'settings':
        return (
          <div className="settings-view">
            <div className="settings-content">
              <h2>Settings</h2>
              <div className="settings-section">
                <h3>Connection</h3>
                <ConnectionList />
              </div>
            </div>
          </div>
        );

      default:
        return null;
    }
  };

  return (
    <div className="app">
      <Navbar onNewConnection={() => setIsConnectionModalOpen(true)} />
      
      <div className="app-content">
        <Sidebar 
          currentView={currentView} 
          onViewChange={setCurrentView}
        />
        
        <main className="main-content">
          {renderMainContent()}
        </main>
      </div>
      
      <StatusBar />
      
      {/* Modals */}
      <ConnectionModal 
        isOpen={isConnectionModalOpen}
        onClose={() => setIsConnectionModalOpen(false)}
      />
      
      <ImportModal
        isOpen={isImportModalOpen}
        onClose={() => setIsImportModalOpen(false)}
      />
      
      <ExportModal
        isOpen={isExportModalOpen}
        onClose={() => setIsExportModalOpen(false)}
      />
    </div>
  );
}
