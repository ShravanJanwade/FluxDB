// Navbar Component - Top navigation bar
import { 
  Zap, 
  Settings, 
  Plus, 
  Wifi, 
  WifiOff,
  Moon,
  Sun,
  Minus,
  Square,
  X
} from 'lucide-react';
import { useConnectionStore } from '../../stores/connectionStore';
import { useState } from 'react';

interface NavbarProps {
  onNewConnection: () => void;
}

export function Navbar({ onNewConnection }: NavbarProps) {
  const { activeConnection, serverVersion } = useConnectionStore();
  const [isDark, setIsDark] = useState(true);

  const handleMinimize = () => {
    // Electron minimize
    window.electronAPI?.minimize?.();
  };

  const handleMaximize = () => {
    // Electron maximize
    window.electronAPI?.maximize?.();
  };

  const handleClose = () => {
    // Electron close
    window.electronAPI?.close?.();
  };

  return (
    <nav className="navbar">
      <div className="navbar-left">
        <div className="navbar-logo">
          <Zap size={24} />
          <span className="navbar-title">FluxDB Studio</span>
        </div>
        
        {activeConnection && (
          <div className="navbar-connection">
            <div className="connection-badge">
              <Wifi size={14} className="connection-icon connected" />
              <span>{activeConnection.name}</span>
              {serverVersion && (
                <span className="version-badge">v{serverVersion}</span>
              )}
            </div>
          </div>
        )}
      </div>

      <div className="navbar-center">
        {!activeConnection && (
          <div className="navbar-hint">
            <WifiOff size={14} />
            <span>Not connected</span>
          </div>
        )}
      </div>

      <div className="navbar-right">
        <button 
          className="btn-icon navbar-btn"
          onClick={onNewConnection}
          title="New Connection"
        >
          <Plus size={18} />
        </button>
        
        <button 
          className="btn-icon navbar-btn"
          onClick={() => setIsDark(!isDark)}
          title="Toggle Theme"
        >
          {isDark ? <Sun size={18} /> : <Moon size={18} />}
        </button>

        <button 
          className="btn-icon navbar-btn"
          title="Settings"
        >
          <Settings size={18} />
        </button>

        <div className="navbar-divider" />

        {/* Window Controls (Electron) */}
        <div className="window-controls">
          <button className="window-btn" onClick={handleMinimize}>
            <Minus size={14} />
          </button>
          <button className="window-btn" onClick={handleMaximize}>
            <Square size={12} />
          </button>
          <button className="window-btn window-btn-close" onClick={handleClose}>
            <X size={14} />
          </button>
        </div>
      </div>
    </nav>
  );
}
