// Sidebar Component - Navigation sidebar
import { 
  Code2, 
  LayoutDashboard, 
  Database, 
  Upload, 
  Download,
  Settings,
  PenSquare,
  Edit,
  Trash2
} from 'lucide-react';
import { ViewType } from '../../types';

interface SidebarProps {
  currentView: ViewType;
  onViewChange: (view: ViewType) => void;
}

const navItems: { id: ViewType; label: string; icon: typeof Code2 }[] = [
  { id: 'query', label: 'Query Editor', icon: Code2 },
  { id: 'write', label: 'Write Data', icon: PenSquare },
  { id: 'update', label: 'Update Data', icon: Edit },
  { id: 'delete', label: 'Delete Data', icon: Trash2 },
  { id: 'dashboard', label: 'Dashboard', icon: LayoutDashboard },
  { id: 'explorer', label: 'Explorer', icon: Database },
  { id: 'import', label: 'Import Data', icon: Upload },
  { id: 'export', label: 'Export Data', icon: Download },
];

export function Sidebar({ currentView, onViewChange }: SidebarProps) {
  return (
    <aside className="sidebar">
      <nav className="sidebar-nav">
        {navItems.map((item) => {
          const Icon = item.icon;
          return (
            <button
              key={item.id}
              className={`sidebar-item ${currentView === item.id ? 'active' : ''}`}
              onClick={() => onViewChange(item.id)}
              title={item.label}
            >
              <Icon size={20} />
              <span>{item.label}</span>
            </button>
          );
        })}
      </nav>

      <div className="sidebar-footer">
        <button
          className={`sidebar-item ${currentView === 'settings' ? 'active' : ''}`}
          onClick={() => onViewChange('settings')}
          title="Settings"
        >
          <Settings size={20} />
          <span>Settings</span>
        </button>
      </div>
    </aside>
  );
}
