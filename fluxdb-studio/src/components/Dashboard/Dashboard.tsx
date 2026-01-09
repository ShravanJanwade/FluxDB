// Dashboard Component - Server statistics and metrics
import { useEffect, useState } from 'react';
import { 
  Database, 
  HardDrive, 
  Activity, 
  RefreshCw,
  TrendingUp,
  Server,
  Zap
} from 'lucide-react';
import ReactECharts from 'echarts-for-react';
import { useDatabaseStore } from '../../stores/databaseStore';
import { useConnectionStore } from '../../stores/connectionStore';

export function Dashboard() {
  const { stats, fetchStats } = useDatabaseStore();
  const { activeConnection, serverVersion } = useConnectionStore();
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [uptimeData, setUptimeData] = useState<number[]>(Array(60).fill(0));

  useEffect(() => {
    if (activeConnection) {
      fetchStats();
      // Auto-refresh every 10 seconds
      const interval = setInterval(fetchStats, 10000);
      return () => clearInterval(interval);
    }
  }, [activeConnection]);

  useEffect(() => {
    // Simulate uptime metric for visualization
    const interval = setInterval(() => {
      setUptimeData((prev) => {
        const newData = [...prev.slice(1), Math.random() * 100 + 50];
        return newData;
      });
    }, 1000);
    return () => clearInterval(interval);
  }, []);

  const handleRefresh = async () => {
    setIsRefreshing(true);
    await fetchStats();
    setIsRefreshing(false);
  };

  const formatBytes = (bytes: number): string => {
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
  };

  const formatNumber = (num: number): string => {
    if (num >= 1_000_000) {
      return (num / 1_000_000).toFixed(1) + 'M';
    }
    if (num >= 1_000) {
      return (num / 1_000).toFixed(1) + 'K';
    }
    return num.toString();
  };

  const activityChartOptions = {
    backgroundColor: 'transparent',
    grid: {
      top: 10,
      bottom: 30,
      left: 40,
      right: 10,
    },
    xAxis: {
      type: 'category',
      data: Array(60).fill('').map((_, i) => `-${60 - i}s`),
      axisLine: { lineStyle: { color: '#334155' } },
      axisLabel: { color: '#64748b', fontSize: 10, interval: 14 },
    },
    yAxis: {
      type: 'value',
      axisLine: { show: false },
      splitLine: { lineStyle: { color: '#1e293b' } },
      axisLabel: { color: '#64748b', fontSize: 10 },
    },
    series: [
      {
        data: uptimeData,
        type: 'line',
        smooth: true,
        symbol: 'none',
        lineStyle: {
          color: '#3b82f6',
          width: 2,
        },
        areaStyle: {
          color: {
            type: 'linear',
            x: 0,
            y: 0,
            x2: 0,
            y2: 1,
            colorStops: [
              { offset: 0, color: 'rgba(59, 130, 246, 0.3)' },
              { offset: 1, color: 'rgba(59, 130, 246, 0)' },
            ],
          },
        },
      },
    ],
    tooltip: {
      trigger: 'axis',
      backgroundColor: '#1e293b',
      borderColor: '#334155',
      textStyle: { color: '#f1f5f9' },
    },
  };

  const databaseChartOptions = stats?.databases?.length ? {
    backgroundColor: 'transparent',
    tooltip: {
      trigger: 'item',
      backgroundColor: '#1e293b',
      borderColor: '#334155',
      textStyle: { color: '#f1f5f9' },
    },
    series: [
      {
        type: 'pie',
        radius: ['50%', '70%'],
        center: ['50%', '50%'],
        data: stats.databases.map((db, i) => ({
          name: db.name,
          value: db.total_entries,
          itemStyle: {
            color: [
              '#3b82f6',
              '#8b5cf6',
              '#10b981',
              '#f59e0b',
              '#ef4444',
            ][i % 5],
          },
        })),
        label: {
          show: true,
          color: '#94a3b8',
          fontSize: 11,
        },
        labelLine: {
          lineStyle: {
            color: '#334155',
          },
        },
      },
    ],
  } : null;

  if (!activeConnection) {
    return (
      <div className="dashboard">
        <div className="dashboard-empty">
          <Server size={48} />
          <h2>Not Connected</h2>
          <p>Connect to a FluxDB server to view dashboard metrics</p>
        </div>
      </div>
    );
  }

  return (
    <div className="dashboard">
      <div className="dashboard-header">
        <div className="dashboard-title">
          <Zap size={24} />
          <h2>Server Dashboard</h2>
          {serverVersion && (
            <span className="dashboard-version">v{serverVersion}</span>
          )}
        </div>
        <button 
          className="btn-secondary"
          onClick={handleRefresh}
          disabled={isRefreshing}
        >
          <RefreshCw size={14} className={isRefreshing ? 'spin' : ''} />
          Refresh
        </button>
      </div>

      <div className="dashboard-grid">
        {/* Stats Cards */}
        <div className="stats-card">
          <div className="stats-card-icon blue">
            <Database size={24} />
          </div>
          <div className="stats-card-content">
            <span className="stats-card-label">Databases</span>
            <span className="stats-card-value">{stats?.database_count || 0}</span>
          </div>
        </div>

        <div className="stats-card">
          <div className="stats-card-icon purple">
            <Activity size={24} />
          </div>
          <div className="stats-card-content">
            <span className="stats-card-label">Total Entries</span>
            <span className="stats-card-value">{formatNumber(stats?.total_entries || 0)}</span>
          </div>
        </div>

        <div className="stats-card">
          <div className="stats-card-icon green">
            <HardDrive size={24} />
          </div>
          <div className="stats-card-content">
            <span className="stats-card-label">Storage Used</span>
            <span className="stats-card-value">{formatBytes(stats?.total_size_bytes || 0)}</span>
          </div>
        </div>

        <div className="stats-card">
          <div className="stats-card-icon orange">
            <TrendingUp size={24} />
          </div>
          <div className="stats-card-content">
            <span className="stats-card-label">SSTables</span>
            <span className="stats-card-value">
              {stats?.databases?.reduce((acc, db) => acc + db.sstables, 0) || 0}
            </span>
          </div>
        </div>
      </div>

      <div className="dashboard-charts">
        {/* Activity Chart */}
        <div className="dashboard-card wide">
          <h3>
            <Activity size={16} />
            Server Activity
          </h3>
          <div className="chart-container">
            <ReactECharts 
              option={activityChartOptions} 
              style={{ height: '100%' }}
              opts={{ renderer: 'svg' }}
            />
          </div>
        </div>

        {/* Database Distribution Chart */}
        {databaseChartOptions && (
          <div className="dashboard-card">
            <h3>
              <Database size={16} />
              Database Distribution
            </h3>
            <div className="chart-container">
              <ReactECharts 
                option={databaseChartOptions} 
                style={{ height: '100%' }}
                opts={{ renderer: 'svg' }}
              />
            </div>
          </div>
        )}

        {/* Database Details */}
        <div className="dashboard-card">
          <h3>
            <Database size={16} />
            Database Details
          </h3>
          <div className="database-list">
            {stats?.databases?.map((db) => (
              <div key={db.name} className="database-item">
                <div className="database-info">
                  <span className="database-name">{db.name}</span>
                  <span className="database-entries">{formatNumber(db.total_entries)} entries</span>
                </div>
                <div className="database-meta">
                  <span>MemTable: {formatBytes(db.memtable_size)}</span>
                  <span>SSTables: {db.sstables}</span>
                </div>
              </div>
            ))}
            {!stats?.databases?.length && (
              <div className="database-list-empty">
                No databases found
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
