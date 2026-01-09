// ResultsPanel Component - Display query results
import { useState } from 'react';
import { 
  Table, 
  Code2, 
  Download, 
  Copy,
  FileJson,
  Check,
  AlertCircle,
  Database
} from 'lucide-react';
import { useQueryStore } from '../../stores/queryStore';

type ViewMode = 'table' | 'json';

export function ResultsPanel() {
  const { lastResult, lastError, isExecuting } = useQueryStore();
  const [viewMode, setViewMode] = useState<ViewMode>('table');
  const [copied, setCopied] = useState(false);

  const series = lastResult?.results?.[0]?.series?.[0];
  const columns = series?.columns || [];
  const values = series?.values || [];

  const handleCopy = async () => {
    if (!lastResult) return;
    
    try {
      await navigator.clipboard.writeText(JSON.stringify(lastResult, null, 2));
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch (error) {
      console.error('Failed to copy:', error);
    }
  };

  const handleExportCSV = () => {
    if (!series) return;

    const header = columns.join(',');
    const rows = values.map((row) => 
      row.map((v) => {
        if (v === null) return '';
        if (typeof v === 'string' && v.includes(',')) return `"${v}"`;
        return String(v);
      }).join(',')
    );
    
    const csv = [header, ...rows].join('\n');
    downloadFile(csv, 'query-results.csv', 'text/csv');
  };

  const handleExportJSON = () => {
    if (!lastResult) return;
    const json = JSON.stringify(lastResult, null, 2);
    downloadFile(json, 'query-results.json', 'application/json');
  };

  const downloadFile = (content: string, filename: string, type: string) => {
    const blob = new Blob([content], { type });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    a.click();
    URL.revokeObjectURL(url);
  };

  if (isExecuting) {
    return (
      <div className="results-empty">
        <div className="loading-spinner large" />
        <p>Executing query...</p>
      </div>
    );
  }

  if (lastError) {
    return (
      <div className="results-error">
        <AlertCircle size={32} />
        <h3>Query Error</h3>
        <p>{lastError}</p>
      </div>
    );
  }

  if (!lastResult || !series) {
    return (
      <div className="results-empty">
        <Database size={32} />
        <p>No results to display</p>
        <span>Execute a query to see results here</span>
      </div>
    );
  }

  return (
    <div className="results-panel">
      <div className="results-toolbar">
        <div className="results-toolbar-left">
          <div className="view-mode-toggle">
            <button 
              className={`view-mode-btn ${viewMode === 'table' ? 'active' : ''}`}
              onClick={() => setViewMode('table')}
              title="Table View"
            >
              <Table size={14} />
            </button>
            <button 
              className={`view-mode-btn ${viewMode === 'json' ? 'active' : ''}`}
              onClick={() => setViewMode('json')}
              title="JSON View"
            >
              <Code2 size={14} />
            </button>
          </div>
          
          <span className="results-count">
            {values.length} row{values.length !== 1 ? 's' : ''}
          </span>
        </div>

        <div className="results-toolbar-right">
          <button 
            className="btn-icon"
            onClick={handleCopy}
            title="Copy to Clipboard"
          >
            {copied ? <Check size={14} /> : <Copy size={14} />}
          </button>
          <button 
            className="btn-icon"
            onClick={handleExportCSV}
            title="Export CSV"
          >
            <Download size={14} />
          </button>
          <button 
            className="btn-icon"
            onClick={handleExportJSON}
            title="Export JSON"
          >
            <FileJson size={14} />
          </button>
        </div>
      </div>

      <div className="results-content">
        {viewMode === 'table' ? (
          <div className="results-table-container">
            <table className="results-table">
              <thead>
                <tr>
                  <th className="row-number">#</th>
                  {columns.map((col, i) => (
                    <th key={i}>{col}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {values.map((row, rowIndex) => (
                  <tr key={rowIndex}>
                    <td className="row-number">{rowIndex + 1}</td>
                    {row.map((cell, cellIndex) => (
                      <td key={cellIndex}>
                        <CellValue value={cell} />
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <div className="results-json">
            <pre>{JSON.stringify(lastResult, null, 2)}</pre>
          </div>
        )}
      </div>
    </div>
  );
}

function CellValue({ value }: { value: any }) {
  if (value === null || value === undefined) {
    return <span className="cell-null">NULL</span>;
  }

  if (typeof value === 'boolean') {
    return <span className="cell-boolean">{value ? 'true' : 'false'}</span>;
  }

  if (typeof value === 'number') {
    // Format timestamp if it looks like nanoseconds
    if (value > 1_000_000_000_000_000) {
      const date = new Date(value / 1_000_000);
      return <span className="cell-timestamp">{date.toISOString()}</span>;
    }
    return <span className="cell-number">{value}</span>;
  }

  return <span className="cell-string">{String(value)}</span>;
}
