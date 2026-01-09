// ExportModal Component - Data export wizard
import { useState } from 'react';
import { 
  Download, 
  X, 
  FileJson, 
  FileSpreadsheet, 
  Terminal,
  Loader2,
  Check,
  AlertCircle
} from 'lucide-react';
import { useDatabaseStore } from '../../stores/databaseStore';
import { useQueryStore } from '../../stores/queryStore';
import { fluxdb } from '../../services/fluxdb';
import { ExportFormat } from '../../types';

interface ExportModalProps {
  isOpen: boolean;
  onClose: () => void;
}

export function ExportModal({ isOpen, onClose }: ExportModalProps) {
  const { databases } = useDatabaseStore();
  const { currentQuery, currentDatabase } = useQueryStore();
  
  const [format, setFormat] = useState<ExportFormat>('csv');
  const [database, setDatabase] = useState(currentDatabase || databases[0] || 'default');
  const [query, setQuery] = useState(currentQuery || 'SELECT * FROM measurement LIMIT 1000');
  const [fileName, setFileName] = useState('export');
  const [isExporting, setIsExporting] = useState(false);
  const [exportResult, setExportResult] = useState<{ success: boolean; message: string } | null>(null);

  const handleExport = async () => {
    setIsExporting(true);
    setExportResult(null);
    
    try {
      const result = await fluxdb.query(database, query);
      
      const series = result.results?.[0]?.series?.[0];
      if (!series) {
        throw new Error('No data returned from query');
      }
      
      const { columns, values } = series;
      let content: string;
      let mimeType: string;
      let extension: string;
      
      switch (format) {
        case 'csv':
          const header = columns.join(',');
          const rows = values.map((row) => 
            row.map((v) => {
              if (v === null) return '';
              if (typeof v === 'string' && (v.includes(',') || v.includes('"'))) {
                return `"${v.replace(/"/g, '""')}"`;
              }
              return String(v);
            }).join(',')
          );
          content = [header, ...rows].join('\n');
          mimeType = 'text/csv';
          extension = 'csv';
          break;
          
        case 'json':
          const jsonData = values.map((row) => {
            const obj: Record<string, any> = {};
            columns.forEach((col, i) => {
              obj[col] = row[i];
            });
            return obj;
          });
          content = JSON.stringify(jsonData, null, 2);
          mimeType = 'application/json';
          extension = 'json';
          break;
          
        case 'line_protocol':
          // Assuming first column is time, second is measurement/series
          const lines = values.map((row) => {
            const timestamp = row[0];
            const fields: Record<string, any> = {};
            columns.slice(1).forEach((col, i) => {
              if (row[i + 1] !== null) {
                fields[col] = row[i + 1];
              }
            });
            return fluxdb.formatLineProtocol('exported_data', {}, fields, timestamp as number);
          });
          content = lines.join('\n');
          mimeType = 'text/plain';
          extension = 'txt';
          break;
          
        default:
          throw new Error('Unknown format');
      }
      
      // Download file
      const blob = new Blob([content], { type: mimeType });
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `${fileName}.${extension}`;
      a.click();
      URL.revokeObjectURL(url);
      
      setExportResult({
        success: true,
        message: `Exported ${values.length} rows to ${fileName}.${extension}`,
      });
    } catch (error) {
      setExportResult({
        success: false,
        message: error instanceof Error ? error.message : 'Export failed',
      });
    } finally {
      setIsExporting(false);
    }
  };

  const handleClose = () => {
    setExportResult(null);
    onClose();
  };

  if (!isOpen) return null;

  return (
    <div className="modal-overlay" onClick={handleClose}>
      <div className="modal modal-md export-modal" onClick={(e) => e.stopPropagation()}>
        <div className="modal-header">
          <h2>
            <Download size={20} />
            Export Data
          </h2>
          <button className="btn-icon" onClick={handleClose}>
            <X size={20} />
          </button>
        </div>

        <div className="modal-body">
          {exportResult ? (
            <div className={`export-result ${exportResult.success ? 'success' : 'error'}`}>
              {exportResult.success ? (
                <>
                  <Check size={48} />
                  <h3>Export Successful</h3>
                  <p>{exportResult.message}</p>
                </>
              ) : (
                <>
                  <AlertCircle size={48} />
                  <h3>Export Failed</h3>
                  <p>{exportResult.message}</p>
                </>
              )}
            </div>
          ) : (
            <>
              <div className="form-group">
                <label className="form-label">Export Format</label>
                <div className="format-buttons">
                  <button 
                    className={`format-btn ${format === 'csv' ? 'selected' : ''}`}
                    onClick={() => setFormat('csv')}
                  >
                    <FileSpreadsheet size={20} />
                    CSV
                  </button>
                  <button 
                    className={`format-btn ${format === 'json' ? 'selected' : ''}`}
                    onClick={() => setFormat('json')}
                  >
                    <FileJson size={20} />
                    JSON
                  </button>
                  <button 
                    className={`format-btn ${format === 'line_protocol' ? 'selected' : ''}`}
                    onClick={() => setFormat('line_protocol')}
                  >
                    <Terminal size={20} />
                    Line Protocol
                  </button>
                </div>
              </div>

              <div className="form-row">
                <div className="form-group">
                  <label className="form-label">Database</label>
                  <select 
                    className="form-input"
                    value={database}
                    onChange={(e) => setDatabase(e.target.value)}
                  >
                    {databases.map((db) => (
                      <option key={db} value={db}>{db}</option>
                    ))}
                  </select>
                </div>
                <div className="form-group">
                  <label className="form-label">File Name</label>
                  <input
                    type="text"
                    className="form-input"
                    value={fileName}
                    onChange={(e) => setFileName(e.target.value)}
                    placeholder="export"
                  />
                </div>
              </div>

              <div className="form-group">
                <label className="form-label">Query</label>
                <textarea
                  className="form-input form-textarea"
                  value={query}
                  onChange={(e) => setQuery(e.target.value)}
                  rows={4}
                  placeholder="SELECT * FROM measurement LIMIT 1000"
                />
              </div>
            </>
          )}
        </div>

        <div className="modal-footer">
          {exportResult ? (
            <button className="btn-primary" onClick={handleClose}>
              Done
            </button>
          ) : (
            <>
              <button className="btn-secondary" onClick={handleClose}>
                Cancel
              </button>
              <button 
                className="btn-primary"
                onClick={handleExport}
                disabled={isExporting || !query.trim()}
              >
                {isExporting ? (
                  <>
                    <Loader2 size={16} className="spin" />
                    Exporting...
                  </>
                ) : (
                  <>
                    <Download size={16} />
                    Export
                  </>
                )}
              </button>
            </>
          )}
        </div>
      </div>
    </div>
  );
}
