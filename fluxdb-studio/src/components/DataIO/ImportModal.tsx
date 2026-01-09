// ImportModal Component - Data import wizard
import { useState, useRef, useCallback } from 'react';
import { 
  Upload, 
  FileText, 
  Check, 
  X, 
  AlertCircle,
  ChevronRight,
  ChevronLeft,
  FileJson,
  FileSpreadsheet,
  Terminal,
  Loader2
} from 'lucide-react';
import { useDatabaseStore } from '../../stores/databaseStore';
import { fluxdb } from '../../services/fluxdb';
import { ImportFormat, ImportConfig } from '../../types';

interface ImportModalProps {
  isOpen: boolean;
  onClose: () => void;
}

type Step = 'format' | 'file' | 'configure' | 'preview' | 'import';

export function ImportModal({ isOpen, onClose }: ImportModalProps) {
  const { databases } = useDatabaseStore();
  const fileInputRef = useRef<HTMLInputElement>(null);
  
  const [step, setStep] = useState<Step>('format');
  const [format, setFormat] = useState<ImportFormat>('csv');
  const [file, setFile] = useState<File | null>(null);
  const [fileContent, setFileContent] = useState<string>('');
  const [config, setConfig] = useState<ImportConfig>({
    format: 'csv',
    database: databases[0] || 'default',
    measurement: '',
    timestampField: 'timestamp',
    timestampPrecision: 'ns',
    skipHeader: true,
    tagColumns: [],
  });
  const [preview, setPreview] = useState<string[]>([]);
  const [isImporting, setIsImporting] = useState(false);
  const [importResult, setImportResult] = useState<{ success: boolean; message: string } | null>(null);
  const [dragActive, setDragActive] = useState(false);

  const handleDrag = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    if (e.type === 'dragenter' || e.type === 'dragover') {
      setDragActive(true);
    } else if (e.type === 'dragleave') {
      setDragActive(false);
    }
  }, []);

  const handleDrop = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    setDragActive(false);
    
    if (e.dataTransfer.files && e.dataTransfer.files[0]) {
      handleFileSelect(e.dataTransfer.files[0]);
    }
  }, []);

  const handleFileSelect = async (selectedFile: File) => {
    setFile(selectedFile);
    const content = await selectedFile.text();
    setFileContent(content);
    
    // Auto-detect format from extension
    if (selectedFile.name.endsWith('.csv')) {
      setFormat('csv');
      setConfig((c) => ({ ...c, format: 'csv' }));
    } else if (selectedFile.name.endsWith('.json')) {
      setFormat('json');
      setConfig((c) => ({ ...c, format: 'json' }));
    } else if (selectedFile.name.endsWith('.txt') || selectedFile.name.endsWith('.lp')) {
      setFormat('line_protocol');
      setConfig((c) => ({ ...c, format: 'line_protocol' }));
    }
    
    // Generate preview
    const lines = content.split('\n').slice(0, 10);
    setPreview(lines);
    
    setStep('configure');
  };

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    if (e.target.files && e.target.files[0]) {
      handleFileSelect(e.target.files[0]);
    }
  };

  const generateLineProtocol = (): string[] => {
    if (format === 'line_protocol') {
      return fileContent.split('\n').filter((line) => line.trim() && !line.startsWith('#'));
    }
    
    if (format === 'json') {
      try {
        const data = JSON.parse(fileContent);
        const rows = Array.isArray(data) ? data : [data];
        return rows.map((row) => {
          const tags: Record<string, string> = {};
          const fields: Record<string, any> = {};
          let timestamp: number | undefined;
          
          Object.entries(row).forEach(([key, value]) => {
            if (key === config.timestampField) {
              timestamp = Number(value);
            } else if (config.tagColumns?.includes(key)) {
              tags[key] = String(value);
            } else {
              fields[key] = value;
            }
          });
          
          return fluxdb.formatLineProtocol(
            config.measurement || 'imported_data',
            tags,
            fields,
            timestamp
          );
        });
      } catch (e) {
        return [];
      }
    }
    
    if (format === 'csv') {
      const lines = fileContent.split('\n');
      const headers = lines[0].split(',').map((h) => h.trim());
      const dataLines = config.skipHeader ? lines.slice(1) : lines;
      
      return dataLines
        .filter((line) => line.trim())
        .map((line) => {
          const values = line.split(',').map((v) => v.trim());
          const record: Record<string, string> = {};
          headers.forEach((h, i) => {
            record[h] = values[i];
          });
          
          const tags: Record<string, string> = {};
          const fields: Record<string, any> = {};
          let timestamp: number | undefined;
          
          Object.entries(record).forEach(([key, value]) => {
            if (key === config.timestampField) {
              timestamp = parseInt(value, 10);
            } else if (config.tagColumns?.includes(key)) {
              tags[key] = value;
            } else {
              const num = parseFloat(value);
              if (!isNaN(num)) {
                fields[key] = num;
              } else if (value === 'true' || value === 'false') {
                fields[key] = value === 'true';
              } else {
                fields[key] = value;
              }
            }
          });
          
          return fluxdb.formatLineProtocol(
            config.measurement || 'imported_data',
            tags,
            fields,
            timestamp
          );
        });
    }
    
    return [];
  };

  const handleImport = async () => {
    setIsImporting(true);
    setImportResult(null);
    
    try {
      const lines = generateLineProtocol();
      const data = lines.join('\n');
      
      await fluxdb.write({
        database: config.database,
        data,
        precision: config.timestampPrecision,
      });
      
      setImportResult({
        success: true,
        message: `Successfully imported ${lines.length} data points`,
      });
      setStep('import');
    } catch (error) {
      setImportResult({
        success: false,
        message: error instanceof Error ? error.message : 'Import failed',
      });
      setStep('import');
    } finally {
      setIsImporting(false);
    }
  };

  const resetAndClose = () => {
    setStep('format');
    setFormat('csv');
    setFile(null);
    setFileContent('');
    setPreview([]);
    setImportResult(null);
    onClose();
  };

  if (!isOpen) return null;

  return (
    <div className="modal-overlay" onClick={resetAndClose}>
      <div className="modal modal-lg import-modal" onClick={(e) => e.stopPropagation()}>
        <div className="modal-header">
          <h2>
            <Upload size={20} />
            Import Data
          </h2>
          <button className="btn-icon" onClick={resetAndClose}>
            <X size={20} />
          </button>
        </div>

        <div className="import-steps">
          <div className={`import-step ${step === 'format' ? 'active' : ''}`}>
            <span className="step-number">1</span>
            Format
          </div>
          <ChevronRight size={16} />
          <div className={`import-step ${step === 'file' ? 'active' : ''}`}>
            <span className="step-number">2</span>
            File
          </div>
          <ChevronRight size={16} />
          <div className={`import-step ${step === 'configure' ? 'active' : ''}`}>
            <span className="step-number">3</span>
            Configure
          </div>
          <ChevronRight size={16} />
          <div className={`import-step ${step === 'preview' || step === 'import' ? 'active' : ''}`}>
            <span className="step-number">4</span>
            Import
          </div>
        </div>

        <div className="modal-body">
          {/* Step 1: Format Selection */}
          {step === 'format' && (
            <div className="import-format-step">
              <h3>Select Data Format</h3>
              <div className="format-options">
                <button 
                  className={`format-option ${format === 'csv' ? 'selected' : ''}`}
                  onClick={() => setFormat('csv')}
                >
                  <FileSpreadsheet size={32} />
                  <span className="format-name">CSV</span>
                  <span className="format-desc">Comma-separated values</span>
                </button>
                <button 
                  className={`format-option ${format === 'json' ? 'selected' : ''}`}
                  onClick={() => setFormat('json')}
                >
                  <FileJson size={32} />
                  <span className="format-name">JSON</span>
                  <span className="format-desc">JSON array or objects</span>
                </button>
                <button 
                  className={`format-option ${format === 'line_protocol' ? 'selected' : ''}`}
                  onClick={() => setFormat('line_protocol')}
                >
                  <Terminal size={32} />
                  <span className="format-name">Line Protocol</span>
                  <span className="format-desc">InfluxDB line protocol</span>
                </button>
              </div>
            </div>
          )}

          {/* Step 2: File Selection */}
          {step === 'file' && (
            <div className="import-file-step">
              <div 
                className={`file-drop-zone ${dragActive ? 'active' : ''}`}
                onDragEnter={handleDrag}
                onDragLeave={handleDrag}
                onDragOver={handleDrag}
                onDrop={handleDrop}
                onClick={() => fileInputRef.current?.click()}
              >
                <Upload size={48} />
                <p>Drop your file here or click to browse</p>
                <span className="file-types">
                  Supported: .csv, .json, .txt, .lp
                </span>
                <input
                  ref={fileInputRef}
                  type="file"
                  accept=".csv,.json,.txt,.lp"
                  onChange={handleFileChange}
                  hidden
                />
              </div>
              
              {file && (
                <div className="selected-file">
                  <FileText size={20} />
                  <span>{file.name}</span>
                  <span className="file-size">{(file.size / 1024).toFixed(1)} KB</span>
                </div>
              )}
            </div>
          )}

          {/* Step 3: Configuration */}
          {step === 'configure' && (
            <div className="import-configure-step">
              <div className="config-form">
                <div className="form-row">
                  <div className="form-group">
                    <label className="form-label">Database</label>
                    <select 
                      className="form-input"
                      value={config.database}
                      onChange={(e) => setConfig({ ...config, database: e.target.value })}
                    >
                      {databases.map((db) => (
                        <option key={db} value={db}>{db}</option>
                      ))}
                    </select>
                  </div>
                  <div className="form-group">
                    <label className="form-label">Measurement Name</label>
                    <input
                      type="text"
                      className="form-input"
                      value={config.measurement}
                      onChange={(e) => setConfig({ ...config, measurement: e.target.value })}
                      placeholder="my_measurement"
                    />
                  </div>
                </div>

                {format !== 'line_protocol' && (
                  <>
                    <div className="form-row">
                      <div className="form-group">
                        <label className="form-label">Timestamp Field</label>
                        <input
                          type="text"
                          className="form-input"
                          value={config.timestampField}
                          onChange={(e) => setConfig({ ...config, timestampField: e.target.value })}
                          placeholder="timestamp"
                        />
                      </div>
                      <div className="form-group">
                        <label className="form-label">Timestamp Precision</label>
                        <select 
                          className="form-input"
                          value={config.timestampPrecision}
                          onChange={(e) => setConfig({ ...config, timestampPrecision: e.target.value as any })}
                        >
                          <option value="ns">Nanoseconds</option>
                          <option value="us">Microseconds</option>
                          <option value="ms">Milliseconds</option>
                          <option value="s">Seconds</option>
                        </select>
                      </div>
                    </div>

                    <div className="form-group">
                      <label className="form-label">Tag Columns (comma-separated)</label>
                      <input
                        type="text"
                        className="form-input"
                        value={config.tagColumns?.join(', ') || ''}
                        onChange={(e) => setConfig({ 
                          ...config, 
                          tagColumns: e.target.value.split(',').map((t) => t.trim()).filter(Boolean)
                        })}
                        placeholder="sensor_id, location"
                      />
                    </div>
                  </>
                )}
              </div>

              {preview.length > 0 && (
                <div className="preview-section">
                  <h4>File Preview</h4>
                  <pre className="file-preview">{preview.join('\n')}</pre>
                </div>
              )}
            </div>
          )}

          {/* Step 4: Preview & Import */}
          {step === 'preview' && (
            <div className="import-preview-step">
              <h3>Ready to Import</h3>
              <div className="import-summary">
                <div className="summary-item">
                  <span className="summary-label">Database:</span>
                  <span className="summary-value">{config.database}</span>
                </div>
                <div className="summary-item">
                  <span className="summary-label">Measurement:</span>
                  <span className="summary-value">{config.measurement || 'imported_data'}</span>
                </div>
                <div className="summary-item">
                  <span className="summary-label">Format:</span>
                  <span className="summary-value">{format.toUpperCase()}</span>
                </div>
                <div className="summary-item">
                  <span className="summary-label">Lines:</span>
                  <span className="summary-value">{generateLineProtocol().length}</span>
                </div>
              </div>
            </div>
          )}

          {/* Import Result */}
          {step === 'import' && importResult && (
            <div className="import-result-step">
              {importResult.success ? (
                <div className="result-success">
                  <Check size={48} />
                  <h3>Import Successful</h3>
                  <p>{importResult.message}</p>
                </div>
              ) : (
                <div className="result-error">
                  <AlertCircle size={48} />
                  <h3>Import Failed</h3>
                  <p>{importResult.message}</p>
                </div>
              )}
            </div>
          )}
        </div>

        <div className="modal-footer">
          {step !== 'import' && (
            <>
              {step !== 'format' && (
                <button 
                  className="btn-secondary"
                  onClick={() => {
                    const steps: Step[] = ['format', 'file', 'configure', 'preview'];
                    const currentIndex = steps.indexOf(step);
                    if (currentIndex > 0) {
                      setStep(steps[currentIndex - 1]);
                    }
                  }}
                >
                  <ChevronLeft size={16} />
                  Back
                </button>
              )}
              <div style={{ flex: 1 }} />
              {step === 'format' && (
                <button 
                  className="btn-primary"
                  onClick={() => setStep('file')}
                >
                  Continue
                  <ChevronRight size={16} />
                </button>
              )}
              {step === 'configure' && (
                <button 
                  className="btn-primary"
                  onClick={() => setStep('preview')}
                  disabled={!config.measurement && format !== 'line_protocol'}
                >
                  Continue
                  <ChevronRight size={16} />
                </button>
              )}
              {step === 'preview' && (
                <button 
                  className="btn-primary"
                  onClick={handleImport}
                  disabled={isImporting}
                >
                  {isImporting ? (
                    <>
                      <Loader2 size={16} className="spin" />
                      Importing...
                    </>
                  ) : (
                    <>
                      <Upload size={16} />
                      Import Data
                    </>
                  )}
                </button>
              )}
            </>
          )}
          {step === 'import' && (
            <button className="btn-primary" onClick={resetAndClose}>
              Done
            </button>
          )}
        </div>
      </div>
    </div>
  );
}
