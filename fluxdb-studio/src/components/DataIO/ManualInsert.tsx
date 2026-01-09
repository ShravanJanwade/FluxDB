// ManualInsert Component - Manual data entry form
import { useState } from 'react';
import { 
  Plus, 
  Trash2, 
  Send, 
  Loader2,
  Check,
  AlertCircle
} from 'lucide-react';
import { useDatabaseStore } from '../../stores/databaseStore';
import { fluxdb } from '../../services/fluxdb';

interface Tag {
  key: string;
  value: string;
}

interface Field {
  key: string;
  value: string;
  type: 'float' | 'integer' | 'string' | 'boolean';
}

export function ManualInsert() {
  const { databases } = useDatabaseStore();
  
  const [database, setDatabase] = useState(databases[0] || 'default');
  const [measurement, setMeasurement] = useState('');
  const [tags, setTags] = useState<Tag[]>([{ key: '', value: '' }]);
  const [fields, setFields] = useState<Field[]>([{ key: 'value', value: '', type: 'float' }]);
  const [useCustomTimestamp, setUseCustomTimestamp] = useState(false);
  const [timestamp, setTimestamp] = useState('');
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [result, setResult] = useState<{ success: boolean; message: string } | null>(null);

  const addTag = () => {
    setTags([...tags, { key: '', value: '' }]);
  };

  const removeTag = (index: number) => {
    setTags(tags.filter((_, i) => i !== index));
  };

  const updateTag = (index: number, field: 'key' | 'value', value: string) => {
    const newTags = [...tags];
    newTags[index][field] = value;
    setTags(newTags);
  };

  const addField = () => {
    setFields([...fields, { key: '', value: '', type: 'float' }]);
  };

  const removeField = (index: number) => {
    setFields(fields.filter((_, i) => i !== index));
  };

  const updateField = (index: number, key: keyof Field, value: string) => {
    const newFields = [...fields];
    (newFields[index] as any)[key] = value;
    setFields(newFields);
  };

  const handleSubmit = async () => {
    if (!measurement.trim()) {
      setResult({ success: false, message: 'Measurement name is required' });
      return;
    }
    
    const validFields = fields.filter((f) => f.key.trim() && f.value.trim());
    if (validFields.length === 0) {
      setResult({ success: false, message: 'At least one field is required' });
      return;
    }

    setIsSubmitting(true);
    setResult(null);

    try {
      const tagObj: Record<string, string> = {};
      tags.forEach((t) => {
        if (t.key.trim() && t.value.trim()) {
          tagObj[t.key.trim()] = t.value.trim();
        }
      });

      const fieldObj: Record<string, any> = {};
      validFields.forEach((f) => {
        const key = f.key.trim();
        const val = f.value.trim();
        
        switch (f.type) {
          case 'integer':
            fieldObj[key] = parseInt(val, 10);
            break;
          case 'float':
            fieldObj[key] = parseFloat(val);
            break;
          case 'boolean':
            fieldObj[key] = val.toLowerCase() === 'true' || val === '1';
            break;
          case 'string':
          default:
            fieldObj[key] = val;
            break;
        }
      });

      const ts = useCustomTimestamp && timestamp 
        ? parseInt(timestamp, 10)
        : undefined;
      
      const line = fluxdb.formatLineProtocol(measurement.trim(), tagObj, fieldObj, ts);
      
      await fluxdb.write({
        database,
        data: line,
        precision: 'ns',
      });
      
      setResult({ success: true, message: 'Data point inserted successfully' });
      
      // Reset form
      setMeasurement('');
      setTags([{ key: '', value: '' }]);
      setFields([{ key: 'value', value: '', type: 'float' }]);
      setTimestamp('');
    } catch (error) {
      setResult({
        success: false,
        message: error instanceof Error ? error.message : 'Insert failed',
      });
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <div className="manual-insert">
      <div className="insert-header">
        <h2>Manual Data Insert</h2>
        <p>Insert a single data point into FluxDB</p>
      </div>

      {result && (
        <div className={`insert-result ${result.success ? 'success' : 'error'}`}>
          {result.success ? <Check size={16} /> : <AlertCircle size={16} />}
          <span>{result.message}</span>
        </div>
      )}

      <div className="insert-form">
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
            <label className="form-label">Measurement *</label>
            <input
              type="text"
              className="form-input"
              value={measurement}
              onChange={(e) => setMeasurement(e.target.value)}
              placeholder="temperature"
            />
          </div>
        </div>

        {/* Tags Section */}
        <div className="form-section">
          <div className="section-header">
            <label className="form-label">Tags (optional)</label>
            <button className="btn-icon-sm" onClick={addTag} title="Add Tag">
              <Plus size={14} />
            </button>
          </div>
          
          {tags.map((tag, i) => (
            <div key={i} className="key-value-row">
              <input
                type="text"
                className="form-input"
                value={tag.key}
                onChange={(e) => updateTag(i, 'key', e.target.value)}
                placeholder="Key"
              />
              <span>=</span>
              <input
                type="text"
                className="form-input"
                value={tag.value}
                onChange={(e) => updateTag(i, 'value', e.target.value)}
                placeholder="Value"
              />
              {tags.length > 1 && (
                <button 
                  className="btn-icon-sm danger"
                  onClick={() => removeTag(i)}
                >
                  <Trash2 size={14} />
                </button>
              )}
            </div>
          ))}
        </div>

        {/* Fields Section */}
        <div className="form-section">
          <div className="section-header">
            <label className="form-label">Fields *</label>
            <button className="btn-icon-sm" onClick={addField} title="Add Field">
              <Plus size={14} />
            </button>
          </div>
          
          {fields.map((field, i) => (
            <div key={i} className="field-row">
              <input
                type="text"
                className="form-input"
                value={field.key}
                onChange={(e) => updateField(i, 'key', e.target.value)}
                placeholder="Field name"
              />
              <span>=</span>
              <input
                type="text"
                className="form-input"
                value={field.value}
                onChange={(e) => updateField(i, 'value', e.target.value)}
                placeholder="Value"
              />
              <select
                className="form-input type-select"
                value={field.type}
                onChange={(e) => updateField(i, 'type', e.target.value)}
              >
                <option value="float">Float</option>
                <option value="integer">Integer</option>
                <option value="string">String</option>
                <option value="boolean">Boolean</option>
              </select>
              {fields.length > 1 && (
                <button 
                  className="btn-icon-sm danger"
                  onClick={() => removeField(i)}
                >
                  <Trash2 size={14} />
                </button>
              )}
            </div>
          ))}
        </div>

        {/* Timestamp Section */}
        <div className="form-section">
          <div className="section-header">
            <label className="form-label">
              <input
                type="checkbox"
                checked={useCustomTimestamp}
                onChange={(e) => setUseCustomTimestamp(e.target.checked)}
              />
              Custom Timestamp
            </label>
          </div>
          
          {useCustomTimestamp && (
            <div className="form-group">
              <input
                type="text"
                className="form-input"
                value={timestamp}
                onChange={(e) => setTimestamp(e.target.value)}
                placeholder="Nanoseconds since epoch"
              />
              <span className="form-hint">
                Leave empty to use current time
              </span>
            </div>
          )}
        </div>

        <div className="form-actions">
          <button 
            className="btn-primary"
            onClick={handleSubmit}
            disabled={isSubmitting}
          >
            {isSubmitting ? (
              <>
                <Loader2 size={16} className="spin" />
                Inserting...
              </>
            ) : (
              <>
                <Send size={16} />
                Insert Data Point
              </>
            )}
          </button>
        </div>
      </div>
    </div>
  );
}
