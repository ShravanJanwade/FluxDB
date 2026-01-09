// WritePanel Component - Visual data insertion interface
import { useState, useCallback } from 'react';
import { 
  PlusCircle, 
  Trash2, 
  Send, 
  ListPlus,
  Tag,
  Clock,
  Database,
  Layers
} from 'lucide-react';
import { useDatabaseStore } from '../../stores/databaseStore';
import { fluxdb } from '../../services/fluxdb';

interface FieldEntry {
  name: string;
  value: string;
  type: 'float' | 'integer' | 'string' | 'boolean';
}

interface TagEntry {
  name: string;
  value: string;
}

export function WritePanel() {
  const { selectedDatabase, databases } = useDatabaseStore();
  const [measurement, setMeasurement] = useState('');
  const [tags, setTags] = useState<TagEntry[]>([{ name: '', value: '' }]);
  const [fields, setFields] = useState<FieldEntry[]>([{ name: 'value', value: '', type: 'float' }]);
  const [timestamp, setTimestamp] = useState('');
  const [useCurrentTime, setUseCurrentTime] = useState(true);
  const [precision, setPrecision] = useState<'ns' | 'us' | 'ms' | 's'>('ns');
  const [isWriting, setIsWriting] = useState(false);
  const [writeResult, setWriteResult] = useState<{ success: boolean; message: string } | null>(null);
  const [lineProtocolPreview, setLineProtocolPreview] = useState('');

  // Add a new tag
  const addTag = () => {
    setTags([...tags, { name: '', value: '' }]);
  };

  // Remove a tag
  const removeTag = (index: number) => {
    setTags(tags.filter((_, i) => i !== index));
  };

  // Update a tag
  const updateTag = (index: number, field: 'name' | 'value', value: string) => {
    const newTags = [...tags];
    newTags[index][field] = value;
    setTags(newTags);
    updatePreview();
  };

  // Add a new field
  const addField = () => {
    setFields([...fields, { name: '', value: '', type: 'float' }]);
  };

  // Remove a field
  const removeField = (index: number) => {
    if (fields.length > 1) {
      setFields(fields.filter((_, i) => i !== index));
    }
  };

  // Update a field
  const updateField = (index: number, key: keyof FieldEntry, value: string) => {
    const newFields = [...fields];
    (newFields[index] as any)[key] = value;
    setFields(newFields);
    updatePreview();
  };

  // Generate line protocol string
  const generateLineProtocol = useCallback(() => {
    if (!measurement) return '';

    let line = measurement;

    // Add tags
    const validTags = tags.filter(t => t.name && t.value);
    if (validTags.length > 0) {
      const tagStr = validTags.map(t => `${t.name}=${t.value}`).join(',');
      line += `,${tagStr}`;
    }

    // Add fields
    const validFields = fields.filter(f => f.name && f.value);
    if (validFields.length === 0) return '';

    const fieldStr = validFields.map(f => {
      switch (f.type) {
        case 'string':
          return `${f.name}="${f.value}"`;
        case 'integer':
          return `${f.name}=${f.value}i`;
        case 'boolean':
          return `${f.name}=${f.value.toLowerCase() === 'true'}`;
        default:
          return `${f.name}=${f.value}`;
      }
    }).join(',');

    line += ` ${fieldStr}`;

    // Add timestamp
    if (!useCurrentTime && timestamp) {
      line += ` ${timestamp}`;
    }

    return line;
  }, [measurement, tags, fields, timestamp, useCurrentTime]);

  // Update preview
  const updatePreview = () => {
    setTimeout(() => {
      setLineProtocolPreview(generateLineProtocol());
    }, 0);
  };

  // Write data
  const handleWrite = async () => {
    if (!selectedDatabase) {
      setWriteResult({ success: false, message: 'Please select a database' });
      return;
    }

    const lineProtocol = generateLineProtocol();
    if (!lineProtocol) {
      setWriteResult({ success: false, message: 'Please fill in measurement and at least one field' });
      return;
    }

    setIsWriting(true);
    setWriteResult(null);

    try {
      await fluxdb.writeLineProtocol(selectedDatabase, lineProtocol, precision);
      setWriteResult({ success: true, message: 'Data written successfully!' });
      
      // Clear form after successful write
      setMeasurement('');
      setTags([{ name: '', value: '' }]);
      setFields([{ name: 'value', value: '', type: 'float' }]);
      setTimestamp('');
      setLineProtocolPreview('');
    } catch (error: any) {
      setWriteResult({ 
        success: false, 
        message: error.message || 'Failed to write data'
      });
    } finally {
      setIsWriting(false);
    }
  };

  return (
    <div className="write-panel">
      <div className="write-panel-header">
        <div className="write-panel-title">
          <PlusCircle size={20} />
          <span>Insert Data</span>
        </div>
        <p className="write-panel-subtitle">
          Create and insert new data points using the visual builder
        </p>
      </div>

      <div className="write-panel-content">
        <div className="write-form">
          {/* Database Selection */}
          <div className="form-section">
            <label className="form-section-label">
              <Database size={14} />
              Target Database
            </label>
            <select 
              className="form-select"
              value={selectedDatabase || ''}
              disabled
            >
              <option value="">{selectedDatabase || 'No database selected'}</option>
            </select>
          </div>

          {/* Measurement Name */}
          <div className="form-section">
            <label className="form-section-label">
              <Layers size={14} />
              Measurement Name
            </label>
            <input
              type="text"
              className="form-input"
              placeholder="e.g., temperature, cpu_usage"
              value={measurement}
              onChange={(e) => {
                setMeasurement(e.target.value);
                updatePreview();
              }}
            />
          </div>

          {/* Tags */}
          <div className="form-section">
            <div className="form-section-header">
              <label className="form-section-label">
                <Tag size={14} />
                Tags (Indexed Metadata)
              </label>
              <button className="btn-add" onClick={addTag}>
                <ListPlus size={14} />
                Add Tag
              </button>
            </div>
            <div className="entries-list">
              {tags.map((tag, index) => (
                <div key={index} className="entry-row">
                  <input
                    type="text"
                    className="form-input entry-name"
                    placeholder="Tag name"
                    value={tag.name}
                    onChange={(e) => updateTag(index, 'name', e.target.value)}
                  />
                  <span className="entry-separator">=</span>
                  <input
                    type="text"
                    className="form-input entry-value"
                    placeholder="Tag value"
                    value={tag.value}
                    onChange={(e) => updateTag(index, 'value', e.target.value)}
                  />
                  <button 
                    className="btn-remove"
                    onClick={() => removeTag(index)}
                    disabled={tags.length === 1}
                  >
                    <Trash2 size={14} />
                  </button>
                </div>
              ))}
            </div>
          </div>

          {/* Fields */}
          <div className="form-section">
            <div className="form-section-header">
              <label className="form-section-label">
                <ListPlus size={14} />
                Fields (Values)
              </label>
              <button className="btn-add" onClick={addField}>
                <ListPlus size={14} />
                Add Field
              </button>
            </div>
            <div className="entries-list">
              {fields.map((field, index) => (
                <div key={index} className="entry-row field-row">
                  <input
                    type="text"
                    className="form-input entry-name"
                    placeholder="Field name"
                    value={field.name}
                    onChange={(e) => updateField(index, 'name', e.target.value)}
                  />
                  <span className="entry-separator">=</span>
                  <input
                    type="text"
                    className="form-input entry-value"
                    placeholder="Value"
                    value={field.value}
                    onChange={(e) => updateField(index, 'value', e.target.value)}
                  />
                  <select
                    className="form-select entry-type"
                    value={field.type}
                    onChange={(e) => updateField(index, 'type', e.target.value)}
                  >
                    <option value="float">Float</option>
                    <option value="integer">Integer</option>
                    <option value="string">String</option>
                    <option value="boolean">Boolean</option>
                  </select>
                  <button 
                    className="btn-remove"
                    onClick={() => removeField(index)}
                    disabled={fields.length === 1}
                  >
                    <Trash2 size={14} />
                  </button>
                </div>
              ))}
            </div>
          </div>

          {/* Timestamp */}
          <div className="form-section">
            <label className="form-section-label">
              <Clock size={14} />
              Timestamp
            </label>
            <div className="timestamp-options">
              <label className="checkbox-label">
                <input
                  type="checkbox"
                  checked={useCurrentTime}
                  onChange={(e) => setUseCurrentTime(e.target.checked)}
                />
                Use current time
              </label>
              {!useCurrentTime && (
                <div className="timestamp-input-group">
                  <input
                    type="text"
                    className="form-input"
                    placeholder="Unix timestamp"
                    value={timestamp}
                    onChange={(e) => {
                      setTimestamp(e.target.value);
                      updatePreview();
                    }}
                  />
                  <select
                    className="form-select"
                    value={precision}
                    onChange={(e) => setPrecision(e.target.value as any)}
                  >
                    <option value="ns">Nanoseconds</option>
                    <option value="us">Microseconds</option>
                    <option value="ms">Milliseconds</option>
                    <option value="s">Seconds</option>
                  </select>
                </div>
              )}
            </div>
          </div>

          {/* Line Protocol Preview */}
          <div className="form-section">
            <label className="form-section-label">Line Protocol Preview</label>
            <div className="protocol-preview">
              <code>{lineProtocolPreview || 'Fill in the fields above to see the preview'}</code>
            </div>
          </div>

          {/* Write Result */}
          {writeResult && (
            <div className={`write-result ${writeResult.success ? 'success' : 'error'}`}>
              {writeResult.message}
            </div>
          )}

          {/* Submit Button */}
          <button 
            className="btn-primary btn-write"
            onClick={handleWrite}
            disabled={isWriting || !measurement || fields.every(f => !f.name || !f.value)}
          >
            {isWriting ? (
              <>
                <div className="loading-spinner" />
                Writing...
              </>
            ) : (
              <>
                <Send size={16} />
                Write Data
              </>
            )}
          </button>
        </div>
      </div>
    </div>
  );
}
