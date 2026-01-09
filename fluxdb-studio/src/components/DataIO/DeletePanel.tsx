// DeletePanel Component - Visual data deletion interface
import { useState } from 'react';
import { 
  Trash2, 
  AlertTriangle, 
  Database,
  Search,
  Clock,
  Tag as TagIcon
} from 'lucide-react';
import { useDatabaseStore } from '../../stores/databaseStore';
import { useQueryStore } from '../../stores/queryStore';
import { fluxdb } from '../../services/fluxdb';

interface DeleteCondition {
  type: 'time' | 'tag';
  field: string;
  operator: '=' | '>' | '<' | '>=' | '<=';
  value: string;
}

export function DeletePanel() {
  const { selectedDatabase } = useDatabaseStore();
  const { executeQuery } = useQueryStore();
  
  const [measurement, setMeasurement] = useState('');
  const [conditions, setConditions] = useState<DeleteCondition[]>([
    { type: 'time', field: 'time', operator: '>', value: '' }
  ]);
  const [isDeleting, setIsDeleting] = useState(false);
  const [isPreviewing, setIsPreviewing] = useState(false);
  const [previewCount, setPreviewCount] = useState<number | null>(null);
  const [deleteResult, setDeleteResult] = useState<{ success: boolean; message: string } | null>(null);
  const [showConfirm, setShowConfirm] = useState(false);

  // Add a condition
  const addCondition = () => {
    setConditions([
      ...conditions, 
      { type: 'tag', field: '', operator: '=', value: '' }
    ]);
  };

  // Remove a condition
  const removeCondition = (index: number) => {
    if (conditions.length > 1) {
      setConditions(conditions.filter((_, i) => i !== index));
    }
  };

  // Update a condition
  const updateCondition = (index: number, key: keyof DeleteCondition, value: string) => {
    const newConditions = [...conditions];
    (newConditions[index] as any)[key] = value;
    setConditions(newConditions);
  };

  // Generate WHERE clause
  const generateWhereClause = () => {
    const validConditions = conditions.filter(c => c.field && c.value);
    if (validConditions.length === 0) return '';
    
    return validConditions.map(c => {
      if (c.type === 'time') {
        return `time ${c.operator} '${c.value}'`;
      }
      return `${c.field} ${c.operator} '${c.value}'`;
    }).join(' AND ');
  };

  // Preview affected rows
  const handlePreview = async () => {
    if (!selectedDatabase || !measurement) {
      setDeleteResult({ success: false, message: 'Please select a database and measurement' });
      return;
    }

    setIsPreviewing(true);
    setPreviewCount(null);

    try {
      const whereClause = generateWhereClause();
      const query = whereClause 
        ? `SELECT COUNT(*) FROM ${measurement} WHERE ${whereClause}`
        : `SELECT COUNT(*) FROM ${measurement}`;
      
      const result = await fluxdb.query(selectedDatabase, query);
      
      // Extract count from result
      if (result.rows && result.rows.length > 0) {
        const count = result.rows[0].values?.[0] || 0;
        setPreviewCount(typeof count === 'number' ? count : parseInt(String(count), 10) || 0);
      } else {
        setPreviewCount(0);
      }
    } catch (error: any) {
      setDeleteResult({ 
        success: false, 
        message: `Preview failed: ${error.message}`
      });
    } finally {
      setIsPreviewing(false);
    }
  };

  // Execute delete
  const handleDelete = async () => {
    if (!selectedDatabase || !measurement) {
      setDeleteResult({ success: false, message: 'Please select a database and measurement' });
      return;
    }

    const whereClause = generateWhereClause();
    if (!whereClause) {
      setDeleteResult({ 
        success: false, 
        message: 'DELETE requires at least one condition for safety'
      });
      return;
    }

    setIsDeleting(true);
    setDeleteResult(null);
    setShowConfirm(false);

    try {
      // Note: This would require backend support for DELETE statements
      const query = `DELETE FROM ${measurement} WHERE ${whereClause}`;
      await executeQuery(query);
      
      setDeleteResult({ 
        success: true, 
        message: `Successfully deleted data from ${measurement}` 
      });
      
      // Reset form
      setMeasurement('');
      setConditions([{ type: 'time', field: 'time', operator: '>', value: '' }]);
      setPreviewCount(null);
    } catch (error: any) {
      setDeleteResult({ 
        success: false, 
        message: error.message || 'Failed to delete data'
      });
    } finally {
      setIsDeleting(false);
    }
  };

  return (
    <div className="delete-panel">
      <div className="delete-panel-header">
        <div className="delete-panel-title">
          <Trash2 size={20} />
          <span>Delete Data</span>
        </div>
        <p className="delete-panel-subtitle">
          Remove data points based on conditions
        </p>
      </div>

      <div className="delete-panel-content">
        <div className="warning-banner">
          <AlertTriangle size={20} />
          <div>
            <strong>Warning:</strong> This operation cannot be undone. 
            Always preview the affected data before deleting.
          </div>
        </div>

        <div className="delete-form">
          {/* Database */}
          <div className="form-section">
            <label className="form-section-label">
              <Database size={14} />
              Target Database
            </label>
            <input
              type="text"
              className="form-input"
              value={selectedDatabase || ''}
              disabled
            />
          </div>

          {/* Measurement */}
          <div className="form-section">
            <label className="form-section-label">
              Measurement Name
            </label>
            <input
              type="text"
              className="form-input"
              placeholder="e.g., temperature"
              value={measurement}
              onChange={(e) => setMeasurement(e.target.value)}
            />
          </div>

          {/* Conditions */}
          <div className="form-section">
            <div className="form-section-header">
              <label className="form-section-label">
                Delete Conditions
              </label>
              <button className="btn-add" onClick={addCondition}>
                Add Condition
              </button>
            </div>
            <div className="entries-list">
              {conditions.map((condition, index) => (
                <div key={index} className="entry-row condition-row">
                  <select
                    className="form-select condition-type"
                    value={condition.type}
                    onChange={(e) => updateCondition(index, 'type', e.target.value)}
                  >
                    <option value="time">Time</option>
                    <option value="tag">Tag</option>
                  </select>
                  {condition.type === 'tag' && (
                    <input
                      type="text"
                      className="form-input condition-field"
                      placeholder="Field name"
                      value={condition.field}
                      onChange={(e) => updateCondition(index, 'field', e.target.value)}
                    />
                  )}
                  <select
                    className="form-select condition-op"
                    value={condition.operator}
                    onChange={(e) => updateCondition(index, 'operator', e.target.value)}
                  >
                    <option value="=">=</option>
                    <option value=">">&gt;</option>
                    <option value="<">&lt;</option>
                    <option value=">=">&gt;=</option>
                    <option value="<=">&lt;=</option>
                  </select>
                  <input
                    type="text"
                    className="form-input condition-value"
                    placeholder={condition.type === 'time' ? 'ISO timestamp' : 'Value'}
                    value={condition.value}
                    onChange={(e) => updateCondition(index, 'value', e.target.value)}
                  />
                  <button 
                    className="btn-remove"
                    onClick={() => removeCondition(index)}
                    disabled={conditions.length === 1}
                  >
                    <Trash2 size={14} />
                  </button>
                </div>
              ))}
            </div>
          </div>

          {/* Preview Section */}
          <div className="form-section">
            <button 
              className="btn-secondary btn-preview"
              onClick={handlePreview}
              disabled={isPreviewing || !measurement}
            >
              <Search size={16} />
              {isPreviewing ? 'Previewing...' : 'Preview Affected Data'}
            </button>
            
            {previewCount !== null && (
              <div className="preview-result">
                <strong>{previewCount.toLocaleString()}</strong> data point(s) will be deleted
              </div>
            )}
          </div>

          {/* Result Message */}
          {deleteResult && (
            <div className={`delete-result ${deleteResult.success ? 'success' : 'error'}`}>
              {deleteResult.message}
            </div>
          )}

          {/* Delete Button */}
          {!showConfirm ? (
            <button 
              className="btn-danger btn-delete"
              onClick={() => setShowConfirm(true)}
              disabled={!measurement || conditions.every(c => !c.value)}
            >
              <Trash2 size={16} />
              Delete Data
            </button>
          ) : (
            <div className="confirm-delete">
              <p>Are you sure you want to delete this data?</p>
              <div className="confirm-buttons">
                <button 
                  className="btn-secondary"
                  onClick={() => setShowConfirm(false)}
                >
                  Cancel
                </button>
                <button 
                  className="btn-danger"
                  onClick={handleDelete}
                  disabled={isDeleting}
                >
                  {isDeleting ? 'Deleting...' : 'Confirm Delete'}
                </button>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
