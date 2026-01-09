// UpdatePanel Component - Visual data update interface
import { useState } from 'react';
import { 
  Edit3, 
  Search, 
  Save,
  Database,
  RefreshCcw,
  AlertCircle
} from 'lucide-react';
import { useDatabaseStore } from '../../stores/databaseStore';
import { useQueryStore } from '../../stores/queryStore';
import { fluxdb } from '../../services/fluxdb';

interface UpdateCondition {
  field: string;
  operator: '=' | '>' | '<' | '>=' | '<=';
  value: string;
}

interface UpdateAssignment {
  field: string;
  newValue: string;
}

export function UpdatePanel() {
  const { selectedDatabase } = useDatabaseStore();
  const { executeQuery } = useQueryStore();

  const [measurement, setMeasurement] = useState('');
  const [conditions, setConditions] = useState<UpdateCondition[]>([
    { field: '', operator: '=', value: '' }
  ]);
  const [assignments, setAssignments] = useState<UpdateAssignment[]>([
    { field: '', newValue: '' }
  ]);
  const [isUpdating, setIsUpdating] = useState(false);
  const [isSearching, setIsSearching] = useState(false);
  const [matchCount, setMatchCount] = useState<number | null>(null);
  const [updateResult, setUpdateResult] = useState<{ success: boolean; message: string } | null>(null);

  // Condition handlers
  const addCondition = () => {
    setConditions([...conditions, { field: '', operator: '=', value: '' }]);
  };

  const removeCondition = (index: number) => {
    if (conditions.length > 1) {
      setConditions(conditions.filter((_, i) => i !== index));
    }
  };

  const updateCondition = (index: number, key: keyof UpdateCondition, value: string) => {
    const newConditions = [...conditions];
    (newConditions[index] as any)[key] = value;
    setConditions(newConditions);
  };

  // Assignment handlers
  const addAssignment = () => {
    setAssignments([...assignments, { field: '', newValue: '' }]);
  };

  const removeAssignment = (index: number) => {
    if (assignments.length > 1) {
      setAssignments(assignments.filter((_, i) => i !== index));
    }
  };

  const updateAssignment = (index: number, key: keyof UpdateAssignment, value: string) => {
    const newAssignments = [...assignments];
    (newAssignments[index] as any)[key] = value;
    setAssignments(newAssignments);
  };

  // Generate WHERE clause
  const generateWhereClause = () => {
    const validConditions = conditions.filter(c => c.field && c.value);
    if (validConditions.length === 0) return '';
    
    return validConditions.map(c => {
      return `${c.field} ${c.operator} '${c.value}'`;
    }).join(' AND ');
  };

  // Generate SET clause
  const generateSetClause = () => {
    const validAssignments = assignments.filter(a => a.field && a.newValue);
    if (validAssignments.length === 0) return '';
    
    return validAssignments.map(a => {
      return `${a.field} = ${a.newValue}`;
    }).join(', ');
  };

  // Search for matching records
  const handleSearch = async () => {
    if (!selectedDatabase || !measurement) {
      setUpdateResult({ success: false, message: 'Please select a database and measurement' });
      return;
    }

    setIsSearching(true);
    setMatchCount(null);

    try {
      const whereClause = generateWhereClause();
      const query = whereClause 
        ? `SELECT COUNT(*) FROM ${measurement} WHERE ${whereClause}`
        : `SELECT COUNT(*) FROM ${measurement}`;
      
      const result = await fluxdb.query(selectedDatabase, query);
      
      if (result.rows && result.rows.length > 0) {
        const count = result.rows[0].values?.[0] || 0;
        setMatchCount(typeof count === 'number' ? count : parseInt(String(count), 10) || 0);
      } else {
        setMatchCount(0);
      }
    } catch (error: any) {
      setUpdateResult({ 
        success: false, 
        message: `Search failed: ${error.message}`
      });
    } finally {
      setIsSearching(false);
    }
  };

  // Execute update
  const handleUpdate = async () => {
    if (!selectedDatabase || !measurement) {
      setUpdateResult({ success: false, message: 'Please select a database and measurement' });
      return;
    }

    const setClause = generateSetClause();
    if (!setClause) {
      setUpdateResult({ success: false, message: 'Please specify at least one assignment' });
      return;
    }

    const whereClause = generateWhereClause();
    if (!whereClause) {
      setUpdateResult({ 
        success: false, 
        message: 'UPDATE requires at least one WHERE condition for safety'
      });
      return;
    }

    setIsUpdating(true);
    setUpdateResult(null);

    try {
      // Generate UPDATE statement
      const query = `UPDATE ${measurement} SET ${setClause} WHERE ${whereClause}`;
      await executeQuery(query);
      
      setUpdateResult({ 
        success: true, 
        message: `Successfully updated data in ${measurement}` 
      });
      
      // Reset form
      setMeasurement('');
      setConditions([{ field: '', operator: '=', value: '' }]);
      setAssignments([{ field: '', newValue: '' }]);
      setMatchCount(null);
    } catch (error: any) {
      setUpdateResult({ 
        success: false, 
        message: error.message || 'Failed to update data'
      });
    } finally {
      setIsUpdating(false);
    }
  };

  return (
    <div className="update-panel">
      <div className="update-panel-header">
        <div className="update-panel-title">
          <Edit3 size={20} />
          <span>Update Data</span>
        </div>
        <p className="update-panel-subtitle">
          Modify existing data points based on conditions
        </p>
      </div>

      <div className="update-panel-content">
        <div className="info-banner">
          <AlertCircle size={18} />
          <span>Note: Time-series data updates may affect data integrity. Use with caution.</span>
        </div>

        <div className="update-form">
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

          {/* WHERE Conditions */}
          <div className="form-section">
            <div className="form-section-header">
              <label className="form-section-label">
                WHERE Conditions
              </label>
              <button className="btn-add" onClick={addCondition}>
                Add Condition
              </button>
            </div>
            <div className="entries-list">
              {conditions.map((condition, index) => (
                <div key={index} className="entry-row">
                  <input
                    type="text"
                    className="form-input"
                    placeholder="Field name"
                    value={condition.field}
                    onChange={(e) => updateCondition(index, 'field', e.target.value)}
                  />
                  <select
                    className="form-select"
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
                    className="form-input"
                    placeholder="Value"
                    value={condition.value}
                    onChange={(e) => updateCondition(index, 'value', e.target.value)}
                  />
                  <button 
                    className="btn-remove"
                    onClick={() => removeCondition(index)}
                    disabled={conditions.length === 1}
                  >
                    ×
                  </button>
                </div>
              ))}
            </div>
          </div>

          {/* SET Assignments */}
          <div className="form-section">
            <div className="form-section-header">
              <label className="form-section-label">
                SET Values
              </label>
              <button className="btn-add" onClick={addAssignment}>
                Add Assignment
              </button>
            </div>
            <div className="entries-list">
              {assignments.map((assignment, index) => (
                <div key={index} className="entry-row">
                  <input
                    type="text"
                    className="form-input"
                    placeholder="Field name"
                    value={assignment.field}
                    onChange={(e) => updateAssignment(index, 'field', e.target.value)}
                  />
                  <span className="entry-separator">→</span>
                  <input
                    type="text"
                    className="form-input"
                    placeholder="New value"
                    value={assignment.newValue}
                    onChange={(e) => updateAssignment(index, 'newValue', e.target.value)}
                  />
                  <button 
                    className="btn-remove"
                    onClick={() => removeAssignment(index)}
                    disabled={assignments.length === 1}
                  >
                    ×
                  </button>
                </div>
              ))}
            </div>
          </div>

          {/* Search Preview */}
          <div className="form-section">
            <button 
              className="btn-secondary"
              onClick={handleSearch}
              disabled={isSearching || !measurement}
            >
              <Search size={16} />
              {isSearching ? 'Searching...' : 'Search Matching Records'}
            </button>
            
            {matchCount !== null && (
              <div className="search-result">
                <strong>{matchCount.toLocaleString()}</strong> record(s) match your criteria
              </div>
            )}
          </div>

          {/* Result Message */}
          {updateResult && (
            <div className={`update-result ${updateResult.success ? 'success' : 'error'}`}>
              {updateResult.message}
            </div>
          )}

          {/* Update Button */}
          <button 
            className="btn-primary btn-update"
            onClick={handleUpdate}
            disabled={isUpdating || !measurement || 
              conditions.every(c => !c.field || !c.value) ||
              assignments.every(a => !a.field || !a.newValue)}
          >
            <Save size={16} />
            {isUpdating ? 'Updating...' : 'Update Data'}
          </button>
        </div>
      </div>
    </div>
  );
}
