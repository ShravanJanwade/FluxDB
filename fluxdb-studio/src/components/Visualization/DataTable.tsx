// DataTable Component - Virtual scrolling data table
import { useState, useMemo, useCallback, useRef, useEffect } from 'react';
import { 
  ChevronUp, 
  ChevronDown, 
  Download, 
  Copy,
  Search,
  Filter
} from 'lucide-react';

export interface Column {
  key: string;
  title: string;
  width?: number;
  sortable?: boolean;
  filterable?: boolean;
  render?: (value: any, row: any) => React.ReactNode;
}

export interface DataTableProps {
  columns: Column[];
  data: any[];
  rowHeight?: number;
  maxHeight?: number;
  onRowClick?: (row: any, index: number) => void;
  selectable?: boolean;
  exportable?: boolean;
}

export function DataTable({
  columns,
  data,
  rowHeight = 40,
  maxHeight = 500,
  onRowClick,
  selectable = false,
  exportable = true,
}: DataTableProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const [scrollTop, setScrollTop] = useState(0);
  const [sortColumn, setSortColumn] = useState<string | null>(null);
  const [sortDirection, setSortDirection] = useState<'asc' | 'desc'>('asc');
  const [filter, setFilter] = useState('');
  const [selectedRows, setSelectedRows] = useState<Set<number>>(new Set());

  // Filter data
  const filteredData = useMemo(() => {
    if (!filter) return data;
    const lowerFilter = filter.toLowerCase();
    return data.filter(row => 
      columns.some(col => {
        const value = row[col.key];
        return value?.toString().toLowerCase().includes(lowerFilter);
      })
    );
  }, [data, filter, columns]);

  // Sort data
  const sortedData = useMemo(() => {
    if (!sortColumn) return filteredData;
    
    return [...filteredData].sort((a, b) => {
      const aVal = a[sortColumn];
      const bVal = b[sortColumn];
      
      if (aVal === null || aVal === undefined) return 1;
      if (bVal === null || bVal === undefined) return -1;
      
      let comparison = 0;
      if (typeof aVal === 'number' && typeof bVal === 'number') {
        comparison = aVal - bVal;
      } else {
        comparison = String(aVal).localeCompare(String(bVal));
      }
      
      return sortDirection === 'asc' ? comparison : -comparison;
    });
  }, [filteredData, sortColumn, sortDirection]);

  // Virtual scrolling calculations
  const visibleCount = Math.ceil(maxHeight / rowHeight) + 2;
  const startIndex = Math.max(0, Math.floor(scrollTop / rowHeight) - 1);
  const endIndex = Math.min(sortedData.length, startIndex + visibleCount);
  const visibleRows = sortedData.slice(startIndex, endIndex);
  const totalHeight = sortedData.length * rowHeight;
  const offsetY = startIndex * rowHeight;

  // Handle scroll
  const handleScroll = useCallback((e: React.UIEvent<HTMLDivElement>) => {
    setScrollTop(e.currentTarget.scrollTop);
  }, []);

  // Handle sort
  const handleSort = (key: string) => {
    if (sortColumn === key) {
      setSortDirection(prev => prev === 'asc' ? 'desc' : 'asc');
    } else {
      setSortColumn(key);
      setSortDirection('asc');
    }
  };

  // Handle row selection
  const toggleRowSelection = (index: number) => {
    setSelectedRows(prev => {
      const newSet = new Set(prev);
      if (newSet.has(index)) {
        newSet.delete(index);
      } else {
        newSet.add(index);
      }
      return newSet;
    });
  };

  // Export to CSV
  const exportCSV = () => {
    const headers = columns.map(c => c.title).join(',');
    const rows = sortedData.map(row => 
      columns.map(col => {
        const value = row[col.key];
        const str = value?.toString() || '';
        return str.includes(',') ? `"${str}"` : str;
      }).join(',')
    );
    const csv = [headers, ...rows].join('\n');
    
    const blob = new Blob([csv], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'data-export.csv';
    a.click();
    URL.revokeObjectURL(url);
  };

  // Copy to clipboard
  const copyToClipboard = () => {
    const headers = columns.map(c => c.title).join('\t');
    const rows = sortedData.map(row => 
      columns.map(col => row[col.key]?.toString() || '').join('\t')
    );
    const text = [headers, ...rows].join('\n');
    navigator.clipboard.writeText(text);
  };

  return (
    <div className="data-table-wrapper">
      <div className="data-table-toolbar">
        <div className="data-table-search">
          <Search size={14} />
          <input
            type="text"
            placeholder="Filter data..."
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
          />
        </div>
        
        {exportable && (
          <div className="data-table-actions">
            <button className="btn-icon-sm" onClick={copyToClipboard} title="Copy to clipboard">
              <Copy size={14} />
            </button>
            <button className="btn-icon-sm" onClick={exportCSV} title="Export CSV">
              <Download size={14} />
            </button>
          </div>
        )}
      </div>

      <div className="data-table-info">
        Showing {sortedData.length.toLocaleString()} of {data.length.toLocaleString()} rows
        {selectedRows.size > 0 && ` (${selectedRows.size} selected)`}
      </div>

      <div 
        ref={containerRef}
        className="data-table-container"
        style={{ maxHeight }}
        onScroll={handleScroll}
      >
        <table className="data-table">
          <thead>
            <tr>
              {selectable && (
                <th className="select-col">
                  <input 
                    type="checkbox"
                    onChange={() => {
                      if (selectedRows.size === sortedData.length) {
                        setSelectedRows(new Set());
                      } else {
                        setSelectedRows(new Set(sortedData.map((_, i) => i)));
                      }
                    }}
                    checked={selectedRows.size === sortedData.length && sortedData.length > 0}
                  />
                </th>
              )}
              {columns.map(col => (
                <th 
                  key={col.key}
                  style={{ width: col.width }}
                  className={col.sortable !== false ? 'sortable' : ''}
                  onClick={() => col.sortable !== false && handleSort(col.key)}
                >
                  <div className="th-content">
                    <span>{col.title}</span>
                    {sortColumn === col.key && (
                      sortDirection === 'asc' 
                        ? <ChevronUp size={14} />
                        : <ChevronDown size={14} />
                    )}
                  </div>
                </th>
              ))}
            </tr>
          </thead>
          <tbody style={{ height: totalHeight }}>
            <tr style={{ height: offsetY }} className="spacer" />
            {visibleRows.map((row, localIndex) => {
              const globalIndex = startIndex + localIndex;
              return (
                <tr 
                  key={globalIndex}
                  className={`${selectedRows.has(globalIndex) ? 'selected' : ''}`}
                  onClick={() => {
                    if (selectable) toggleRowSelection(globalIndex);
                    onRowClick?.(row, globalIndex);
                  }}
                  style={{ height: rowHeight }}
                >
                  {selectable && (
                    <td className="select-col">
                      <input 
                        type="checkbox"
                        checked={selectedRows.has(globalIndex)}
                        onChange={() => toggleRowSelection(globalIndex)}
                        onClick={(e) => e.stopPropagation()}
                      />
                    </td>
                  )}
                  {columns.map(col => (
                    <td key={col.key}>
                      {col.render 
                        ? col.render(row[col.key], row)
                        : row[col.key]?.toString() ?? '-'
                      }
                    </td>
                  ))}
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

export default DataTable;
