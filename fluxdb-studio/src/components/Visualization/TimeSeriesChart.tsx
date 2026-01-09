// TimeSeriesChart Component - Real-time time-series visualization
import { useEffect, useRef, useState } from 'react';
import { TrendingUp, ZoomIn, ZoomOut, RefreshCw } from 'lucide-react';

export interface DataPoint {
  time: number | Date;
  value: number;
  series?: string;
}

export interface TimeSeriesChartProps {
  data: DataPoint[];
  title?: string;
  yAxisLabel?: string;
  xAxisLabel?: string;
  showLegend?: boolean;
  animate?: boolean;
  height?: number;
  colors?: string[];
}

export function TimeSeriesChart({
  data,
  title = 'Time Series',
  yAxisLabel = 'Value',
  xAxisLabel = 'Time',
  showLegend = true,
  animate = true,
  height = 300,
  colors = ['#3b82f6', '#8b5cf6', '#06b6d4', '#10b981', '#f59e0b'],
}: TimeSeriesChartProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const [zoom, setZoom] = useState(1);
  const [pan, setPan] = useState(0);

  // Group data by series
  const seriesData = data.reduce((acc, point) => {
    const series = point.series || 'default';
    if (!acc[series]) acc[series] = [];
    acc[series].push(point);
    return acc;
  }, {} as Record<string, DataPoint[]>);

  const seriesNames = Object.keys(seriesData);

  useEffect(() => {
    const canvas = canvasRef.current;
    const container = containerRef.current;
    if (!canvas || !container || data.length === 0) return;

    const ctx = canvas.getContext('2d');
    if (!ctx) return;

    // Set canvas size
    const rect = container.getBoundingClientRect();
    const dpr = window.devicePixelRatio || 1;
    canvas.width = rect.width * dpr;
    canvas.height = height * dpr;
    canvas.style.width = `${rect.width}px`;
    canvas.style.height = `${height}px`;
    ctx.scale(dpr, dpr);

    const width = rect.width;
    const padding = { top: 40, right: 20, bottom: 40, left: 60 };
    const chartWidth = width - padding.left - padding.right;
    const chartHeight = height - padding.top - padding.bottom;

    // Clear
    ctx.fillStyle = 'transparent';
    ctx.fillRect(0, 0, width, height);

    // Calculate data range
    const allTimes = data.map(d => new Date(d.time).getTime());
    const allValues = data.map(d => d.value).filter(v => !isNaN(v));
    
    const minTime = Math.min(...allTimes);
    const maxTime = Math.max(...allTimes);
    const minValue = Math.min(...allValues) * 0.95;
    const maxValue = Math.max(...allValues) * 1.05;

    // Apply zoom and pan
    const timeRange = (maxTime - minTime) / zoom;
    const zoomedMinTime = minTime + pan * (maxTime - minTime);
    const zoomedMaxTime = zoomedMinTime + timeRange;

    // Scale functions
    const scaleX = (time: number) => {
      return padding.left + ((time - zoomedMinTime) / (zoomedMaxTime - zoomedMinTime)) * chartWidth;
    };

    const scaleY = (value: number) => {
      return padding.top + chartHeight - ((value - minValue) / (maxValue - minValue)) * chartHeight;
    };

    // Draw grid
    ctx.strokeStyle = 'rgba(71, 85, 105, 0.3)';
    ctx.lineWidth = 1;

    // Y-axis grid lines
    const yTicks = 5;
    for (let i = 0; i <= yTicks; i++) {
      const y = padding.top + (i / yTicks) * chartHeight;
      ctx.beginPath();
      ctx.moveTo(padding.left, y);
      ctx.lineTo(width - padding.right, y);
      ctx.stroke();

      // Y-axis labels
      const value = maxValue - (i / yTicks) * (maxValue - minValue);
      ctx.fillStyle = '#94a3b8';
      ctx.font = '11px Inter, sans-serif';
      ctx.textAlign = 'right';
      ctx.fillText(value.toFixed(1), padding.left - 8, y + 4);
    }

    // X-axis labels
    const xTicks = 6;
    ctx.textAlign = 'center';
    for (let i = 0; i <= xTicks; i++) {
      const time = zoomedMinTime + (i / xTicks) * (zoomedMaxTime - zoomedMinTime);
      const x = scaleX(time);
      const date = new Date(time);
      ctx.fillText(
        date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }),
        x,
        height - padding.bottom + 20
      );
    }

    // Draw lines for each series
    seriesNames.forEach((series, seriesIndex) => {
      const points = seriesData[series];
      if (points.length < 2) return;

      const color = colors[seriesIndex % colors.length];

      // Sort by time
      const sorted = [...points].sort(
        (a, b) => new Date(a.time).getTime() - new Date(b.time).getTime()
      );

      // Draw gradient fill
      ctx.beginPath();
      ctx.moveTo(scaleX(new Date(sorted[0].time).getTime()), scaleY(sorted[0].value));
      sorted.forEach((point) => {
        ctx.lineTo(scaleX(new Date(point.time).getTime()), scaleY(point.value));
      });
      ctx.lineTo(scaleX(new Date(sorted[sorted.length - 1].time).getTime()), height - padding.bottom);
      ctx.lineTo(scaleX(new Date(sorted[0].time).getTime()), height - padding.bottom);
      ctx.closePath();

      const gradient = ctx.createLinearGradient(0, padding.top, 0, height - padding.bottom);
      gradient.addColorStop(0, color + '40');
      gradient.addColorStop(1, color + '00');
      ctx.fillStyle = gradient;
      ctx.fill();

      // Draw line
      ctx.beginPath();
      ctx.strokeStyle = color;
      ctx.lineWidth = 2;
      sorted.forEach((point, index) => {
        const x = scaleX(new Date(point.time).getTime());
        const y = scaleY(point.value);
        if (index === 0) {
          ctx.moveTo(x, y);
        } else {
          ctx.lineTo(x, y);
        }
      });
      ctx.stroke();

      // Draw points
      sorted.forEach((point) => {
        const x = scaleX(new Date(point.time).getTime());
        const y = scaleY(point.value);
        ctx.beginPath();
        ctx.arc(x, y, 4, 0, Math.PI * 2);
        ctx.fillStyle = color;
        ctx.fill();
        ctx.strokeStyle = '#0f172a';
        ctx.lineWidth = 2;
        ctx.stroke();
      });
    });

  }, [data, height, zoom, pan, colors, seriesData, seriesNames]);

  const handleZoomIn = () => setZoom(prev => Math.min(prev * 1.5, 10));
  const handleZoomOut = () => setZoom(prev => Math.max(prev / 1.5, 1));
  const handleReset = () => {
    setZoom(1);
    setPan(0);
  };

  return (
    <div className="time-series-chart">
      <div className="chart-header">
        <div className="chart-title">
          <TrendingUp size={16} />
          <span>{title}</span>
        </div>
        <div className="chart-controls">
          <button className="chart-btn" onClick={handleZoomIn} title="Zoom In">
            <ZoomIn size={14} />
          </button>
          <button className="chart-btn" onClick={handleZoomOut} title="Zoom Out">
            <ZoomOut size={14} />
          </button>
          <button className="chart-btn" onClick={handleReset} title="Reset">
            <RefreshCw size={14} />
          </button>
        </div>
      </div>
      
      <div ref={containerRef} className="chart-container">
        <canvas ref={canvasRef} />
      </div>

      {showLegend && seriesNames.length > 1 && (
        <div className="chart-legend">
          {seriesNames.map((series, index) => (
            <div key={series} className="legend-item">
              <span 
                className="legend-color" 
                style={{ backgroundColor: colors[index % colors.length] }}
              />
              <span className="legend-label">{series}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

export default TimeSeriesChart;
