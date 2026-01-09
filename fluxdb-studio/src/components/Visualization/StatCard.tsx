// StatCard Component - Premium metric display cards
import { TrendingUp, TrendingDown, Minus } from 'lucide-react';
import { ReactNode, useEffect, useState } from 'react';

export interface StatCardProps {
  title: string;
  value: number | string;
  unit?: string;
  icon?: ReactNode;
  trend?: 'up' | 'down' | 'neutral';
  trendValue?: string;
  format?: 'number' | 'currency' | 'percentage' | 'bytes' | 'duration';
  animate?: boolean;
  variant?: 'default' | 'primary' | 'success' | 'warning' | 'error';
}

export function StatCard({
  title,
  value,
  unit,
  icon,
  trend,
  trendValue,
  format = 'number',
  animate = true,
  variant = 'default',
}: StatCardProps) {
  const [displayValue, setDisplayValue] = useState(typeof value === 'number' ? 0 : value);
  const [isAnimating, setIsAnimating] = useState(false);

  // Animate number value
  useEffect(() => {
    if (!animate || typeof value !== 'number') {
      setDisplayValue(value);
      return;
    }

    setIsAnimating(true);
    const duration = 1000;
    const startTime = performance.now();
    const startValue = typeof displayValue === 'number' ? displayValue : 0;
    const endValue = value;

    const animateValue = (currentTime: number) => {
      const elapsed = currentTime - startTime;
      const progress = Math.min(elapsed / duration, 1);
      
      // Ease out cubic
      const eased = 1 - Math.pow(1 - progress, 3);
      const current = startValue + (endValue - startValue) * eased;
      
      setDisplayValue(current);

      if (progress < 1) {
        requestAnimationFrame(animateValue);
      } else {
        setIsAnimating(false);
      }
    };

    requestAnimationFrame(animateValue);
  }, [value, animate]);

  // Format value for display
  const formatValue = (val: number | string): string => {
    if (typeof val === 'string') return val;

    switch (format) {
      case 'currency':
        return new Intl.NumberFormat('en-US', {
          style: 'currency',
          currency: 'USD',
          minimumFractionDigits: 0,
          maximumFractionDigits: 2,
        }).format(val);

      case 'percentage':
        return `${val.toFixed(1)}%`;

      case 'bytes':
        const units = ['B', 'KB', 'MB', 'GB', 'TB'];
        let unitIndex = 0;
        let size = val;
        while (size >= 1024 && unitIndex < units.length - 1) {
          size /= 1024;
          unitIndex++;
        }
        return `${size.toFixed(1)} ${units[unitIndex]}`;

      case 'duration':
        if (val < 1000) return `${val.toFixed(0)}ms`;
        if (val < 60000) return `${(val / 1000).toFixed(1)}s`;
        if (val < 3600000) return `${(val / 60000).toFixed(1)}m`;
        return `${(val / 3600000).toFixed(1)}h`;

      default:
        if (val >= 1e9) return `${(val / 1e9).toFixed(2)}B`;
        if (val >= 1e6) return `${(val / 1e6).toFixed(2)}M`;
        if (val >= 1e3) return `${(val / 1e3).toFixed(2)}K`;
        return val.toLocaleString(undefined, { maximumFractionDigits: 2 });
    }
  };

  const renderTrend = () => {
    if (!trend) return null;

    const TrendIcon = trend === 'up' ? TrendingUp : trend === 'down' ? TrendingDown : Minus;
    const trendClass = trend === 'up' ? 'trend-up' : trend === 'down' ? 'trend-down' : 'trend-neutral';

    return (
      <div className={`stat-trend ${trendClass}`}>
        <TrendIcon size={14} />
        {trendValue && <span>{trendValue}</span>}
      </div>
    );
  };

  return (
    <div className={`stat-card stat-card-${variant}`}>
      <div className="stat-card-header">
        <span className="stat-title">{title}</span>
        {icon && <div className="stat-icon">{icon}</div>}
      </div>
      
      <div className="stat-value-container">
        <span className={`stat-value ${isAnimating ? 'animating' : ''}`}>
          {formatValue(displayValue)}
        </span>
        {unit && <span className="stat-unit">{unit}</span>}
      </div>
      
      {renderTrend()}
    </div>
  );
}

export default StatCard;
