/** Building blocks shared by every console screen. */

import {
  useCallback,
  useEffect,
  useId,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from "react";
// The `core` entry takes the echarts instance we register components on,
// instead of pulling in the full pre-bundled build.
import ReactECharts from "echarts-for-react/lib/core";
import * as echarts from "echarts/core";
import { BarChart, LineChart } from "echarts/charts";
import {
  GridComponent,
  LegendComponent,
  TooltipComponent,
} from "echarts/components";
import { CanvasRenderer } from "echarts/renderers";
import type { EChartsOption } from "echarts";
import {
  AlertTriangle,
  Check,
  ChevronDown,
  Copy,
  Info,
  Loader2,
  X,
} from "lucide-react";
import { RANGES, type RangeKey } from "../lib/time";
import { useTheme } from "../lib/theme";

echarts.use([
  LineChart,
  BarChart,
  GridComponent,
  TooltipComponent,
  LegendComponent,
  CanvasRenderer,
]);

// ---------------------------------------------------------------------------
// Page furniture
// ---------------------------------------------------------------------------

export function PageHeader({
  title,
  description,
  actions,
  children,
}: {
  title: string;
  description?: ReactNode;
  actions?: ReactNode;
  children?: ReactNode;
}) {
  return (
    <header className="page-head">
      <div className="page-head-row">
        <div className="page-head-text">
          <h1>{title}</h1>
          {description && <p>{description}</p>}
        </div>
        {actions && <div className="page-head-actions">{actions}</div>}
      </div>
      {children}
    </header>
  );
}

export function Section({
  title,
  description,
  actions,
  children,
  compact = false,
}: {
  title?: string;
  description?: ReactNode;
  actions?: ReactNode;
  children: ReactNode;
  compact?: boolean;
}) {
  return (
    <section className={`panel${compact ? " panel-compact" : ""}`}>
      {(title || actions) && (
        <div className="panel-head">
          <div>
            {title && <h2>{title}</h2>}
            {description && <p>{description}</p>}
          </div>
          {actions && <div className="panel-actions">{actions}</div>}
        </div>
      )}
      <div className="panel-body">{children}</div>
    </section>
  );
}

export function EmptyState({
  icon,
  title,
  description,
  action,
}: {
  icon?: ReactNode;
  title: string;
  description?: ReactNode;
  action?: ReactNode;
}) {
  return (
    <div className="empty">
      {icon && <span className="empty-icon">{icon}</span>}
      <h3>{title}</h3>
      {description && <p>{description}</p>}
      {action && <div className="empty-action">{action}</div>}
    </div>
  );
}

export function Notice({
  tone = "info",
  title,
  children,
  action,
}: {
  tone?: "info" | "warning" | "danger" | "success";
  title?: string;
  children: ReactNode;
  action?: ReactNode;
}) {
  const Icon =
    tone === "info" ? Info : tone === "success" ? Check : AlertTriangle;
  return (
    <div className={`notice notice-${tone}`}>
      <Icon size={16} aria-hidden />
      <div>
        {title && <strong>{title}</strong>}
        <div>{children}</div>
      </div>
      {action && <div className="notice-action">{action}</div>}
    </div>
  );
}

export function Spinner({ label }: { label?: string }) {
  return (
    <div className="loading" role="status">
      <Loader2 size={18} className="loading-spin" aria-hidden />
      <span>{label ?? "Loading…"}</span>
    </div>
  );
}

/** Placeholder rows that hold the layout while a panel's data is in flight. */
export function SkeletonRows({ rows = 4 }: { rows?: number }) {
  return (
    <div className="skeleton-rows" aria-hidden>
      {Array.from({ length: rows }, (_, index) => (
        <div key={index} className="skeleton" style={{ height: 34 }} />
      ))}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

export function StatTile({
  label,
  value,
  hint,
  tone,
  spark,
  unit,
}: {
  label: string;
  value: string;
  hint?: ReactNode;
  tone?: "default" | "success" | "warning" | "danger";
  spark?: EChartsOption;
  unit?: string;
}) {
  return (
    <div className={`stat stat-${tone ?? "default"}`}>
      <span className="stat-label">{label}</span>
      <strong className="stat-value">
        {value}
        {unit && <em>{unit}</em>}
      </strong>
      {hint && <span className="stat-hint">{hint}</span>}
      {spark && (
        <div className="stat-spark">
          <Chart option={spark} height={40} />
        </div>
      )}
    </div>
  );
}

export function UsageBar({
  used,
  limit,
  label,
  format,
}: {
  used: number;
  limit: number;
  label: string;
  format: (value: number) => string;
}) {
  const percent = limit > 0 ? Math.min(100, (used / limit) * 100) : 0;
  const tone = percent > 90 ? "danger" : percent > 70 ? "warning" : "ok";
  return (
    <div className="usage">
      <div className="usage-row">
        <span>{label}</span>
        <span className="mono">
          {format(used)} / {format(limit)}
        </span>
      </div>
      <div className={`usage-track usage-${tone}`}>
        <div style={{ width: `${percent}%` }} />
      </div>
    </div>
  );
}

/**
 * ECharts wrapper. Charts are re-created when the theme changes, because their
 * colours come from CSS custom properties that ECharts has already resolved
 * into its own option object.
 */
export function Chart({
  option,
  height = 260,
  empty,
}: {
  option: EChartsOption | null;
  height?: number;
  empty?: ReactNode;
}) {
  const { appearance } = useTheme();
  if (!option) {
    return (
      <div className="chart-empty" style={{ height }}>
        {empty ?? "No data in this range"}
      </div>
    );
  }
  return (
    <ReactECharts
      key={appearance}
      echarts={echarts}
      option={option}
      style={{ height, width: "100%" }}
      opts={{ renderer: "canvas" }}
      notMerge
      lazyUpdate
    />
  );
}

// ---------------------------------------------------------------------------
// Controls
// ---------------------------------------------------------------------------

export function RangePicker({
  value,
  onChange,
  disabled,
}: {
  value: RangeKey;
  onChange: (value: RangeKey) => void;
  disabled?: boolean;
}) {
  return (
    <label className="range-picker">
      <span className="visually-hidden">Time range</span>
      <select
        className="select"
        value={value}
        disabled={disabled}
        onChange={(event) => onChange(event.target.value as RangeKey)}
      >
        {RANGES.map((range) => (
          <option key={range.key} value={range.key}>
            {range.label}
          </option>
        ))}
      </select>
    </label>
  );
}

export function CopyButton({
  value,
  label = "Copy",
  className = "btn btn-sm",
}: {
  value: string;
  label?: string;
  className?: string;
}) {
  const [copied, setCopied] = useState(false);
  return (
    <button
      type="button"
      className={className}
      onClick={async () => {
        try {
          await navigator.clipboard.writeText(value);
          setCopied(true);
          window.setTimeout(() => setCopied(false), 1500);
        } catch {
          // Clipboard permission can be refused; the value stays selectable.
        }
      }}
    >
      {copied ? (
        <Check size={14} aria-hidden />
      ) : (
        <Copy size={14} aria-hidden />
      )}
      {copied ? "Copied" : label}
    </button>
  );
}

export function CodeBlock({
  code,
  language,
  copy = true,
}: {
  code: string;
  language?: string;
  copy?: boolean;
}) {
  return (
    <div className="codeblock">
      <div className="codeblock-head">
        <span>{language ?? "shell"}</span>
        {copy && <CopyButton value={code} className="btn btn-sm btn-ghost" />}
      </div>
      <pre>
        <code>{code}</code>
      </pre>
    </div>
  );
}

/** Accessible dropdown used by the workspace switchers and row menus. */
export function Menu({
  trigger,
  children,
  align = "start",
  label,
}: {
  trigger: ReactNode;
  children: (close: () => void) => ReactNode;
  align?: "start" | "end";
  label: string;
}) {
  const [open, setOpen] = useState(false);
  const container = useRef<HTMLDivElement>(null);
  const close = useCallback(() => setOpen(false), []);

  useEffect(() => {
    if (!open) return;
    const onPointerDown = (event: MouseEvent) => {
      if (!container.current?.contains(event.target as Node)) close();
    };
    const onKey = (event: KeyboardEvent) => {
      if (event.key === "Escape") close();
    };
    document.addEventListener("mousedown", onPointerDown);
    document.addEventListener("keydown", onKey);
    return () => {
      document.removeEventListener("mousedown", onPointerDown);
      document.removeEventListener("keydown", onKey);
    };
  }, [open, close]);

  return (
    <div className="menu" ref={container}>
      <button
        type="button"
        className="menu-trigger"
        aria-haspopup="menu"
        aria-expanded={open}
        aria-label={label}
        onClick={() => setOpen((current) => !current)}
      >
        {trigger}
        <ChevronDown size={14} aria-hidden className="menu-chevron" />
      </button>
      {open && (
        <div className={`menu-panel menu-${align}`} role="menu">
          {children(close)}
        </div>
      )}
    </div>
  );
}

/**
 * Modal dialog. Focus moves in on open, is trapped while open, and returns to
 * the element that opened it on close.
 */
export function Modal({
  title,
  description,
  onClose,
  children,
  footer,
  width = 520,
}: {
  title: string;
  description?: ReactNode;
  onClose: () => void;
  children: ReactNode;
  footer?: ReactNode;
  width?: number;
}) {
  const panel = useRef<HTMLDivElement>(null);
  const titleId = useId();
  const opener = useRef<Element | null>(null);

  useEffect(() => {
    opener.current = document.activeElement;
    const focusable = () =>
      Array.from(
        panel.current?.querySelectorAll<HTMLElement>(
          'button, [href], input:not([type="hidden"]), select, textarea, [tabindex]:not([tabindex="-1"])',
        ) ?? [],
      ).filter((element) => !element.hasAttribute("disabled"));

    focusable()[0]?.focus();

    const onKey = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        event.stopPropagation();
        onClose();
        return;
      }
      if (event.key !== "Tab") return;
      const elements = focusable();
      if (elements.length === 0) return;
      const first = elements[0];
      const last = elements[elements.length - 1];
      if (event.shiftKey && document.activeElement === first) {
        event.preventDefault();
        last.focus();
      } else if (!event.shiftKey && document.activeElement === last) {
        event.preventDefault();
        first.focus();
      }
    };
    document.addEventListener("keydown", onKey, true);
    const { overflow } = document.body.style;
    document.body.style.overflow = "hidden";
    return () => {
      document.removeEventListener("keydown", onKey, true);
      document.body.style.overflow = overflow;
      (opener.current as HTMLElement | null)?.focus?.();
    };
  }, [onClose]);

  return (
    <div
      className="modal-scrim"
      onMouseDown={(event) => {
        if (event.target === event.currentTarget) onClose();
      }}
    >
      <div
        className="modal"
        style={{ maxWidth: width }}
        role="dialog"
        aria-modal="true"
        aria-labelledby={titleId}
        ref={panel}
      >
        <div className="modal-head">
          <div>
            <h2 id={titleId}>{title}</h2>
            {description && <p>{description}</p>}
          </div>
          <button
            type="button"
            className="btn btn-ghost btn-icon"
            onClick={onClose}
            aria-label="Close dialog"
          >
            <X size={16} aria-hidden />
          </button>
        </div>
        <div className="modal-body">{children}</div>
        {footer && <div className="modal-foot">{footer}</div>}
      </div>
    </div>
  );
}

/**
 * Confirmation for an irreversible action. When `confirmText` is supplied the
 * visitor has to type it, which is reserved for operations that destroy data.
 */
export function ConfirmDialog({
  title,
  description,
  confirmLabel,
  confirmText,
  danger = true,
  busy = false,
  onConfirm,
  onClose,
}: {
  title: string;
  description: ReactNode;
  confirmLabel: string;
  confirmText?: string;
  danger?: boolean;
  busy?: boolean;
  onConfirm: () => void;
  onClose: () => void;
}) {
  const [typed, setTyped] = useState("");
  const satisfied = !confirmText || typed === confirmText;
  return (
    <Modal
      title={title}
      onClose={onClose}
      width={470}
      footer={
        <>
          <button
            type="button"
            className="btn"
            onClick={onClose}
            disabled={busy}
          >
            Cancel
          </button>
          <button
            type="button"
            className={`btn ${danger ? "btn-danger" : "btn-primary"}`}
            disabled={!satisfied || busy}
            onClick={onConfirm}
          >
            {busy && <Loader2 size={15} className="loading-spin" aria-hidden />}
            {confirmLabel}
          </button>
        </>
      }
    >
      <div className="confirm-body">{description}</div>
      {confirmText && (
        <label className="field confirm-field">
          <span className="label">
            Type <code>{confirmText}</code> to confirm
          </span>
          <input
            className="input"
            value={typed}
            onChange={(event) => setTyped(event.target.value)}
            autoComplete="off"
            spellCheck={false}
          />
        </label>
      )}
    </Modal>
  );
}

export type Column<T> = {
  key: string;
  header: ReactNode;
  render: (row: T) => ReactNode;
  width?: string;
  align?: "left" | "right";
  /** Hidden below the narrow breakpoint. */
  secondary?: boolean;
};

export function DataTable<T>({
  columns,
  rows,
  rowKey,
  empty,
  dense = false,
  onRowClick,
}: {
  columns: Column<T>[];
  rows: T[];
  rowKey: (row: T) => string;
  empty?: ReactNode;
  dense?: boolean;
  onRowClick?: (row: T) => void;
}) {
  if (rows.length === 0 && empty) {
    return <>{empty}</>;
  }
  return (
    <div className="table-wrap">
      <table className={`table${dense ? " table-dense" : ""}`}>
        <thead>
          <tr>
            {columns.map((column) => (
              <th
                key={column.key}
                style={{ width: column.width, textAlign: column.align }}
                className={column.secondary ? "col-secondary" : undefined}
              >
                {column.header}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr
              key={rowKey(row)}
              onClick={onRowClick ? () => onRowClick(row) : undefined}
              className={onRowClick ? "is-clickable" : undefined}
            >
              {columns.map((column) => (
                <td
                  key={column.key}
                  style={{ textAlign: column.align }}
                  className={column.secondary ? "col-secondary" : undefined}
                >
                  {column.render(row)}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export function Tabs<T extends string>({
  tabs,
  value,
  onChange,
}: {
  tabs: { id: T; label: string; count?: number }[];
  value: T;
  onChange: (id: T) => void;
}) {
  return (
    <div className="tabs" role="tablist">
      {tabs.map((tab) => (
        <button
          key={tab.id}
          type="button"
          role="tab"
          aria-selected={tab.id === value}
          className={tab.id === value ? "is-active" : undefined}
          onClick={() => onChange(tab.id)}
        >
          {tab.label}
          {tab.count !== undefined && (
            <span className="tab-count">{tab.count}</span>
          )}
        </button>
      ))}
    </div>
  );
}

/** Debounced value, for search fields that drive a request. */
export function useDebounced<T>(value: T, delay = 250): T {
  const [debounced, setDebounced] = useState(value);
  useEffect(() => {
    const timer = window.setTimeout(() => setDebounced(value), delay);
    return () => window.clearTimeout(timer);
  }, [value, delay]);
  return debounced;
}

/**
 * Load data for a screen, with in-flight cancellation and a manual reload.
 * Every console screen uses this so a fast navigation cannot leave a stale
 * response to overwrite the new page's state.
 */
export function useLoader<T>(
  load: (signal: AbortSignal) => Promise<T>,
  deps: unknown[],
): {
  data: T | null;
  error: string | null;
  loading: boolean;
  reload: () => void;
} {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [nonce, setNonce] = useState(0);
  // The loader identity changes on every render for inline arrow functions, so
  // the caller's explicit dependency list is what drives reloading.
  const loadRef = useRef(load);
  loadRef.current = load;

  useEffect(() => {
    const controller = new AbortController();
    setLoading(true);
    loadRef
      .current(controller.signal)
      .then((result) => {
        if (controller.signal.aborted) return;
        setData(result);
        setError(null);
      })
      .catch((cause: unknown) => {
        if (controller.signal.aborted) return;
        if (cause instanceof DOMException && cause.name === "AbortError")
          return;
        setError(
          cause instanceof Error ? cause.message : "Something went wrong",
        );
      })
      .finally(() => {
        if (!controller.signal.aborted) setLoading(false);
      });
    return () => controller.abort();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [...deps, nonce]);

  return useMemo(
    () => ({
      data,
      error,
      loading,
      reload: () => setNonce((current) => current + 1),
    }),
    [data, error, loading],
  );
}

/** Run an async action with a busy flag, so buttons can disable themselves. */
export function useAction() {
  const [busy, setBusy] = useState<string | null>(null);
  const run = useCallback(async (key: string, action: () => Promise<void>) => {
    setBusy(key);
    try {
      await action();
    } finally {
      setBusy(null);
    }
  }, []);
  return { busy, run, isBusy: (key: string) => busy === key };
}
