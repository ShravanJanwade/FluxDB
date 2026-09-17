/** Transient confirmations and failures. */

import {
  createContext,
  useCallback,
  useContext,
  useMemo,
  useState,
  type ReactNode,
} from "react";
import { AlertTriangle, CheckCircle2, Info, X } from "lucide-react";

export type ToastKind = "success" | "error" | "info";

type Toast = {
  id: number;
  kind: ToastKind;
  message: string;
  /** Extra detail shown under the message, for API errors worth reading. */
  detail?: string;
};

type ToastContextValue = {
  notify: (kind: ToastKind, message: string, detail?: string) => void;
  success: (message: string, detail?: string) => void;
  /** Accepts an `Error` so call sites can hand over whatever they caught. */
  failure: (error: unknown, fallback?: string) => void;
};

const ToastContext = createContext<ToastContextValue | null>(null);

/** Errors are given longer on screen than confirmations: they need reading. */
const LIFETIME: Record<ToastKind, number> = {
  success: 3_200,
  info: 4_000,
  error: 7_000,
};

let nextId = 1;

export function ToastProvider({ children }: { children: ReactNode }) {
  const [toasts, setToasts] = useState<Toast[]>([]);

  const dismiss = useCallback((id: number) => {
    setToasts((current) => current.filter((toast) => toast.id !== id));
  }, []);

  const notify = useCallback(
    (kind: ToastKind, message: string, detail?: string) => {
      const id = nextId++;
      setToasts((current) => [
        ...current.slice(-3),
        { id, kind, message, detail },
      ]);
      window.setTimeout(() => dismiss(id), LIFETIME[kind]);
    },
    [dismiss],
  );

  const value = useMemo<ToastContextValue>(
    () => ({
      notify,
      success: (message, detail) => notify("success", message, detail),
      failure: (error, fallback = "That did not work") => {
        const message =
          error instanceof Error && error.message ? error.message : fallback;
        notify("error", message);
      },
    }),
    [notify],
  );

  return (
    <ToastContext.Provider value={value}>
      {children}
      {/* Announced politely so a screen reader hears the outcome without
          losing the user's place. */}
      <div className="toast-stack" role="status" aria-live="polite">
        {toasts.map((toast) => (
          <div key={toast.id} className={`toast toast-${toast.kind}`}>
            {toast.kind === "success" ? (
              <CheckCircle2 size={17} />
            ) : toast.kind === "error" ? (
              <AlertTriangle size={17} />
            ) : (
              <Info size={17} />
            )}
            <div className="toast-body">
              <span>{toast.message}</span>
              {toast.detail && <small>{toast.detail}</small>}
            </div>
            <button
              type="button"
              className="toast-close"
              onClick={() => dismiss(toast.id)}
              aria-label="Dismiss notification"
            >
              <X size={14} />
            </button>
          </div>
        ))}
      </div>
    </ToastContext.Provider>
  );
}

export function useToast(): ToastContextValue {
  const context = useContext(ToastContext);
  if (!context) throw new Error("useToast must be used inside ToastProvider");
  return context;
}
