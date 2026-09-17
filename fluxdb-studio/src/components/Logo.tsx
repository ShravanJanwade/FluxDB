/** Wordmark and glyph. The glyph is a time series compressed into a square:
 *  the same motion the database is about. */

export function LogoGlyph({ size = 28 }: { size?: number }) {
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 32 32"
      role="img"
      aria-label="FluxDB"
    >
      <defs>
        <linearGradient id="flux-glyph" x1="0" y1="0" x2="1" y2="1">
          <stop offset="0%" stopColor="var(--brand-500)" />
          <stop offset="100%" stopColor="var(--cyan-500)" />
        </linearGradient>
      </defs>
      <rect width="32" height="32" rx="9" fill="url(#flux-glyph)" />
      <path
        d="M5 21.5 L10 21.5 L13 12 L16.5 24.5 L20 8.5 L23 18 L27 18"
        fill="none"
        stroke="#fff"
        strokeWidth="2.3"
        strokeLinecap="round"
        strokeLinejoin="round"
        opacity="0.96"
      />
    </svg>
  );
}

export function Logo({
  size = 28,
  showWordmark = true,
}: {
  size?: number;
  showWordmark?: boolean;
}) {
  return (
    <span className="logo">
      <LogoGlyph size={size} />
      {showWordmark && (
        <span className="logo-word">
          Flux<span>DB</span>
        </span>
      )}
    </span>
  );
}
