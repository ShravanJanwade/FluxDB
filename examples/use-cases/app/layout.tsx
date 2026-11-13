import type { Metadata, Viewport } from "next";
import { Inter, JetBrains_Mono } from "next/font/google";
import Link from "next/link";
import { FLUXDB_CONSOLE_URL, SOURCE_URL } from "../lib/links";
import "./globals.css";

const sans = Inter({ subsets: ["latin"], variable: "--font-sans", display: "swap" });
const mono = JetBrains_Mono({ subsets: ["latin"], variable: "--font-mono", display: "swap" });

export const metadata: Metadata = {
  title: { default: "FluxDB use cases", template: "%s · FluxDB use cases" },
  description:
    "Three small apps that keep their data in FluxDB, a time-series database written in Rust: smart-building energy, hospital ward vitals and crypto market data.",
};

export const viewport: Viewport = {
  themeColor: [
    { media: "(prefers-color-scheme: light)", color: "#f7f7f5" },
    { media: "(prefers-color-scheme: dark)", color: "#0d0d0d" },
  ],
};

const NAV = [
  { href: "/energy", label: "Energy" },
  { href: "/healthcare", label: "Healthcare" },
  { href: "/markets", label: "Markets" },
];

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" className={`${sans.variable} ${mono.variable}`}>
      <body>
        <a className="skip-link" href="#main">
          Skip to content
        </a>
        <header className="site-header">
          <div className="site-header-inner">
            <Link href="/" className="brand" aria-label="FluxDB use cases, home">
              <svg viewBox="0 0 24 24" width="22" height="22" aria-hidden="true">
                <rect width="24" height="24" rx="6" fill="var(--brand)" />
                <path d="M5 15.5l3.5-4 3 2.5L15 8.5l4 4" stroke="#fff" strokeWidth="2" fill="none" strokeLinecap="round" strokeLinejoin="round" />
              </svg>
              <span>
                FluxDB <span className="brand-sub">use cases</span>
              </span>
            </Link>
            <nav aria-label="Use cases" className="site-nav">
              {NAV.map((item) => (
                <Link key={item.href} href={item.href}>
                  {item.label}
                </Link>
              ))}
            </nav>
            <div className="site-header-links">
              <a href={FLUXDB_CONSOLE_URL}>FluxDB console</a>
              <a href={SOURCE_URL}>Source</a>
            </div>
          </div>
        </header>
        <main id="main">{children}</main>
        <footer className="site-footer">
          <p>
            Every chart on this site is read from{" "}
            <a href={FLUXDB_CONSOLE_URL}>FluxDB</a>, a time-series database written in Rust. The energy and ward data are
            simulated; market prices are real, from Coinbase. Nothing here is medical or financial advice.
          </p>
          <p>
            <a href={SOURCE_URL}>Source on GitHub</a>
          </p>
        </footer>
      </body>
    </html>
  );
}
