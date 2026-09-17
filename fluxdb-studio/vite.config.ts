import { defineConfig, loadEnv } from "vite";
import react from "@vitejs/plugin-react";
export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, ".", "FLUXDB_");
  return {
    plugins: [react()],
    server: {
      proxy: Object.fromEntries(
        [
          "/api",
          "/write",
          "/query",
          "/metrics",
          "/health",
          "/ping",
          "/stats",
          "/databases",
        ].map((path) => [
          path,
          {
            target: env.FLUXDB_PROXY_TARGET || "http://127.0.0.1:8086",
            changeOrigin: true,
          },
        ]),
      ),
    },
    build: {
      outDir: "dist",
      // The charts chunk is echarts core plus the line and bar renderers. It
      // is loaded only when a console screen mounts, never on the marketing
      // page, so its size is not on the critical path.
      chunkSizeWarningLimit: 700,
      rollupOptions: {
        // A chunk cycle is not a style problem: one chunk evaluates before its
        // dependency is initialised, so the bundle throws on load and ships a
        // blank page. Rollup only warns, and a warning scrolls past in CI.
        onwarn(warning, defaultHandler) {
          if (warning.message?.includes("Circular chunk")) {
            throw new Error(
              `${warning.message}

` +
                "A chunk cycle ships a blank page: the first chunk to evaluate " +
                "reads an uninitialised export from the other. Fix the " +
                "manualChunks split instead of silencing this.",
            );
          }
          defaultHandler(warning);
        },
        output: {
          manualChunks(id) {
            // Only echarts is split out by hand, and only because it is a leaf:
            // nothing else imports it, so it cannot form a cycle with another
            // chunk. echarts and zrender import each other, so they share one.
            //
            // Everything else is left to Rollup. Hand-splitting react away from
            // the rest of node_modules shipped a blank page: react-router-dom
            // landed in the react chunk and pulled @remix-run/router into
            // vendor, while vendor's lucide-react imported react back. Rollup
            // warned "Circular chunk: vendor -> react -> vendor", and at
            // runtime vendor evaluated first and read forwardRef off an
            // uninitialised module.
            if (id.includes("/echarts/") || id.includes("/zrender/")) {
              return "charts";
            }
          },
        },
      },
    },
  };
});
