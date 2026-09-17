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
        output: {
          manualChunks(id) {
            if (!id.includes("node_modules")) return;
            // echarts and zrender import each other, so they belong in one
            // chunk; splitting them produced a circular chunk graph.
            if (id.includes("/echarts/") || id.includes("/zrender/")) {
              return "charts";
            }
            if (
              id.includes("/react/") ||
              id.includes("/react-dom/") ||
              id.includes("/react-router") ||
              id.includes("/scheduler/")
            ) {
              return "react";
            }
            return "vendor";
          },
        },
      },
    },
  };
});
