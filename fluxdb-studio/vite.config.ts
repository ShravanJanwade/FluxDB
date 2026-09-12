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
      rollupOptions: {
        output: {
          manualChunks(id) {
            if (!id.includes("node_modules")) return;
            if (id.includes("/zrender/")) return "canvas";
            if (id.includes("/echarts/")) return "charts";
            return "vendor";
          },
        },
      },
    },
  };
});
