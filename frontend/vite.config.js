import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

const backend = process.env.BACKEND_URL || "http://127.0.0.1:5050";

export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      "/api": backend,
      "/healthz": backend,
      "/readyz": backend,
      "/rta.js": backend,
      "/ws": { target: backend.replace(/^http/, "ws"), ws: true },
    },
  },
});
