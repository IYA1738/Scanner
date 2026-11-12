import { defineConfig } from "vite";
import { fileURLToPath } from "node:url";

const proxy = {
  "/events": {
    target: process.env.BACKEND_URL || "http://127.0.0.1:8000",
    changeOrigin: true,
  },
};

export default defineConfig({
  build: {
    rolldownOptions: {
      input: {
        login: fileURLToPath(new URL("./index.html", import.meta.url)),
        workspace: fileURLToPath(new URL("./workspace.html", import.meta.url)),
        scanner: fileURLToPath(new URL("./modules/scanner/index.html", import.meta.url)),
      },
    },
  },
  server: { host: "127.0.0.1", port: 5173, strictPort: true, proxy },
  preview: { host: "127.0.0.1", port: 4173, strictPort: true, proxy },
});
