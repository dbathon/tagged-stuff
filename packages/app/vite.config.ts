import { fileURLToPath, URL } from "node:url";

import { defineConfig, Plugin } from "vite";
import { visualizer } from "rollup-plugin-visualizer";

import vue from "@vitejs/plugin-vue";

const plugins: Plugin<any>[] = [];

if (process.env.ENABLE_VISUALIZER) {
  plugins.push(
    visualizer({
      open: true,
      filename: "rollup-stats.html",
      gzipSize: true,
    }),
  );
}

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [vue()],
  resolve: {
    alias: {
      "@": fileURLToPath(new URL("./src", import.meta.url)),
    },
  },
  build: {
    minify: false,
    rollupOptions: { plugins },
  },
});
