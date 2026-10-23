import { defineConfig } from 'vitest/config'
import vue from '@vitejs/plugin-vue'

export default defineConfig({
  plugins: [vue()],
  server: { proxy: { '/healthz': 'http://127.0.0.1:8080', '/api': 'http://127.0.0.1:8080' } },
  test: { exclude: ['**/node_modules/**', '**/dist/**', '**/e2e/**'], environment: 'jsdom', clearMocks: true },
})
