import { defineConfig } from '@playwright/test'
import base from './playwright.config'
export default defineConfig({ ...base, testMatch: ['information-mode.spec.ts', 'topic-lineage-mock.spec.ts'], use: { ...base.use, baseURL: 'http://127.0.0.1:5174' }, webServer: { command: 'pnpm dev --port 5174', url: 'http://127.0.0.1:5174', reuseExistingServer: true } })
