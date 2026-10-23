import { defineConfig } from '@playwright/test'
export default defineConfig({testDir:'./e2e',timeout:30000,fullyParallel:false,workers:1,reporter:'list',use:{baseURL:'http://127.0.0.1:5173',browserName:'chromium',channel:'msedge',headless:true,viewport:{width:1500,height:1050}}})
