import { createApp } from 'vue'
import ElementPlus from 'element-plus'
import 'element-plus/dist/index.css'
import App from './App.vue'
import { router } from './router'
import './style.css'
import { warmLayoutWorker } from './topology/layout-client'

createApp(App).use(router).use(ElementPlus).mount('#app')
warmLayoutWorker()
