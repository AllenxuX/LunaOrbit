<script setup lang="ts">
import { computed, shallowRef, watch } from 'vue'
import { useRoute } from 'vue-router'
import AppIcon from './components/AppIcon.vue'
import { provideRuntime } from './composables/useRuntime'

const route = useRoute()
const { runtime, loading } = provideRuntime()
const navigationOpen = shallowRef(false)
const collapsed = shallowRef(false)
const allNavigation = [
  { to: '/topology', label: '同步拓扑', icon: 'topology', group: '工作空间' },
  { to: '/kafka/clusters', label: 'Kafka 集群', icon: 'database', group: '资源管理' },
  { to: '/security', label: '身份与权限', icon: 'shield', group: '运维治理' },
  { to: '/access-control', label: '平台访问控制', icon: 'shield', group: '' },
  { to: '/changes', label: '变更管理', icon: 'changes', group: '' },
]
const navigation = computed(() => !runtime.value ? allNavigation.slice(0, 2) : runtime.value.information_mode ? [{ ...allNavigation[0]!, group: '' }, { to: '/topic-lineage', label: 'Topic 链路', icon: 'topology', group: '' }, { to: '/applications', label: '应用信息', icon: 'database', group: '' }] : allNavigation)
function active(path: string) {
  if (path === '/security') return route.path === '/security' || route.path.endsWith('/security')
  if (path === '/kafka/clusters') return route.path.startsWith(path) && !route.path.endsWith('/security')
  return route.path === path
}
const currentPage = computed(() => navigation.value.find(item => active(item.to))?.label ?? '资源详情')
const environment = computed(() => loading.value ? '正在连接数据源' : runtime.value?.information_mode ? '内网信息平台' : runtime.value?.mode === 'admin-lab' ? '实验环境' : runtime.value?.mode === 'readonly-lab' ? '实验环境 · 只读' : runtime.value?.mode === 'mock' ? '演示环境' : '数据源未连接')
watch(() => route.fullPath, () => { navigationOpen.value = false })
</script>

<template>
  <div class="admin-shell" :class="{ 'compact-shell': runtime?.information_mode, 'sidebar-collapsed': collapsed, 'navigation-open': navigationOpen }" @keydown.esc="navigationOpen = false">
    <a class="skip-link" href="#main-content">跳到主要内容</a>
    <button v-if="navigationOpen" class="navigation-scrim" aria-label="关闭导航菜单" @click="navigationOpen = false" />
    <aside id="admin-sidebar" class="admin-sidebar" aria-label="控制台导航">
      <router-link class="brand" to="/topology" aria-label="LunaOrbit 同步拓扑">
        <span class="brand-mark"><svg width="30" height="30" viewBox="0 0 32 32" fill="none" aria-hidden="true"><ellipse cx="16" cy="16" rx="13" ry="6.5" transform="rotate(-35 16 16)" stroke="currentColor" stroke-width="1.7" /><circle cx="16" cy="16" r="5" fill="currentColor" /><circle cx="27" cy="10" r="2.5" fill="#75a6ff" /></svg></span>
        <span class="brand-copy">LunaOrbit<small>{{ runtime?.information_mode ? 'Kafka 信息平台' : 'Kafka 运维控制台' }}</small></span>
      </router-link>
      <nav class="main-navigation" aria-label="主导航">
        <template v-for="item in navigation" :key="item.to">
          <p v-if="item.group" class="nav-group">{{ item.group }}</p>
          <router-link :to="item.to" class="nav-link" :class="{ 'is-active': active(item.to) }" :aria-current="active(item.to) ? 'page' : undefined" :title="item.label">
            <AppIcon :name="item.icon" :size="19" /><span>{{ item.label }}</span><span v-if="item.to === '/topology'" class="nav-annotation">MM2</span>
          </router-link>
        </template>
      </nav>
      <div class="sidebar-bottom">
        <div class="sidebar-context"><span class="context-dot" /><div>统一运维工作空间<small>Kafka & MirrorMaker 2</small></div></div>
        <button class="sidebar-toggle" :aria-label="collapsed ? '展开侧边栏' : '收起侧边栏'" @click="collapsed = !collapsed"><AppIcon :name="collapsed ? 'chevron-right' : 'chevron-left'" /><span>收起导航</span></button>
      </div>
    </aside>
    <div class="admin-body">
      <header class="admin-topbar">
        <div class="topbar-leading"><button class="icon-button mobile-menu" aria-label="打开导航菜单" aria-controls="admin-sidebar" :aria-expanded="navigationOpen" @click="navigationOpen = !navigationOpen"><AppIcon name="menu" /></button><nav class="breadcrumb" aria-label="面包屑"><span>工作空间</span><AppIcon name="chevron-right" :size="13" /><strong>{{ currentPage }}</strong></nav></div>
        <div class="topbar-trailing"><span class="environment-badge" :class="{ 'is-connected': runtime?.mode === 'readonly-lab' || runtime?.mode === 'admin-lab' }"><span class="context-dot" />{{ environment }}</span><template v-if="runtime && !runtime.information_mode"><span class="topbar-divider" /><router-link class="account-link" to="/security"><span class="account-avatar"><AppIcon name="user" :size="17" /></span><span>{{ runtime?.mode === 'admin-lab' ? '实验管理员' : '会话管理' }}</span></router-link></template></div>
      </header>
      <main id="main-content" class="admin-content" tabindex="-1"><router-view v-if="!loading && runtime" /><div v-else-if="loading" class="panel loading-panel" role="status">正在连接工作空间…</div><div v-else class="panel loading-panel" role="alert">工作空间暂不可用，请刷新重试。</div></main>
      <footer class="admin-footer"><span>LunaOrbit · Kafka 与 MM2 {{ runtime?.information_mode ? '信息平台' : '统一运维平台' }}</span></footer>
    </div>
  </div>
</template>
