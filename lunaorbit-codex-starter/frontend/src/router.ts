import { createRouter, createWebHistory } from 'vue-router'
import { sanitizeAddress } from './topology/unregistered'
import NotFound from './views/NotFound.vue'
import Clusters from './views/Clusters.vue'
import { getRuntime } from './api/runtime'

export const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', redirect: '/topology' },
    { path: '/kafka/clusters/:id/details', component: () => import('./views/KafkaWorkspace.vue'), props: true },
    { path: '/kafka/clusters/:id/topics/:topicName', component: () => import('./views/TopicDetails.vue'), props: true },
    { path: '/kafka/clusters/:id/brokers/:brokerId', component: () => import('./views/BrokerDetails.vue'), props: true },
    { path: '/kafka/clusters/:id/groups/:groupName', component: () => import('./views/ConsumerGroupDetails.vue'), props: true },
    { path: '/kafka/clusters/:id/applications/:applicationId', component: () => import('./views/AccessResourceDetails.vue'), props: true },
    { path: '/kafka/clusters/:id/accounts/:accountId', component: () => import('./views/AccessResourceDetails.vue'), props: true },
    { path: '/kafka/clusters/:id/read-only', component: () => import('./views/KafkaDetails.vue'), props: true },
    { path: '/changes', component: () => import('./views/Changes.vue') },
    { path: '/access-control', component: () => import('./views/PlatformAccess.vue') },
    { path: '/security', component: () => import('./views/Security.vue') },
    { path: '/kafka/clusters/:id/security', component: () => import('./views/Security.vue'), props:true },
    { path: '/endpoints/:id/topics/:topicName', component: () => import('./views/CatalogTopic.vue'), props: true },
    { path: '/topic-lineage', component: () => import('./views/TopicLineage.vue') },
    { path: '/topology', component: () => import('./views/Topology.vue') },
    { path: '/applications', component: () => import('./views/InformationApplications.vue') },
    { path: '/topology/:scopeId/links/:linkId', component: () => import('./views/SyncLinkDetails.vue'), props: true },
    { path: '/kafka/clusters', component: Clusters, props: route => ({ focusId: route.hash.startsWith('#cluster-') ? route.hash.slice(9) : undefined, prefill: route.query.register === '1' ? { display_name: String(route.query.name ?? '').slice(0, 120), bootstrap_servers: String(route.query.bootstrap ?? '').split(',').map(sanitizeAddress).filter(Boolean) } : undefined }) },
    { path: '/:pathMatch(.*)*', component: NotFound },
  ],
})

router.beforeEach(async to => {
  const restricted = /^\/(security|changes|access-control)(\/|$)/.test(to.path) || /^\/kafka\/clusters\/[^/]+\/(security|accounts|applications)(\/|$)/.test(to.path)
  if (!restricted && to.path !== '/applications') return
  try {
    const runtime = await getRuntime()
    if (restricted && runtime.information_mode) return '/applications'
    if (to.path === '/applications' && !runtime.information_mode) return '/'
  } catch { return '/' }
})
