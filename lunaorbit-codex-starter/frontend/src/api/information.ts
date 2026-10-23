import { registryRequest } from './registry'
import type { Page } from './kafkaops'

export interface ApplicationRelation { cluster_id: string; topic: string; mode: 'produce' | 'consume'; consumer_group?: string; account?: string; client_id?: string }
export interface ApplicationDraft { name: string; owner: string; environment: string; description: string; location?: string; relations: ApplicationRelation[] }
export interface InformationApplication extends ApplicationDraft { id: string; revision: number; updated_at?: string }
const path = '/information/applications'
export const listInformationApplications = (page = 1, query = '') => registryRequest<Page<InformationApplication>>(`${path}?${new URLSearchParams({ page: String(page), page_size: '20', query })}`)
export const saveInformationApplication = (draft: ApplicationDraft, item?: InformationApplication) => registryRequest<InformationApplication>(`${path}${item ? '/' + encodeURIComponent(item.id) : ''}`, item ? 'PATCH' : 'POST', { ...draft, relations: draft.relations.map(({ cluster_id, topic, mode, consumer_group, account, client_id }) => ({ cluster_id, topic, mode, ...(account?.trim() ? {account:account.trim()} : {}), ...(client_id?.trim() ? {client_id:client_id.trim()} : {}), ...(mode === 'consume' && consumer_group ? { consumer_group } : {}) })), ...(item ? { revision: item.revision } : {}) })
export const deleteInformationApplication = (item: InformationApplication) => registryRequest(`${path}/${encodeURIComponent(item.id)}?revision=${item.revision}`, 'DELETE')
