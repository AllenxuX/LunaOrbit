import { beforeEach, expect, it } from 'vitest'
import { readLocalView, saveLocalView } from './local-view'
beforeEach(() => localStorage.clear())
it('keeps only layout preferences isolated by workspace, scope and mode', () => {
  saveLocalView('one', 'scope', 'mm2', { revision: 2, positions: { a: { x: 12, y: 24 } }, filters: { room: 'east', environment: '' }, layers: { kafka: true, workloads: false }, password: 'never-store' } as never)
  expect(readLocalView('one', 'scope', 'mm2')?.positions.a).toEqual({ x: 12, y: 24 })
  expect(readLocalView('two', 'scope', 'mm2')).toBeUndefined()
  expect(readLocalView('one', 'scope', 'relations')).toBeUndefined()
  expect(Object.values(localStorage).join('')).not.toContain('never-store')
})
it('ignores malformed storage and unsafe coordinates', () => {
  saveLocalView('one', 'scope', 'mm2', { revision: 0, positions: {}, filters: { room: '', environment: '' }, layers: { kafka: true, workloads: true } })
  const key = localStorage.key(0)!
  localStorage.setItem(key, '{broken')
  expect(readLocalView('one', 'scope', 'mm2')).toBeUndefined()
  localStorage.setItem(key, JSON.stringify({ positions: { a: { x: 'bad', y: 2 } }, filters: { room: '', environment: '' }, layers: { kafka: true, workloads: true } }))
  expect(readLocalView('one', 'scope', 'mm2')).toBeUndefined()
})
