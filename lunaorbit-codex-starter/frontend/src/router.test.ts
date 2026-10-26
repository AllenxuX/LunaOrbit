import { expect, it } from 'vitest'
import { router } from './router'
it('maps /kafka/clusters?register=1 to a registration prefill', async () => {
  const route = router.resolve('/kafka/clusters?register=1&name=k9&bootstrap=k9-kafka.svc%3A9092%2Ck9-b%3A9092')
  const props = (route.matched[0]!.props.default as (r: typeof route) => Record<string, unknown>)(route)
  expect(props.prefill).toEqual({ display_name: 'k9', bootstrap_servers: ['k9-kafka.svc:9092', 'k9-b:9092'] })
  expect((((router.resolve('/kafka/clusters').matched[0]!.props.default) as (r: unknown) => Record<string, unknown>)(router.resolve('/kafka/clusters'))).prefill).toBeUndefined()
})
