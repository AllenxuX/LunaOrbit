阅读所有项目约束，仅实现 Phase 01：Kite 只读 HTTP Adapter。以 docs/KITE_API_CONTRACT.md 为唯一已验证的 API 契约，不要猜响应 envelope，也不要写 Kite 后端。

要求：
- KiteClient interface + HTTP 实现，支持 Namespace 列表、ConfigMap/Deployment/Pod 列表与单个查询。
- `Authorization` API Key 原值，禁止 Bearer；`x-cluster-name` 必填；API URL 白名单/防路径注入；请求超时和 context 传播；任何日志不得打印 Key。
- 用 net/http/httptest 模拟 Kite：正确头部、GET 路由、403、404、429、5xx、超时、错误脱敏。若 Kite 实际响应格式未知，将反序列化封装到可替换层，并记录待现场验证。
- 只读 CLI/内部调试 API 可跑全 mock 数据；真实连接必须显式配置，默认脱机。
- 实际运行相关测试和 lint；停在本阶段。
