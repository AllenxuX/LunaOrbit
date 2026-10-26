# Phase 01 Kite 只读 Adapter

依据：prompts/01-kite-readonly.md、KITE_API_CONTRACT.md。当前会话顺序实施，完成后停止。

## 设计与范围
- backend/internal/kite：KiteClient 接口、HTTPClient、统一脱敏错误、可替换 Decoder。
- backend/cmd/kitecheck：默认启动进程内 httptest Mock，遍历全部七个 GET 路由；真实模式必须显式选择并由环境变量提供连接参数与允许 origin。
- backend/internal/kite/testdata：合成响应和说明，不宣称来自真实 Kite。Mock Decoder 仅用于测试/演示；生产默认 decoder 返回 RESPONSE_FORMAT_UNVERIFIED。
- 响应原文只保存在内部 Document 私有字段，不能 JSON 序列化或经格式化打印；解码器在服务端访问原文，CLI 只输出固定资源类型、HTTP 状态及格式确认状态，不输出原始配置。
- 不新增 HTTP 调试接口，避免在 LDAP/RBAC 之前暴露集群资源；不连接实际 Kite、不使用 SSH 主机上的 Kubernetes 资源。

## 契约边界
只实现 namespaces/_all、configmaps/deployments/pods 的 namespace list 和单资源 GET。无管理员集群枚举、跨 namespace list、分页参数、Watch、日志和写操作。真实响应 envelope/错误细节/分页仍待授权的脱敏样本；不根据 Mock 声称验证真实格式。

## 安全与可靠性
BaseURL 仅允许 origin、无账号/路径/query/fragment；必须精确匹配服务端允许 origin。默认 HTTPS；HTTP 仅用于回环测试地址。禁止重定向。Namespace/资源名称必须为 Kubernetes DNS 名称，禁止 slash、转义和 _all 注入。Key 原值传 Authorization，cluster 每次必填且禁止控制字符。
请求带 context deadline，全调用（含重试）统一超时；最多两次重试，只重试 429/5xx，延迟受 context 约束。不自动重试 401/403/404、网络错误、超时或未知响应。响应大小限制，关闭响应体，错误不包含 URL、原始错误或上游 body。Document 的 JSON/fmt 格式只输出 redacted 标识。

## 实施与验证
1. [x] 写 httptest 测试，覆盖头部和7路由、必填cluster、路径/URL白名单、重定向、401/403/404/429/5xx、重试上限、取消/超时、限长及脱敏；先实际运行确认失败。
2. [x] 实现 Adapter、Decoder 边界与合成 Fixture。
3. [x] 实现 Mock CLI 与显式真实模式；不打印 env/URL/资源名/Key/原始响应。
4. [x] gofmt、go test ./...、go vet ./...、构建、CLI smoke；前端未变更但重跑既有验收命令。
5. [x] 独立只读复核并处理问题；更新 Phase 01 ROADMAP 与开发日志，保留真实格式未验证说明。

> 2026-10-26 LUNA-62 补充：Kite BaseURL 不再限制 HTTPS/回环，允许 http 与 https 及集群内域名；仍禁止账号、路径、query、fragment 与重定向，Key 原样放 Authorization、不记录不回显。
