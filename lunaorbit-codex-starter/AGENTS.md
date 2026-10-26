# LunaOrbit — Codex 常驻指令

## 产品目标

自动发现优先的内网 Kafka/MM2 拓扑与信息平台：从 Kite 与 MM2 配置自动读出集群和同步关系，人工只负责补充和登记业务信息。能力：Kite/MM2 自动发现 Kafka 集群与同步关系、Kafka 资源只读查询、同步拓扑、应用生产消费信息和浏览器个人布局；手工登记只补充未被 MM2 覆盖的集群和业务字段。2026-10-23用户明确取消登录、LDAP、RBAC、团队授权、Kafka账号/密码/ACL管理及实际配置写操作；当前产品使用PLATFORM_MODE=information。2026-10-23后续用户明确允许应用关系登记Kafka账号名称/Principal、Client ID及部署位置；这是信息绑定，不恢复账号密码/ACL管理，不验证或推断实际流量。历史相关实现仅保留兼容和归档，不重新暴露入口或API。业务信息登记可以修改；集群连接参数来自 MM2 配置，不作为手工配置前提；不能修改Kafka/MM2基础设施。新要求优先于下文历史授权约束。

## 不可违反的架构边界

1. **MM2 的运行配置不能依赖 Git 仓库。** 使用 Kite API 获取 ConfigMap、Deployment、Pod；只通过 Kite API 执行 Kubernetes 资源写操作。代码仓库仍可使用 Git。
2. Kafka 管理使用 Go Kafka 客户端（优先 franz-go/kadm），不通过 Kite 管理裸机 Kafka Broker；Kite 仅负责 Kubernetes 资源。
3. 请先阅读 `docs/PRD.md`、`docs/ARCHITECTURE.md`、`docs/KITE_API_CONTRACT.md`、`docs/SECURITY.md`、`ROADMAP.md`。
4. 不得编造 Kite API 路径、请求参数、响应结构；未记录的功能必须封装成待验证接口、可替换实现或暂不实现。
5. Kite API Key：原样放 `Authorization` 头，**禁止** `Bearer` 前缀；集群资源请求必须带 `x-cluster-name`。不记录或回显密钥。
6. 默认开发与测试 **只读**。生产 ConfigMap PATCH/PUT、Deployment 重启、Kafka ACL 变更等写操作均默认禁用，需独立显式批准与审计。
7. 不得无批准连接/修改生产服务。使用 `httptest`、fixture 与本地测试环境验证。禁止自动运行真实删改操作。
8. 真实拓扑的状态分离为 `declared`（配置）、`observed`（运行资源）和 `verified`（链路/业务确认）。不可把 ConfigMap 存在等同于同步健康。
9. `mars + charon -> moon-prod` 双源写入是已确认的架构意图，展示为批准的多源路径；不可据相同目标 Topic 简单定为 P0。
10. `styx -> saturn-uat` 为实际目标；历史文件名 `styx2saturn-res` 是历史别名，不能用于覆盖已确认的路由结论。
11. `moon-prod` 默认 RF 可能为 1，不得推断各业务 Topic 的实际 RF。
12. 不得将敏感 ConfigMap 内容、Kite Key、Kafka 认证信息、LDAP 凭据传到前端或审计日志。

## 实现习惯

- 单体 Go 后端 + 模块边界清晰的异步发现 Worker；不要起步就微服务。
- 后端 Go 代码 `gofmt`、`go test ./...`、`go vet ./...`；前端 TypeScript 严格模式 + lint + typecheck + 测试。
- 外部依赖必须经接口隔离（KiteClient、KafkaAdmin、MetricProvider、IdentityProvider）。HTTP 使用 context timeout、重试限制和错误分类。
- 禁止硬编码 IP、账号、Token、Namespace、Kite 集群名称；配置及映射由环境或数据库提供。
- API 统一错误类型，资源查询提供分页/过滤；长周期发现不可阻塞 Web 请求。
- 每次变更只处理一个阶段；先计划、测试、实现、验证，再报告结果。未执行的测试不得宣称通过。
- 遵循 `docs/ACCEPTANCE.md` 的验收口径。每个阶段结束更新 `ROADMAP.md` 完成状态，并写 `docs/IMPLEMENTATION_LOG.md`（真实测试命令、结果、风险、未完成项）。

## 回复格式（每次开发任务结束）

1. 本阶段完成的可运行能力
2. 文件变更摘要
3. 运行的命令和输出结论
4. 未完成项或风险
5. 下一阶段建议（不要自行开工）


## Kafka 连接（2026-10-26 自动发现优先，取代 v2 手工注册约束；第 14 条截图凭据禁令保留）

13. Kafka 集群由 Kite/MM2 自动发现：认证信息由 MM2 配置提供、平台只读使用；不再要求前端为每个集群手工配置 bootstrap、SASL、TLS。手工补充（部署配置）只用于未被 MM2 覆盖的集群，同地址时自动发现优先。
14. 用户提供的 Kafka 管理界面截图含明显密钥信息，**绝对禁止**将截图中的用户名/密码、原始 JAAS、秘钥内容复制到代码、测试 Fixture、日志、文档或者示例。所有演示数据必须合成。
15. Go franz-go 必须将 MM2 配置中的 JAAS 语义映射为结构化 Go SASL 配置（PLAIN、SCRAM-SHA-256、SCRAM-SHA-512），不能把 JAAS 字符串作为认证逻辑传递或展示。
16. 例外（经用户书面批准）：平台可从 MM2 ConfigMap 的 sasl.jaas.config 解析 Kafka 凭据，仅用于只读元数据采集。凭据只在采集进程内存中存在，不写入数据库、发现快照、日志或任何接口响应；不得在页面、日志、导出中展示用户名、密码或 jaas 原文。采集客户端只允许只读请求类型，写路由不注册。该能力由独立开关 KAFKA_MM2_CREDENTIALS 控制，代码默认关闭，部署时显式开启。开启后页面显示"凭据来源：MM2 配置（只读）"。
16a. 只读白名单：Metadata、ApiVersions、DescribeCluster、DescribeConfigs、ListOffsets、OffsetFetch、DescribeGroups、ListGroups、FindCoordinator、DescribeLogDirs；其余请求类型在客户端拒绝，不发出网络包。凭据只在采集进程内存中，重启即失；`SASL_PLAINTEXT` 仍提示未加密风险。
17. Kafka 3.7/3.9.2 走主路径，3.6/2.8 走兼容能力探测；不根据用户填的版本推断特定管理 API 一定可用，也不得从 `ApiVersions` 直接断言 Broker 的精确发行版本。
18. 每集群连接先验证 seed broker，再验证 `advertised.listeners` 发现的 Broker 可达性；错误须区分网络、TLS、认证、授权及 API 不支持。
19. 所有 Kafka 集群注册必须受到 SSRF/任意内网探测防护、用户资源范围限制、客户端池上限和探测速率限制。
20. `docs/KAFKA_CLUSTER_MANAGEMENT.md` 是历史 v2 方案，已被自动发现取代；其中 SSRF 与探测限制条款仍然有效。

- Kite 地址：`KITE_BASE_URL` 允许 http/https 与集群内域名（不再要求 https 或回环）；仍禁止用户信息/路径/query，Key 原样放 Authorization、不记录不回显，Kite 只读。
