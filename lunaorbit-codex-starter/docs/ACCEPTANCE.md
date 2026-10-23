# LunaOrbit v2 可执行验收标准（与 ROADMAP Phase 00–08 对齐）

## Phase 00：工程骨架
- `go test ./...`、`go vet ./...`、前端 build/typecheck/lint 实际执行并记录；`/healthz` 可访问。
- 不包含真实生产凭据、图片中的认证配置、真实连接逻辑；环境样例使用合成值。

## Phase 01：Kite 只读
- 用 `httptest` 验证 Kite API Key 放在 Authorization（无 Bearer），`x-cluster-name` 必填，Namespace/ConfigMap/Deployment/Pod 查询及权限错误区分。
- 没有 Kite 写入；提供可复现的脱敏 Mock Fixtures。

## Phase 02：Kafka 集群注册与认证（v2 新增）
- 注册、编辑、停用、切换、连接测试流程可在 Vue 页面完成；当前集群是每个登录用户独立状态。
- 单集群独立 bootstrap、认证机制（PLAIN、SCRAM-SHA-256、SCRAM-SHA-512）、安全协议（含 TLS）及 SecretRef；列表/接口/日志永不返回密码或原始 JAAS。
- 非生产 Kafka 3.7 与 3.9.2 的连接适配完成；Mock 覆盖认证失败、TLS 失败、网络失败、权限不足和 advertised.listeners 不可达。
- 所有集群数据不可由未授权用户读取，恶意内部地址不能被任意连接探测。

## Phase 03：MM2 自动发现
- 识别 MM2 Deployment→ConfigMap→源目标 alias/Topic 规则，关联 Phase 02 的 Kafka Cluster ID 或待确认候选。
- 对发现证据不足和未知映射显示 unknown；有 approved paths：mars+charon→moon-prod 与 styx→saturn-uat。
- 支持 YAML / properties 有代表性的测试 Fixture；不因一条 Kite 采集失败删除既有资产。

## Phase 04：拓扑 UI
- 图层、节点拖动、按机房/环境过滤，证据侧栏可查 ConfigMap/Pod/Deployment；布局保存不影响真实发现关系。

## Phase 05：Kafka 只读管理
- Broker、Topic、Partitions/RF/ISR、Groups/lag、API 能力标记；未知版本/旧版本优雅降级。moon-prod RF 根据 Topic 实际元数据计算。

## Phase 06：LDAP / RBAC / Kafka ACL
- LDAP 登录模拟与资源范围限制，通过 Kafka Admin 只读查看 ACL；写操作仅审批流程，不直接修改生产 Kafka。

## Phase 07：MM2 变更（专用测试环境）
- Kite PATCH/PUT 前快照和 resourceVersion 并发控制、审计、Deployment rollout 验证，失败清晰标注；`ENABLE_KITE_WRITES=false` 时绝不写入。

## Phase 08：观测与分析
- Kafka/MM2 关键指标有来源、采集时间和 stale 状态；可按故障节点解释潜在下游影响，不凭 Topic 同名推断消息同源。
