# LunaOrbit v2 可执行验收标准（与 ROADMAP Phase 00–08 对齐）

## Phase 00：工程骨架
- `go test ./...`、`go vet ./...`、前端 build/typecheck/lint 实际执行并记录；`/healthz` 可访问。
- 不包含真实生产凭据、图片中的认证配置、真实连接逻辑；环境样例使用合成值。

## Phase 01：Kite 只读
- 用 `httptest` 验证 Kite API Key 放在 Authorization（无 Bearer），`x-cluster-name` 必填，Namespace/ConfigMap/Deployment/Pod 查询及权限错误区分。
- 没有 Kite 写入；提供可复现的脱敏 Mock Fixtures。

## Phase 02：Kafka 集群注册与认证（v2，已作废）
> 2026-10-26 作废：注册、编辑、停用、连接测试流程被自动发现取代，不再作为验收项。以下为替代验收。

## 自动发现验收（替代 Phase 02）
- 从已扫描范围内 MM2 配置自动生成端点：别名 + bootstrap（host:port，主机名按 hostAliases 解析）；与手工补充/历史登记按 Cluster ID、再按地址交集合并，同一集群只出现一次；地址命中多个登记显示"映射歧义·待确认"，不自动选；别名只作候选。
- 端点状态区分：凭据未开启或不可读、地址无法解析、连接失败（超时、拒绝、TLS、认证失败）。页面只显示别名 + IP:端口和访问状态。
- KAFKA_MM2_CREDENTIALS 关闭时不读取配置凭据、不发起自动连接；开启后页面显示"凭据来源：MM2 配置（只读）"。
- Topic 链路在端点不可读时沿 MM2 配置继续，节点标"Topic 是否存在无法确认"；只有地址无法解析或配置没有该方向才中断并说明原因。

## 专项：无凭据泄漏、无写请求
- 在 MM2 配置放入唯一假密码，对页面文本、全部接口响应、日志、导出、发现快照、数据库逐一搜索，命中为 0。
- 使用有写权限的测试账号：Produce、CreateTopics、AlterConfigs、OffsetCommit 等写请求被客户端拒绝，fake broker 未收到对应请求。

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
