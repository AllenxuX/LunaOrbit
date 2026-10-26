# Kafka 工作空间与应用授权实施计划

用户已批准将现有接口演示式管理改成集群工作台、Topic管理、应用账号、生产/消费授权及执行核验的闭环，本轮直接实施并在授权实验环境验证。

## 设计与边界

- 资源身份后台仍按登记ID隔离；页面始终展示名称。平台身份、应用资料、Kafka Principal、认证凭据、Broker ACL分别建模，不自动创建Kafka认证用户。
- 集群详情采用概览/Topics/消费组/应用与权限/同步链路/操作记录/团队授权导航，复用现有原生只读查询。Topic支持创建、增加分区、读取配置及白名单保留策略调整；不包含删除Topic/重置Offset等未实现的破坏性动作。
- 应用资料记录名称、负责人、用途和显式Principal，不保存密码。按生产/消费用途生成最小规则，消费同时要求消费组，支持明确的精确/前缀范围。保留高级原始ACL查询。
- 授权/撤权先预览精确差异（新增、已存在、删除、保留），保存变更单；校验应用及请求修订、当前Broker规则快照、资源权限及写开关，执行逐条记录并回读。部分成功/未知/失败保留真实状态；不把规则摘要称为完整有效权限判断，不自动删外部规则。
- 团队授权采用集群+用户/可信LDAP组+角色+Topic/消费组前缀范围；默认保持原所有者可访问，显式授权后共享，不引入全局管理员越权读取所有资源。授权和撤销记审计。
- 默认禁写，真实写只在显式admin-lab与配置白名单集群启用。真实Kafka使用原franz-go安全工厂及原出站策略；Kite契约与MM2管理保持原边界。
- 新业务元数据提供内存与PostgreSQL存储/迁移，事务保存修订与审计。实验临时存储与实际持久化验证分别报告。

## 实施任务

- [x] Kafka原生Topic配置/创建/扩分区/保留策略及ACL精确删除适配器，协议级失败/权限/回读测试。
- [x] Registry资源授权、可信组与共享查询、团队授权API/迁移，跨用户/角色/范围/撤销回归。
- [x] Workspace应用、授权计划/审批/执行核验、操作记录、Topic变更HTTP与持久化；幂等与部分失败测试。
- [x] Vue集群工作台、应用资料、授权向导与差异预览、原生ACL、操作/团队授权页面及表单状态隔离。
- [x] 端到端与真实实验Kafka测试、必要的实验配置、部署；独立复核、路线图/开发日志更新。

验证：Go全量test/vet/build，前端test/typecheck/lint/build，真实Edge工作空间闭环；Kafka无authorizer时明确不支持，若实验显式配置authorizer，仅用合成Principal/Topic做授权CRUD核验，不能据此宣称企业认证或权限执行矩阵已验收。

## Workspace metadata capacity and persistence checks

Each registration admits at most 128 application records and 128 saved change
requests. Admission of application edits/deletes and new requests stops at 16,000
audit entries with `CAPACITY_EXCEEDED` (HTTP 429). Existing approved requests can
still finish: audit space is reserved for approval, claiming, up to 100 per-rule
results, and completion for all admitted requests. Audit history is never silently
truncated. A full workspace requires an explicit retention/archive policy before
further admission; this version does not delete operational history automatically.

Optional PostgreSQL checks run with `WORKSPACE_TEST_DATABASE_URL` via
`go test ./internal/workspace -run TestWorkspacePostgres -v`. The existing registry
DSN validator only permits loopback addresses and dedicated
`lunaorbit_phase00_test` / `lunaorbit_phase02_test` databases. The test creates and
removes only its randomly identified workspace row, applies migration 009's table
DDL twice, checks application revisions, races two executors, and recreates store
instances to check persisted audit and partial results. Migration 009 uses the shared transaction advisory lock 71000 and schema_migrations
version 9 guard, so concurrent migration runners serialize and sequential reruns skip. Runtime workspace updates serialize
with an insert-on-conflict followed by `SELECT ... FOR UPDATE` in one transaction.


Application names and explicit Kafka principals remain visible to authenticated
cluster readers so scoped ACL administrators can select registered applications;
these records contain no authentication secrets. Application metadata creation,
editing, and deletion require unrestricted cluster application management. Request
lists/details and audit entries are filtered using every input and planned Topic
and consumer-group resource, including rules that already exist on Kafka.


---
> 2026-10-26 追加说明（LUNA-60）：产品定位调整为自动发现优先的内网 Kafka/MM2 拓扑与信息平台：从 Kite 与 MM2 配置自动读出集群和同步关系，人工只负责补充和登记业务信息。本文为历史记录，原文不改；手工集群登记相关内容已被自动发现取代，现行约束见 AGENTS.md 与 SECURITY.md。
