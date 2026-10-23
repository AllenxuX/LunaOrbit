# Phase 03 MM2 自动发现方案

依据 prompts/03-mm2-discovery.md、ARCHITECTURE、KAFKA_CLUSTER_MANAGEMENT、KITE_API_CONTRACT 和 ACCEPTANCE。只实施本阶段，X6/布局/拓扑交互留到 Phase 04。

## 模块与执行
使用并行任务技能分离独立模块：parser 负责受限 properties/YAML 提取；collector 负责 Phase 01 KiteClient 的资源读取与显式 Decoder；Store 负责脱敏快照/人工绑定持久化。主会话负责关联引擎、候选匹配、异步 Worker、受保护 API、CLI 与整体验证。

## 关键约束
- 真实 Kite envelope 仍未知，不猜测。扩展 Document 的显式资源 Decoder 边界，nil decoder 失败关闭；只有合成 Fixture 使用合成 Decoder。
- 只用既有 7 GET 契约中的 Namespace/Deployment/ConfigMap/Pod 列表；无资源写入、管理员枚举、Pod 日志或新 Kite 路由。
- 规范化 Kubernetes 投影仅在后端内存存在，持久化/输出只含允许的配置语义和证据。密码、JAAS、Token、原始 ConfigMap/Pod env 不落库、不打印；错误只有固定代码。
- 引用按同 namespace 的 volume.configMap、env.valueFrom.configMapKeyRef、envFrom.configMapRef 关联，考虑 initContainers 和多个容器。单 topics 字段不足以确认 MM2；保留方向和正则原文，不把正则数量当实际 Topic 数。
- Kafka 地址匹配仅产生候选，单候选也不自动标 verified。Cluster ID 确认需要独立且非 Mock 的 alias 身份证据与已验证登记 ID 一致；同 ID 多登记不盲合并。人工绑定须限定当前用户、scope、namespace、workload UID 和 alias。
- declared/observed/verified 分离；已识别 ConfigMap 不等于 Pod 加载或同步健康。approved business intent 不等于业务链路验证。moon-prod RF 一律 unknown/null，直至 Topic 元数据实测。
- mars+charon→moon-prod 为批准的双源路径；styx→saturn-uat 为确认目标，历史 Deployment/文件名不能覆盖配置路由。配置冲突则保留声明证据并标待确认，不能偷偷改边。
- 采集失败/资源不再出现不物理删除历史：保留最后成功 observed_at，标 stale/unknown；只对完整且成功采集的 namespace 重建当前结果。

## 交付 API（LunaOrbit 自身，非 Kite）
复用 Phase 02 签名开发会话、Origin 校验与资源所有者隔离。默认禁用；DISCOVERY_MODE=mock 且已显式启用 registry Mock 才挂载。
- GET /api/v1/discovery/scopes：仅服务端配置的 scope 列表。
- POST /api/v1/discovery/scopes/:scope/runs：{scenario?:'success'|'partial_failure'|'unavailable'}；异步 202，场景仅 Mock。
- GET /api/v1/discovery/scopes/:scope/runs/latest：任务状态/固定错误码。
- GET /api/v1/discovery/scopes/:scope/snapshot?page=1&page_size=20：脱敏 Snapshot，links 分页、nodes/edges/evidence/observed_at/stale。
- PUT /api/v1/discovery/scopes/:scope/bindings：{namespace,workload_uid,alias,registration_id}；当前用户登记存在且属于当前 scope 的已观测 workload/alias 才可确认，记录审批人和时间。下次发现重算映射。

CLI `cmd/mm2discover` 默认全部合成、仅本地 GET，输出脱敏快照；没有真实模式偷用 Mock envelope。后端 API 只读展示发现数据和在平台数据库保存绑定，不更改 Kite/Kafka。

## 验证
先运行失败测试，再实现；覆盖 properties/YAML、证据不足、三类引用、多容器、独立双方向、秘密脱敏、未知/歧义/人工/Cluster ID 映射、批准多源、历史别名、RF unknown、partial failure/stale、用户隔离及超时。实跑 Go test/vet/build、前端既有测试/typecheck/lint/build、CLI 与 HTTP smoke；可在已批准实验机使用专用 PostgreSQL 验证 003 迁移/快照/绑定事务，绝不触碰 Kubernetes/Kafka。
