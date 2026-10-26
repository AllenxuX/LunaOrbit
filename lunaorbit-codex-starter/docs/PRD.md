> 2026-10-26 定位调整（LUNA-60）：自动发现优先的内网 Kafka/MM2 拓扑与信息平台：从 Kite 与 MM2 配置自动读出集群和同步关系，人工只负责补充和登记业务信息。
> 当前 P0：自动发现（Kite + MM2 配置读出集群与同步关系）、Kafka 只读查询、同步拓扑与 Topic 链路。不做：登录、RBAC、写 Kafka/K8s、追消息内容、从配置推断业务流量。新增：自动发现不等于信任；凭据按 AGENTS.md 认证 B 例外只读使用。下方历史 P0/P1/P2 仅供理解旧实现。

> 2026-10-23 当前产品调整：内网无登录的共享消息拓扑与信息记录平台。登录、权限、业务账号密码交付、ACL和基础设施写操作退出当前范围；历史条目仅用于理解旧实现，不覆盖当前要求。信息登记可写，Kafka/MM2只采集查询，连接秘密仍仅供后台使用。现行设计见 [信息平台方案](superpowers/specs/2026-10-23-topology-information-platform.md)。

# 产品需求：LunaOrbit

## 用户群体
- 新人/开发者：理解 Kafka 和 MM2 拓扑、追踪一个 Topic 的来源。
- SRE：跨机房集群总览、状态巡检、日志定位、同步链路健康分析。
- 平台管理员：Kafka ACL 管理、Kite MM2 配置审核/变更、审计与回退。
- 架构师：数据流依赖、双源写入、故障影响、可定制业务视图。

## P0（最小可用产品）
1. 注册 Kite 实例；配置多个 Kite `x-cluster-name` 映射，不必调用需 admin 角色的集群枚举接口。
2. 可读地发现选定 Namespace 的 Deployment、Pod、ConfigMap，并安全地显示查询错误/权限不足。
3. 自动识别 MM2 Deployment 和关联 ConfigMap，解析 `*.properties` 的 `source->target.topics`、bootstrap、alias、policy；对证据不足的匹配标注“不确定”。
4. 图形化展示 Kafka Cluster → MM2 Deployment → Kafka Cluster 的拓扑；按机房/环境/链路筛选，查看证据来源、更新时间、实际 Pod 状态。
5. Kafka 集群从手工注册或脱敏资产导入开始，Admin API 获取 Cluster ID、Broker、Topic、Partition、Consumer Group 概况。
6. 平台账号与基本 RBAC；生产写操作关闭；变更审计基础设施。

## P1
- LDAP 登录和 LDAP 组映射角色。
- Kafka ACL 查询、权限申请、审批；启用写入前验证目标集群认证授权机制与实际 Principal 映射。
- MM2 ConfigMap 版本差异、历史快照、测试环境变更及 Deployment 安全重启（全流程 Kite API）。
- Prometheus/VM 监控，MM2 专有指标及同步延迟，告警聚合。

## P2
- 个性化拓扑保存布局、团队共享视图、按 Topic 回溯/影响分析、文档自动导出。
- 更强的数据血缘：加入业务应用/Owner 的声明式元数据或其它可信数据源。
- 审批链、策略引擎、例行巡检报表与容量趋势。

## 业务已确认事实
- MM2 在 K8s Deployment 运行；依据现有拓扑资料，主要分布在 mars、moon-prod、pluto、saturn-res 对应的 4 个 Kubernetes 集群；运行清单需在线确认。
- `mars` 和 `charon` 同时向 `moon-prod` 同名 Topic 写入是有意设计。
- `styx2saturn-res` 名字过时；其实际目标是 `saturn-uat`。
- moon-prod 的 RF=1 尚未证实是所有 Topic 的实际副本数。

## 非目标
- 不重新实现 Kubernetes/Kite 通用管理平台。
- 不依赖 Git 作为 MM2 配置真相源。
- 不在第一版执行 Kafka Broker 主机级启停、Kafka 消息明文批量导出或数据消费回放。
- 不宣称仅靠 MM2 配置就能识别所有业务消费者或保证语义去重。


## v2 Kafka 集群管理器（已被自动发现取代）

> 本节为历史 v2 方案，2026-10-26 被自动发现取代：集群来自 MM2 配置，手工新增/编辑/测试连接/切换/停用入口隐藏，数据保留。原文见 [KAFKA_CLUSTER_MANAGEMENT.md](KAFKA_CLUSTER_MANAGEMENT.md)。
