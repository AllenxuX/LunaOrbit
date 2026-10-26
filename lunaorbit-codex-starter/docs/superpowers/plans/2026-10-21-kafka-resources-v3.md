# Kafka资源管理v3第2批实施计划

用户要求继续资源管理，权限认证功能暂缓考虑。本轮不修改账号、密码、LDAP或ACL产品行为，沿用既有授权及实验写开关。

**设计依据：** ../specs/2026-10-21-kafka-management-v3-design.md 第2批。Go/Gin/franz-go/PG JSONB + Vue3/TS管理平台布局；本会话内实施，代码不提交推送。

## 工程边界

- 所有配置查询提供值、source、默认值（不可得就未知）、type、文档、synonyms、敏感/只读标记；敏感值及其同义值不进入响应和快照。
- 修改配置采用增量SET/DELETE及Kafka validate-only，DELETE表达恢复继承，不写入猜测默认。Broker区分单节点与集群默认覆盖；静态配置不自动重启。
- 删除Topic要求输入完整名称、检查非内部资源，预览/审批/精确目标重验；提交成功不等于删除完成。
- 分区副本重分配与取消提供影响预览、真实观测与重启可恢复记录；不推断replica与ISR一致即全链路健康。不自动更改未选择的节流配置。
- 复用现有topic.manage与明确无范围约束的Broker管理权限，不增加认证/RBAC方案。生产禁写；实际写验证仅新建合成实验资源。

## 任务与验证

- [x] 原生适配：kafkaadmin/resource_configs.go、lifecycle.go及协议测试；Broker定向请求、validate-only、敏感默认降级、删除开关、重分配实际响应校验。
- [x] 领域与持久化：workspace/resources*.go，独立ResourceGateway、ResourcePlan与ResourceOperations状态；默认独立审批、request_id幂等、外部漂移和Topic身份变化保护、PG事务内CAS；执行只提交后进入observing，refresh核验真实完成。
- [x] 装配：Native与状态共享Mock适配、旧资源列表一致、TopicID用于后端重建检测；server挂载；不改账户与ACL模块。
- [x] 前端：ResourceConfigs、TopicLifecycle、ResourceOperations及Topic/Broker工作台入口；查询过滤、恢复继承、分区副本编辑、输入名称确认与真实结果刷新，切集群清理状态。
- [x] 实验：两版本Kafka配置SET/DELETE与来源回读、静态/敏感阻止、合成Topic删除；可行时扩展专用合成source集群第二Broker验证迁移/取消及清理节流，不接生产。
- [x] Go全量test/vet/build；前端test/typecheck/lint/build；真实PG保存与重启、浏览器闭环、原MM2复制回归；更新路线图/日志并停止。

重点复核：敏感synonym、错误Broker路由、删除后同名重建、异步接受误报成功、默认覆盖与单Broker覆盖混淆、并发修订/重试、测试资源被误当业务Topic。



实施结论：本批全部任务完成。实际结果见 IMPLEMENTATION_LOG 第2批条目。工程决定：Kafka 空 Broker 配置仅提供显式覆盖，目录来自真实节点但不混入节点生效值；迁移查询显式列举实际分区；客户端请求保持策略总超时且写不重试；专用实验 DNS 只修正无效的合成命名空间搜索补全。权限认证后续产品决策暂缓，第3批未开工。


---
> 2026-10-26 追加说明（LUNA-60）：产品定位调整为自动发现优先的内网 Kafka/MM2 拓扑与信息平台：从 Kite 与 MM2 配置自动读出集群和同步关系，人工只负责补充和登记业务信息。本文为历史记录，原文不改；手工集群登记相关内容已被自动发现取代，现行约束见 AGENTS.md 与 SECURITY.md。
