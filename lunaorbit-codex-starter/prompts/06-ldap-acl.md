# Phase 06 — LDAP、RBAC 与 Kafka ACL

先完善 LDAP 登录及平台用户资源范围授权；再用 Phase 02 KafkaRegistry 的独立 credentials 查询 ACL。ACL 列表需显示 Principal、Resource Type、Pattern、Operation、Permission、来源集群。ACL 修改首先实现只读预览及审批申请，写入生产环境保持关闭；没有 Kafka authorizer 或 SASL Principal 真实映射时不得声称 ACL 有效。使用 Mock 验证有权限/无权限、版本能力差异。严格遵守 AGENTS.md，完成后停止。
