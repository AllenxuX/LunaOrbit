# 数据模型草案（Go 领域模型及 PostgreSQL）

遵循主键 ID、时间戳、软删除/停用标记、采集时间戳；平台不要持久化敏感 ConfigMap 原文或明文密钥。

| 表 / 实体 | 核心字段 | 用途 |
|---|---|---|
| `kite_instances` | id, display_name, base_url, secret_ref, status | Kite 实例登记，Key 存密钥管理系统 |
| `k8s_clusters` | id, kite_instance_id, kite_cluster_name, room, env, enabled | Kite 集群映射，不需要管理员集群枚举 |
| `kafka_clusters` | id, name, cluster_id, bootstrap_ref, auth_ref, room, env, observed_at | Kafka 资产及元数据；Cluster ID 可为未知 |
| `k8s_workloads` | id, k8s_cluster_id, namespace, name, uid, generation, observed_generation, replicas, ready, observed_at | Deployment 观测状态 |
| `config_sources` | id, workload_id, cm_namespace, cm_name, cm_uid, resource_version, key, fingerprint, observed_at | 配置与 Deployment 关联的证据；只保存脱敏摘要 |
| `mm2_links` | id, workload_id, source_alias, target_alias, source_cluster_id, target_cluster_id, topics_raw, policy, confidence, observed_at | 每条 source→target 方向一条关系，不要假设每个 Deployment 仅一条方向 |
| `kafka_topics` | id, kafka_cluster_id, topic, partition_count, rf_min, rf_max, observed_at | 实际 Topic 分区和副本查询结果 |
| `topology_edges` | id, src_type, src_id, dst_type, dst_id, relation, evidence, status, last_seen | 基础图关系 |
| `topology_views` | id, owner_id, scope, filters_json, layout_json, shared | 个性化布局；不改写基础图 |
| `users` / `roles` / `bindings` | identity, ldap_subject, role, resource_scope | 平台 LDAP/RBAC |
| `ops_requests` | id, action, target, initiator, approver, before_hash, after_hash, state, started_at | 操作审批和审计 |
| `config_snapshots` | id, ops_request_id, encrypted_snapshot_ref, cm_resource_version, created_at | 经授权加密存储的回滚来源 |
| `discovery_runs` | id, source, started_at, finished_at, result, error_summary | 采集任务、失败原因、状态老化 |

附加约束：唯一键应包含 Kite 实例/K8s 集群/Namespace/资源 UID，不要把 Deployment 名字当全局唯一；Kafka Cluster ID 与人工业务别名分开保存；`topology_edges` 保留 evidence 与置信度；用户自定义布局只能写 `topology_views`。


## v2 补充：Kafka Registry 必须细化（见 KAFKA_CLUSTER_MANAGEMENT.md）

原 `kafka_clusters` 资产表不能承载完整连接配置。至少拆分 `kafka_cluster_registrations`（展示信息、地址和标识）、`kafka_cluster_credentials`（仅凭据引用/脱敏元数据）、`kafka_cluster_capabilities`（按集群探测到的管理能力）和 `mm2_cluster_alias_bindings`（MM2 别名绑定）。**严禁**将截图中的密码或原始 `sasl.jaas.config` 存库。不可用用户填的版本替代运行态 API 探测。
