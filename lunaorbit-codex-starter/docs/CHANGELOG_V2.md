# v2 设计变更摘要

- Kafka 集群管理从基础资产登记升级为截图式 **集群连接管理 + 集群切换 + 独立 SASL/TLS**。
- 因为 Broker 认证机制不同，所有 Kafka Registry 密钥按集群隔离存储，不使用 JAAS 明文库表字段。
- v2 增加连接探测、网络 egress 限制、TLS 安全警示、`advertised.listeners` 校验、API 能力矩阵。
- Kafka Registry 前置为 Phase 02，MM2 自动发现和图形展示分别后移至 Phase 03/04。
- Go 使用 franz-go/kadm；Kafka 3.7、3.9.2 为第一优先级，3.6/2.8 单独兼容检测。
- 保留之前的 Kite API + ConfigMap 运行态发现、MM2 Deployment、LDAP、ACL、多源合流约束。
- 本启动包仅是开发规范和任务，不包含连接生产系统的凭据、实际实现代码或已验证生产接口。
