# Phase 03 — MM2 与 Kafka 集群关联

先阅读 AGENTS.md、docs/ARCHITECTURE.md、docs/KAFKA_CLUSTER_MANAGEMENT.md。使用 Phase 01 Kite 资源读取接口和 Phase 02 Kafka 注册信息：解析 MM2 Deployment/ConfigMap、clusters/aliases/源目标规则，建立每个 MM2 alias 到 Kafka 集群的候选映射；优先采用已验证 Kafka Cluster ID，其他采用手工确认或标记低置信度；不导入 ConfigMap 中的 Kafka 明文密码。输出拓扑数据模型及 evidence / observed_at / stale。包括双源写入正常视图、styx→saturn-uat、RF 未核验用例。仅执行本阶段，运行测试并停止。
