# Phase 05 — Kafka Broker / Topic / Consumer Group 只读管理

先阅读 AGENTS.md 和 docs/KAFKA_CLUSTER_MANAGEMENT.md。基于 Phase 02 的 KafkaRegistry 实现，不再自建第二套 Kafka 认证信息。

实现 Broker Metadata、Topic 列表/详情、分区 RF/ISR、Consumer Group 列表及 offset/lag 查询，并提供分页过滤。错误须区分 Kafka ACL 无权、网络失败和 API 不支持。按版本能力做降级，并在界面展示未支持原因。Kafka ACL 读写留到 Phase 06。

离线测试/Mock 与非生产 Kafka 3.7、3.9.2 集成测试，实际执行测试命令并记录；未验证的数据标注 pending。完成后停止。
