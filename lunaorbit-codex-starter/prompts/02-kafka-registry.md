# Phase 02 — Kafka 集群注册、认证和切换（Codex 开发任务）

先阅读 AGENTS.md、ROADMAP.md、docs/KAFKA_CLUSTER_MANAGEMENT.md、docs/SECURITY.md。**本轮只执行本阶段**。

交付内容：
1. PostgreSQL migration：Kafka registration、credential references、capabilities、probe status；所有 SQL 参数化，约束唯一键。
2. Go 领域接口：KafkaRegistry / KafkaAdminGateway / SecretStore，完成内存密钥测试实现，生产 SecretStore 只定义边界，不伪造 KMS。
3. 基于 franz-go 的连接工厂：PLAINTEXT、SSL、SASL_PLAINTEXT 和 SASL_SSL；SASL PLAIN/SCRAM-256/SCRAM-512；不直接解析 Java JAAS 执行认证。
4. 集群注册 API（只返回脱敏 DTO）、连接探测 API（受限超时/限流）、认证错误归类、审计；测试外部网络目标访问限制。
5. Vue 3 集群列表与「新增/编辑 Kafka 集群」弹窗：地址、机制、用户、密码（仅写入时）、TLS，连接测试、保存、切换集群；不得显示明文密码或 JAAS 原始配置。
6. 纯测试环境与 Mock：成功、SASL 密码错误、TLS 证书错误、Broker 不可达、advertised.listeners 不可达、用户无管理权限、Kafka 3.7/3.9.2 能力差异。
7. Go: gofmt、go test ./...、go vet ./...；前端 lint/typecheck/test；更新 ROADMAP 和实施日志。

禁止：任何生产配置写入、默认连接截图里的生产 IP、把图片中的账号/密码放入测试或示例、声称从 Kafka 协议获取了准确的发行版本。完成 Phase 02 后停止。
