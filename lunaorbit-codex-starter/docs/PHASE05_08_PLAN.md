# Phase 05–08 连续开发与验证计划

用户已授权后续阶段自行完成验证，无需逐阶段等待确认。依序完成05、06、07、08，每阶段独立记录实测结果；这不授权任何生产连接或写入。

- Phase05：复用Registry的所有者/凭据/网络策略，新增kafkaops只读领域模型与franz-go Reader；Broker/Topic详情、RF/ISR、Groups/offset/lag，分页过滤与分类错误。Vue详情页，协议Mock与专用Kafka3.7/3.9.2容器联调（无法取得环境时记录失败，不假装成功）。
- Phase06：独立IdentityProvider/LDAP TLS认证边界与Mock组→角色映射；平台按登记ID限定范围。ACL沿用同一Registry连接读取；只允许预览/申请/独立审批，不执行Kafka ACL写入；LDAP身份不自动等于Kafka Principal。测试认证失败、越权、资源范围、能力降级、审批隔离和秘密不回显。
- Phase07：独立changes状态机、AES-GCM快照、resourceVersion CAS、双人审批与审计。仅合成Kite写适配器可演示；真实写Adapter只实现契约规定PUT ConfigMap/PATCH Deployment，真实Decoder未验证时拒绝。默认写开关继续false，生产路径不启用；成功必须generation/ready及复制指标证据齐备，否则人工复核。
- Phase08：MetricProvider接口、受控Prometheus/VM读取Adapter及默认Mock，来源/时间/过期/unknown；在拓扑叠加潜在影响与明确业务确认的血缘，不依同名Topic或ConfigMap推断。受用户范围约束，测试环路/断链/过期/未确认关系。

实施顺序：每阶段先测试再实现，Go test/vet/build，前端test/typecheck/lint/build，HTTP和实际Edge E2E；持久化变更提供幂等迁移与实验PG验证。共享文件由主程集成，独立模块按dispatching-parallel-agents技能委派。最终全量回归和独立复核，更新路线图/开发日志/启动说明与待验证项。继续使用现有未提交工作区，不擅自推送或整仓提交。

当前Phase05接口：GET /api/v1/kafka/clusters/:id/{brokers,topics,groups}，GET topics/:topic，GET groups/:group/offsets；列表items/total/page/page_size与synthetic/observed_at/stale/state标记。分页1..100000、size1..100、query<=128。HTTP仅显式Mock开发模式挂载。

## 执行结果

Phase05–08实现与统一自动验收完成。68项前端测试、Go全量test/vet/build、7项实际Edge浏览器测试、Kafka3.7.2/3.9.2读取及无authorizer负向验证、PG001–007迁移/事务/容量/完整进程重启生命周期已实际通过。实验资源已清理，真实写开关维持false。环境缺口、精确测试条件和剩余风险见IMPLEMENTATION_LOG最终统一验收；启动与身份/审批配置见README。
