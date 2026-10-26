# LunaOrbit 仓库入口

实际项目位于 `lunaorbit-codex-starter/`，所有应用命令在该目录执行。

开始接续开发前，完整阅读：
1. 根目录 `DEVELOPMENT_HANDOFF.md`（当前交付状态、启动、实验访问及风险）。
2. `lunaorbit-codex-starter/AGENTS.md`（项目约束，必须遵守）。
3. 项目README、ROADMAP及docs/PRD.md、ARCHITECTURE.md、SECURITY.md、KITE_API_CONTRACT.md。

当前定位：自动发现优先的内网 Kafka/MM2 拓扑与信息平台：从 Kite 与 MM2 配置自动读出集群和同步关系，人工只负责补充和登记业务信息。PLATFORM_MODE=information。Kafka 集群连接参数不再以手工配置为前提：集群和认证信息来自 MM2 配置，手工登记只用于补充未被 MM2 覆盖的集群和业务信息。不能根据历史实现恢复登录、RBAC、账号密码交付、ACL管理或Kafka/MM2写入口；不追消息内容，不从配置推断业务流量；自动发现不等于信任。应用可以登记账号名称/Principal、Client ID和Topic绑定，证据仍为人工记录。

业务场景为同名Topic复制；实验使用IdentityReplicationPolicy、无环A→B→C及分支。集群+Topic才是节点身份，不按Topic名称跨集群合并。不得把MM2的Connect协调组当作业务消费组，不得把反向回访或分支汇入当成有向环路。

仅可使用用户已授权的专用实验环境，不能访问生产。秘密和远端数据库不属于Git交付，禁止提交；每批完成后测试、更新路线图/日志并停止，不自行推进下一阶段。


## 认证 B 例外条款

例外（经用户书面批准）：平台可从 MM2 ConfigMap 的 sasl.jaas.config 解析 Kafka 凭据，仅用于只读元数据采集。凭据只在采集进程内存中存在，不写入数据库、发现快照、日志或任何接口响应；不得在页面、日志、导出中展示用户名、密码或 jaas 原文。采集客户端只允许只读请求类型，写路由不注册。该能力由独立开关 KAFKA_MM2_CREDENTIALS 控制，代码默认关闭，部署时显式开启。开启后页面显示"凭据来源：MM2 配置（只读）"。

- Kite 地址：`KITE_BASE_URL` 允许 http/https 与集群内域名（不再要求 https 或回环）；仍禁止用户信息/路径/query，Key 原样放 Authorization、不记录不回显，Kite 只读。
