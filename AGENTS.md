# LunaOrbit 仓库入口

实际项目位于 `lunaorbit-codex-starter/`，所有应用命令在该目录执行。

开始接续开发前，完整阅读：
1. 根目录 `DEVELOPMENT_HANDOFF.md`（当前交付状态、启动、实验访问及风险）。
2. `lunaorbit-codex-starter/AGENTS.md`（项目约束，必须遵守）。
3. 项目README、ROADMAP及docs/PRD.md、ARCHITECTURE.md、SECURITY.md、KITE_API_CONTRACT.md。

当前产品为无登录的内网拓扑与信息记录平台，PLATFORM_MODE=information。不能根据历史实现恢复登录、RBAC、账号密码交付、ACL管理或Kafka/MM2写入口。应用可以登记账号名称/Principal、Client ID和Topic绑定，证据仍为人工记录。

业务场景为同名Topic复制；实验使用IdentityReplicationPolicy、无环A→B→C及分支。集群+Topic才是节点身份，不按Topic名称跨集群合并。不得把MM2的Connect协调组当作业务消费组，不得把反向回访或分支汇入当成有向环路。

仅可使用用户已授权的专用实验环境，不能访问生产。秘密和远端数据库不属于Git交付，禁止提交；每批完成后测试、更新路线图/日志并停止，不自行推进下一阶段。
