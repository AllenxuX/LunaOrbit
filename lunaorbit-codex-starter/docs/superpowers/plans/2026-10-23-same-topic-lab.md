# 同名Topic实验与消费提示修正

用户明确要求实验匹配唯一业务场景：同名Topic复制。仅专用lab切换IdentityReplicationPolicy；保留四集群A→B、A→C、D→B、B→C有向无环拓扑，B→C仅匹配同名业务Topic。通过既定Kite PUT ConfigMap/Deployment PATCH契约修改MM2，私有备份+resourceVersion约束。平台基础设施写入口仍关闭，历史前缀Topic不删除。

- [x] Kafka Connect工作组不作为业务消费组解析，实际异常带集群/组名；先写失败测试再实现。
- [x] 实验只读预检、Kite受限资源变更、两MM2就绪、唯一消息同名复制验证。
- [x] 浏览器用同名Topic验证四集群、分支与上下游；不按Topic名合并集群节点。
- [x] 全量测试与实验console部署，更新说明/路线图/日志并停止。


---
> 2026-10-26 追加说明（LUNA-60）：产品定位调整为自动发现优先的内网 Kafka/MM2 拓扑与信息平台：从 Kite 与 MM2 配置自动读出集群和同步关系，人工只负责补充和登记业务信息。本文为历史记录，原文不改；手工集群登记相关内容已被自动发现取代，现行约束见 AGENTS.md 与 SECURITY.md。
