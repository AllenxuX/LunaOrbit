# 同名Topic实验与消费提示修正

用户明确要求实验匹配唯一业务场景：同名Topic复制。仅专用lab切换IdentityReplicationPolicy；保留四集群A→B、A→C、D→B、B→C有向无环拓扑，B→C仅匹配同名业务Topic。通过既定Kite PUT ConfigMap/Deployment PATCH契约修改MM2，私有备份+resourceVersion约束。平台基础设施写入口仍关闭，历史前缀Topic不删除。

- [x] Kafka Connect工作组不作为业务消费组解析，实际异常带集群/组名；先写失败测试再实现。
- [x] 实验只读预检、Kite受限资源变更、两MM2就绪、唯一消息同名复制验证。
- [x] 浏览器用同名Topic验证四集群、分支与上下游；不按Topic名合并集群节点。
- [x] 全量测试与实验console部署，更新说明/路线图/日志并停止。
