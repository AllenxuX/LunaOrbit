# Topic 全链路图实施计划

**Goal:** 在无登录信息模式提供按集群+Topic追踪的生产应用、MM2同步、消费组与消费应用关系图。
**Spec:** docs/superpowers/specs/2026-10-23-topology-information-platform.md 的Topic链路第一版。
**Architecture:** Go有界只读聚合服务，复用registry原生Kafka读取、现有Kite发现与应用信息记录；明确规则语义、Topic身份及证据来源。Vue独立页面和X6画布复用ELK布局，按需追踪，不开放任何基础设施写API。

- [x] MM2 allowlist语义补齐：include/exclude、Default/Identity命名及分隔符，未知自定义策略不猜；旧快照缺少语义时显示待刷新。
- [x] Kafka消费证据：有界成员分配/订阅与已提交位点分离，失败/不支持明确未知。
- [x] 后端topic-lineage有界聚合API：同名跨集群、正反向、多跳、环路、多源分支、缺失目标、人工关系，限制深度/节点/读取次数/超时，明确部分失败和截断。
- [x] 前端Topic链路页、Topic详情入口、集群与Topic选择、深度和方向、X6图/节点详情/来源与时间、局部问题和空状态。
- [x] 自动测试与Mock浏览器，专用实验只读验证真实A→B→C各段Topic名，人工应用关系测试记录清理。
- [x] 全量检查、审查、部署同一实验空间，更新README/路线图/日志，本阶段停止。

API contract: GET /api/v1/topic-lineage?cluster_id=<registration>&topic=<name>&depth=2&direction=both。响应root_id,observed_at,nodes,edges,issues,truncated,depth；nodes.kind=topic/mm2/application/consumer_group/unresolved；edges含source,target,kind,evidence(observed/configured/manual/historical/unknown),observed_at，可选scope_id/link_id/policy/rule；nodes含label及可选cluster_id,cluster_name,topic,group,application_id,link_id,exists,evidence,observed_at。issues含code,message及可选cluster_id/link_id。

Ruling: 默认深度2段，支持1～4段和upstream/downstream/both；查询有界，保留根Topic。人工登记不是实际消息流，ACL不是生产消费证据，目标Topic存在不等于复制已验证。不接入业务SDK/探针，不抓取消息内容，不修改Kafka/MM2。
