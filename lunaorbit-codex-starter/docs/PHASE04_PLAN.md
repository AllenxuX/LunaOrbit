# Phase 04 — 拓扑 UI 实施计划

目标：读取Phase03图，提供X6拖动、图层、机房/环境过滤、来源证据和用户独立布局。仅本阶段；不接入真实服务。

设计：Vue路由/topology懒加载X6并复用开发会话；unknown、stale与verified独立展示。过滤只取confirmed registration_id对应的当前用户Registry；未知端点可选未知，不按别名猜测。匹配任一端点保留整条链路。Pod只展示API已有候选数量与关联程度，不编造名称。

布局独立表topology_views，每用户/范围一个私人视图，revision CAS。只接受有限坐标、白名单过滤/图层字段，不接受边/HTML/图模型。GET/PUT /api/v1/topology/scopes/:scope/view，复用鉴权，范围必须能读取发现快照。默认revision0，保存后递增。不写发现图。

任务：
- [x] internal/topology内存/PG Store、HTTP与004迁移；先测所有者隔离/CAS/NaN/越界/发现关系不变，失败后实现。
- [x] api/topology.ts、topology/model.ts、TopologyCanvas.vue、Topology.vue；先测完整分页、未知过滤、共享节点、多方向、过期状态；失败不拼接截断图；最多1024关系。
- [x] 页面测试：无快照、发现轮询、错误、证据、保存冲突与拖动；列表提供键盘证据入口，布局显式保存。
- [x] 实跑Go test/vet/build与前端test/typecheck/lint/build、本地HTTP、可用浏览器及PG专库；更新README/ROADMAP/日志，停止。

复核重点：候选地址不成为确认环境；多页不得缺边；切换范围/卸载后旧响应不可写回；输入上限；布局无权改关系或读他人视图。

执行：沿用用户先计划后执行的授权，在当前未提交工作区实施，不擅自整仓提交；TDD与完成前验证。不访问生产，不使用GitOps，不进入Phase05。
