# 无登录信息平台第一批实施计划

**Goal:** 共享集群/拓扑/应用信息，无登录、无权限与基础设施写操作。
**Spec:** docs/superpowers/specs/2026-10-23-topology-information-platform.md
**Architecture:** 新增显式information模式与固定共享workspace上下文；旧模式兼容测试但正式实验部署切换information。敏感接口不挂载/拒绝，不能仅隐藏菜单。信息记录独立于ACL模型，持久化PostgreSQL并使用revision并发控制。布局在information模式保存本浏览器，不发送个人视图写请求。

- [x] 后端：information配置、共享上下文、路由收敛、匿名读/集群信息写/采集安全边界，先写失败测试。
- [x] 应用记录：独立JSON模型与数据库表，名称/负责人/环境/说明/Topic关系（集群+Topic、produce/consume、group），CRUD与并发测试。
- [x] 前端：导航/旧路由收敛、无会话请求、只读资源查询、应用信息页、本地布局；组件与浏览器测试。
- [x] 实验数据：备份与预览，仅选定实验目录的单一owner共享化，禁止全owner隐式合并；保留历史密码记录但不可访问。
- [x] 集成：全量Go/frontend检查、两个浏览器上下文共享记录/隔离布局、旧敏感端点不可达、实际四Kafka拓扑回归。
- [x] 更新项目约束/路线图/日志，部署指定实验环境，本批结束。不进入Topic全链路第二批。

Ruling: 保留旧实现源文件及历史密文，正式information模式不挂载旧敏感能力，避免本轮破坏性清库。共享workspace ID通过配置显式指定。信息页提供人工登记的生产/消费关系，标记人工登记，不自动声称观测事实。


Ruling: 专用实验选取一个明确的既有namespace作为共享工作空间，原地采用其ID而非批量改owner；预览确认4个登记、4个不同Kafka原生ID及readonly-lab快照。原有其他owner记录完全保持隔离，不合并、不删除。部署前保留私有数据库dump与Deployment备份。布局不迁移到共享空间，由本浏览器重新保存。

验证完成：Go全量test/vet、245项前端测试、typecheck/lint/build、独立Mock浏览器2项、实际专用实验浏览器7项；记录PostgreSQL测试和控制台重启持久化验证通过。所有合成记录已清理，信息模式健康HTTP200，写/ACL/变更能力false。


---
> 2026-10-26 追加说明（LUNA-60）：产品定位调整为自动发现优先的内网 Kafka/MM2 拓扑与信息平台：从 Kite 与 MM2 配置自动读出集群和同步关系，人工只负责补充和登记业务信息。本文为历史记录，原文不改；手工集群登记相关内容已被自动发现取代，现行约束见 AGENTS.md 与 SECURITY.md。
