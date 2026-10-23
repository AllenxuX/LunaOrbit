# Phase 00 工程方案与实施计划

日期：2026-10-19。依据：prompts/00-foundation.md；执行方式：当前会话顺序实施。

## 设计决策
- 使用现有启动包目录作为应用根目录，不移动原始规范。Go 单体按 internal 模块分层，Vue 使用同源 API，Vite 开发代理仅访问本地后端。
- Gin 为固定框架。PostgreSQL 只建审计基础表及迁移版本表，不提前实现 Kafka Registry 等业务模型。
- DATA_MODEL 的旧 Kafka 表与 v2 拆表方案存在覆盖关系，后续 Phase 02 以 KAFKA_CLUSTER_MANAGEMENT 为准；PRD 的 P0 是产品优先级，不代表全部属于 Phase 00。
- Kite 路由规范与未知响应封装同时成立：本阶段不实现请求或假定响应结构。生产写入开关即使设 true 也拒绝启动，防止骨架被误用。
- /healthz 只表示进程存活，不宣称数据库、Kafka、Kite 健康。数据库迁移由显式本地任务执行，不在 Web 启动时连接。
- 审计事件只允许 actor_id/action/target_id/outcome，不接收原始配置、凭据或自由 JSON。SQL Sink 为后续服务提供基础，当前无审计读取/写入 HTTP API。

## 工程与依赖
backend/cmd/server、internal/config、internal/httpapi、internal/audit；Gin + 标准库 slog/database/sql；pgx stdlib 驱动仅用于显式启用的真实 PostgreSQL 集成测试。
frontend/src：Vue3、TypeScript strict、Vite、Element Plus、Vue Router；Vitest、ESLint、vue-tsc。
migrations：事务型、带 advisory lock 的版本迁移；scripts：PowerShell task/migrate/smoke；Compose：本地 PostgreSQL。
LDAP、X6、franz-go、Kite 等业务集成留到对应阶段，避免骨架加载不使用的外部客户端。

## 实施步骤
1. [x] 编写配置验证、健康检查、统一错误和 API Client 的基础测试并先运行确认失败。
2. [x] 实现后端配置与只读安全门、JSON 日志、请求 ID、panic 恢复及优雅退出。
3. [x] 实现前端基础壳、首页空状态、路由回退、超时和安全错误显示。
4. [x] 创建审计表迁移与本地 Compose、显式迁移脚本、开发及验证任务和环境样例。
5. [x] 实跑 Go test/vet/build、前端 test/typecheck/lint/build、启动后 HTTP smoke。
6. [x] 检查日志及响应不泄漏 URL 查询/请求体/原始异常，记录数据库与外部接口的验证缺口；只更新 Phase 00 路线状态。

## 验证重点
非法端口/日志等级/写开关必须拒绝；404/405/500 使用统一结构；请求日志不含 query/body；前端网络/超时/非 JSON 错误必须安全显示；迁移重复执行不得重建表。
首次本机 Docker 缺失时如实记录缺口；用户随后授权 SSH 实验机，已在那里完成真实 Compose/PostgreSQL 验证，详见开发日志追加记录。不进入 Phase 01。
