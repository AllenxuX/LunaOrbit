你现在在 LunaOrbit 项目根目录。请完整阅读 AGENTS.md、docs/PRD.md、docs/ARCHITECTURE.md、docs/KITE_API_CONTRACT.md、docs/SECURITY.md、docs/ACCEPTANCE.md 和 ROADMAP.md。

仅实现 Phase 00：
1. 初始化 monorepo：backend（Go，可用 Gin）、frontend（Vue3+TS+Vite+Element Plus）、migrations、docs、scripts。
2. 后端实现 /healthz、结构化日志、统一错误响应、env 配置校验；前端实现基础壳、路由、空状态、API Client。
3. PostgreSQL 使用本地 Docker Compose 启动；有至少一条迁移，可重复执行；所有凭据只能来自环境变量。
4. 增加 Makefile 或任务脚本，提供启动/测试/类型检查/构建命令；配置 .gitignore。写一个本地无需 Kite Key 的 README。
5. 添加基础自动化测试和一个简单 smoke test。不要实现 MM2 写操作、生产接入或 LDAP。
6. 先给出目录/依赖/测试计划再编码；完成后**实际运行** go test ./...、go vet ./...、pnpm typecheck/build（如果环境无法运行，请如实报告）。
7. 仅更新 ROADMAP 中 Phase 00 状态和 docs/IMPLEMENTATION_LOG.md，不要自动进入 Phase 01。
