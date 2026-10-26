# LunaOrbit

Go + Vue 的自动发现优先的内网 Kafka/MM2 拓扑与信息平台：从 Kite 与 MM2 配置自动读出集群和同步关系，人工只负责补充和登记业务信息。

集群无需先登记：平台通过 Kite 读取 MM2 配置自动发现 Kafka 集群和同步关系；手工补充只用于未被 MM2 覆盖的集群。

**接续开发先读：[开发总结与交接说明](DEVELOPMENT_HANDOFF.md)。**

应用源码、设计文档、迁移与脚本统一位于 [`lunaorbit-codex-starter/`](lunaorbit-codex-starter/)。这是现有工作目录，本次交付保留结构，避免改动部署和测试路径。

```powershell
cd lunaorbit-codex-starter
pnpm --dir frontend install --frozen-lockfile
# 两个终端分别启动
pwsh -File scripts/tasks.ps1 information-backend
pwsh -File scripts/tasks.ps1 frontend
```

本地Mock访问 http://127.0.0.1:5173 。详细说明见[项目README](lunaorbit-codex-starter/README.md)，产品约束见[AGENTS](AGENTS.md)。

当前已提供无登录共享信息、Kafka只读资源查询、MM2拓扑、同名Topic全链路、应用/账号名称/Topic绑定。业务账号密码、权限/ACL管理和基础设施写入口均不开放。仓库不包含数据库数据、运行凭据、依赖目录或构建镜像。
