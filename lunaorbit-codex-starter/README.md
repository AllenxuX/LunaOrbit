# LunaOrbit

换电脑或接续开发请先读仓库根目录的 [开发总结与交接说明](../DEVELOPMENT_HANDOFF.md)。

内网使用的 Kafka / MirrorMaker 2 消息拓扑与信息记录平台。

- 无登录，所有访问者共享同一工作空间的集群和应用记录。
- 查询集群、Broker、Topic、分区、消费组和配置；观察MM2同步拓扑与上下游路径。
- 登记应用、负责人、环境、部署位置，以及Kafka账号/Client ID与生产/消费Topic、消费组的绑定；关系注明人工登记，不冒充实时流量。
- 浏览器各自保存拓扑布局和筛选偏好。
- 不提供平台授权、Kafka账号、密码交付、ACL或基础设施配置修改。

新增 **Topic 全链路图**：从左侧「Topic 链路」或Topic详情「查看链路」进入，选择集群与Topic，追踪1～4段上下游、MM2逐跳命名、消费组和人工登记的应用。实际观测、同步配置、历史位点、人工登记及未知证据分别标注；不把配置存在视为消息已复制。专用实验当前使用同名Topic复制（IdentityReplicationPolicy），A/B/C选择 `lunaorbit-lab-events` 即可查看。详见 [Topic链路说明](docs/TOPIC_LINEAGE.md)。

## 本地启动

需要 Go、Node、pnpm 和 PowerShell。两个终端分别运行：

```powershell
pwsh -File scripts/tasks.ps1 information-backend
pwsh -File scripts/tasks.ps1 frontend
```

打开 http://127.0.0.1:5173 。该命令默认使用内存Mock；重启后信息记录会清空。要保存记录，先配置PostgreSQL并执行migrations，再在启动终端设置 `REGISTRY_MODE=postgres-mock`、`DATABASE_URL`、固定的 `INFORMATION_WORKSPACE_ID`。Go读取进程环境，不会自动载入 `.env`。

`.env.example`仅提供本地配置示例；数据库密码、Kafka连接凭据与Kite Key不要提交。集群SASL/TLS连接凭据仅用于后台采集，不能读取原值。目前连接Secret Provider仍为内存实现；生产持久化秘密提供方尚未接入，重启后可能需要重新录入连接秘密，不能把数据库中的引用当成秘密已持久化。专用实验的PLAINTEXT目录不受此限制。

## 运行配置

`PLATFORM_MODE=information` 是当前产品模式，`INFORMATION_WORKSPACE_ID` 必须明确指定。无LDAP、会话Cookie或角色授权。服务只展示该空间的记录，不会自动合并历史用户数据。

默认仅接受本地Host；内网部署需明确配置 `INFORMATION_ALLOWED_HOSTS`（含端口），跨源开发才设置 `INFORMATION_BROWSER_ORIGINS`。两项实际写开关保持false，`CHANGES_MODE=disabled`。保留出站地址限制、采集超时、请求脱敏及信息修改的revision冲突检查。

数据库迁移010创建独立应用信息表，旧账号及授权历史不删除，也不通过信息模式开放。实验升级脚本 `scripts/deploy-information-lab.py` 仅用于已授权的专用lab，要求明确选择单一历史空间，保留数据库及部署备份；不能用于生产。

## 检查

```powershell
pwsh -File scripts/tasks.ps1 test
pwsh -File scripts/tasks.ps1 typecheck
pwsh -File scripts/tasks.ps1 lint
pwsh -File scripts/tasks.ps1 build
```

设计：[信息平台收敛方案](docs/superpowers/specs/2026-10-23-topology-information-platform.md)。历史功能说明：[历史README](docs/HISTORICAL_README.md)。进展见ROADMAP与docs/IMPLEMENTATION_LOG。
