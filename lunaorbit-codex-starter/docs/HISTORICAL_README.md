# 历史开发说明（2026-10-23前）

仅保留实现历史；登录、权限、账号和实际配置写操作不再属于当前产品范围。现行说明以根README和信息平台设计为准。

# LunaOrbit · Codex 开发启动包 v2

## SCRAM 应用访问管理 v3（2026-10-21 首批）

进入集群「应用与权限」，点击「新增应用并开通账号」：填写应用、选择生产/消费用途、Topic及消费组，平台生成SCRAM-SHA-256账号和随机密码，审批后逐步开通并回读。支持录入已有账号密码（仅认证验证，不改原密码/ACL）、关联共享身份、查询未归属账号及完整原生ACL；全部规则支持ALLOW/DENY、Host和原生资源/匹配方式。平台团队授权移到「平台访问控制」。

托管密码通过独立授权的显式操作查看，关闭/离开/切集群及30秒后清除；Kafka原生不会返回原始密码。持久化部署配置独立 `ACCOUNT_MASTER_KEY`（随机32字节的十六进制），不要提交或打印。普通Mock自动生成临时密钥；PG模式不自动生成不可恢复密钥。业务秘密加密保存在现有workspace JSONB，旧应用Principal仅升级为身份资料，不自动写Kafka。

入口：[实验控制台](http://127.0.0.1:8088/kafka/clusters)。实验增加9094 SCRAM业务listener，9092保留给MM2及实验管理；真实认证/资源允许拒绝、消费组消费提交与已有账号录入验证见开发日志。v3后续配置管理、Topic生命周期、消息与配额批次尚未实施，完整范围见 `docs/superpowers/specs/2026-10-21-kafka-management-v3-design.md`。

## Kafka 工作台与应用授权（2026-10-20）

集群列表进入详情即打开工作台：概览、Topics、消费组、应用与权限、同步链路、操作记录、团队授权。支持创建 Topic、增加分区、调整保留时间；应用按生产/消费用途生成精确或前缀 ACL，消费授权必须指定消费组。提交前预览差异，审批后执行并实际回读，撤权只处理明确选中的规则。应用资料不创建 Kafka 认证账号，也不保存密码。

团队授权按集群、用户/可信 LDAP 组、角色和 Topic/消费组前缀管理。平台 RBAC 控制谁能操作平台资源，Kafka ACL 控制 Broker 上的 Principal 权限。默认保持独立审批和真实禁写；仅明确配置的 admin-lab 允许白名单实验目标写入与实验自动批准。

本地数据库迁移通过 `scripts/migrate.ps1` 按顺序应用 001–009。工作台应用、变更结果与审计、团队授权支持 PostgreSQL；Mock 模式提供状态一致的合成 Topic/ACL。实验访问：[Kafka 集群工作台](http://127.0.0.1:8088/kafka/clusters)，依赖当前机器的 SSH 隧道。测试和未验证项见开发日志末尾。

## Phase 02 Kafka 集群管理

无需 Kafka、Kite Key 或 Docker 即可操作完整离线演示。从本 README 所在应用根目录在两个终端分别运行：

```powershell
pwsh -File scripts/tasks.ps1 registry-backend
pwsh -File scripts/tasks.ps1 frontend
# 后端和前端均启动后，可在第三个终端执行：
pwsh -File scripts/tasks.ps1 registry-smoke
```

打开 [本地 Kafka 集群管理](http://127.0.0.1:5173/kafka/clusters)。默认普通 `backend` 任务仍不启用注册 API；`registry-backend` 明确选择仅回环监听的 Mock 模式。页面所有连接结果均为模拟，不会探测输入的地址。每个浏览器 Cookie 会话仅能访问自己登记的资源；切换选择只在当前会话的 sessionStorage 保存登记 ID，不保存密码、证书或原始 JAAS。

表单支持 PLAINTEXT/SSL/SASL_PLAINTEXT/SASL_SSL、PLAIN/SCRAM256/SCRAM512、CA/客户端证书/ServerName，密码和 PEM 只提交后端，不经查询读回。编辑时秘密留空表示保留；API 若显式传空串则清除并标记未配置。新增可以保存为 unverified，连接测试使用显式 Mock 场景；人工填写版本来源为 manual，不决定能力或精确 Broker 版本。未探测能力显示“未探测”，不能当成不支持。

需要真实 PostgreSQL 的登记持久化时：先将 Compose .env 的数据库名改为专用 `lunaorbit_phase02_test`，运行 `db` 和 `migrate`（现在依次执行 001、002）。然后在安全的进程环境注入指向数字回环地址、该专用数据库的 DATABASE_URL（仅允许 sslmode 参数），设置 REGISTRY_MODE=postgres-mock，并注入 32 字节随机 DEV_SESSION_KEY 的十六进制值；执行 `backend` 任务。不要打印或提交这些环境值。会话签名密钥保持不变且 Cookie 未过期时，服务重启后仍可访问原登记；丢失 Cookie 或更换签名密钥后，开发身份无法恢复。此模式的凭据仍只存内存，重启后必须重新录入，绝不声称已接入 KMS。

`DEV_BROWSER_ORIGIN` 默认为本机 Vite 的 http://127.0.0.1:5173；改变前端地址须同时更新该进程环境值。后端校验回环 Host、精确同源或该显式回环 Origin，并禁止 cross-site 请求；不启用通配 CORS。开发会话 HttpOnly + SameSite Strict，有效期 8 小时；所有注册变更都带 revision 并执行原子审计。

franz-go 工厂已独立实现四种安全协议、三种 SASL、严格 TLS/CA/mTLS 配置、逐 seed 与 advertised Broker 验证。出站策略默认拒绝，必须明确配置 CIDR 和端口；可附加主机名限制。解析出的所有地址都需通过校验，拨号固定 IP；临时客户端在有界超时、Broker 数量和并发预算内关闭。本阶段 HTTP 演示模式不会启用真实工厂；真实接入仍需生产 IdentityProvider、SecretStore 和运维出站策略，不能将演示会话用于生产。

已验证的流程与未验证的真实 Kafka 版本/SASL/mTLS 集成项详见开发日志。本阶段未实现 Broker/Topic/Group/ACL 详情或 MM2 关联，这些分别属于后续阶段；只有连接结果缓存中的 Broker 摘要可展示。

## Phase 01 Kite 只读诊断

从本 README 所在的应用根目录运行，无需 Docker、SSH 或 Kite Key：

```powershell
pwsh -File scripts/tasks.ps1 kite-mock
# 或在 backend 目录执行：go run ./cmd/kitecheck
```

默认是进程内 httptest Mock，执行契约中的 7 个 GET 路由，输出 7 行 JSON，`response_format=synthetic`。Fixture 完全合成，并非 Kite 真实响应；不会联系任何外部集群。该命令短时完成后退出，不提供网页地址。

真实模式仅供获批的非生产只读验证：由安全的终端环境注入 `KITE_BASE_URL`、`KITE_ALLOWED_ORIGINS`（逗号分隔的精确 origin）、`KITE_API_KEY`、`KITE_CLUSTER_NAME`。不把 Key 放入命令行、截图或日志；不在前端设置这些变量。随后在 backend 执行 `go run ./cmd/kitecheck -mode real -resource namespaces`。查询 `configmaps`、`deployments` 或 `pods` 时还须注入 `KITE_NAMESPACE`；设置 `KITE_RESOURCE_NAME` 则查询单个资源，留空则查询 namespace 列表。真实模式不接受 `-resource all`。

默认不连接真实 Kite；Web server 保持脱机。仅支持 HTTPS origin；HTTP 只允许数字回环地址用于本地测试。BaseURL 不允许账号、路径、query 或 fragment，并须匹配允许 origin；禁止重定向。Authorization 原样传 Key，无 Bearer；每个请求必须有 x-cluster-name。CLI 没有写操作、管理员集群枚举、跨 namespace 查询或任意 URL 入口。

真实响应封装尚未验证，因此收到 HTTP 200 后只报告 `transport=ok, response_format=unverified`，以非零状态退出；不会输出原始响应或声称完成资源发现。需先取得获批且脱敏的真实响应样本，再实现对应 Decoder。契约已知的仅是路由和认证规则，Mock 不代表真实 Kite envelope。

可替换 Decoder 位于 `backend/internal/kite`；响应对象的 JSON/fmt 输出均脱敏。5 秒调用预算涵盖重试和响应读取；CLI 真实模式最多重试一次，Adapter 最多允许两次重试，仅针对 429/5xx；无 Key、URL、上游 body 或底层错误回显。后续范围见 [Phase 01 方案](docs/PHASE01_PLAN.md)。

## Phase 00 本地运行

应用根目录为本 README 所在目录。需要 Go 1.25+、Node 24、pnpm 10.18.3、PowerShell 7；数据库需要 Docker Compose v2。无需 Kite Key、Kafka 或 LDAP 环境。所有外部连接尚未实现，两个写入开关只能为 false。

```powershell
# 在应用根目录执行
Copy-Item .env.example .env
# 本机编辑 .env，填入自行生成的本地 POSTGRES_PASSWORD，勿提交或共享该文件
pwsh -File scripts/tasks.ps1 db
pwsh -File scripts/tasks.ps1 migrate
# 再运行一次验证幂等
pwsh -File scripts/tasks.ps1 migrate

Set-Location backend
go mod download
Set-Location ../frontend
pnpm install --frozen-lockfile
Set-Location ..

# 分别在两个终端执行，均从应用根目录开始
pwsh -File scripts/tasks.ps1 backend
pwsh -File scripts/tasks.ps1 frontend
```

打开 [本地控制台](http://127.0.0.1:5173)；健康检查为 [本地 API](http://127.0.0.1:8080/healthz)。Vite 默认代理到 8080；若改变 HTTP_ADDR，需同时修改本地开发代理配置。生产部署的同源反向代理与认证不属于本阶段，不应将骨架公开暴露到网络。

Docker Compose 自动读取根目录 .env；Go 后端读取进程环境（不自动载入 .env），可用 `$env:LOG_LEVEL='debug'` 设置。HTTP_ADDR 默认 127.0.0.1:8080，LOG_LEVEL 支持 debug/info/warn/error。数据库凭据只供 Compose PostgreSQL 使用，不传入前端，不由 Web 进程读取。

```powershell
pwsh -File scripts/tasks.ps1 test       # go test + go vet + Vitest
pwsh -File scripts/tasks.ps1 typecheck
pwsh -File scripts/tasks.ps1 lint
pwsh -File scripts/tasks.ps1 build      # backend/bin 与 frontend/dist
pwsh -File scripts/tasks.ps1 smoke      # 需本地 API 已启动，只允许回环地址
docker compose stop                    # 停止本地数据库，保留数据
```

无 Docker 时可独立运行前后端和基础测试；`/healthz` 只证明进程存活，不检查数据库或外部服务。迁移是供 psql 执行的事务脚本，带 advisory lock 与 schema_migrations 版本保护。审计 Sink 目前仅为后续服务提供参数化 SQL 接口，尚未接入 HTTP、身份认证或数据库驱动；不提供任意审计写入入口。

目录与约束决策见 [Phase 00 方案](docs/PHASE00_PLAN.md)，实际测试与未验证项见 [开发日志](docs/IMPLEMENTATION_LOG.md)。

Phase 00 独立实验数据库可将 `POSTGRES_DB` 设置为 `lunaorbit_phase00_` 开头的合成名称。在 Linux 实验机上设置与启动时一致的 `COMPOSE_PROJECT_NAME` 后，运行 `bash scripts/verify-postgres.sh`，可校验重复/并发迁移及版本表和审计索引；脚本拒绝普通业务数据库名称，不自动删除任何数据。

审计实际落库测试为显式启用：在进程环境中设置 `LUNAORBIT_TEST_DATABASE_URL` 后，于 backend 执行 `go test -v ./internal/audit -run '^TestSQLSinkPostgres$' -count=1`。仅允许连接 127.0.0.1 且数据库名称以 `lunaorbit_phase00_` 开头；未设置时显示 SKIP，不计为数据库验收通过。测试只写入合成审计事件；不要将连接串或密码写入代码、命令历史及日志。

Kafka / MirrorMaker 2 统一运维平台。架构：Go + Gin / Vue3 / PostgreSQL / franz-go / Kite API / LDAP / RBAC / AntV X6。**本版新增了参考用户 Kafka 集群切换界面的注册、认证、连接测试和集群独立权限配置**；禁止保存或展示真实截图中的敏感信息。

## 必看文件
- `AGENTS.md`：Codex 全局开发规则
- `ROADMAP.md`：新阶段顺序（集群登记前置）
- `docs/KAFKA_CLUSTER_MANAGEMENT.md`：本次的核心新增方案、字段、表/API、验收与连接安全
- `docs/ARCHITECTURE.md`、`docs/DATA_MODEL.md`、`docs/SECURITY.md`：其余基础约束

## 推荐 Codex 操作
1. 在新的代码仓库中解压启动包，打开项目根目录运行 Codex。
2. 让 Codex 完整阅读 `AGENTS.md` 与设计文档，执行 `prompts/00-foundation.md`。
3. 完成每阶段验收后，按 `ROADMAP.md` 顺序运行新 prompts：`01-kite-readonly.md`、`02-kafka-registry.md`、`03-mm2-discovery.md`、`04-topology-ui.md`、`05-kafka-ops-readonly.md`、`06-ldap-acl.md`、`07-mm2-changes.md`、`08-observability.md`。
4. 已保留旧版 prompt 供对比，**不要使用旧的 `prompts/04-kafka-readonly.md` 或 `prompts/02-mm2-discovery.md` 作为新阶段的依据**。
5. 任何真实服务集成前准备脱敏测试数据、Kite 非生产权限账号与非生产 Kafka 连接凭据；不要将凭据交给 AI。

## 非目标
- 不以 Git 管理生产 MM2 配置；但可用 Git 管理 LunaOrbit 本身的源码。
- 不自建通用 Kubernetes 控制台；Kite 是 MM2 的控制入口。
- 不从真实图片复制账号密码、认证字段原文到 Fixture。

## Phase 03 MM2 自动发现（离线 Mock）

```powershell
# 无服务依赖，输出脱敏 JSON；保留旧证据的失败演示加 -stale-demo
Set-Location backend
go run ./cmd/mm2discover
go run ./cmd/mm2discover -stale-demo
Set-Location ..
# 开发 API：会话创建/集群登记沿用 Phase 02，仅监听回环
pwsh -File scripts/tasks.ps1 discovery-backend
# 另一个终端执行 HTTP 验收
pwsh -File scripts/tasks.ps1 discovery-smoke
```

默认 `DISCOVERY_MODE=disabled`。显式 mock 需要 `REGISTRY_MODE=mock` 或 `postgres-mock`；后者使用既有开发数据库配置，并先执行 001–003 迁移。合成名称属于隔离 fixture，不代表环境配置。API 挂载在已鉴权的 `/api/v1/discovery/scopes` 下：GET scopes，POST `/:scope/runs`，GET `/:scope/runs/latest`，GET `/:scope/snapshot?page=1&page_size=20`，PUT `/:scope/bindings`。发现异步运行、限流，快照/审批均按用户和范围隔离。开发会话仍不能替代 LDAP。

部署引用的 properties/YAML 配置只提取别名、方向、Topic 规则和白名单字段；原始配置、密码、JAAS 不进入快照。地址重合只产生候选；人工确认必须关联自己的启用登记，不能声称已验证数据同步。独立的实际 Cluster ID 证明优先匹配算法已单测，但真实身份采集源尚未接入。失败时保留证据并标记 stale/unknown；RF 始终待实际查询。

Kite 仍只使用既有 GET 契约。真实资源 envelope 未核验，生产解码器未启用；synthetic decoder 不代表真实 Kite 响应结构。X6 可视化属于 Phase 04，本阶段提供图数据 API。详见 `docs/PHASE03_PLAN.md` 和开发日志。

## Phase 04 同步拓扑

打开 [本地同步拓扑](http://127.0.0.1:5173/topology)，首次进入点击“运行发现”。页面通过 Phase03 API 读取合成资源，展示 Kafka → MM2 Deployment → Kafka 方向、可拖动节点、机房/环境过滤、图层开关、证据侧栏和个人布局保存。

```powershell
# 两个终端分别从应用根目录启动
pwsh -File scripts/tasks.ps1 discovery-backend
pwsh -File scripts/tasks.ps1 frontend
# 验证真实 X6 渲染/拖动/恢复、筛选和登记跳转（前后端需已启动）
pwsh -File scripts/tasks.ps1 topology-e2e
```

E2E 默认使用本机已安装 Microsoft Edge 的无头模式；普通 `pnpm test` 只运行 Vitest，不隐含浏览器验收。浏览器输出在被忽略的 frontend/test-results；本机预览截图为 topology-preview.png。

机房/环境只来自**已确认映射的当前用户登记**；初始合成图没有人工绑定时会显示未知。地址候选不参与确认筛选；匹配任一端点保留完整方向。Phase03 bindings API 可做人工确认，本阶段没有新增审批表单。源目标登记链接按具体登记ID读取，不受列表第一页限制。

个人视图 GET/PUT `/api/v1/topology/scopes/:scope/view` 仅保存 revision、positions、filters、layers。复用开发会话与 Origin 校验，并要求当前用户能读取该范围快照；并发版本冲突返回409。数据库仅写独立 topology_views，绝不修改发现关系。Mock 内存模式重启即丢失；postgres-mock 模式需迁移001–004并使用既有固定会话签名密钥。应用仍仅供回环开发，LDAP和生产认证未完成。

证据面板只显示已提供的脱敏 Deployment/ConfigMap 摘要；Pod 仅有标签候选数量与关联程度，无已核验的名称/归属。未验证、unknown、禁用声明和过期关系分别标注，RF不推断。大图最多1024条关系，完整分页后才呈现；无法取得完整图或版本变化会报错，不静默显示截断结果。

## Phase 05–08 已实现功能与运行

连续阶段的实施与自动验收已完成，真实外部系统的未验证边界单独保留。使用两个终端运行：

```powershell
pwsh -File scripts/tasks.ps1 discovery-backend
pwsh -File scripts/tasks.ps1 frontend
```

打开 http://127.0.0.1:5173。该任务启用 Registry、Discovery、Changes、Observability 的内存 Mock；集群详情提供 Broker、Topic 分区 RF/ISR、消费组与 offset/lag，安全页提供身份入口和 ACL 只读/申请，变更页提供双人审批与模拟执行，拓扑提供指标、潜在下游影响和人工业务血缘确认。先运行发现以生成合成图；合成资源均明确标记。内存模式重启会丢失数据。默认 dev 身份不支持 LDAP 登录，不代表企业目录已经接入。

持久化实验使用 `REGISTRY_MODE=postgres-mock`、`DISCOVERY_MODE=mock`、`CHANGES_MODE=mock`、`OBSERVABILITY_MODE=mock`；顺序执行迁移001–007，安全注入 `DATABASE_URL`、稳定的 `DEV_SESSION_KEY` 与32字节十六进制 `CHANGE_SNAPSHOT_KEY`。Go读取进程环境，不自动加载 `.env`。不要将真实值写进仓库。密文快照密钥丢失后无法解密历史备份。

身份可显式设置 `IDENTITY_MODE=ldap`，同时配置 `.env.example` 中的 LDAPS URL、地址/CIDR白名单、CA、服务绑定及组角色映射；启用后关闭旧开发会话入口。`IDENTITY_MODE=mock` 仅接受服务端注入的合成账号。`ACL_APPROVAL_SCOPES` 和 `CHANGE_APPROVAL_SCOPES` 均为服务端 JSON 对象，将可信 subject ID 映射到允许审批的登记ID或目标ID数组，默认空拒绝；申请人不能自批。LDAP身份不自动等于Kafka Principal。

`ENABLE_KITE_WRITES`、`ENABLE_KAFKA_WRITES` 必须保持false。ACL批准仅记录待执行，MM2变更仅支持合成执行；复制指标缺失时进入人工复核。Kite transport严格使用已有契约，但真实响应Decoder和实际写路径尚未启用。业务血缘确认不会把发现关系改为 verified，潜在影响也不代表故障已经传播。

验证命令：backend运行 `go test ./... -count=1`、`go vet ./...`、`go build ./...`；frontend运行 `pnpm test`、`pnpm typecheck`、`pnpm lint`、`pnpm build`、`pnpm test:e2e`。普通测试中显式外部集成SKIP不算验收成功；身份/双人变更浏览器测试需专用18080合成身份服务及 `LUNAORBIT_IDENTITY_E2E=1`。本轮已实际运行全部7项浏览器测试、两种真实Kafka版本和实验PostgreSQL重启生命周期，详细条件见开发日志。`scripts/lab-lifecycle.py` 仅面向专用回环实验库，不用于生产。

仍待真实环境验证：企业LDAP与生产SecretStore、Kafka多Broker/SASL/mTLS/真实authorizer、Kite响应封装与真实受控变更/回退、Prometheus/MM2业务指标。当前实现不作为生产上线验收结论。

## 真实Minikube实验（2026-10-20）

已在授权Linux实验机的独立profile部署Kite v0.16.0、Kafka3.7.2/3.9.2和MM2，实际通过唯一合成消息跨集群复制、七条Kite GET、只读权限拒绝及Go采集/原生Kafka读取。复现与SSH隧道见 [Minikube实验说明](deploy/lab/MINIKUBE.md)。实验保留运行；当前本地网页仍是Mock模式，平台真实写开关保持false。本次只核验了该Kite版本的小样本响应，没有启用生产Decoder或真实变更执行。

## 管理平台界面与真实只读模式

2026-10-20完成前端管理控制台改版：侧边导航、顶栏/面包屑、统一表格/表单/筛选/分页与响应式布局，覆盖全部现有页面。总览仅显示实际登记与快照数量，未取得数据保持未知。

显式启用真实实验读取的环境项见.env.example：LUNAORBIT_RUNTIME_MODE=readonly-lab、REGISTRY_MODE=mock、DISCOVERY_MODE=readonly-lab；KITE_BASE_URL/KEY/CLUSTER_NAME/NAMESPACE/KITE_LAB_VERSION与Kafka地址/CIDR/端口白名单必须同时配置。这里REGISTRY_MODE=mock表示临时存储及开发会话，真实原生读取由独立runtime模式控制；页面来源以/api/runtime和查询synthetic字段为准。真实写入仍禁用，ACL/变更/指标未接入时隐藏相关动作。

已部署的实验入口为 http://127.0.0.1:8088 （本机SSH转发已启动）；前后端实际在授权Linux实验Pod中运行。隧道失效后重新建立：`ssh -N -o ServerAliveInterval=15 -o ServerAliveCountMax=3 -L 8088:127.0.0.1:18088 root@192.168.111.128`，密码交互输入，不放命令行。服务端port-forward需保持运行，详细复现见deploy/lab/MINIKUBE.md。实验登记按会话所有者隔离且内存存储，不将其他测试会话的数据暴露给新用户。


实验入口为当前管理员会话自动初始化已部署的源、目标 Kafka 登记，并执行真实连接探测。该行为仅在服务端显式配置 KAFKA_LAB_REGISTRATIONS 的只读实验模式启用；保留所有者隔离和用户后续编辑、删除。刷新集群页面即可加载，无需手动再次录入。

授权实验部署已支持 admin-lab 管理模式：拓扑自动发现并映射已部署两端 Kafka，MM2 Topic 规则可预览、批准、执行。实际写入仅限服务端白名单资源，通过 Kite 完成；默认部署仍不启用写入。Kafka ACL 的可执行性取决于 Broker authorizer，当前 PLAINTEXT 实验 Broker 尚未配置 authorizer。


---
> 2026-10-26 追加说明（LUNA-60）：产品定位调整为自动发现优先的内网 Kafka/MM2 拓扑与信息平台：从 Kite 与 MM2 配置自动读出集群和同步关系，人工只负责补充和登记业务信息。本文为历史记录，原文不改；手工集群登记相关内容已被自动发现取代，现行约束见 AGENTS.md 与 SECURITY.md。
