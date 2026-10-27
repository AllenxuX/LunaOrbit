# 开发日志

## LUNA-UX-05 · UX-v2 后端与契约

- 唯一契约：`docs/UX_V2_API_CONTRACT.md`；复用发现任务/快照、目录、Topic链路与分离的Topic配置查询，不新增健康或逐消息接口。
- 最小实现：Scopes公开namespaces和namespace_mode，准确确认整个scope采集边界；不增加namespace局部重试。只改后端/测试/文档，不部署、不改依赖或基础设施。
- TDD：TestUXScopeCollectionRange先因namespace_mode缺失失败，再实现通过。全量首跑暴露两个旧JSON精确断言，针对新增字段更新期望后通过。
- 实际验证：`go test ./...` exit 0；`go vet ./...` exit 0；`go test ./internal/discovery -run 'TestUXScopeCollectionRange|TestDiscoveryHTTPUsesRegistryIdentityAndPagination' -count=1 -v` exit 0；`git diff --check`通过。Go使用 `/opt/homebrew/opt/go@1.25/bin/go`。
- 完整日志：`/Users/xuallen/.hermes/profiles/rd-dev/cache/scratch/LUNA-UX-05-go-test.log`、同目录 `LUNA-UX-05-go-vet.log`、`LUNA-UX-05-smoke.log`。
- 限制：未执行真实环境采集/部署/独立QA；PG真实测试仍取决于原有测试环境变量，默认通过不代表真实PG验收。快照stale不是自动TTL；链路生成时间不是发现时间；配置响应无采集时间。重新读取链路/对象可能访问Kafka只读元数据，不能说完全不访问外部。

## 2026-10-19 · Phase 00 工程骨架

状态：Phase 00 已完成。首次本机应用验证后缺少 Docker，随后在用户明确授权的 SSH 实验机补齐数据库运行验收，见文末追加记录。未进入 Phase 01。

### 本阶段能力与文件
- backend：Go 1.25 / Gin 1.12，GET /healthz；JSON slog、服务端请求 ID、安全的 404/405/500 错误；env 校验、拒绝启用两个写开关、HTTP 超时与信号退出。
- backend/internal/audit：metadata-only Sink 接口与参数化 SQL 实现；无审计 HTTP 写口，Web 启动不连接数据库，后续业务使用时再注入实际存储；pgx 只用于下文追加的集成测试。验证错误与数据库错误不回显输入/驱动原文。
- frontend：Vue3 + TS strict + Vite + Element Plus + Router；总览、空状态、404、安全 API Client、手动重试、进程健康与外部健康明确区分。
- migrations/001_audit.sql：审计表及索引、迁移版本记录、事务锁与重复执行保护；为 psql 脚本，不是可直接交给普通 SQL driver 的文件。
- compose.yaml、scripts/*.ps1：回环地址 PostgreSQL、环境凭据、显式迁移、启动/测试/类型检查/lint/构建/smoke 任务。更新 README、.gitignore、合成环境样例及方案记录。

### 约束裁决
- 按用户要求使用 Gin；Kafka v2 设计优先于旧资产表草案，实际业务模型延后到 Phase 02。
- PRD P0 是产品范围，当前交付范围以 prompts/00-foundation.md 为准。
- /healthz 仅表示 Web 进程可用，不把数据库或任何集群标成健康。
- 不实现或请求 Kite/Kafka/LDAP；没有 GitOps、生产访问、生产写入或提交凭据。
- 使用既有启动包子目录作为应用根，不移动原有规范。所有新增代码均在该目录，未创建分支、提交、发布或推送。

### 实际执行与结果

环境：Windows PowerShell、Go 1.25.6、Node 24.19.0；依赖安装使用 pnpm 10.18.3，系统 pnpm 11.20.0 也成功执行任务。

| 工作目录 | 命令 | 结果 |
|---|---|---|
| backend | 初次 `go test ./...` | 预期失败：Load 未实现，Gin 尚未安装；随后实现并安装依赖 |
| frontend | 初次 `pnpm test` | 预期失败：API Client 文件尚未实现 |
| backend | `gofmt -w .`、`go mod tidy` | 完成 |
| backend | `go test ./... -count=1` | audit/config/httpapi 三个包通过；cmd/server 无测试 |
| backend | `go vet ./...` | 退出码 0 |
| backend | `go build -o bin/lunaorbit.exe ./cmd/server` | 退出码 0，构建后本地启动 |
| frontend | `pnpm test` | 2 个文件，7 项测试通过 |
| frontend | `pnpm typecheck` | 退出码 0 |
| frontend | `pnpm lint` | 退出码 0，无 lint 警告 |
| frontend | `pnpm build` | 退出码 0；存在 >500 kB JS chunk 提示 |
| frontend | `pnpm audit` | 更新依赖后 No known vulnerabilities found |
| 应用根 | `./scripts/tasks.ps1 test` | Go test + Go vet + 前端 7 项测试通过 |
| 应用根 | `./scripts/tasks.ps1 smoke` | 本机 8080：healthz=200、请求 ID 存在、统一 404 通过 |
| frontend | `pnpm dev --strictPort` | 本机 5173 启动成功 |
| 应用根 | `Invoke-WebRequest http://127.0.0.1:5173/` | HTML=200 且标题为 LunaOrbit |
| 应用根 | `./scripts/smoke.ps1 -BaseUrl http://127.0.0.1:5173` | Vite 到后端的代理 health/404 检查通过 |
| 应用根 | PowerShell Parser 检查 scripts/*.ps1 | 三份脚本解析通过 |
| 应用根 | `docker --version`、`docker compose config` | 无法运行：系统未安装/未提供 docker 命令 |

测试覆盖：配置默认值、非法端口/日志级别/写开关；HTTP 健康、404/405、panic 500、请求 ID 及 query/panic 脱敏；审计非法事件与存储缺失失败关闭；前端 HTTP/网络/响应格式/超时错误及页面空状态、失败重试。

执行问题记录：初次 npx 使用系统 npm 缓存遇到 EEXIST/EBADF，改为独立临时缓存后安装成功，未删除系统缓存；首次依赖审计发现旧 Vue/Vite/Vitest 等版本漏洞，已更新依赖及锁文件并重跑全部检查；一次在应用根误跑 go test ./... 因 Go module 位于 backend 失败，随后在 backend 重新执行通过。只读复核未发现阻塞问题；复核不代表实际数据库验证。

### 未验证项与风险
- 首次交付时 Docker/PostgreSQL 验收未完成；此缺口已在后续授权实验机验证中补齐，详见下文。Windows 本机仍未安装 Docker。
- Kite 只读接口（namespaces/_all、configmaps/deployments/pods 的 namespace 列表及单资源 GET）未实现、未联网验证；Authorization 原样 Key、x-cluster-name、真实响应外层/错误/分页/版本差异均留待 Phase 01 的契约测试及授权非生产样本确认。Mock 测试目前仅覆盖平台自己的 HTTP/API，不宣称验证 Kite。
- Kite PUT/PATCH、ConfigMap resourceVersion 冲突与 Deployment rollout 均未实现；Kafka franz-go 连接、LDAP、RBAC/ACL、X6、监控均留待对应阶段。
- 前端整包引入 Element Plus，JS 约 1 MB（gzip 约 325 kB），可在后续页面增多时做按需导入；当前构建提示已记录，不影响本地骨架运行。
- 本阶段不是可公开部署的认证控制台，没有用户登录及生产配置；没有浏览器端 E2E，已完成 Vue 组件测试和实际 HTML/HTTP 代理 smoke。

后续建议：Phase 00 数据库运行验收已补齐；由用户批准后再进入 Phase 01，不自动开工。

## 2026-10-19 · Phase 00 授权 SSH 实验机补验

授权范围：用户明确提供 SSH 实验机，允许作为实验环境。只操作本次新建的独立 Docker Compose 项目，不访问 minikube/Kubernetes、Kafka 或生产资源。主机登录口令没有写入源码、脚本、文档或日志；实验数据库密码在远端随机生成，仅存于权限 0600 的临时 .env，未回显。

实验目录：`/root/lunaorbit-phase00-lab-20261019`；项目：`lunaorbit_phase00_20261019`；数据库仅绑定实验机 `127.0.0.1:25432`。该目录不是 Web 页面。记录日期使用客户端 Asia/Shanghai 时间；实验机自身时钟与客户端不同，不以远端输出的创建日期作为开发日期。

### 新增文件
- `backend/internal/audit/audit_integration_test.go`：通过 pgx 标准驱动验证 SQLSink 真实插入及查询、数据库长度约束错误脱敏；必须显式设置 LUNAORBIT_TEST_DATABASE_URL，仅允许回环地址与专用 Phase 00 库名。常规本机测试没有实验 DSN 时明确 SKIP，不冒充集成成功。
- `scripts/verify-postgres.sh`：Linux 实验机的重复/并发迁移、版本表与审计索引断言；拒绝非专用实验数据库。
- `backend/go.mod/go.sum` 增加测试所需 pgx 依赖；README 增加实验验收运行方法。Web 服务仍不自动连接数据库，不引入任何外部业务集成。

### 实际命令与证据
| 环境 | 命令 / 检查 | 结果 |
|---|---|---|
| 实验机 | `docker version` / `docker compose version` | Docker 29.5.2 / Compose v5.1.4 |
| 实验机 | `docker pull postgres:17-alpine` | 首次 100 秒超时，有限重试后成功；实际 PostgreSQL 17.11 |
| 实验机 | `docker compose -p lunaorbit_phase00_20261019 config --quiet` | 退出码 0，不输出展开后的凭据 |
| 实验机 | `docker compose -p lunaorbit_phase00_20261019 up -d --wait --wait-timeout 60 postgres` | running、healthy，端口仅回环地址 |
| 实验机 | 迁移前执行 audit-integration.test | 预期失败：audit persistence failed，证明测试需要真实审计表 |
| 实验机 | `COMPOSE_PROJECT_NAME=lunaorbit_phase00_20261019 bash scripts/verify-postgres.sh` | 连续两次及并行两次迁移成功；版本 1 只有一行；审计表及索引存在 |
| 实验机 | 环境注入 DSN 后执行 `./audit-integration.test -test.v -test.run '^TestSQLSinkPostgres$'` | PASS，事件字段一致；长度约束错误被转换为安全错误 |
| 实验机 | 原始 Compose 默认磁盘卷重建后再次执行迁移及落库测试 | 全部 PASS；未依赖内存卷覆盖配置 |
| 实验机 | `docker compose -p lunaorbit_phase00_20261019 up -d --force-recreate --wait --wait-timeout 60 postgres` 后 SQL 断言 | PASS：审计事件和迁移版本记录在容器重建后仍保留 |
| 实验机 | `bash -n scripts/verify-postgres.sh` | 退出码 0 |
| Windows backend | `gofmt`、`go mod tidy`、`go test ./... -count=1`、`go vet ./...`、`go build -o bin/lunaorbit.exe ./cmd/server` | 全部通过；本机 PostgreSQL 集成项按配置明确 SKIP，实际集成 PASS 来自实验机 |
| Windows backend | `GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go test -c -o bin/audit-integration-linux.test ./internal/audit` | 成功；上传该测试二进制，避免在实验机下载 Go 依赖 |

磁盘保护：初始根分区仅余约 593 MB；镜像下载后短时仅余约 119 MB，因此先采用专用 tmpfs 卷完成验证。随后观测到可用空间回升至约 4.6 GB，才重建为原始 Compose 默认磁盘卷，并完成相同迁移、落库与容器重建持久性验证。未清理已有镜像、修改 Docker daemon 或操作 minikube。

完成后只清理本次专用 Compose 项目及合成数据卷、新下载的 postgres:17-alpine 镜像、远端临时 .env 和测试二进制；保留不含凭据的实验配置及脚本。后续复用实验机需重新生成环境凭据。

最终状态：Phase 00 已勾选完成。Kite、Kafka、LDAP 等此前列出的外部接口仍未实现、未验证，分别属于后续阶段；本次没有新增应用 Web 部署地址。

复核修正：发现 pgx 连接串查询参数可覆盖 URL 中的目标，原有集成测试仅检查主机/路径不足以限制目标。已加入查询参数白名单，只允许单个已知 sslmode 值，拒绝 host/hostaddr/dbname/database/service、编码参数名及重复参数。8 个拒绝场景在修复前实际失败、修复后通过；所有本机 Go 测试及 vet/build 重新通过。此修正仅加固测试连接入口，未改变已在实验机验证的 SQLSink 和迁移逻辑；真实实验所用连接串仅包含 sslmode=disable，与新校验一致。

## 2026-10-19 · Phase 01 Kite 只读 Adapter

状态：用户批准继续下一阶段，已完成 Phase 01 的 Mock 契约验收，停在本阶段。真实 Kite 响应封装仍未验证；不把本阶段勾选等同于生产接入成功。

### 可运行能力及文件
- `backend/internal/kite/types.go`：KiteClient 七个只读方法、结构化错误码、私有原文 Document、可替换 Decoder 与 metadata Snapshot。Document 的 fmt/JSON 输出固定脱敏；nil decoder 明确返回 RESPONSE_FORMAT_UNVERIFIED。
- `backend/internal/kite/client.go`：精确 origin 允许列表，BaseURL 禁止账号/path/query/fragment，HTTPS（HTTP 仅数字回环测试地址）、禁止重定向；Namespace/DNS 资源名校验、原值 Authorization、每请求必填 x-cluster-name；全调用 context 超时、最多两次且只针对 429/5xx 的重试、1 MiB 响应上限。
- `backend/internal/kite/mock/`：仅本地演示用 httptest Handler、合成 Fixture 与专用 Decoder；明确标注 synthetic_fixture，不是 Kite 后端实现或真实 envelope。
- `backend/cmd/kitecheck/`：默认 Mock、覆盖全部 7 个 GET；真实模式需显式 -mode real、-resource 和环境配置；没有任意 URL 参数、生产写口或原文输出。真实 HTTP 200 的响应格式未知时仅报告 transport=ok/response_format=unverified，并非零退出。
- `.env.example` 增加空的可选 Kite CLI 配置名；README 增加运行和未验证说明；`scripts/tasks.ps1` 增加 kite-mock 任务及 CLI 构建。
- 无新增前端业务页面、数据库迁移、Kafka 注册、MM2 发现、LDAP/RBAC 或写操作。Web 进程继续脱机；没有新增对外 Web 地址。

### 真实执行命令与结论
| 工作目录 | 命令 | 结果 |
|---|---|---|
| backend | 首次 `go test ./internal/kite` | 预期失败：HTTPClient/Config 等尚未实现 |
| backend | 首次 `go test ./cmd/kitecheck` | 预期失败：run 尚未实现 |
| backend | `gofmt -w internal/kite cmd/kitecheck` | 完成 |
| backend | `go test ./... -count=1` | 全部包通过；Phase 00 的 PostgreSQL 实验项未注入 DSN，按既定规则 SKIP |
| backend | `go vet ./...` | 退出码 0 |
| backend | `go build -o bin/kitecheck.exe ./cmd/kitecheck`、`go build -o bin/lunaorbit.exe ./cmd/server` | 均退出码 0 |
| backend | `go test -cover ./internal/kite ./cmd/kitecheck` | 分别 88.2% / 70.9% statement coverage；新增共 12 组顶层测试，含表驱动子场景 |
| backend | `go run ./cmd/kitecheck` | 7 行 JSON，全部 synthetic 格式，每条含 1 个合成条目 |
| 应用根 | `./scripts/tasks.ps1 kite-mock` | 7 条只读 Mock 查询成功，无外部访问 |
| 应用根 | `./scripts/tasks.ps1 test` | Go 测试/vet、前端 7 项测试通过 |
| frontend | `pnpm test`、`pnpm typecheck`、`pnpm lint`、`pnpm build` | 全部通过；保留既有约 1 MB JS chunk 的构建提示 |
| 应用根 | 启动构建后的 Web 服务，执行 `./scripts/tasks.ps1 smoke` | /healthz 和统一 404 通过，随后停止本次测试服务 |
| 应用根 | PowerShell Parser 检查 tasks.ps1、`git diff --check` | 通过 |

测试覆盖：七路由/GET/头部原值、缺失或控制字符 cluster、Namespace/name 路径注入、URL 允许列表/不安全配置/非法 Key、401/403/404/429/500/503、重试成功及上限、跨 origin 重定向阻止、调用与响应体超时、context 取消、重试等待纳入调用预算、网络失败脱敏、响应限长、Document 多种 fmt/JSON 输出、Decoder 错误脱敏、默认 Mock 不读取真实配置、显式 real 在 httptest 中不猜测响应。

修正记录：状态范围检查初版将所有 >=500 状态当成 5xx；新增 600 状态回归场景后实际出现错误重试 3 次，修正为 [500,600) 后通过，避免对未知状态自动重试。独立只读复核未发现阻塞 Phase 01 的重要问题；复核不替代已执行测试。

### 尚未验证与边界
- 没有连接真实 Kite，也未使用 SSH 实验机或 minikube。本阶段不需要外部凭据，所有测试数据均合成。
- 真正 Kite 的 Namespace/ConfigMap/Deployment/Pod 响应 envelope、内部错误结构、分页参数及版本差异仍需获批的脱敏只读样本。Adapter 不猜测这些结构，不附加未知分页参数；目前 List 为有大小上限的单次请求，不声称覆盖真实全集。
- 未实现日志、Watch、跨 namespace 通配查询、管理员集群枚举、Kite PUT/PATCH、MM2 rollout、Kafka API 或 LDAP。没有把 ConfigMap 存在当成同步健康，也没有解析或输出敏感 ConfigMap 内容。
- Mock Decoder 只能用于合成演示。真实 CLI 故意不启用它，HTTP 成功不代表 envelope 已验证或链路健康。
- 应用运行时没有新的第三方依赖。未改动既有 Phase 00 数据库实现，无需重复创建数据库实验资源。

下一阶段建议：用户批准后执行 prompts/02-kafka-registry.md；真实 Kite envelope 的样本确认作为明确的集成验证事项保留，不自行扩展路由或进入 Phase 02。

## 2026-10-19 · Phase 02 Kafka 集群注册与连接管理

状态：用户明确批准继续后，已完成本阶段实现及 Mock/实验数据库验收。止于 Phase 02；真实 Kafka 跨版本与生产身份/SecretStore 验证仍有明确缺口，不宣称生产可用。

### 已交付
- `backend/internal/registry`：KafkaRegistry/KafkaAdminGateway/SecretStore/IdentityProvider 边界，内存 SecretStore，安全 DTO、注册/可选字段编辑/凭据 revision 替换/停用/软删除、分页筛选、异步探测任务及审计。actor 来自签名 HttpOnly Cookie，不信任自报用户或权限头；admin/operator/viewer 具有固定基础动作限制，数据始终按所有者 ID 隔离。
- 注册保存为 unverified；模拟成功为 mock_connected，绝不写入或修改实际 Kafka。API/日志无密码、用户名原文、PEM、JAAS 或 SecretRef。秘密省略保留、显式空串清除；已清除/服务重启后缺失的秘密阻止探测。SASL/PLAIN 风险由界面提示，推荐 SASL_SSL。
- 全局同时探测 2 个、每登记最少间隔 2 秒、每用户 10 秒最多 5 次、最多保留 256 个任务状态；探测 5 秒预算，最终查询/落库使用独立 2 秒预算。正在探测的登记拒绝并发修改，避免旧凭据结果覆盖新配置。失败不回显底层异常。
- `backend/internal/kafkaadmin`：franz-go 四协议、PLAIN/SCRAM256/SCRAM512、CA/ServerName/可选 mTLS；默认拒绝的 CIDR+端口策略（可额外收窄主机名）、校验所有 DNS 地址、固定解析 IP 拨号、逐 seed/advertised Broker 的 Metadata 探测、集群 ID 不一致保护、连接与 Broker 数量上限、临时客户端及时关闭。仅成功执行的 Metadata 标 observed，其他能力 not_probed，不读取精确发行版本。
- `migrations/002_kafka_registry.sql`：registration JSONB 权威快照，以及凭据引用/能力/探测状态事务投影；CAS、所有者和版本约束、注册变更与 audit_events 同事务。SQL 全部参数化；原始密码/用户名/PEM 不存库。`scripts/migrate.ps1` 依序运行 001、002，支持重复执行。
- `frontend`：Kafka 列表、筛选分页、新增/编辑弹窗、独立 SASL/TLS、只写秘密、模拟连接场景、当前会话切换、停用和确认软删除；sessionStorage 只存 active cluster ID。凭据在提交、失败、取消、卸载时清除。未探测能力显示“未探测”，非“不支持”。
- 默认 REGISTRY_MODE=disabled；显式 mock 只用内存。postgres-mock 提供专库登记持久化，必须有稳定的环境签名密钥；Cookie 未过期且密钥不变可恢复所有者，秘密仍内存并明确提示重启后重填。生产 LDAP/KMS 不伪造；HTTP 演示不能启用真实网络 Factory。
- 新增 `scripts/registry-smoke.ps1`、registry-backend/registry-smoke 任务及运行说明；更新路线图。未实现 Topic/Group/ACL 详情、MM2 发现或任何生产配置写入。

### 实际验证
| 环境/目录 | 命令与检查 | 结果 |
|---|---|---|
| backend | 初次 registry/service 测试 | 预期失败：CreateInput/NewService 等未实现；随后实现通过 |
| backend | `gofmt`、`go test ./... -count=1` | 全部包通过；本机未注入 PostgreSQL DSN 的集成项明确 SKIP，真实实验结果见下 |
| backend | `go vet ./...` | 退出码 0 |
| backend | `go build -o bin/lunaorbit.exe ./cmd/server`、`go build -o bin/kitecheck.exe ./cmd/kitecheck` | 退出码 0 |
| backend | `go test -cover ./internal/registry ./internal/kafkaadmin` | 首次集成统计分别 62.6% / 74.5%，未将覆盖率作为真实服务验证替代 |
| backend | kafkaadmin 本地 TCP Kafka wire fixture | ApiVersions/Metadata 成功；advertised 地址拒绝与实际连接失败、DNS 重绑定、不可信 TLS、SASL/TLS 工厂及错误分类通过 |
| backend | registry 行为及 HTTP 测试 | 凭据脱敏、跨所有者 404、viewer 禁写、CAS、秘密清除/旋转、十类 Mock 场景、超时最终状态、签名会话防篡改、分页/Origin、限流通过 |
| frontend | `pnpm test` | 4 个文件 18 项测试通过 |
| frontend | `pnpm typecheck`、`pnpm lint`、`pnpm build` | 全部通过；约 1.03 MB JS chunk 的既有打包提示保留 |
| frontend | `pnpm audit` | No known vulnerabilities found |
| 应用根 | `./scripts/registry-smoke.ps1` | 真实 HTTP 会话、登记、DTO、跨用户隔离、异步任务、429、停用和软删除全部通过 |
| 应用根 | `./scripts/registry-smoke.ps1 -BaseUrl http://127.0.0.1:5173` | 真实 Vite 代理下相同流程通过 |
| 应用根 | `./scripts/tasks.ps1 registry-smoke`、`./scripts/smoke.ps1` | 注册任务及既有 health/统一 404 通过 |
| 应用根 | GET /kafka/clusters（Vite） | 前端路由 HTML=200，标题正确；没有声称完成浏览器 E2E |
| 应用根 | PowerShell Parser、`git diff --check` | 全部脚本解析与差异检查通过 |
| 实验机 | Docker Compose 专用项目，001/002 连续执行两轮 | PostgreSQL healthy；版本 1、2 各一行；重复迁移成功 |
| 实验机 | 交叉编译 registry-integration.test，安全环境注入 DSN 后 `-test.run '^TestPostgresStoreIntegration$'` | PASS：CAS、所有者隔离、凭据引用/能力/Broker 读取、审计失败事务回滚、软删除及审计数量 |
| 实验机 | postgres-mock 应用 HTTP 生命周期测试 | PASS：签名会话与登记在进程重启后恢复，内存凭据失效正确标注，重填凭据及软删除成功 |

实验仅使用用户已批准的 SSH 主机上的新 Compose 项目 `lunaorbit_phase02_20261019`、数字回环端口与合成数据。密码和会话签名密钥随机生成于远端环境，没有打印或保存到仓库；完成后停止测试应用，删除专用容器/网络/数据卷、临时 .env、二进制和测试日志，输出 PHASE02_LAB_CLEANUP_PASS。保留公共 PostgreSQL 镜像缓存和不含凭据的配置；未操作 Kubernetes/minikube、Kite 或 Kafka 资源。

### 复核与修正
- 实际失败复现了 probe ctx 取消后读取 Store 导致任务误为 CONFLICT、登记残留 probing；独立持久化 context 覆盖最终 Get 和 Save，回归通过。
- 修正不适用协议的秘密先保存后丢失引用导致的内存容量耗尽：保存前拒绝，并跟踪本次新增 refs 确保失败/丢弃时清理。
- 修正开发数据库前缀过宽，改为精确允许 Phase00/02 专库；修正显式空密码无法清除的契约差异；缺失凭据明确阻止探测。
- 修正 PostgreSQL 模式重启后无法恢复登记所有者：使用配置的 HMAC 签名密钥；凭据仍明确为易失测试存储，不假称 KMS。
- Vite 代理 smoke 实测曾返回 403；增加精确的回环 DEV_BROWSER_ORIGIN 白名单，不放宽 Host 检查或启用通配 CORS，修复后代理 smoke 通过。
- 修正真实 Factory advertised 网络失败与 seed 错误混淆：advertised_unreachable 独立分类，TLS/认证/授权/API 错误仍保留专门类别；回归先失败后通过。
- 前端将 not_probed 显示为“不支持”的问题已修正；未知能力结果显示“待确认”。独立复核的上述重要问题已修复并由测试验证。
- 初始尝试安装最新版 franz-go/kadm 要求 Go1.26，随后移除不使用的 kadm，固定 Go1.25 兼容的 franz-go1.19.5/kmsg1.11.2 并 tidy；项目 go 版本仍1.25.0。Go 工具链下载缓存不属于仓库产物。

### 尚未验证 / 风险
- 未连接真实 Kafka 3.7.x/3.9.2；3.6/2.8 也未实测。跨版本差异用清晰标注的 Mock 场景覆盖，不能据此宣称真实兼容成功。
- 完整 SASL 服务端握手成功、可信 CA/mTLS 成功与实际 Broker 授权器集成仍待专用环境验证；当前有工厂配置、协议 Mock、错误分类及 TLS 拒绝测试。
- IdentityProvider 与生产 SecretStore 只有边界；HTTP 只提供受限的本机开发会话及内存秘密，不具备上线认证/加密密钥托管能力。签名密钥变化/Cookie 丢失或过期后，开发身份无法恢复；不可用该机制替代 LDAP。
- 未执行浏览器端 E2E；Vue 组件/Client 测试和真实 HTTP/Vite 代理 smoke 已执行。Windows CGO 未启用，-race 无法运行；不宣称 race 检测通过。
- Kafka 实际 Topic/ACL 功能及 MM2 关联分别留在后续阶段；本阶段仅提供连接 Metadata 能力，不根据填写版本启用功能。Kite 的真实 envelope 缺口继续保留。

下一阶段仅在用户批准后实施 Phase 03 MM2 自动发现。本阶段实现完成；生产接入前仍需补齐上列真实环境验证。

## Phase 03 — MM2 自动发现（2026-10-19）

### 完成范围和工程选择
- 按 `prompts/03-mm2-discovery.md` 实施。新增 discovery/resource、collector、parser、匹配/构图引擎、异步 Service、HTTP API、内存/PostgreSQL Store、`cmd/mm2discover` 与迁移003；方案见 PHASE03_PLAN.md。
- properties/YAML 受限解析和引用追踪；别名优先使用独立且同范围的实际 Cluster ID 证明，否则人工绑定或低置信地址候选。HTTP 不接收客户端伪造身份或审批人；真实证明提供者未接入。
- declared/observed/verified 分开，运行副本或配置存在不代表同步健康。部分失败保留旧 observed_at，更新 attempted_at 并标 stale。mars/charon 双源意图和 styx 实际目标保留；未知 RF 不猜测。
- 原始配置只在采集/解析边界短暂存在；快照只保存白名单字段、指纹和来源证据。Kite Decoder 显式可替换，默认不解析未经证实的真实 envelope；没有新增 Kite 路径或写操作。
- 修正复核发现：绑定的当前非 stale 工作负载校验与写入在 Store 中原子执行（PG FOR SHARE）；任务容量仅拒绝新增 owner/scope，已有条目可重跑；完整校验来源 scope，避免相同 ID 不同来源混淆。增加回归用例。

### 实际验证
| 位置 | 命令/检查 | 结果 |
| --- | --- | --- |
| backend | `go test ./... -count=1`、`go vet ./...` | PASS；默认数据库测试跳过，下面单独启用实际落库测试 |
| backend | `go test ./internal/discovery -count=1` | 新增容量/范围与原子绑定回归 PASS |
| backend | `go build ./cmd/server`、`go test -c ./internal/discovery`（Linux交叉编译） | PASS |
| frontend | `pnpm test`、`pnpm typecheck`、`pnpm lint`、`pnpm build` | 4文件18测试 PASS，其余通过；现有主包大于500kB构建提示仍存在 |
| 本地 HTTP | `scripts/discovery-smoke.ps1`，直接8080与Vite5173代理分别执行 | PASS：3链路、用户隔离、候选/人工确认、过期保留、未知RF、分页图证据与脱敏 |
| 本地 HTTP | `scripts/registry-smoke.ps1`、`scripts/smoke.ps1` | PASS：Phase02与健康检查回归 |
| 本地 CLI | `scripts/tasks.ps1 discovery-mock`；默认和stale-demo另有Go测试 | PASS：3条合成链路、无原始秘密 |
| 脚本 | 全部 PowerShell Parser、`git diff --check` | PASS |
| 授权实验机 | 隔离Compose项目，迁移001–003重复两轮，查询schema_migrations | PASS：版本1、2、3各一行 |
| 授权实验机 | Linux测试二进制 `-test.v -test.run '^TestPostgresDiscoveryStoreIntegration$'`，环境安全注入DSN | PASS：快照/审批持久化、所有者隔离、禁用登记/过期工作负载拒绝、时间倒退拒绝、审计回滚 |

实验仅创建本阶段专用 Compose 项目 `lunaorbit_phase03_20261019`，数据库数字回环端口；没有操作 minikube、生产 Kubernetes、Kafka 或真实 Kite。测试后删除专用容器/网络/数据卷与临时环境文件/二进制，输出 PHASE03_LAB_CLEANUP_PASS；保留不含凭据的配置与镜像缓存。未将密码、DSN或Key提交到仓库。

### 未验证项与阶段边界
- 真实 Kite envelope、分页和版本差异尚无脱敏授权样本；真实 Deployment/ConfigMap/Pod 的端到端发现尚未验收。合成响应显式标识，不冒充真实契约。
- 独立 Kafka Cluster ID 证据获取与真实 MM2/Kafka 环境映射尚未联调；算法以Mock证明覆盖，不将Mock metadata当实际身份。人工绑定也不证明业务链路健康。
- PostgreSQL Store 已实际验证；本阶段未新增 PostgreSQL 模式进程重启的整条发现 HTTP 生命周期测试，不能据Store测试声称已验证完整重启流程。Job 状态目前进程内，不提供跨进程任务恢复。
- Windows CGO缺失，未执行race检测；LDAP、生产SecretStore、X6页面及真实RF/延迟/业务健康查询均不在本阶段。认证沿用仅回环的开发会话。

Phase 03 完成并停止；下一阶段建议 Phase 04 拓扑 UI，等待用户批准。

## Phase 04 — 基础拓扑 UI（2026-10-19）

### 完成范围
- 严格按 prompts/04-topology-ui.md：新增 /topology Vue 路由、X6画布、图层/拖动/缩放、机房/环境过滤、来源证据及个人布局；未实施Phase05。
- 新增 frontend/src/api/topology.ts（完整分页/版本与边端点校验）、topology/model.ts（过滤、方向布局和状态）、TopologyCanvas.vue、Topology.vue及测试；新增 backend/internal/topology、004_topology_views.sql，并接入既有鉴权模块。App导航、Clusters精确登记跳转与脚本/文档同步更新。
- 筛选来自confirmed registration_id对应的当前用户登记，候选和未知保持未知，任一端点满足时保留完整方向。声明禁用、unknown、未验证、过期分别展示；Deployment就绪和ConfigMap存在均不等同于链路健康。
- GET/PUT /api/v1/topology/scopes/:scope/view 受签名开发会话/Origin与快照所有者范围保护。只接受布局字段、有限坐标与输入大小，revision CAS拒绝并发覆盖，PG事务同时写审计；不接受图关系字段。viewer可保存自己的私人视图，不因此获得资产写权限。
- 全部资源仍为合成；Kite只读契约未改，不访问生产资源或真实Kafka，不使用GitOps，不保存敏感凭据。

### 实际验证
| 位置 | 命令/检查 | 结果 |
| --- | --- | --- |
| backend | go test ./... -count=1；go vet ./...；go build ./... | PASS；默认显式数据库集成测试SKIP，真实落库见下行 |
| backend | topology Store/HTTP测试 | PASS：用户隔离、CAS、NaN/越界拒绝、未授权请求拒绝、布局保存不改变发现JSON；开发鉴权HTTP实际挂载复用原模块 |
| frontend | pnpm test | 7文件32项 PASS |
| frontend | pnpm typecheck；pnpm lint；pnpm build | PASS；X6懒加载单独块，已有主包与拓扑块仍有大于500kB提示，无构建错误 |
| frontend | pnpm test:e2e（本机实际Microsoft Edge无头，真实X6和本地后端） | 2项 PASS：9节点/6边、拖动、保存、刷新后的实际SVG坐标恢复、图层、过期证据、已确认机房环境过滤、精确登记跳转；无页面异常 |
| 本地 HTTP | discovery-smoke（8080与Vite5173）、registry-smoke、smoke | PASS；既有发现、集群登记和健康检查回归 |
| 脚本 | 全部PowerShell Parser，git diff --check | PASS；仓库当前仍未提交，git diff对未跟踪文件的覆盖有限，不将其当整仓审查 |
| 授权实验机 | 专用Compose，001–004各迁移重复两轮 | PASS；schema_migrations版本1–4各一行 |
| 授权实验机 | Linux交叉编译 topology-integration.test，显式DSN注入后 -test.v -test.run '^TestPostgresViewIntegration$' | PASS：布局落库/新Store读取、CAS、所有者隔离、更新、审计失败事务回滚 |

### 调试与复核
- 先写Store、页面/模型及重要回归的失败测试再实现；独立只读复核确认鉴权、Origin、CAS与发现图分离无绕过。
- 实际构建/浏览器复现X6引用tslib但未作为包运行依赖提供；显式固定tslib2.8.1后构建与页面加载通过。X6固定3.1.8，锁文件更新，不依赖隐式提升安装。
- 实际浏览器发现autoResize默认观察父容器，而带边框的图容器参与父高度计算导致无限增长；增加固定外层和独立图宿主后交互稳定。图层重绘的clear/add异步顺序导致旧节点残留，改为resetCells原子重建，E2E覆盖9→6→9节点/边切换。
- 复核发现禁用声明显示不清，补state/enabled与统一标签，失败回归转绿。补悬空/缺失方向边拒绝，避免残缺图冒充完整结果；补保存期间继续拖动仍保留dirty的回归。
- 默认排列按实际边方向分配源/目标列，避免以哈希节点顺序决定左右。E2E重载同时核对实际SVG transform，不仅比较服务端坐标JSON。
- 全阶段未提交/推送代码或启动生产部署。实验项目lunaorbit_phase04_20261019的专用容器/网络/数据卷、临时.env与测试二进制均已清理，输出PHASE04_LAB_CLEANUP_PASS；仅保留不含凭据的配置及镜像缓存。

### 边界与尚未验证
- 真实Kite响应封装、分页/版本差异、真实独立Cluster ID采集源仍待授权样本和专用环境；本阶段未扩大外部接口契约，拓扑不能宣称真实业务链路已验收。
- 当前Phase03 API只有Pod标签候选数量与关联程度，页面明确未核验归属，没有Pod名称/OwnerReference证据；不编造Pod详情。完整Pod归属需要后续受控补齐发现证据。
- E2E运行内存Mock模式。PG Store实际持久化已验证，未执行整个PG应用进程重启后的浏览器生命周期，不宣称该流程已实测。内存模式重启会丢失视图，PG模式必须保留稳定开发会话密钥。
- LDAP、生产SecretStore和完整资源RBAC仍属后续阶段；开发会话只适用于回环。Windows CGO缺失，未运行race；构建包体积提示保留。
- 未知/歧义端点不会按别名合并；同名moon-prod可暂时显示为多个独立待确认节点。Phase03人工绑定后才可按登记ID合并，避免把业务别名当物理身份。

本阶段完成并停止。下一阶段建议Phase05 Kafka只读详情，需用户批准后实施。

## Phase 05 — Kafka只读详情（2026-10-19）
- 完成Registry复用的Broker/Topics/Topic详情/Groups/offset-lag API、分页过滤、两槽有界等待/5秒超时/每登记用户限流，Mock显式场景与安全分类错误。Vue详情 /kafka/clusters/:id/details 已接通，RF由实际分区计算，缺失与未知offset/lag保持null。
- 新增kafkaadmin Reader与原生协议fixture，复用既有凭据、TLS/SASL和seed/advertised网络策略，不引入第二套认证；真实HTTP运行仍默认Mock，真实Reader仅在显式受控集成中调用。客户端按请求关闭。
- 实测：go test ./... -count=1、go vet ./...、server构建通过；前端全量37项测试、typecheck/lint通过；3项实际Edge E2E通过（详情RF/ISR、unknown lag、授权/API失败、既有拓扑回归）。完整构建在阶段总回归再次执行。
- 授权实验机新Compose项目lunaorbit_phase05_20261019启动apache/kafka:3.7.2与3.9.2，主机端口仅数字回环。仅在新容器创建lunaorbit-phase05-synthetic单分区RF1 Topic，写3条合成消息、设置专用group提交offset1；交叉编译测试TestDedicatedLoopbackKafkaLab在两个版本均PASS：Broker、Topic RF/ISR、消费组、committed1、log_end3、lag2。没有连接既有Kafka或Kubernetes。
- 真实版本测试只覆盖本轮PLAINTEXT单Broker KRaft；完整多Broker、SASL/mTLS及真实授权器矩阵仍待验证，不宣称生产连接就绪。Kafka2.8/3.6只有显式Mock能力降级，未进行真实版本集成。镜像首次下载慢导致SSH操作超时，复查确认两镜像完整拉取后才启动测试。
- 用户已授权连续开发后续阶段，故阶段内验收记录后推进06，不再等待用户人工验证。最终统一记录专用实验清理结果。

## Phase 06 — 身份 / RBAC / ACL只读申请（2026-10-19）
- 完成LDAPS IdentityProvider、会话Realm、组角色映射、资源所有者隔离及security-admin只读角色；显式IDENTITY_MODE=ldap/mock时旧开发会话入口关闭，不回退到原开发cookie。默认dev模式沿用既有本地演示，不假装LDAP已接入。
- LDAPS要求受控地址+CIDR/DNS全部校验、TLS CA/hostname验证、服务绑定秘密引用、过滤转义、稳定entryUUID身份、限速/容量/超时、HttpOnly随机会话与注销。依赖go-ldap/v3 3.4.10，Go1.25兼容。LDAP账号不能自动当Kafka Principal。
- ACL只读表、白名单预览/申请、服务端StaticApprovals精确登记ID授权的独立审批收件箱；自批拒绝、CAS+审计事务。批准仅approved_not_executed；没有Kafka ACL写入口。迁移005独立请求/审计表。
- 验证：全量Go test/vet与server build通过；前端阶段内46测试及typecheck/lint通过。实际Edge分别运行security.spec.ts与显式LUNAORBIT_IDENTITY_E2E=1 identity.spec.ts，均PASS；后者使用独立18080回环Mock身份服务器及浏览器API转发，覆盖登录、密码清除、注销后401、viewer跨用户404、禁止开发登录回退、viewer写403及无浏览器密码存储。
- LDAP成功/错误密码/组映射/filter转义/CA失败通过真正TLS LDAP BER协议fixture验证，不等于已联调企业目录；生产LDAP地址、CA、组及服务绑定账号尚未提供。Mock身份仅合成测试账号，未把真实密码落库或输出。
- 真实Kafka3.7.2与3.9.2未启用authorizer的负向ACL集成TestACLRealNoAuthorizerOptIn均PASS，明确AUTHORIZER_UNVERIFIED；没有将未启授权器当成功权限。首次测试名筛选未命中（no tests to run）不计通过，随后按准确名称实际执行通过。带authorizer/SASL Principal真实映射仍未验证。
- 授权实验PG专库迁移001–005重复成功；TestPostgresTransactionsOptIn实际PASS：申请落库、CAS和审计约束失败回滚。默认测试无env显示SKIP，实际测试环境安全注入DSN。
- Windows当前CGO未启用，race命令失败原因已确认，不声称race通过。ACL审批范围须服务端配置，默认空拒绝；已有owner隔离不是完整企业共享资产授权治理，不将开发身份Realm作为上线方案。

## Phase 07 — 受控MM2变更（2026-10-19）
- 完成固定合成目标目录、单Topic规则脱敏预览/申请、精确targetID授权的独立审批、revision CAS防重复模拟执行、完整ConfigMap AES-256-GCM密文快照及AAD绑定owner/request/目标/CM UID/Deployment UID/resourceVersion，备份审计落库后才允许更新。
- 新增changes模块、迁移006、Vue /changes，SourceAuthorizer依据当前用户发现快照的namespace/workload UID/configmap UID证据过滤目标。GET targets读取实时RV，执行前再次读UID/RV，冲突立即停止，不盲覆盖；重启核验generation/observedGeneration/ready与复制指标，缺失或失败均manual_review，不自动回退。
- Kite写transport只实现契约中的完整ConfigMap PUT与Deployment merge PATCH，Authorization原值、x-cluster-name、UID/RV、唯一UTC restart annotation；默认disabled，固定scope白名单，409/失败/timeout不重试，禁止redirect、原始对象脱敏、未验证envelope仍需显式Decoder。真实Adapter没有接入应用执行，ENABLE_KITE_WRITES/ENABLE_KAFKA_WRITES仍只接受false。
- 验证：changes/kite/config/server Go test/vet与构建通过；阶段前端55项全量测试及typecheck/lint通过。实际Edge在独立18080合成身份/变更服务器中走完：申请人先发现→预览/申请→不能自批/提前执行→另一明确授权管理员批准→申请人模拟执行metrics_missing→人工复核且复制健康未知，PASS；同一端点未接真实资源。
- 实验PG迁移006重复两轮，TestPostgresChangeTransactionsOptIn实际PASS，覆盖CAS、密文备份与审计事务回滚；加密错误key/AAD、变更Deployment UID无法解密等边界另有Go测试。
- 独立只读复核身份/Kite传输未发现当前路径权限绕过/敏感回显/契约不符。真实Kite响应样本、实际复制指标与完整企业审批治理仍待环境；没有把Mock成功算真实变更成功。PG应用崩溃恢复/真实回退未做端到端验收；executing残留必须人工复核，不自动重试写入。

## Phase 08 — 观测与业务血缘（2026-10-19）
- 完成MetricProvider、受限Prometheus/VM HTTP Adapter及显式Mock；查询模板固定，地址/CIDR/DNS/TLS、重定向、响应大小、并发和超时均受控。数值、单位、来源、采样时间和expires_at明确标记；无数据、NaN、负值、未来时间、过期和部分错误保留unknown，不能推断健康。
- 完成按当前用户发现快照过滤的指标、潜在下游影响和人工业务血缘API，迁移007持久化确认。BFS环路去重，只沿当前有效声明/观察关系计算候选，快照过期直接unknown。人工确认不改变discovery verified；独立审计和容量限制防止无界写入。
- 拓扑叠加影响与业务确认，保留拖动坐标；前端按expires_at每秒更新过期状态并清理计时器。缺省/无效时间沿用服务端标记，不自行编造有效期。真实指标服务未配置，仅HTTP协议fixture和Mock已验收。

## Phase 05–08 最终统一验收与交付（2026-10-19）
用户已授权连续开发及自行验证，完成后续四阶段，无需用户逐阶段人工验收；未访问生产Kubernetes/Kafka，未引入GitOps，未启用真实写操作。

| 验证 | 实际结果 |
| --- | --- |
| backend `go test ./... -count=1`、`go vet ./...`、server build | PASS；无外部env的显式集成测试SKIP，实际实验结果见下列记录 |
| frontend `pnpm test` | 16文件68项PASS |
| frontend typecheck / lint / build | PASS；主包与X6块仍有大于500kB提示 |
| 实际Microsoft Edge，`LUNAORBIT_IDENTITY_E2E=1 pnpm test:e2e` | 7项全部PASS：Kafka详情、身份隔离、ACL申请、双人变更、指标/影响/业务确认、两项X6拖动布局与筛选回归 |
| 专用Kafka 3.7.2 / 3.9.2 | 两版本真实Broker/Topic RF/ISR/Group/offset1-end3-lag2 PASS；无authorizer明确拒绝的负向ACL PASS |
| 实验PostgreSQL迁移001–007 | 重复执行PASS，版本1–7各唯一一行 |
| 实验PG access / changes / observability显式测试 | PASS：不可变身份、CAS、审计失败事务回滚、审批收件箱大量无关请求隔离、密文快照、确认与审计4096容量边界 |
| 实验PG完整应用生命周期 `python3 lab-lifecycle.py ./lunaorbit-server` | PASS：应用进程重启保留签名身份、登记、发现、布局、业务确认、ACL批准及加密模拟变更；两人审批后执行，再次重启结果保留；原发现JSON不变 |
| 本地smoke / registry-smoke / discovery-smoke | PASS（实际API8080及Vite5173） |
| PowerShell Parser / git diff --check | PASS；源码尚未跟踪，diff检查不代表完整仓库审查 |

独立复核后的修正均补回归：ACL Store CAS保护所有不可变身份字段；审批收件箱先过滤授权再分页，避免无关请求挤掉结果；ACL typed及wrapped错误按固定白名单映射403/501/503/504，未知错误脱敏；过期影响返回SNAPSHOT_STALE；PG确认与审计容量事务检查；指标有效期与浏览器计时失效同步。Overview入口和启动脚本同步到当前演示范围。

专用Kafka容器/网络/卷及集成二进制已删除；随后专用PG容器/网络/卷、临时.env、测试二进制、服务二进制与日志清理完成，输出PHASE05_08_LAB_CLEANUP_PASS。保留不含凭据的实验脚本、迁移和Docker镜像缓存，未改动原有服务或minikube。

仍未验证且不计通过：企业LDAP实际目录（仅真实TLS BER fixture通过）、生产SecretStore与完整共享资产授权治理、Kafka多Broker及完整SASL/mTLS/authorizer矩阵、Kafka2.8/3.6真实环境、Kite实际envelope与受控写/回退、真实Prometheus/MM2指标和业务同步健康。Windows缺CGO，race未运行成功。模拟变更不构成生产变更验收，executing残留需人工复核，不自动重试。前述阶段中“待下一阶段LDAP/持久化重启”的记录为当时状态，本节更新为已实现Provider及已实际PG生命周期验证；企业真实接入仍待配置。

实施完成后停止扩展范围；不自动提交、推送或部署生产。

最终重建后二次全量浏览器复测：首次重启独立18080测试服务时，合成用户JSON误用了ID字段而非约定user_id，服务按设计拒绝启动，导致2项连接失败（不计通过）。修正测试进程环境字段后重跑完整7项全部PASS；无产品代码修改。最终Go全量test/vet/server build与前端68项/typecheck/lint/build再次PASS。

## Minikube真实集成验证（2026-10-20）

用户明确授权在原Linux实验机启动minikube并安装Kite/Kafka/MM2验证。本轮新建lunaorbit-lab profile（Docker、K8s v1.35.1、2400MiB/2CPU）及lunaorbit-integration namespace；原minikube profile保持Stopped，既有mongo等容器未启动或修改。没有生产连接或平台写开关变更。

### 已实际通过

| 验证 | 结果 |
| --- | --- |
| 专用profile节点、CoreDNS、两Broker/Kite/MM2 | Ready/Running；最后部署的Pod重启计数为0，详见实验机状态核验 |
| Kafka3.7.2 source → MM2 3.9.2 → Kafka3.9.2 target | MM2_REPLICATION_PASS：源端写入本次唯一随机合成标记，目标source.lunaorbit-lab-events显式分区消费断言收到；独立随机Cluster ID后重新通过 |
| Kite v0.16.0真实七条GET | KITE_LAB_GETS_PASS；namespaces/_all、ConfigMaps/Deployments/Pods列表与单资源均200 |
| Kite认证与权限负向 | 匿名401、Bearer前缀401、reader访问admin403、跨kube-system Namespace403；Key原样Authorization，带x-cluster-name |
| 实际Go TestMinikubeKiteReadAndDiscovery | PASS：原HTTPClient、collector、discovery.Build读取真实资源，Deployment/ConfigMap UID/RV证据、source→target方向与图成立；observed不擅自升verified |
| 实际Go TestMinikubeKafkaNativeReads（专用MM2 Pod内） | PASS（0.14s）：真实franz-go seed/advertised网络策略、不同Cluster ID、两端Topic实际RF1/ISR1及Groups读取 |
| 本地 go test ./... -count=1 / go vet ./... / go build ./... | PASS；默认外部env未配置的新增测试明确SKIP，不代替上面的实际集成执行 |
| 两个Python脚本AST语法与git diff --check | PASS；前端无修改，本轮不重复以前端测试替代真实联调 |

### 实际调试与边界

1. 首次Broker128MiB堆被默认cleaner去重buffer耗尽，日志为SkimpyOffsetMap/Java heap space。实验fixture调整16MiB cleaner buffer及192MiB堆后通过，未以关闭压缩掩盖问题。
2. Kite受限ServiceAccount无法同步默认全局缓存，初次请求404由集群Client未就绪导致，不是猜错API路径。核对v0.16.0源码后使用DISABLE_CACHE=true，保持资源最小权限；重建Kite后重新创建随机Key及reader角色，七条GET通过。
3. Apache镜像默认Cluster ID相同，不适合作为两独立集群身份验证。两端改为不同随机ID，重新部署和复制；实际franz-go断言两个ID不同。
4. 重建期间MM2旧worker与Broker空数据重建产生竞态，内部offset Topic误为cleanup.policy=delete；按Connect要求将本实验目标端三个内部Topic显式初始化为compact，停止再启动MM2，重新验证通过。首次get-offsets找不到远程Topic和复制超时不算成功；最终唯一标记断言才计通过。
5. 宿主继承localdomain外部DNS不可达，Pod的ndots5搜索导致Go探测8秒超时和MM2连接异常。仅MM2 Pod切为集群DNS、内部搜索域和ndots1；未改宿主或旧集群DNS，Go原生探测随后0.14秒通过。
6. 首次复核脚本在prepare-mm2缩容期间提前执行，MM2 Pod不存在导致失败；待rollout后重新执行认证/七GET/负向检查通过。未把Running当复制健康证据。
7. 实验机系统日期与客户端2026-10-20有偏差，本轮未修改宿主时钟；实际延迟/过期指标不据此验收。

真实响应v0.16.0小样本：list为items+metadata，metadata有resourceVersion；单CM为metadata+data，Deployment/Pod为metadata+spec+status，Pod列表项可含metrics。原始合成响应仅留实验机私有目录，不复制到仓库。新增解码器仅为test私有，拒绝continue/未知结构，不默认启用生产Decoder，不推广为所有版本/分页的契约。

新增scripts/minikube-lab.py（专用资源、随机凭据/Cluster ID、内部Topic初始化、唯一消息断言）、scripts/kite-lab-check.py（源码核实后的实验身份配置及七GET/负向验证）、backend/internal/discovery/collector/minikube_integration_test.go、deploy/lab/MINIKUBE.md及计划文档。README/ROADMAP/Kite契约补充本次实测范围。

### 保留状态和未完成项

环境按本次用途保留运行，宿主仅127.0.0.1:28080端口转发到Kite；通过SSH隧道访问。私有目录/root/lunaorbit-minikube-20261020权限0700，随机凭据/Key文件0600，未回显或提交。两Broker和Kite存储均为临时实验数据，不承诺Deployment重建持久性。平台网页仍为原本显式Mock开发模式，本轮实际通过的是实验部署和Go适配器/采集器集成，不声称UI已经切换真实数据。真实Kite写与回退、企业LDAP、SASL/mTLS/authorizer矩阵、真实Prometheus/MM2延迟指标仍未验证。未提交/推送源码，未操作生产资源。

最终状态复核：四个实验Pod均1/1 Running、重启0；Kite /healthz HTTP200；Go真实Kite测试再次PASS（0.05s）；凭据文件0600/目录0700断言PASS。专用节点约1.791GiB/2.344GiB，宿主剩余6.6GB磁盘。实验环境保留，SSH控制连接关闭。

## 管理控制台改版与真实只读运行模式（2026-10-20）

按用户要求使用frontend-design和vue-best-practices重做管理平台布局。App只负责壳层/导航/数据源上下文：深蓝可收起侧栏、白色顶栏、面包屑、当前运行模式、统一工作区；手机宽度使用可关闭导航。全站采用统一字阶、间距、边框、按钮层级、筛选栏、内容面板、数据表格与分页。总览、集群、Kafka详情、拓扑、权限、变更六个视图全部调整；保留原有数据请求/CAS/审批和X6交互。查询来源/能力和演示设置折叠，不以大段开发说明占用主工作区。

总览读取实际登记total与当前用户发现快照，分页启用计数明确标注本页；API失败/无快照保持未知，不生成流量或健康百分比。/api/runtime提供可信能力与来源标记；真实模式隐藏Mock场景和不可用ACL/变更/指标模块；元数据缺失显示待确认。运行上下文由App提供，异步请求卸载取消，路由页面待上下文确定后加载。

新增LUNAORBIT_RUNTIME_MODE=readonly-lab / DISCOVERY_MODE=readonly-lab：显式Kite v0.16.0实验Decoder、真实Kafka Factory/Reader、server-owned native探测标记、精确主机/CIDR/端口策略。Kite响应拒绝未验证分页、缺失关键metadata/spec与非法对象；ConfigMap空data、Pod/Deployment未提供status按Kubernetes语义保留未知。默认Mock未变，所有真实写开关继续false。

实验机新增lunaorbit-console专用Deployment（API+nginx双容器），app不挂ServiceAccountToken，Kite Key通过Secret引用且不进镜像/浏览器。API只监听Pod回环，nginx同源代理API及localhost Kite代理，外部仍通过回环port-forward与SSH隧道。本地访问http://127.0.0.1:8088，服务实际运行在原授权Linux的独立minikube中。

真实E2E首轮发现原生探测虽然成功，Registry仍硬编码mock_connected。补回归后由服务端native模式记录connected，默认Mock保留原状态；实际两端Cluster ID不同、真实RF/ISR与发现图随后通过。独立复核另修复ACL关闭时仍请求收件箱、未知runtime被当Mock、Decoder接受缺spec投影等问题，均有失败→修复→通过记录。默认布局排除显式禁用的反向边参与列排序，仍展示所有节点/关系并保留个人坐标；未知循环不会被丢弃。

本地执行go test ./... -count=1、go vet ./...、go build ./...通过。前端19文件82测试、typecheck、lint、build通过；既有主包/X6块超过500kB警告仍保留。7项Mock实际Edge回归通过；2项真实实验Edge测试通过，覆盖原生探测、不同Cluster ID、实际RF1/ISR1、真实Kite发现/人工映射、拓扑不升级verified、禁用能力不提供动作，以及390px导航和文档无横向溢出。最终构建部署后再次复测与截图检查，精确结果在本节末尾补充。

新增AppIcon、api/runtime、useRuntime、App/runtime/视图回归测试、readonly-lab.spec.ts、scripts/deploy-readonly-lab.py；更新各业务页、全局样式、router原功能不变、后端配置/实验连接/Decoder及测试，.env.example/README/路线图/复现说明同步。代码未提交或推送。

仍有限制：实验身份沿用回环开发会话与所有者隔离，数据/秘密为内存状态，Pod重建会丢失本轮登记/布局；新浏览器会话不能看到其他所有者的测试登记。生产LDAP/SecretStore、ACL真写、MM2受控真写/回退、真实指标仍未启用。实验机时钟偏差延续，未将运行就绪等同同步健康或基于其验收真实延迟。本轮没有修改生产服务，没有引入GitOps。

补充最终验收：合并后9项实际Edge E2E全部通过（19.4s），82项前端测试通过；后续仅修正画布正反向关系文字重叠，新增同一Canvas回归断言先红后绿，typecheck/lint/build通过。权限页原测试绑定main标签，在改为单一主内容地标后更新选择器；临时Python SSH转发的Windows10053连接中止已定位，Linux Pod无重启且健康200，换用系统OpenSSH与15秒保活后全量重跑通过。

显示修正发布后完整9项Edge再次全部通过，桌面/390px截图已复核，正反向边标签错位、手机导航关闭后不遮挡、内容无文档级横向溢出。最终前端产物已部署，API+web两容器Ready，真实查询入口HTTP200且/api/runtime为readonly-lab/write_enabled=false；原minikube仍Stopped。仅保留实验服务和系统OpenSSH保活隧道；不继续扩展下一阶段。

### 2026-10-20 实验 Kafka 登记可见性修复

用户实际浏览器看不到此前自动化会话创建的登记，原因是开发身份按 Cookie 所有者隔离。新增显式只读实验配置 KAFKA_LAB_REGISTRATIONS：受原网络白名单约束的两个无凭据 PLAINTEXT 登记。管理员当前会话通过 POST /api/v1/lab/registrations 初始化自己的登记并启动真实原生探测；并发幂等，保留后续编辑、删除及自建登记，不共享其他所有者资源。普通 Mock 和非管理员会话不启用。页面先完成初始化及有界探测等待，再读取登记列表；不会伪造连接成功。

前端19文件84项测试、typecheck、lint、build通过；真实浏览器回归改为普通页面访问自动出现两套登记，避免由测试专门创建数据掩盖问题。最终部署与真实回归结果见后续补充。

最终验收：修复版已部署至专用实验 console，API/web 2/2 Ready。2项真实 Edge E2E 全部通过（7.8s）：两个独立新浏览器会话普通访问即显示源/目标登记；幂等初始化无重复，真实探测 connected 且 Cluster ID 各异，Topic RF/ISR 与发现流程通过，手机导航通过。后端全量 go test ./...、go vet ./... 与 Linux 构建通过。实验写入仍禁用；用户刷新 http://127.0.0.1:8088/kafka/clusters 即可加载。

### 2026-10-20 实验拓扑关联与管理员操作

用户明确要求补齐两套 Kafka 在 MM2 拓扑中的信息，并授权实验环境开放管理操作。原默认只读与默认禁写保持；新增显式 admin-lab 运行模式、独立 ENABLE_KITE_WRITES/ENABLE_KAFKA_WRITES 开关。实验部署仅给 Kite ServiceAccount 对 mm2-config、mm2 两个命名资源的 update/patch 权限，其他工作负载及生产资源不在可写范围。Kite API 使用已记录 PUT ConfigMap 与 PATCH Deployment 契约，保留 UID/resourceVersion 检查、加密备份、审计和结果状态。实验独立开关允许管理员批准自己的实验申请；默认审批约束不变。

拓扑先为当前所有者初始化登记；新会话无快照时自动发现。可信服务端实验别名配置只在精确 Namespace/Deployment 名称、实际观察证据、唯一同地址候选及原生连接身份均成立时建立映射；保留用户手工映射，未知/歧义不自动确认。Kafka 名称、地址、Cluster ID 进入图与证据面板，RF 和复制健康不推断。

Kafka ACL 通过原生 API 实际查询与执行，Broker 未启用 authorizer 时显示真实不支持，不用合成成功掩盖。真实指标和生产 LDAP 仍依赖实际环境配置，不混入 Mock。测试/部署最终结果将在完成后补充。

实际联调记录：新浏览器普通打开拓扑即自动发现并确认两端当前所有者登记，真实 Cluster ID 各异，图与证据面板包含名称/地址。通过页面预览、提交、管理员批准、实际 PUT ConfigMap + PATCH Deployment，随后用同一受控 API 恢复原 Topic 规则，资源 generation 实际递增；缺少持续复制指标时正确保留 manual_review。首轮 ACL 页面错误提示过于通用，发现 registryRequest 丢弃机器错误码，增加严格白名单保留 AUTHORIZER_UNVERIFIED 并以回归验证不传递上游正文或其他任意错误码。

补充复制验收首次因测试 CLI 使用 Broker 级 JVM 堆，在小内存容器内触发目标 Broker OOM；目标为临时容器存储，内置 Connect Topic 随重启重建后 cleanup.policy=delete，MM2 拒绝启动。限制短时 CLI 堆为64MiB，并用既有 prepare-mm2 流程恢复专用内部 Topic 的 compact 策略后，唯一合成消息复制再次通过（MM2_REPLICATION_PASS）。实验 Broker 仍为临时数据，不把它作为持久化验收；未变更生产资源。

独立只读代码复核未发现重要缺陷。Go 全量测试、vet、Linux构建通过；前端19文件90项测试、typecheck、lint、build通过（原有大包体积警告保留）。最终管理员版页面 ACL 预览/批准/执行负向流程与部署复测结果继续补充。

最终管理员版发布验收：API/web 2/2 Ready，/api/runtime=admin-lab、write_enabled=true。两项实际 Edge E2E 全部通过（5.7s），原只读部署专用两项按模式明确跳过；覆盖两个独立新所有者的自动登记/发现/映射、Kafka真实ID、MM2页面实际变更及恢复、ACL页面预览→批准→执行→真实AUTHORIZER_UNVERIFIED失败并保留审计记录、390px导航无横向溢出。最后ACL返回值增加下一动作权限标记并先红后绿验证，Go全量test/vet/构建通过。最终前端90项测试通过，typecheck/lint/build通过；仅最终data属性排序修正按lint/build复核。代码未提交推送。

最终发布后再次执行合成消息复制，MM2_REPLICATION_PASS；目标 Broker 保持 Running 且无新增重启，MM2 generation16/observed16/ready1。只保留实验服务与本地SSH访问隧道，完成本轮修复与实验管理能力开放，不扩展其他阶段。

### 2026-10-20 拓扑按启用方向展示

用户指出单向同步仍显示显式禁用的反向灰线。原因是 filterGraph 只做机房/环境与图层筛选，disabled 声明仍参与画布。调整为默认排除 state=disabled 的关系及所属边、孤立节点，双向均启用时保留两个方向；未知/过期关系继续保留其未知状态，不擅自当禁用。新增“显示禁用关系”入口供明确查看原始证据；不改 MM2 配置、后端快照与健康结论。

修改 frontend/src/topology/model.ts、views/Topology.vue，新增单向/双向/全部禁用/保留原快照回归断言以及实际 X6 浏览器断言。新测试先失败后通过。pnpm test：19文件91项通过；typecheck、lint、build通过，既有大包警告保留。补强双向四条边测试后相关8项与lint再次通过。实验部署API/web 2/2 Ready；实际Edge两项通过（4.4s）：默认仅source→target路径（经MM2共两段边），勾选后反向禁用证据才出现（四段边），取消后恢复；390px导航无横向溢出。未执行Kafka/MM2写操作，未连接生产。

### 2026-10-20 拓扑详情面向运维精简

根据用户反馈，右侧默认只保留源/目标集群名称、Topic配置匹配规则、MM2实例、就绪副本和最近发现时间。集群关联调整与UID/resourceVersion/指纹等诊断信息默认折叠；Kafka Cluster ID和登记ID不作为任何拓扑文本展示，不以ID作缺失名称回退。画布与关系列表统一使用当前所有者登记名称，未确认候选仍明确标注待确认。集群管理列表同步移除Cluster ID展示，后台真实身份判定与资源隔离不变。仍仅展示已取得的Topic配置规则，不声称完整实际同步Topic清单或健康。

修改Topology.vue、topology/model.ts、TopologyCanvas.vue、Clusters.vue及对应回归/E2E。新增名称优先、隐藏Kafka ID与默认折叠诊断断言，失败后修复通过。前端19文件92项测试通过；typecheck、lint、build通过，大包体积警告保留。实验发布与最终实际浏览器结果后续补充。本轮未改后端/Kite契约，未执行Kafka/MM2配置写入。

最终发布后实际Edge两项通过（4.3s）：使用集群名称显示源/目标、整页文本不含两端Kafka Cluster ID/登记ID、诊断默认折叠、单向隐藏禁用边与390px导航。截图已人工复核。另起本地隔离Mock补跑原拓扑两项E2E通过（9.2s），确认真实X6拖动、个人布局保存、过期证据、已确认机房环境筛选及按名称跳转到正确详情。最终lint通过，实验入口HTTP200。原只读E2E同步更新名称选择器，本轮未以只读部署运行它。

### 2026-10-20 Kafka 工作台、应用授权与资源范围 RBAC

用户批准本轮将集群管理和 ACL/RBAC 改为面向运维任务的工作台。实施计划见 docs/superpowers/plans/2026-10-20-kafka-workspace.md。本轮不新增路线图之外的自动化生产操作，不改 Kite 契约，不引入 GitOps。

可运行能力与改动：
- backend/internal/kafkaadmin/management.go 增加原生 Topic 配置读取、创建、扩分区、白名单 retention.ms 调整与全部字段精确匹配的 ACL 删除；复用原出站限制、探测与池边界，写请求不自动重试。
- backend/internal/registry/grants*、migrations/008_cluster_grants.sql：用户/可信 LDAP 组的集群角色与 Topic/消费组前缀范围，默认所有者隔离，显式共享；列表/详情/偏移量按范围过滤。重复有效授权原子冲突，撤销后立即失权，平台全局角色不越过资源授权。组来源只接受服务端身份提供者，不相信浏览器自报角色或组。
- backend/internal/workspace、migrations/009_workspace.sql：应用资料、生产/消费最小 ACL 模板、精确撤权、差异预览、快照指纹、CAS、独立审批、执行前及每条规则前重新授权、逐条结果与实际回读。Principal 创建后不可修改；应用资料不创建认证用户。保留外部规则，不把 ACL 查询摘要当完整有效权限判断。部分结果与未知状态如实保存，不自动回退。
- backend/cmd/server/workspace.go 按运行模式装配 Mock/原生 gateway 与内存/PG store，普通模式不启用真实写。admin-lab 的独立开关只允许服务端实验目录中的精确连接目标；实验自动批准记录真实操作者，不依赖所有者曾经打开页面的进程内种子记录。
- frontend/src/views/KafkaWorkspace.vue、components/workspace/*、api/workspace.ts：概览/Topics/消费组/应用与权限/同步链路/操作记录/团队授权，名称优先。授权和 Topic 变更先预览、明确确认、提交/执行并展示回读结果。保留 /read-only 历史查询入口。Mock 变更与只读列表通过 BindReader 共用状态，避免模拟创建后列表不更新。

实际验证：
1. backend：go test ./...、go vet ./...、Linux 静态 go build ./cmd/server 全通过。原生协议与业务测试覆盖错误、越权详情、范围过滤、执行中撤权、修订冲突、重复执行、部分执行及审批约束。
2. frontend：pnpm typecheck、pnpm lint、pnpm test、pnpm build 全通过，24 文件 110 项单测；既有大包体积警告保留。
3. 真实 Edge Mock E2E：LUNAORBIT_WORKSPACE_E2E=1 playwright test e2e/workspace.spec.ts，1/1通过（8.9s）。跨会话团队共享、独立审批、创建/扩分区/retention 回读、应用生产消费授权及四条规则精确撤销、团队收回后404。历史 kafka-details.spec.ts 改走 /read-only 后1/1通过。
4. 授权 Linux 专用 Minikube：新部署 API/web/PostgreSQL 3/3 Ready，REGISTRY_MODE=postgres-mock，专用回环数据库 lunaorbit_phase02_test，PVC1Gi，固定签名/加密密钥只保存在私有文件/Secret，应用无 ServiceAccount Token。001–009实际应用并重复执行通过。实验脚本仅允许 synthetic-integration 标签的专用 namespace/profile。
5. 真实 PG：scripts/verify-workspace-postgres-lab.py 实际运行 TestWorkspacePostgresPersistenceAndConcurrency、TestWorkspacePostgresApproveSingleConnection、TestPostgresGrantsIntegration，3/3通过；检查跨 store 持久性、并发执行一次、部分结果、单连接无审批死锁、跨实例授权/撤销与重复授权冲突。首次测试业务断言通过但清理因 defer db.Close 早于 t.Cleanup 失败，修正生命周期后重新运行全部通过，未把失败轮次记为成功。
6. scripts/verify-workspace-restart-lab.py：同一私有 Cookie 会话创建资料，实际重建 console Pod后 WORKSPACE_POD_RECREATION_PERSISTENCE_PASS，原身份、登记、应用内容与修订均保持。远程端口转发在旧 Pod 断开后重建。
7. 实验两套 Kafka3.7.2/3.9.2 启用 StandardAuthorizer，严格目标检查通过；scripts/kafka-acl-lab.py 实际验证 literal+prefixed 创建、查询、只删除 literal 保留 prefixed、最终清除合成规则，均通过。环境变更重建了临时 Broker，重新 prepare-mm2 后唯一合成消息 MM2_REPLICATION_PASS。
8. 最新实际部署 LUNAORBIT_LAB_E2E=1 playwright test e2e/workspace-lab.spec.ts：1/1通过（10.7s），页面实际创建 Topic、扩为2分区、retention=172800000、登记应用、生产消费授权、四条规则精确撤销并确认实际 ACL为0、操作记录及390px无横向溢出。首轮业务全部通过但移动端断言发生在页面仍加载时失败；加等待可见工作台后重跑通过。
9. 最新实验 admin-lab.spec.ts 3/3通过（6.5s）：单向拓扑与禁用关系、真实 MM2受控写和恢复、第二个移动端会话。旧无 authorizer 负向断言按实际能力分支保留，新环境正向写由 workspace-lab 覆盖。

验证边界：实验是 PLAINTEXT + ANONYMOUS superuser，以保证 MM2 既有连通，仅证明 ACL 原生管理与回读，不证明 SASL 业务身份的允许/拒绝执行矩阵。企业 LDAP 实际组、SASL/mTLS、多 Broker 故障、Kafka2.8/3.6真实兼容矩阵、生产 SecretStore、真实指标仍未联调。Topic 删除、Offset 重置、SCRAM 用户创建、任意高级 ACL 写与生产写均不在本轮。每集群最多128应用/128变更申请，审计容量达限拒绝新建并保留已受理执行，不静默删除历史。实验 Broker 数据仍为临时容器存储；此次 PVC持久化验收只针对平台 PostgreSQL。代码未提交推送，停止本轮，不自动继续下一阶段。

最终收尾补充：原 MM2 管理回归后追加复制测试时，目标 Broker 在384MiB容器限额内发生 OOMKilled（137），导致临时内部Topic丢失，复制检查失败；并发重试曾增加短时CLI竞争。因此没有以首轮复制通过掩盖最终环境故障。将受守卫的两套实验Broker限额调整为640MiB（Broker堆仍192MiB、CLI堆64MiB），重建并执行prepare-mm2后，最终唯一合成消息 MM2_REPLICATION_PASS。随后最新真实workbench全链路浏览器1/1再次通过（11.0s），两套新Broker均Running、0重启，MM2 Running，console3/3 Ready。此修复仅为专用实验资源配置，未接触生产。入口 http://127.0.0.1:8088/kafka/clusters 最终HTTP200，运行模式admin-lab/synthetic=false/write_enabled=true。所有临时测试服务器已停，只保留实验服务、数据库PVC及原本机访问隧道。

### 2026-10-21 Kafka完整管理v3第1批：SCRAM账号与原生ACL融合

用户批准书面设计并要求继续实施。本轮只交付第1批，设计见 docs/superpowers/specs/2026-10-21-kafka-management-v3-design.md，实施记录见 docs/superpowers/plans/2026-10-21-scram-access-v3.md；不自动开始第2批。

可运行能力与文件：
- backend/internal/kafkaadmin/scram.go：原生SCRAM身份查询、SHA256/8192迭代随机salt创建、受限短时业务身份认证验证，管理连接与业务listener明确分离。凭据仅内存注入短时SASL客户端，不写共享SecretReader，写入不盲重试。创建前检查同名，Kafka原生upsert无外部CAS的边界如实保留。
- backend/internal/workspace/accounts*.go、model/store/service：账号与应用多对多、既有Principal身份资料升级、来源及机制状态、已有账号密码验证录入、共享关联；密码AES-GCM加密并绑定集群/账号/用户名AAD，主密钥独立注入，普通JSON/审计没有密码。显式查看要求credentials.read、no-store与审计。新增账号的不可变权限意图随审批保存，执行SCRAM→认证→逐条ACL写/回读，部分失败明确保留状态，不盲目重新生成账号。
- 创建账号要求UUID request_id；应用创建可携带同一请求ID。匹配重试返回原记录，意图改变冲突，并发提交不重复创建。初次独立复核发现幂等缺口、双SCRAM机制重复/误标、删除应用遗留关联，均已修正并回归；删除资料只清理关联，不删除Kafka身份、凭据或外部ACL。仅SHA512身份如实显示并拒绝按SHA256录入。
- registry/config/server：account.manage与credentials.read独立授权，要求无Topic/Group前缀的集群管理员；默认禁写、默认独立审批，实验自动批准仍受admin-lab及精确目标守卫。PG没有主密钥时不自动生成不可恢复密钥；纯内存Mock可使用临时随机密钥。旧集群管理密码、LDAP密码、Kite Key不在业务凭据查看例外内。
- workspace/acl_custom.go与原生ACL验证：全部规则及能力矩阵、Topic/Group/Cluster/Transactional ID/Delegation Token、有效operation组合、ALLOW/DENY、Host、精确/前缀/通配符，预览新增/保留/删除，逐条执行前校验外部漂移，精确撤销全部字段匹配；资源范围过滤先于分页，非Topic/Group和通配符须明确无范围授权。
- frontend WorkspaceAccess/Accounts/ACLRules、api/accounts及PlatformAccess：应用入口新增“新增应用并开通账号”，填写新应用与所需Topic/Group，一并申请账号+ACL；支持全部账号与录入/关联、全部原生规则。密码显式查看30秒后清除，关闭、切集群、离开页面清除，不使用前端持久化缓存。团队授权移到平台访问控制，旧?tab=teams仅兼容。旧应用资料和审批流程继续兼容。
- 复用现有009 workspace JSONB，旧数据在受控读取/更新时补账号结构，不需要新增DDL或重建现有表；没有通过数据升级自动修改Kafka。

实际执行的验证：
1. Go：go test ./...、go vet ./...、Linux静态构建server与scramcheck均通过。新增协议fixture完成真实SCRAM proof/server-signature交换，正确/错误密码、业务无Metadata ACL仍能认证、独立listener端口策略、创建冲突和无重试验证通过。
2. 前端：pnpm test 28文件119项通过；pnpm typecheck、pnpm lint、pnpm build通过。最终模式切换清除用户名/密码修正后，Accounts相关测试及构建再次通过；原有大包体积警告保留。
3. 真实PG：verify-workspace-postgres-lab.py实际运行 TestAccountsPostgresIntegration、TestWorkspacePostgresPersistenceAndConcurrency、TestWorkspacePostgresApproveSingleConnection、TestPostgresGrantsIntegration，4/4通过。账号单连接审批/执行、密文落库不含明文、重建store解密、请求幂等、已有执行/授权并发回归均通过。复核曾发现事务回调内调用Registry SQL会死锁，已移到事务前授权并保留事务内CAS；实际MaxOpenConns=1回归通过。
4. 授权Linux专用Minikube两套Kafka3.7.2/3.9.2增加SASL_PLAINTEXT:9094业务listener，9092保留MM2与ANONYMOUS实验管理。初次脚本因镜像需要KAFKA_OPTS及CLI属性文件SCRAM键处理失败；修正启动选项、移除CLI凭据创建并采用平台原生API。脚本只有只读readiness重试，绝不重试凭据写。所有合成账号由平台生成，业务身份不在superusers内。
5. verify-scram-access-lab.py在两套Kafka均通过：正确/错误密码、允许Topic生产/精确offset读取、禁止Topic写入、Group允许/拒绝、真实ConsumerGroup消费与Offset提交，9个固定阶段标记严格验收。第二个平台所有者录入既有账号，错误密码被拒绝，正确密码可加密托管；随后再次运行权限矩阵，原身份及ACL保持不变。输出 SCRAM_ACCESS_LAB_ACCEPTANCE_PASS。首次Group拒绝检查遗漏了franz-go在FindCoordinator阶段直接返回顶层GroupAuthorizationFailed，修正为只接受精确授权错误或响应码30后重跑通过，未把其他错误当拒绝成功。
6. 最新部署实际Edge：LUNAORBIT_LAB_E2E=1 playwright test e2e/scram-accounts-lab.spec.ts e2e/platform-access.spec.ts e2e/workspace-lab.spec.ts e2e/admin-lab.spec.ts，6/6通过（22.4s）。覆盖主入口新应用+账号+四条ACL、真实开通、密码查看清除、不泄漏普通JSON、共享关联、DENY预览取消不改规则、团队范围与旧入口兼容、Topic管理、拓扑/MM2实际写及恢复、移动端。初轮SCRAM浏览器业务完成但Principal预览显示断言失败，补上后实际重跑全部通过。凭据测试关闭trace/screenshot/video并在finally清除密码面板。
7. 实际console Pod重建后 verify-workspace-restart-lab.py verify 输出 SCRAM_ENCRYPTED_CREDENTIAL_POD_RECREATION_PASS 和 WORKSPACE_POD_RECREATION_PERSISTENCE_PASS，同一签名Cookie、应用内容、账号修订与托管密码hash保持。固定加密主密钥留在Linux0600私有文件和Secret，未进入仓库或日志。
8. 最终MM2回归后再次运行唯一合成消息复制，MM2_REPLICATION_PASS；两套Broker均Running且0重启，console API/web/PostgreSQL 3/3 Ready，MM2 Running；本机8088入口HTTP200。原profile和生产未访问修改。

限制与下一批：当前账号管理主路径仅SCRAM-SHA-256，用户名采用常规ASCII名称白名单；不从Kafka还原原始密码。实际实验是PLAINTEXT管理listener + SCRAM业务listener，仅证明该明确模型的认证与业务授权；企业LDAP、生产SecretStore/KMS、SASL_SSL/mTLS、多Broker和2.8/3.6真实兼容尚待验证。密码轮换/删除、配额、完整Topic/Broker配置及后续破坏性操作仍按后续批次实施，不把本批称为全部Kafka管理完成。实验Broker仍是临时存储，平台PG持久化验收不等于Broker持久化。Go race测试因当前CGO禁用未执行。代码未提交推送，只保留实验服务和访问隧道。

### 2026-10-21 Kafka 完整管理 v3 第2批：配置与资源生命周期

按用户“先往下开发，权限认证暂缓考虑”的指示，本轮只推进资源管理，沿用既有授权和实验写开关，不扩展账号、LDAP 或 ACL 产品行为。

交付内容：
- 原生适配：resource_configs.go、lifecycle.go 支持 Topic/Broker 完整配置元数据、敏感值及 synonyms 清理、指定 Broker 路由、validate-only 后增量 SET/DELETE、Topic 删除及分区副本迁移/取消。恢复继承删除覆盖，不写猜测默认值；静态配置不自动重启。删除按实际 Topic UUID 提交，防止现代 Kafka 同名重建误删；旧协议无法提供同等原子身份保护。
- Kafka 的空 Broker 名称只返回显式集群覆盖；结合真实 Broker 提供配置目录，但不把节点的静态值或单节点覆盖当成全局生效值。无覆盖显示 NO_CLUSTER_DEFAULT_OVERRIDE，未确定的生效值保持未知。
- workspace/resources*.go 与 Native/Mock 装配：预览指纹、UUID 请求幂等、独立审批、执行前漂移检查、修订 CAS、持久化 submitting/observing 状态、显式回读核验。提交成功不直接显示操作完成；重启后可继续只读核验，不自动重放写操作。复用现有 009 JSONB，无新增 DDL。
- Vue 工作台：Topic“完整配置”“副本调整与删除”、Broker 与集群默认配置选择、配置来源/属性/文档/筛选、SET/恢复继承预览、完整名称确认删除、副本编辑、资源操作记录及实际状态刷新。配置、生命周期、记录分别由独立组件维护。
- 实验源集群增加第二个合成 Broker，保留 Kafka/MM2 Pod 运行。setup-resource-broker-lab.py 严格检查 profile、namespace、标签、镜像及碰撞目标；新增 DNS 修复与迁移取消/MM2 回归脚本均只面向该专用实验环境。

实际验证与修复：
1. Go 全量 go test ./...、go vet ./...、Linux server/test binary 构建通过。双 Broker 本地 socket 协议验证指定节点读/校验/写路由；延迟 1.2 秒的校验响应先复现固定 1 秒超时，再验证改用既定策略超时后通过，仍不自动重试写。
2. 前端 pnpm test：29 文件、125 项通过；pnpm typecheck、pnpm lint、pnpm build 通过。仍有既有 bundle 大小提示。
3. Linux 真实 PostgreSQL：资源操作记录跨 store 恢复、回读不重放执行、配置设置/删除、CAS/敏感值不落快照，以及既有工作台并发、单连接事务、账号持久化、登记授权共 5 项集成测试通过。
4. LUNAORBIT_LAB_E2E=1 pnpm exec playwright test e2e/resources-lab.spec.ts：真实 Edge 1 项跨两集群完整流程通过（14.9 秒）。Kafka 3.7.2 / 3.9.2 的 Topic 与 Broker 配置 SET/DELETE、来源回读、集群默认与指定 Broker、敏感脱敏/只读拒绝、浏览器预览提交执行回读、Topic 删除通过；双 Broker 实际 [1,2]→[2]→[1] 迁移及 ISR 核验通过。
5. verify-resource-cancellation-lab.py：合成 Topic 写入 64 条、约 8 MiB 数据，临时设置明确选择的节流后触发实际未完成迁移，取消并核验恢复原副本及 ISR，通过。最后恢复 Broker 节流覆盖并删除合成 Topic，通过。
6. 实测定位并修复 ListPartitionReassignments 的空 partitions 数组语义错误：空数组选择零分区，现按真实 Metadata 枚举，补充编码/解码回归测试，真实取消通过。
7. 实验 DNS：既有 Pod 的 ndots:5/localdomain 搜索组合向不可达宿主 DNS 转发，导致 Broker 互连和 MM2 偶发超时。fix-resource-dns-lab.py 备份 CoreDNS 并只对合成命名空间的无效搜索补全返回 NXDOMAIN，热加载、不重建 Kafka/MM2。修正后原生 Describe/validate-only 三轮真实诊断通过（0.19 秒）。语法依据 https://coredns.io/plugins/template/ 的 invalid search completions 示例。

边界：生产资源未访问；Kite 契约与 MM2 配置管理方案未修改。Kafka 2.8/3.6、生产多 Broker/SASL_SSL、企业 LDAP、KMS、故障与大规模性能矩阵未实测。复制节流配额的完整管理属于第3批；本轮不自动设置用户未选择的节流。资源状态通过显式刷新核验，尚未新增后台持续轮询 Worker。

8. 最终 MM2 回归：verify-resource-mm2-lab.py 在保持已有配置和进程运行的情况下，验证新的唯一合成消息从 source Kafka 3.7.2 经 MM2 到 target Kafka 3.9.2，通过。旧脚本从 Offset 0 读取会在段已过期时误判；新脚本查询 earliest/latest，只读取最多 100 条当前可读消息，明确检查本次 marker。验证脚本不修改 MM2 配置。

最终部署：实验控制台已更新，http://127.0.0.1:8088/healthz 实测 HTTP 200；工作台 http://127.0.0.1:8088/kafka/clusters。ROADMAP 与本批计划已更新；本轮在第2批停止，第3批消费组/消息/配额未开工。上述结果仅代表授权合成实验，不计为生产验收。

### 2026-10-21 Topic 独立详情与平台文案优化

用户要求点击每个 Topic 进入独立页面，集中查看分区与修改配置，并减少整个平台的冗余文本。实施依据 docs/superpowers/plans/2026-10-21-topic-detail-ui.md，应用 frontend-design、vue-best-practices，沿用现有后台布局与设计变量，不推进权限认证方案或第3批功能。

文件与行为：
- frontend/src/views/TopicDetails.vue 与 router.ts：新增 /kafka/clusters/:id/topics/:topicName 深链接，名称编码、所属集群面包屑、分区/副本/ISR 概要，分区、配置、副本调整、操作记录分栏；删除移到更多操作。刷新、路由切换和离开页面清理旧数据；延迟响应不污染新 Topic。扩分区后只刷新元数据，保留执行结果供用户查看。
- WorkspaceResources.vue：Topic 列表仅承担搜索/分页及导航，不再将详情、四项摘要、完整配置、迁移表单和操作记录全部叠在列表下方。
- ResourceConfigs.vue、config-labels.ts：默认常用配置，全部配置按需切换；保留完整查询能力及未知项原始名称。常用参数提供中文名称、准确的时间/容量单位和中文来源；默认值、类型、文档、同义来源收起。Element Plus 编辑抽屉承载当前值、输入、差异预览和确认；提交后进入该 Topic 操作记录，既有审批/执行/回读行为不变。内部 Topic 配置只读。
- TopicLifecycle.vue：详情页按入口展示副本编辑或删除确认，取消迁移继续可用；只读权限禁用输入。ResourceOperations.vue 按 Topic/Broker 查询记录；后端 resources_http.go 在授权过滤和分页前精确过滤资源，避免当前 Topic 混入其他资源记录。
- App、Overview、Clusters、Topology、Changes、Security、PlatformAccess、KafkaDetails、ObservabilityPanel、BrokerManagement：删除重复页面介绍、运行模式长段解释、登记 ID 提示、重复免责声明及脚注；必要环境/未知/错误/只读/风险信息保留，技术说明与证据按需展开。平台登录、账号及 ACL 的执行语义未修改。
- 截图复核后进一步缩减默认配置列表，并修复全局移动端表格 680px 最小宽度使“修改”位于屏幕外的问题；配置表格在手机上保留名称、当前值和操作，来源可从详情查看。

本轮自动化：Go 全量 go test ./...、go vet ./... 与 Linux server 构建通过；前端 31 文件 131 项测试、pnpm typecheck、pnpm lint、pnpm build 通过。新增路由/深链接/历史返回、跨 Topic 竞态、扩容结果保持、配置单位及常用/完整切换测试；资源范围过滤测试验证分页总数、默认 Broker 空名称与授权隔离。构建仍有既有大包提示。

最终实验验收：新 API/UI 已部署至授权 Minikube 控制台，健康接口 HTTP 200。真实 Edge 两项回归通过（17.5 秒）：Topic 深链接、刷新、返回列表、分区展示、常用/全部配置切换、编辑抽屉、预览/提交/执行/回读、当前 Topic 操作记录、两版本 Kafka 配置/删除与双 Broker 迁移；并验证 390px 页面无整体横向溢出、修改入口可见、移动抽屉交互。既有单向拓扑、禁用方向显式查看、集群名称与折叠诊断回归通过。桌面和移动截图已实际检查，测试仅使用合成 Topic 并清理。

已知边界：保留 Kafka 原始参数名和原始文档供高级排查，并非把所有参数翻译为业务表单；高级配置、风险确认与审批语义不省略。扩分区仍复用原审批记录模块，资源配置/副本记录按 Topic 隔离。本轮不新增消息、消费位点或配额功能，未访问生产环境。路线图及本轮设计记录已更新，本轮结束。

### 2026-10-21 Topic / Broker 配置说明仅中文

按用户“只需要中文说明”的要求，ResourceConfigs.vue 不再展示 Kafka 返回的 documentation，也不保留原始英文入口或渲染 HTML。config-labels.ts 新增简短中文说明目录，覆盖 Topic 配置与常见 Broker 参数，并处理 Broker 参数别名及部分监听器参数；未知参数明确显示“暂无中文说明。”，不猜测、不回退到英文。参数原名、当前值、默认值、来源及类型仍保留。

测试先复现旧界面回显英文与 HTML 的问题，再验证修复：前端 31 文件 134 项测试、typecheck/lint/build 通过；Go 全量 test/vet 通过。新增 Topic/Broker 两种资源的英文和 HTML 哨兵测试、未知参数回退测试。真实实验 Edge configuration-copy-lab.spec.ts 通过（3.8 秒），分别展开 Topic 与 Broker 的 min.insync.replicas，核对中文说明，并检查全部配置说明均为中文、没有原始英文段落或 HTML 标记；测试只读配置，没有修改 Kafka 参数。

新前端已部署至原实验控制台，页面截图已检查。本轮仅变更配置说明及测试，权限、API 契约和 Kafka 执行逻辑保持原样；后续参数或尚未收录的高级参数显示暂无中文说明，需逐项补充准确说明。本轮完成即停。

### 2026-10-21 账号密码入口与平台用词

用户要求三个账号应有明确的查看密码入口，去掉“托管”等实现术语。设计见 docs/superpowers/plans/2026-10-21-account-password-ux.md，沿用 frontend-design 与 vue-best-practices。本轮使用 verification-before-completion 验证实际结果。

排查：对用户当前集群配置做只读检查，确认关联实验目标 Kafka、Kafka 中有三个 SCRAM-SHA-256 账号，当前集群配置没有平台密码保存记录。旧界面隐藏没有保存密码的账号按钮，导致用户找不到入口。未读取其他用户保存的密码，也未重置这三个 Kafka 密码。Kafka SCRAM 描述接口不能返回可逆原密码。

变更：
- WorkspaceAccounts.vue、api/accounts.ts：独立密码列，始终显示查看密码按钮；没有密码或没有权限时禁用并注明原因。缺失密码提供“录入密码”，预填用户名，关联应用可选，沿用现有后端认证验证和加密保存，不修改 Kafka 密码或 ACL。已保存密码通过弹窗查看及手动复制，30 秒或关闭、离页、切换集群/应用、刷新、失去权限后清除；不写 localStorage。
- WorkspaceApplications.vue、WorkspaceAccess.vue：应用行增加查看账号，进入该应用账号列表，可返回全部账号。accounts_http.go 新增本平台账号列表可选 application_id 过滤，在授权之后、分页之前执行；未改 Kite 接口。
- 界面文案统一为“创建账号、保存密码、添加集群、权限规则、检查结果、数据状态”，移除“托管、回读核验”等说法；调整关联应用、账号来源和状态说明，保留必要 Kafka 参数及 ACL 类型。内部状态、审计字段与权限判定不变。手机账号表格改为分块展示，密码按钮无需横向滚动。

验证命令及结果：
1. backend 下 go test ./...、go vet ./...，以及 GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o ../bin/lunaorbit-linux ./cmd/server，全部通过。新增无应用录入、只验证不创建 Kafka 密码、应用账号过滤与分页总数、普通列表不返回秘密测试。测试 fixture 还包含一个旧应用账号，过滤后总数为2，分页返回1项。
2. pnpm --dir frontend test：32 文件、138 项通过；pnpm --dir frontend typecheck、lint、build 通过。新增缺失密码入口、无查看权限、显式查看/复制、30秒清除、延迟响应丢弃、切集群清除、应用跳转/取消过滤测试。保留既有构建大包提示。
3. LUNAORBIT_LAB_E2E=1 pnpm exec playwright test e2e/account-password-ux-lab.spec.ts e2e/admin-lab.spec.ts --grep 'discovered Kafka|single enabled'：真实 Edge 两项通过。验证三个已有账号、查看密码可见且禁用、录入预填、应用可选、未输入密码、不调用 reveal；检查桌面及390px手机按钮和页面无横向溢出，并回归单向拓扑及显式查看禁用方向。截图人工检查，不包含密码。
4. 调试中发现两处文案断言仍期待旧词，已同步更新；桌面缩至手机有布局过渡，浏览器断言改为等待布局稳定后检查，而非删除溢出检查。最终全量前端测试通过后构建。历史 SCRAM 浏览器脚本同步使用新的密码弹窗选择器；本轮未重复创建真实 Kafka 账号。

边界：现有三个账号需要录入已知原密码并验证后，才能在当前集群配置下查看；未声称可从 Kafka 还原 SCRAM 原密码。没有重做权限认证方案，没有新增消息/消费位点/配额，没有访问生产资源或修改 MM2 配置。本轮完成后停止。

最终部署复核：API 与前端已更新至专用实验控制台，Deployment 1/1 可用，原 localhost:8088 转发已恢复，/healthz HTTP 200。最终版本重新运行上述两个实验流程及 configuration-copy-lab.spec.ts，共3项通过；配置中文说明回归也通过。前端最终构建前再次全量138项测试及lint通过。路线图与计划同步完成，本轮结束。

### 2026-10-21 资源详情导航与四集群真实同步实验

按用户授权录入三个实验账号原密码，修复 Broker 配置未知，统一资源进入详情页，并使用两台指定实验主机验证分叉、汇聚、串联。实施计划见 docs/superpowers/plans/2026-10-21-resource-navigation-topology-lab.md。本轮沿用 frontend-design、vue-best-practices，使用 systematic-debugging 先定位根因，独立问题按 dispatching-parallel-agents 并行处理。没有推进消息/配额功能或重新设计权限方案。

文件与行为：
- ResourceConfigs.vue / config-labels.ts：空 Broker 名称查询的是集群动态统一覆盖，不是某台 Broker 的有效配置。NO_CLUSTER_DEFAULT_OVERRIDE+nil 改显示“未设置”，标题/列名明确“集群默认配置/集群覆盖值”；缺失默认值显示“Kafka 未返回”，真实空串显示“空值”。没有填造配置默认值或用一台 Broker 的值冒充全局值。
- BrokerManagement、BrokerDetails、ConsumerGroupDetails、AccessResourceDetails、SyncLinkDetails 与 router：列表→可直达的资源详情，面包屑、刷新、返回、分栏、授权/不存在提示及路由切换竞态处理。应用/账号逐页查找精确 ID，支持 external 用户名和录入后定位。消费组复用实际位点接口，未提供成员信息时明确说明；不添加 offset 写入。Topic 分区里的 Leader/副本/ISR 可点击 Broker，链路源/目标可点击集群，账号密码仍只在显式点击后读取。
- WorkspaceAccess / Applications / Accounts / Resources / Links、Topology：统一应用/账号返回位置和详情入口。拓扑按逻辑 Kafka 边计算层级、SCC 有界处理、共享 MM2 节点不误构业务环路；初次和换快照自动适应画布，拖动/叠加状态不重置视角。前后端实验集群目录及绑定支持有界2～16，保留网络、命名空间、实际观察、禁秘密及用户隔离约束。
- scripts/setup-topology-lab.py、deploy-readonly-lab.py：第二台独立 C/archive、D/upstream 两个真实 Kafka；原 A/source、B/target 与 MM2 保留，新增 mm2-mesh 三个连接器。第一台仅操作已标记的专用 namespace/profile，第二台只操作新建的指定标签容器和数据卷。实验地址从操作者参数和私有配置读取，精确端点加入实验网络白名单，不扩大为任意内网访问。
- cmd/lab-account-import 与 scripts/import-existing-lab-accounts.py：用户本轮明确授权的维护录入。仅指定回环实验数据库、精确实验 Kafka、同 bootstrap 的此前平台创建 lo_ 账号；恢复加密密码后真实 SCRAM-SHA-256 认证，再以目标登记 AAD 重新加密保存，并记录维护审计。没有伪造会话或重置密码。实际用户目标的三个账号导入3、缺失0、失败0；第二次执行已存在3、导入0；部署后只读数据库确认3个 managed/verified。
- cmd/lab-topology-check 与 scripts/verify-topology-lab.py：验证四个不同真实 Cluster ID，发两个唯一合成 marker，检查五条消息路径，读取窗口最多100条。输入和 Dialer 精确白名单、输出固定 PASS 标签，无消息正文或密码。

实际问题与修正：新增 MM2 时原 Minikube 2400MiB 内存接近上限，API 出现 TLS 握手超时；只将该实验 Docker 容器限额增至3GiB并约束新 JVM。根据 Kafka3.9.2源码关闭默认全局 heartbeat herder，取消按 target 共享 Connect group.id，保留各 source 默认隔离，原 MM2不动。最终原 Broker/Kite/MM2均保持0重启。浏览器整合测试发现前端初始化仍硬要求两个集群，已修成与后端一致的2～16并补边界测试。

已执行验证：Go全量 go test ./...、go vet ./... 和 Linux server/两维护工具构建通过；前端39文件175项测试、独立typecheck、lint、build通过。Python四个脚本 ast.parse 语法通过。五条真实消息复制 A→B、A→C、D→B、A→B→C、D→B→C 已完整通过两轮，四Cluster ID不重复。构建仍有既有大包提示；-race 未启用CGO，不计通过。

边界：原 A/B 仍是临时存储实验，不代表生产多Broker/SASL_SSL故障验收。成员查询、生产身份/KMS及持续复制健康监控尚未接入；一次marker验证不会把发现快照自动涂绿。Kite API契约、GitOps边界和生产禁写保持原样。最终浏览器与部署结果在下方补记。

截图复核收尾：未选择链路时隐藏空详情栏，画布占满宽度；选择后再展开详情。具体 Broker 的 log.retention.ms 未设置时，按实际返回的 minutes/hours 配置显示明确来源与时间单位，分钟优先；敏感、缺失或非法值不推算，集群默认覆盖不套用节点值。新增回归后最终39文件178项前端测试及lint/build通过，build含vue-tsc类型检查。

部署磁盘问题：实验主机根分区仅余39MiB，minikube重复导入已存在的PG镜像时返回GUEST_PROVISION_NOSPACE。先核对专用目录绝对路径、构建目录名称及当前镜像，仅清理30个过期 console-build 产物、60个本项目旧镜像缓存和60个未使用的本项目宿主机镜像，保留最新三版（包括当前运行和回滚）；释放后可用2.7GiB。没有清理其他项目、Kafka卷或PG数据。部署脚本增加精确版本镜像已在专用节点时直接复用，重试不再反复导出已存在的PG镜像。

首次会话边界：四集群配置下，服务端同时只允许两个探测；新用户直接进入拓扑时，第二批曾保留unverified。ensureSession现在等待本批真实探测完成，再通过幂等初始化派发剩余项，最多配置目录长度轮。仍running时停止追加，失败不重试，不制造成功状态。新增两批派发、持续阻塞、运行超时、失败不重试四项测试，最终39文件182项前端测试、独立typecheck、lint/build全部通过。

最终实际验收：
1. LUNAORBIT_LAB_E2E=1 pnpm exec playwright test e2e/resource-navigation-lab.spec.ts e2e/configuration-copy-lab.spec.ts e2e/account-password-ux-lab.spec.ts e2e/admin-lab.spec.ts --grep 'fresh session|resources open|configuration explanations|discovered Kafka|single enabled'：5/5通过（14.2秒）。新会话直接打开拓扑，四集群映射全部confirmed；四个Kafka、两个实际MM2节点、四条启用关系；全部资源详情点击/刷新/返回、Broker实际值与集群覆盖区分、保留时间小时来源、390px无横向溢出、拓扑自动适应画布、无空侧栏、链路详情返回、禁用反向显式查看、账号缺失密码入口以及Topic/Broker中文说明回归通过。
2. 最终版本再次运行 LUNAORBIT_LAB_E2E=1 pnpm exec playwright test e2e/resources-lab.spec.ts：1/1通过（20.4秒），两版本Kafka配置SET/DELETE、预览/执行/查询、Topic删除、双Broker迁移及独立Broker导航/刷新返回通过。合成资源清理，原MM2保持运行。截图已检查，未包含密码。
3. 指定当前用户集群配置的只读SQL在最终Pod更新后再次确认3个managed/verified账号；Kafka原密码与ACL没有改动。原Kafka三个Broker Pod、原MM2、Kite与新mm2-mesh均Ready且0重启，控制台三容器Ready；第二台两个带实验标签的Kafka持续运行。主机清理后最终磁盘余2.5GiB，不计为充裕容量。
4. 最终API/UI已更新，/healthz HTTP200，localhost:8088转发恢复。路线图、实验说明与实施计划同步完成，本轮停止，不自动进入第3批。


### 2026-10-22 同步拓扑可读性

用户批准方案后实施，范围为可视化与交互；实施计划：docs/superpowers/plans/2026-10-22-topology-readability.md。沿用 frontend-design、vue-best-practices，按独立模块并行处理 presentation 和 Canvas，集成后交叉审查。没有改变 Kite 契约、发现快照或 Kafka/MM2 运行配置。

实现：
- 新 presentation.ts 只按每个 link.id 精确配对 declared_input/output，默认 Kafka→Kafka 关系视图；共享 MM2 不会产生笛卡尔积假链路。同方向任务聚合，保留精确成员ID；启用/禁用分组独立，反向独立，未知/过期保守合并。
- 紧凑分层、连通分量处理环路、重心排序减少交叉。Canvas 按关系分配端口，双向及跨层走外侧通道，默认取消重复状态文字，仅保留聚合数量；悬停、选择时显示真实状态。
- 页面默认同步关系，运行部署可切换；聚合集合先列任务，用户选定后展示具体Topic规则和MM2；节点直接关联/全部上游/全部下游聚焦。筛选移除焦点自动清除，拖动重算路线。旧部署位置保留，新关系位置使用现有个人视图positions字典的relations:前缀，不增加API字段。

浏览器使用六Kafka、两共享MM2、九任务的明确合成fixture覆盖双向、分叉、汇聚、串联、并行与密集显示；另验证专用四真实Kafka实验页面。本轮不重复写入真实复制marker，链路未因此改为健康。

最终验证：
1. backend：go test ./...、go vet ./... 均通过。本轮后端未修改。
2. frontend：pnpm --dir frontend test 为40文件203项通过；typecheck通过；lint属性顺序的两条警告已修正，最终lint零错误零警告；build通过，仍有既有大chunk提示。
3. pnpm --dir frontend exec playwright test e2e/topology.spec.ts e2e/topology-readability.spec.ts：3/3通过。覆盖真实X6、拖动/保存/恢复、机房筛选、过期证据、模式切换、共享MM2、并行聚合精确选任务、节点聚焦、全节点画布边界及390px无横向溢出。合成数据只在浏览器API拦截中使用，不向实验Kafka创建额外资源。
4. LUNAORBIT_LAB_E2E=1 pnpm --dir frontend exec playwright test e2e/resource-navigation-lab.spec.ts e2e/admin-lab.spec.ts --grep 'fresh session|resources open|single enabled'：3/3通过，11.1秒。默认4Kafka/4边/0重复标签，部署模式6节点/8边，禁用反向显式展示；原有Broker/消费组/账号/应用/同步链路详情导航回归通过。四集群关系图、部署图及密集合成图截图已复核。
5. 独立交叉审查发现拖动坐标原地修改未重布线、筛掉聚焦节点后全图淡化、外侧竖线可能穿节点，均已修正并补测试后再跑最终回归。避障逐段检查扩边矩形，碰撞后有界压缩坐标A*，单边1200/绘图6000预算，超限回退X6 Manhattan。
6. 专用lunaorbit-lab/lunaorbit-integration控制台镜像已更新，Deployment 1/1，localhost:8088转发已恢复；保留原API二进制、数据库与实验Kafka/MM2运行配置，未访问生产资源。

边界：本轮仅改善呈现，不增加实时复制健康探测；超大或手工重叠的拓扑仍可能需要筛选、聚焦或重置排列。构建大包提示尚存，不计为性能验收。本轮完成并停止，未进入消息/消费位点/配额或权限重设计。


### 2026-10-22 同步拓扑工作区重构（第二轮）

用户指出共享MM2部署图仍难追踪方向，批准重新设计后实施。范围限定现有拓扑工作区，不推进其他Kafka管理阶段。

- Topology.vue：收敛为一条工具栏，增加集群搜索；更新时间合并显示，保存/重置/重载/禁用关系收进更多；画布角落提供缩放、适应与全屏。同步关系始终使用Kafka节点，节点名称放大、列间距收紧，窗口尺寸变化重新适应。机房/环境筛选保留关联外部端点并标注其他机房/环境，点击空白清除焦点。
- TopologyTasks.vue：运行部署不再画共享MM2路口，按namespace+workload_uid分组列实际任务，展示源、目标、Topic匹配规则、命名策略、同步状态及详情入口。实例就绪与同步健康分别展示；同名不同UID不合并，运行数据不一致或过期不冒充当前就绪。默认前缀/原名策略使用中文。
- 任务表与具体链路详情放到画布下方，不改变画布宽度。并行关系保留精确成员选择，集群关联编辑和诊断折叠保留，实际Topic列表未接入时只展示规则。部署改为清单后保留旧坐标数据但不再编辑，关系排列仍单独保存。

验证中发现并修正：未分组details自动生成“详细信息”标题；窗口变窄后节点被裁剪；旧脚本仍从顶栏寻找已经移入更多的入口。修正后重跑相关流程，非删除失败断言。

最终验证与部署：
1. pnpm --dir frontend test：41文件208项通过。最终改动后再次全量执行通过；typecheck、lint零错误零警告、build通过。构建仍有既有大chunk提示，未计为已解决。
2. backend目录go test ./...、go vet ./...通过，本轮后端无修改。
3. pnpm --dir frontend exec playwright test e2e/topology.spec.ts e2e/topology-readability.spec.ts：3/3通过，11.1秒。覆盖精确聚合任务选择、下面板、不缩窄画布、部署分组、搜索、空白清焦点、全屏、拖动保存/刷新恢复、过期证据、1150px节点边界及390px无页面横向溢出。
4. LUNAORBIT_LAB_E2E=1 pnpm --dir frontend exec playwright test e2e/resource-navigation-lab.spec.ts e2e/admin-lab.spec.ts --grep 'fresh session|resources open|single enabled'：最终3/3通过，10.8秒。实际四Kafka关系、两个MM2分组及四任务、禁用反向显式查看、其他机房两端点标识、资源详情刷新与返回。旧.link-list脚本选择器改成任务表[data-link-detail]后重跑通过，未删掉导航检查。
5. 复核四集群关系图、跨机房筛选、部署清单、六Kafka九任务密集Mock及窄窗口截图。真实实验未额外生产Kafka消息或修改MM2，应用导航回归仅创建/清理平台合成应用元数据。
6. 既有API二进制与新版UI部署到专用lunaorbit-lab/lunaorbit-integration，Deployment 1/1；恢复实验转发，http://127.0.0.1:8088/healthz为200。未访问生产集群、修改Kafka权限、重置密码或引入新Kite接口。

边界：Topic仍为配置匹配规则，未声称已经枚举实际匹配Topic；命名策略缺失按实际返回显示未知。实时复制健康、独立部署资源详情页及生产环境验收不属于本轮，当前部署按已发现字段展示就绪概要并从任务进入详情。超密图仍可通过搜索、聚焦、全屏和缩放查看；手机表格在自身容器滚动。路线图已更新，本轮结束，不自动开始下一阶段。


### 2026-10-22 恢复 MM2 默认拓扑

用户明确指出MM2拓扑不应被任务表替代，批准实例分组、任务路径独立连接的方案后实施。本轮沿用frontend-design、vue-best-practices，以失败测试先验证精确任务关联，未新增Kite API或修改发现快照。

- 新增MM2Topology.vue：每个namespace+workload_uid为一个真实实例分组；每行由源Kafka引用、输入箭头、MM2同步任务、输出箭头、目标Kafka引用组成。相同Kafka可跨任务重复引用，选择端点按该link.id精确映射回原始节点，不新增资产或伪造实例。多任务预览前三条，支持展开/收起及实例折叠；选中隐藏任务自动展开，选中路径突出、其他路径淡化。同步状态使用真实声明/验证字段，与实例就绪分开；过期状态保守显示。
- Topology.vue：默认MM2拓扑，保留同步关系与同步任务两个独立入口；MM2下的任务表默认折叠，具体链路详情继续在下方。搜索、直接关联/上游/下游聚焦、跨机房/环境标识沿用同一真实Kafka身份，支持?view=relations或?view=tasks直达。
- deployments.ts：统一实例精确分组与运行概要，任务表复用，避免同名不同UID合并或共享实例输入/输出串线。原Kafka全局图仍用X6展示唯一节点；MM2任务视图采用固定方向的可滚动路径，避免自由布局回折。

验证与部署：
1. frontend最终pnpm --dir frontend test：42文件213项通过；lint零错误零警告；独立typecheck及build内vue-tsc通过，build通过。构建大chunk提示仍存在。
2. backend go test ./...、go vet ./...通过，本轮后端未改。
3. pnpm --dir frontend exec playwright test e2e/topology.spec.ts e2e/topology-readability.spec.ts：3/3通过，13.4秒。MM2默认入口、六Kafka九任务、共享实例精确B→C路径、两条箭头、实例/任务折叠、隐藏任务展开、1150px截图和390px无页面横向溢出，以及原Kafka X6拖动保存/恢复与筛选流程通过。
4. LUNAORBIT_LAB_E2E=1 pnpm --dir frontend exec playwright test e2e/resource-navigation-lab.spec.ts e2e/admin-lab.spec.ts --grep 'fresh session|resources open|single enabled'：3/3通过，12.4秒。实际MM2两个分组、四启用任务、八箭头，点击B→C与详情端点一致；Kafka全局图、跨机房标识、独立任务表和资源详情导航回归通过。真实MM2默认页及Mock密集/窄窗口截图已复核。
5. 新增回归发现选中已折叠实例中前三条任务时应自动展开实例，已先观察测试失败、修正后全量重跑。选中尾部任务自动展开预览后仍可主动收起。
6. UI已更新专用lunaorbit-lab/lunaorbit-integration控制台，原API二进制复用；Deployment1/1、三容器Ready，转发恢复，http://127.0.0.1:8088/healthz返回200。没有生产访问、Kafka/ACL/MM2配置变更、新消息写入或新Kite契约。

本轮完成并停止。MM2任务路径是同一真实集群的可重复端点引用，不是额外集群；全局Kafka关系图仍每集群一个节点。Topic仍显示配置匹配规则，未新增实际Topic枚举或持续复制健康验证；手机路径在组内横向滚动，保持名称可读。


### 2026-10-22 连续 MM2 全链路与配置路径追踪

用户指出按实例逐行展示看不到A→B→C，批准将默认MM2图改为连续拓扑：真实Kafka节点全图唯一，每个link.id派生一个MM2同步任务节点，实例名作为任务所属标识；原逐行视图保留“按实例查看”。纯前端派生，不改变API快照、真实实例或Kafka资产。

实现：presentation.ts新增chain模式，按精确输入/输出配对构造Kafka→任务→Kafka；共享实例各任务隔离，避免笛卡尔积假链路。tracePaths仅遍历启用的配置任务，列出有界简单路径，排除环路重复，最多12条/8段/4096搜索状态并明确截断。默认图可直接衔接A→B与B→C；起点/终点选择可区分A→C直达和A→B→C串联。

Canvas复用X6，任务节点点击选择精确link，Kafka点击聚焦上下游；chain与relations坐标独立保存。选定路径进入临时聚焦子图，以五节点四箭头展示两段串联，“返回全图”恢复原图；不把临时路径排列写入个人布局。路径任务列表可逐段打开Topic规则与实例详情。图表示配置关系，不保证同一业务Topic在多段上连续匹配，更不代表健康。

验证中：先补失败测试覆盖唯一B、完整两段、共享实例隔离、禁用/环路/路径上限及任务点击。密集Mock截图显示仅淡化路径仍太小，改为路径聚焦子图并补测试，保证字号与顺序可读。

最终验证与部署：
1. frontend最终pnpm --dir frontend test：42文件218项通过；独立typecheck、lint零错误零警告、build通过，既有大chunk提示仍在。路径聚焦补测后全量重跑，不将此前217项当最终计数。
2. backend go test ./...、go vet ./...通过，后端无修改。
3. pnpm --dir frontend exec playwright test e2e/topology-readability.spec.ts e2e/topology.spec.ts：3/3通过，11.8秒。密集六Kafka九任务中选择A→B→C、返回全图、精确第二段Topic规则、实例视图、全局关系图与任务表、保存恢复和筛选通过。配置路径聚焦验证仅五节点四边，临时坐标不保存。
4. LUNAORBIT_LAB_E2E=1 pnpm --dir frontend exec playwright test e2e/resource-navigation-lab.spec.ts e2e/admin-lab.spec.ts --grep 'fresh session|resources open|single enabled'：3/3通过，13.7秒。真实默认全图8节点/8边，B仅一个节点；读取真实snapshot精确选择A→B与B→C的link ID序列，聚焦后5节点/4边，1150px窗口每个节点宽度大于110px且严格从左到右排列。第二段详情源为实验目标集群，跨机房、禁用关系及资源导航回归通过。
5. continuous-mm2-overview.png与continuous-mm2-real.png实际截图已复核，确认全局连续链路和聚焦两段。MM2声明路径并不自动证明同一Topic全程复制或同步健康，本轮未重复写入marker。
6. 复用原API二进制，将新版UI部署至专用lunaorbit-lab/lunaorbit-integration；Deployment1/1，实验转发恢复，http://127.0.0.1:8088/healthz返回200。无生产访问、Kafka/ACL/MM2配置变更或新Kite接口。

路线图已更新，本轮完成并停止。复杂图可使用配置路径聚焦、搜索、全屏与按实例查看；路径搜索有界，截断会明确提示，未声称列出任意规模图的所有路径。



## 2026-10-22 — X6稳定拖动与ELK布局

本轮针对轻微移动导致大范围绕线的问题，保留X6画布，移除依据全图中间节点强制绕外围及自动翻转端口的规则。整体排列由ELK layered/orthogonal完成，固定右输出左输入端口，初始及显式“整理布局”才计算全图。具体链路MM2任务节点与唯一Kafka节点继续支持A→B→C，保留配置路径、按实例查看、详情和状态证据。

实现：
- 新增routing-types、elk-layout、layout Worker/client和RoutingSession。ELK算法在原生Worker内的独立引擎Worker运行，计算期间不阻塞UI；8秒超时、取消、销毁及过期结果保护。Worker URL经过Vite生产打包，不依赖外部CDN。
- 每条路线缓存完整端口路径。拖动帧只预览关联边；落点事务性计算关联边和真正受新节点遮挡的边。使用旧通道、局部候选与有界正交搜索，弯折/距离/惯性评分；每轴最多64坐标、搜索最多12000状态。1024关联边和重复反向移动有性能回归。
- 拖动不resetCells、不自动fit、不重排无关节点。固定端口不随X坐标反转。节点重叠只调整当前节点到附近空位，路线不可行则恢复已提交位置并显示操作提示，不能静默穿节点。
- 同一结构健康刷新和API返回顺序变化只更新名称/健康/标识，不启动ELK。位置回传增量应用；Worker失败清空旧图，提示重试并通过父页面保存新排列。
- 个人View API契约不变，保存坐标并隔离chain/relations。恢复时按确定性基础布局与保存坐标重新避障；局部通道历史不持久化，不保证重新加载后每个折点完全相同。

验证中发现并修复：
1. 默认ELK fake-worker构造在原生Worker中失效，改为显式引擎Worker；Vite必须识别new Worker(new URL(...))才能正确发出生产资产。真实浏览器及部署构建均验证。
2. 分数端口的浮点尾差导致正交路径被视为斜线，固定端口偏移取整数；加入六Kafka九任务、环路和并行任务真实ELK回归。
3. 节点避让回到原位时也必须恢复实际X6节点；布局失败后不能留下可交互的旧结构。补齐失败清图、恢复、快速模式切换、健康刷新顺序变化及父页面保存整理结果测试。
4. X6异步绘制后才能采样SVG实际路线。浏览器断言等待路线渲染收敛，保留严格穿节点检查，不把鼠标释放当作SVG已更新。

最终验证：
- frontend pnpm test：45文件240项通过；lint零警告、typecheck与生产build通过。既有主包大chunk提示仍存在；新增布局算法只在后台Worker加载。
- backend目录执行go test ./...、go vet ./...通过，后端未修改。
- Mock真实浏览器4项：密集六Kafka九任务、双向/并行/串联、路径详情、实例视图、窄屏、10/30/100px及往返移动、无关路径一致、节点DOM保持、视口保持、坐标保存恢复、既有发现/筛选流程。
- 专用实验浏览器4项：实际四Kafka/两MM2的唯一节点、精确A→B→C两段、跨机房与禁用关系、资源详情导航，以及10/30/100/280px跨层/返回拖动。采样渲染SVG每3单位的点，落点后没有进入节点内部；实际拖动不保存实验用户布局，刷新恢复。
- 代码审查无重要阻断发现；按建议加强多节点交互移动后恢复坐标、固定端口及避障测试。
- 复用实验现有API与数据库，部署前端到指定lunaorbit-lab/lunaorbit-integration，恢复本地隧道，Deployment1/1、healthz 200。仅专用实验UI变更，没有访问生产或变更Kafka/MM2配置、ACL及Kite契约。

边界：任意手动位置不能保证零交叉；本轮保证固定方向、稳定局部更新、节点避障和失败回退。极端布线搜索有界，建议显式整理或路径聚焦。拓扑仍按发现证据显示未验证状态，不把配置路径声明为Topic全程健康。本轮完成后停止。

交付复核：最终部署后4项实验浏览器回归全部通过（16.5秒）；healthz、布局Worker与ELK引擎资产均HTTP 200，Deployment就绪1。最终UI归档SHA256：3b0b6fbdc6e9b7f28dc4bd4ee4251c877fdce61929faef3dd42a1177d304a669。前端与后台布局资产同一构建部署，实验隧道保留用于访问。


## 2026-10-23 — 默认布局汇入箭头排序

用户截图指出下方mm2进入实验目标集群上方入口，跨过上方mm2-mesh连线。根因：先按edge ID分配FIXED_POS端口，阻止ELK优化端口顺序。

采用ELK官方Port Constraints契约（https://eclipse.dev/elk/reference/options/org-eclipse-elk-portConstraints.html）：初始/显式整理时FIXED_SIDE，保留右输出左输入，同时允许算法优化节点及端口顺序；结果端口从实际路由端点回读并冻结，拖动继续使用RoutingSession，不动态跳入口。结果采用1/1024二进制坐标网格消除浮点尾差，保持正交及端点精确一致。未改后端契约、Kafka/MM2配置、权限或生产资源。

回归先红后绿：elk-layout新增四Kafka四任务图，对B/C按来源高度排序后断言目标端口同序；旧版失败51<25，新版通过。实际浏览器新增default fan-in测试，旧部署失败63<37，新部署通过，覆盖两个真实汇入节点。截图optimized-fanin-real.png已复核，上方入上口，下方入下口，红框交叉消除。

验证：45文件241项前端测试通过；lint零错误零警告，build含vue-tsc通过，既有大chunk提示保留。密集Mock浏览器2/2通过（12.9秒），含拖动、保存恢复、双向与串联。专用实验浏览器5/5通过（20.0秒），含四集群/两MM2、A→B→C、禁用方向、资源详情、跨层拖动和默认端口排序。复用原API及数据库，仅部署UI至lunaorbit-lab/lunaorbit-integration，恢复转发；healthz HTTP200。

本轮仅优化默认布局，已有手动坐标继续保留；用户可通过更多→整理布局重新优化并保存。未承诺任意图或手动排列零交叉。本轮完成停止。


## 2026-10-23 — 无登录拓扑与信息平台第一批

用户明确取消平台登录、权限管理，仅保留拓扑及信息记录，随后批准直接开发。本批完成已确定的产品收敛，不推进Topic全链路第二批。

实现与边界：
- PLATFORM_MODE=information + INFORMATION_WORKSPACE_ID 显式单一空间；服务端构建scoped information上下文，忽略Cookie/Bearer/身份Header，不伪造admin。历史grant不能扩展到其他owner资源。
- 独立路由白名单：共享集群登记/修改/删除、连接探测、SASL/TLS采集连接参数、Kafka资源查询、MM2发现/映射及信息记录。账号/SCRAM/密码交付、ACL/团队/角色、会话/登录、变更请求、资源配置修改和基础设施写操作均不注册。config同时拒绝information模式开启写开关和changes。
- 集群连接秘密与业务账号管理分开。允许注册时输入连接参数，允许PUT connection-credentials更新连接参数，没有GET秘密及旧账号密码接口。普通DTO不回传秘密。既有MemorySecrets限制保留：连接秘密不是持久化Provider，重启后可能需重新录入；专用PLAINTEXT实验目录不受影响，未宣称生产秘密持久化已完成。
- 新增internal/information及migration010独立记录表，无principal/ACL/账号字段。应用信息包含负责人、环境、说明、人工生产/消费的cluster+Topic+可选消费组；允许已登记但尚未观测的Topic，标记人工登记。限制请求字段、长度/关系数量/分页，校验集群归属，revision原子CAS，409不覆盖。
- 审查发现更新时间缺失，补齐服务端updated_at；内存时序和PG返回权威数据库时间，旧payload也由数据库列补齐，客户端不能填写时间；并发失败不改变时间。
- 前端无登录请求、无治理导航，旧深层账号/权限/变更页面跳到应用信息，资源配置只能查询。新应用列表/表单支持增改删、人工关系与时间展示。runtime失败时不挂载页面，防止误走旧会话逻辑。
- Topology不访问服务器view接口；布局/筛选按workspace/scope/mode留在当前浏览器，只保存偏好白名单，不存任何凭据。仍可查看真实MM2与A→B→C。

集成中发现并修复：
1. backend runtime.mode=information与前端旧枚举不一致，导致运行信息校验失败；前后端与Mock统一实际mode，加入校验。
2. 连接参数必须保留，不能把集群连接秘密误当成已移除的业务账号密码；新增只写连接配置入口并验证不回显。
3. 实际表单浏览器测试的select标签包含option文本，使用精确label匹配时测试等待超时；按可访问标签非精确匹配定位后实际表单通过，产品逻辑未绕过。

实验数据与部署：
- 仅专用lunaorbit-lab/lunaorbit-integration。预览选取一个现有owner目录，恰有4个有效登记、4个不同原生cluster ID和readonly-lab快照。显式采用该namespace为共享workspace，无owner重写、跨owner合并或删除；历史账号/授权数据保留并不可从信息模式查询。
- 新scripts/deploy-information-lab.py校验专用namespace标签、固定已有目标、明确namespace ID；私有目录保存pg_dump与旧Deployment，预先执行幂等migration010，替换console API/UI镜像，保留PVC。LUNAORBIT_RUNTIME_MODE切到readonly-lab，两写开关false、changes disabled，不再给新Pod注入账号主密钥和写Key。
- 镜像构建标识information-1be608c37fc2。API归档SHA256 acb8bcb018083f72952645d7e2ab493eea7cc3d1f642de056c4d2a1f3965a7c8；UI归档SHA256 c9584ca25eb30dc197d685ff45f761be33b512fb818045ce120f94706a3269ba。

真实验证：
1. backend go test ./...、go vet ./...通过；Linux API构建通过。
2. frontend pnpm test：47文件245项通过；pnpm typecheck/lint/build通过，lint零警告。既有大chunk构建提示保留。
3. 独立Mock浏览器2/2（3.5秒）：应用表单增改删、人工关系、无会话调用、旧深层链接阻断、只读资源。
4. 实际实验浏览器最终7/7（11.4秒）：无Cookie的两个浏览器同一四集群和八节点拓扑；共享记录/CAS冲突/字段白名单；旧认证/密码/ACL/资源写接口不可用；跨站写拒绝；个人布局本地保存且另一浏览器不受影响；真实应用表单生产消费关系保存；旧权限页面跳转、真实Topic资源查询；原有A→B→C精确路径、实例查看、跨机房/筛选保留。
5. Linux编译TestPostgresPersistence在实际实验PG执行通过，覆盖新Store恢复、revision冲突、更新时间和旧JSON无timestamp兼容，合成测试namespace自动清理。
6. 建立一条合成应用记录，重启专用console Deployment（不重启Kafka/MM2），恢复转发后记录ID/revision/updated_at完全相同，再删除该记录；重启持久化PASS。实际浏览器创建的合成记录也已清理，最终应用记录数量0。
7. 部署后/healthz HTTP200，/api/runtime mode=information、information_mode=true、write_enabled/acl_reads/changes全部false；localhost8088隧道保持用于访问。
8. 新information-backend本地任务实际启动，runtime信息模式和匿名应用列表HTTP200；PowerShell脚本语法与Python部署脚本编译检查通过。

文档：AGENTS、PRD、ARCHITECTURE、SECURITY标记新范围优先，README改为现行产品说明，原README存档。路线图与计划已更新。本批完成；Topic跨集群同步+消费组全链路图仍属于下一批，生产者自动观测尚未实现，当前人工登记不冒充真实流量。


## 2026-10-23 Topic 全链路第二批（完成）

范围：用户明确要求Topic全链路图；沿用无登录信息平台，不恢复权限/业务账号密码/ACL或基础设施写功能。实现计划见 docs/superpowers/plans/2026-10-23-topic-lineage.md，使用说明见 docs/TOPIC_LINEAGE.md。本批没有新增数据库迁移。

变更：
- backend/internal/lineage新增独立有界聚合、HTTP适配、Kafka成功结果60秒缓存/并发合并、应用信息即时读取；information服务器只读路由挂载GET /api/v1/topic-lineage。
- discovery parser/model/engine补齐MM2安全语义白名单及传播；旧快照SemanticsKnown=false，不猜测。Default/Identity普通Topic与Default逐跳命名/反向匹配；自定义策略/过滤类、无法安全解释的Java正则、Identity心跳明确未知。独立内部Topic排除、heartbeat例外与别名循环检查有回归测试。
- kafkaops/kafkaadmin通过已有groups资源的指定group详情DescribeGroups读取成员分配/订阅，和已提交位点分离；成员/字节/Topic/分区数均有上限。消费组历史证据不作为当前消费；人工组与实际组不混合身份。
- frontend新增TopicLineage页面、只读X6/ELK画布、API校验、Topic详情入口、导航；支持上下游1～4段、根节点、来源/时间/局部问题、详情、文字列表、缩放/平移/全屏。无节点拖动和基础设施编辑。

实现与审查发现：
1. 初次真实API测试在旧部署得到404（预期失败）；新API部署后相同测试通过。
2. 审查指出仅include/exclude不足：MM2内部Topic有独立命名策略排除。补齐独立判断及separator/internal开关，避免误画不存在的内部同步。
3. 过期/partial消费详情不允许标实际观测，添加守卫及测试；成员没有可解释证据则报告未知。
4. 画布尺寸观察曾引起ResizeObserver反馈；禁用X6 autoResize，观察固定高度容器并手动resize，Mock与实际截图复核无反馈增长。
5. 聚合补齐4096配置链路、16384链路访问预算及循环内部context取消；5000链路与分支预算测试保留根节点并明确截断，不让大图无限遍历。

实际执行：
- backend：go test ./...、go vet ./...均通过；GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build ./cmd/server通过。
- frontend：pnpm lint、pnpm test（50文件256项）、pnpm typecheck、pnpm build通过；现有大chunk提示保留，不影响构建。
- pnpm exec playwright test --config playwright.information.config.ts：最终稳定版本3/3通过（7.9秒），覆盖旧信息平台和新链路页、真实ELK worker、证据与只读交互。
- LUNAORBIT_TOPIC_LINEAGE_LAB_E2E=1 pnpm exec playwright test e2e/topic-lineage-lab.spec.ts：最终3/3通过（4.2秒）。实际A源lunaorbit-lab-events→B source.lunaorbit-lab-events→C target.source.lunaorbit-lab-events存在；A直达C source.lunaorbit-lab-events分支存在，未混入D的mesh Topic。验证depth1与depth2、反向C回A、Topic详情入口、人工生产/消费组应用关系、删除后即时消失、390px无页面横溢和无JS错误。
- LUNAORBIT_INFORMATION_LAB_E2E=1及LUNAORBIT_LAB_E2E=1运行information-platform-lab.spec.ts与resource-navigation-lab.spec.ts指定7项：7/7通过（10.7秒），覆盖匿名共享目录、应用/CAS、已移除接口不可达、浏览器本地布局、实际表单、旧权限页面跳转和四集群MM2首次加载。没有运行资源写入测试。
- 复核桌面默认图、C向上游完整五节点图、应用详情及手机截图。长图以根Topic为中心保持可读比例，支持平移和适应画布；人工应用同时生产消费形成的真实环会保留，不拆假节点。

实验部署与结果：
- 仅已授权专用lunaorbit-lab/lunaorbit-integration控制台；scripts/deploy-information-lab.py保留私有数据库及旧Deployment备份，使用原单一共享空间。镜像information-22b922efddbc。没有修改Kafka/MM2配置、重启Kafka/MM2或访问生产资源。
- Linux API SHA256 c50f2db665efe47d3fba6d7835e0b6776a880bc3176caa932a188ae45f27fef8；UI归档SHA256 d5467978ab6cb1cde64f2dd1e6bbd92a0551d6e8fb35c5374f8c73b753a7849f。
- 部署后通过已有发现接口刷新readonly-lab，job完成，然后进行上述真实测试；未编造Kite接口。localhost8088转发已恢复，/healthz 200、runtime mode=information、information_mode=true、write_enabled=false。合成应用清理后记录数0。
- 实验部分消费组当前成员分配/订阅不可解释，接口与页面报告GROUP_EVIDENCE_UNKNOWN（4项），没有冒充已观测消费；原生协议fixture覆盖当前分配/订阅、不同历史Topic、未知与大小限制。尚未完成生产业务消费者、企业SDK、自定义策略和全版本矩阵验证。

README、路线图、实施计划与使用说明已更新。本批完成并停止；生产者自动观测、逐条消息追踪、持续复制健康不在本批交付范围。


## 2026-10-23 应用、Kafka账号与Topic绑定（完成）

用户明确选择通过应用、账号与Topic绑定记录来源并要求继续开发；实现为信息登记，不恢复登录、角色、业务账号密码交付或Kafka ACL写管理。设计及计划见application-topic-identity文档。

变更：
- information Application新增可选location（512字节）；Relation新增可选account/client_id（各256字节），严格UTF-8/空字节/长度校验和HTTP字段白名单。password等未知敏感字段仍拒绝。旧记录无字段保持兼容；原JSONB持久化，无新迁移。
- Vue应用表单、编辑、列表支持部署位置、Kafka账号名称/Principal、Client ID；账号填写在具体集群/Topic/方向关系内，消费组仍为消费关系字段。表单复制草稿，不修改原记录，切为生产不传旧消费组。
- lineage应用节点详情展示负责人/部署位置，关系详情展示该条绑定账号/Client ID；关系ID加入账号/Client ID，修复同应用同Topic的两个账号关系折叠。账号为属性，不添加一层冗余节点；manual证据不变，不按名称升级为observed，不沿MM2跨集群继承账号。

测试实际执行：
1. 新测试先失败：信息JSON往返丢失位置、同Topic两个账号关系被合并、Vue编辑无新字段；实现后均通过。
2. backend go test ./...及go vet ./...通过，Linux API构建通过。新增HTTP字段往返/嵌套密码拒绝、旧记录兼容、长度与空字节校验、不同账号关系不合并且保持manual证据。
3. frontend pnpm lint（零警告）、pnpm test（52文件258项）、pnpm build（含vue-tsc类型检查）通过。新增表单编辑字段保留/不修改props、API账号/Client ID序列化及生产关系移除旧消费组测试。既有大chunk构建提示保留。
4. 独立Mock浏览器playwright.information.config.ts 3/3通过（8.1秒）。
5. 实际信息平台6项+Topic链路4项共10/10通过（12.3秒）：原有A→B→C和方向/名称、下游人工消费组账号绑定、应用表单创建/刷新/编辑后账号及Client ID保留、部署位置更新、链路API及节点/关系详情显示、只读治理入口阻断。后补截图等待真正X6布局完成，单项重跑1/1通过（3秒），截图复核通过。
6. Linux编译information测试二进制，在专用实验PG执行TestPostgresPersistence通过（0.05秒），覆盖新Store恢复后的location/account/client_id、revision/更新时间及旧payload兼容；使用私有进程环境解析现有实验连接秘密，不回显、不落入代码/日志。临时测试空间自动清理。

部署：
- 仅专用lunaorbit-lab/lunaorbit-integration的console，复用deploy-information-lab.py保留私有备份，镜像information-7e19d39a44d9。无Kafka/MM2配置或账号/ACL写入，无新Kite接口，无生产访问。
- API SHA256 ad3e514ddcc98e97c39a2a481f8ac9548f7b7fb41158dd0e9d53b4d1348f26f6；UI归档SHA256 f9d0feab6113c048f13f5db29cb2e65b427e936d2cee145beecbc5f08ab3ffcb。
- 控制台滚动后旧port-forward断开，校验专用进程后重建转发；localhost8088恢复，healthz HTTP200。实际浏览器合成应用全部删除，最终应用记录数0，不伪造业务数据。

边界：账号名和Client ID由人工提供，不验证Kafka认证、实际连接或ACL，当前生产应用仍需登记；原生消费证据未知时继续如实提示。未来可以补接真实客户端观测，本轮不自动推进。README、AGENTS、链路使用说明、路线图与计划均已更新，本批完成。


## 2026-10-23 同名Topic复制实验与黄色提示修正（完成）

用户截图的黄色区域是消费证据采集提示，不是复制失败告警；四条重复提示主要来自MM2的Connect工作协调组。用户明确要求测试场景仅使用同名Topic复制，授权专用实验变更。

根因与代码：
- lineage消费者补充把ProtocolType=connect的MM2工作组按普通业务消费组解析，触发unsupported_group_protocol。现在只在详情新鲜完整、协议connect且无错误时跳过业务消费解析，不掩盖失败/过期/不支持的真正业务组；后者提示附集群及消费组名称。新增先失败后通过的回归测试，Connect组不再读取业务历史offset。
- 同名Identity实验还发现visited节点重复被误判有向环：both反向回访与A→B→C/A→C汇入都会误报。改为已展开Identity Topic有向边的反向可达检测；仍使用节点/边和遍历预算，真正A→B→A保留告警。新增正向/反向/both无环汇入测试及真实环保留测试。
- lab-topology-check增加显式same_topic输入，默认历史检查保持兼容；测试同名模式和既有严格端点/输入守卫。除A、D唯一标记，额外在B写唯一标记验证独立B→C，避免仅看到A→C直达就当作多跳证明。

实验操作：
- scripts/configure-same-topic-lab.py显式--apply-lab、专用namespace标签检查、私有ConfigMap/Deployment备份；使用已验证的Kite登录及资源GET/PUT ConfigMap、PATCH Deployment契约，携带resourceVersion，不发明接口。沿用实验管理员cookie，无凭据输出。平台读Key/写开关保持不变。
- 为mesh两个明确资源名临时扩展已有实验Role，完成后finally恢复原rules，无namespace范围写权限。只变更mm2-config/mm2-mesh-config与对应两个MM2的重启注解；没有生产访问、Kafka Topic删除或凭据修改。
- 两MM2全局IdentityReplicationPolicy，mesh B→C include改为(lunaorbit-lab-events|lunaorbit-mesh-events)。A→B、A→C、D→B、B→C保持无环。原带前缀Topic保留为历史数据；未被当前同步规则选中。
- 两MM2 rollout成功，刷新readonly-lab发现后平台读取到Identity策略。当前三个集群中lunaorbit-lab-events均存在且分别成节点，另一路D→B→C的lunaorbit-mesh-events同名。

实际验证：
1. backend go test ./...、go vet ./...及Linux API/实验检查器构建通过；最后新增真实未知组提示包含名称测试，scoped lineage测试通过。前端无产品源码变更，pnpm lint通过；浏览器实验断言改为当前同名场景，历史Default单元fixture保留。
2. 脚本语法、MM2 properties转换（旧命名策略替换、B→C同名规则）及运行时镜像名称校验的存在/不存在/错误tag三类检查通过。
3. 同名消息检查：4个不同原生cluster ID，3条独立随机合成标记；A→B、A→C、D→B、C中的A标记、D→B→C和B→C独立标记共6项通过。A在C的标记可能来自直达或经B，故独立B标记才证明B→C；不声称汇入去重或exactly-once。
4. 信息平台6项+Topic链路4项实际浏览器10/10通过（12.2秒）。覆盖A/B/C同名不同节点、depth1/2任务数、反向查询、多源无关Topic不混入、人工下游账号/应用、编辑/清理、旧治理入口及共享目录。截图复核C上游同时显示A→B→C及A→C，无黄色误报。
5. 最终API A根depth2 both：nodes=6、edges=6、issues=[]、truncated=false；/healthz HTTP200。合成应用自动清理。

部署与恢复：
- console最终镜像information-e47c831d6dee；API SHA256 751470935060daf51146f41d48273770860d0bedcb701f354b484befd42baf7f，复用本轮前已验证UI归档，无UI产品代码改动。
- 滚动中实验机磁盘被构建/镜像缓存耗尽，新web镜像未完整进入运行时，ErrImageNeverPull导致页面暂时不可访问。仅删除本会话三个已知构建目录的api/frontend载荷及本会话已用上传二进制/归档；先解析绝对路径检查均在专用root下，保留pg_dump、previous-deployment、所有Kafka/MM2数据和当前构建。没有docker通用prune或删除用户目录。
- 释放空间后只重载缺失的最终UI镜像，console恢复3/3 Running；重建已验证的console端口转发，实际浏览器全部通过。scripts/deploy-information-lab.py已增加镜像加载后的CRI tag存在校验，防止minikube部分载入却返回成功时继续替换可用控制台；脚本更新到实验机。
- 实验机磁盘仍紧张，需要后续单独规划容量；不擅自删除旧实验/回滚镜像。本轮服务已恢复且实际验证通过。

README、Topic链路说明、计划和路线图已更新。本批完成并停止。Kafka官方IdentityReplicationPolicy说明已核对，同名策略需保持拓扑无环；不改为按名称合并所有集群Topic，也不将配置等同于实际消息健康。


## 2026-10-23 开发交接与Git交付

用户要求换电脑继续开发前总结并将全部代码提交到指定GitHub仓库。新增根README、AGENTS、DEVELOPMENT_HANDOFF.md，保留现有lunaorbit-codex-starter应用结构；交接文档覆盖当前信息平台范围、代码模块、同名实验、工具版本、Mock/PG启动、SSH转发恢复、真实验证及剩余限制。明确凭据/数据库/镜像不随Git交付，实验磁盘和连接Secret Provider为已知风险。

远端预检为空仓库，本地此前无提交；建立codex/handoff-2026-10-23分支，未覆盖或强推其他历史。新增根gitignore与统一文本换行策略，实际源文件、锁文件、迁移、设计和测试均纳入；本地env、私钥、依赖、产物和测试截图排除。提交前扫描460个原候选文件，用户实验密码、常见GitHub/Kite/AWS Key与私钥模式无命中；最终暂存内容再次复核。

交接提交前重新执行：scripts/tasks.ps1 test（Go全量test/vet、前端52文件258项）通过；pnpm lint及build（含vue-tsc类型检查）通过。既有大chunk提示保留。Git文本换行/EOF检查和最终暂存敏感模式扫描执行后再提交；不修改应用功能或运行环境。


## 2026-10-26 LUNA-60 阶段 0：文档与条款
- 定位调整为自动发现优先；根与项目 AGENTS 加入认证 B 例外条款、只读白名单与开关 KAFKA_MM2_CREDENTIALS；PRD/ACCEPTANCE/ROADMAP/HANDOFF/README/ARCHITECTURE/SECURITY/DATA_MODEL 按修订清单更新；KAFKA_CLUSTER_MANAGEMENT 标为历史方案；历史文档只追加说明。
- 只改文档，未改代码；未运行测试（无代码变更）。

## 2026-10-26 LUNA-60 阶段 1：自动发现与只读白名单（后端）
- MM2 凭据解析（security.protocol / sasl.mechanism / sasl.jaas.config，全局与别名前缀）只在 KAFKA_MM2_CREDENTIALS=true 时进行，凭据仅存内存，类型对 fmt/slog/json 一律脱敏；hostAliases 解析主机名。
- 只读白名单客户端：连接层按 Kafka 帧检查 API key，非白名单请求不写出（kfake 验证 Produce/CreateTopics/AlterConfigs/OffsetCommit/DeleteTopics 未到达 broker）。
- 端点目录：Cluster ID > 地址交集合并，单一登记附加业务字段，多登记标歧义，未覆盖登记为手工补充，自动不可读时回退登记读取。
- 出站：自动端点只可连接 MM2 配置里的地址及在这些端口上通告的 broker，解析结果必须在 KAFKA_LAB_ALLOWED_CIDRS 内。
- Topic 链路：不可读端点继续沿 MM2 配置展开（存在性无法确认），仅地址无法解析时中断并说明。
- 测试：go test ./... 471 通过 / 20 跳过；canary 假密码在快照、存储、日志、接口响应中命中 0。真实连接待线上 luna-mm2-lab 验证。

## 2026-10-26 LUNA-60 M2/M3：前端自动发现与去掉手工入口（路线图阶段 2；阶段 3 文档归位待线上验证后）
- 阶段 2：拓扑节点按端点目录合并（多条 MM2 指向同一集群只画一个节点）；点击节点打开集群抽屉（别名、IP:端口、访问状态、最近成功采集、Topic 搜索与 20 条分页、分区/副本/ISR 摘要、消费组首次展开才查询），只显示“凭据来源：MM2 配置（只读）”。Topic 链路先选别名再选 Topic，不可读集群保留并可手工输入 Topic；自动端点 Topic 详情直达 /endpoints/:id/topics/:topic。
- 阶段 3（单独 commit）：信息模式导航去掉“Kafka 集群”，集群页隐藏新增/编辑/连接测试/启停/删除，总览隐藏“管理 Kafka 集群”；“未登记/去登记”改为自动发现状态说明。路由与数据保留，可回退。
