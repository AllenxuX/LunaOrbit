# LunaOrbit 开发总结与接续说明

更新时间：2026-10-23。本文用于换电脑或新聊天后恢复开发上下文。它描述目前已交付的系统，历史设计不覆盖本文和最新AGENTS中的产品决定。

## 1. 下次开发先看这里

- 仓库：https://github.com/AllenxuX/LunaOrbit.git
- 本次交接分支：`codex/handoff-2026-10-23`。
- 应用工作目录：`lunaorbit-codex-starter/`。根目录是Git仓库，尚未把应用子目录移到根目录；不要误把子目录当成独立Git仓库。
- 按顺序阅读根AGENTS、本文、项目AGENTS、项目README、ROADMAP、docs/PRD.md、ARCHITECTURE.md、SECURITY.md、KITE_API_CONTRACT.md，再看本次要改的模块。
- 当前阶段已经完成。先核对用户的新要求，不能按最初Phase 00或旧权限方案重新开发，也不要自动推进其他阶段。

可直接给下次助手的说明：

> 请先阅读根目录 DEVELOPMENT_HANDOFF.md、AGENTS.md 和 lunaorbit-codex-starter/AGENTS.md，再检查当前分支及工作区。当前是无登录、共享空间的信息平台；保留Kafka只读查询、MM2/Topic链路和应用登记，不恢复历史权限/账号密码/ACL管理。实验只用同名Topic复制。接续我下一条明确任务；不要擅自重建实验、删除数据或访问生产。

> 2026-10-26 定位调整（LUNA-60，进行中）：自动发现优先的内网 Kafka/MM2 拓扑与信息平台：从 Kite 与 MM2 配置自动读出集群和同步关系，人工只负责补充和登记业务信息。
> - 定位表中「集群与应用登记」改为：集群由自动发现产生，手工补充只用于未被 MM2 覆盖的集群；应用与业务字段仍为可修改的共享信息。
> - 导航：「Kafka 集群」入口将去掉，集群信息放在拓扑节点侧栏，Topic 详情页仍可直达。
> - 已知问题：MM2 凭据只存采集进程内存，重启后需重新扫描；KAFKA_MM2_CREDENTIALS 代码默认关闭，部署时显式开启，关闭时不读凭据、不自动连接。

## 2. 已确认的产品定位

当前产品是**企业内网 Kafka/MM2 拓扑与信息记录平台**，以 `PLATFORM_MODE=information` 运行。

| 内容 | 当前状态 |
| --- | --- |
| 登录、LDAP、平台RBAC、团队授权 | 不需要；信息模式不挂载相关入口及API |
| Kafka业务账号密码、ACL管理、配置修改 | 不开放；旧实现保留为历史兼容，不能重新暴露 |
| Kafka连接参数 | 保留，用于后台采集；连接秘密不能回显 |
| Kafka资源 | 集群、Broker、Topic、分区、副本、配置、消费组等只读查询及独立详情页 |
| 集群与应用登记 | 可修改的共享信息，PostgreSQL持久化，revision防并发覆盖 |
| MM2拓扑 | 默认连续Kafka→MM2任务→Kafka，另有同步关系、按实例及任务视图 |
| Topic全链路 | 按集群+Topic追踪上下游1～4段，MM2、消费组、登记应用与证据详情 |
| 应用账号绑定 | 记录账号名称/Principal、Client ID、Topic、生产/消费和消费组；不是认证或授权管理 |
| 生产者自动识别 | 尚未实现；生产应用来自人工登记，不能把账号/ACL推断为真实流量 |

最初文档与历史代码包含完整运维/LDAP/权限方案，是早期阶段记录。2026-10-23用户已明确取消；当前用户后续允许账号名称用于信息绑定，不等于恢复密码或ACL功能。

## 3. 技术与目录

- 后端：Go 1.25、Gin、franz-go原生Kafka协议；单体模块化结构。
- 前端：Vue 3、TypeScript、Composition API、Element Plus；拓扑AntV X6，ELK负责自动布局。
- 数据：PostgreSQL，migration 001～010；010是独立应用信息记录表，本批字段采用JSONB兼容，无额外迁移。
- Kubernetes/MM2：严格遵循Kite HTTP API契约，不允许发明接口，不使用GitOps管理MM2。
- 本次本机实际工具版本：Go 1.25.6、Node 24.19.0、pnpm 11.20.0；frontend/package.json声明pnpm 10.18.3。版本差异应留意，保留锁文件，不随意升级依赖。

```text
lunaorbit-codex-starter/
  backend/cmd/server/              服务入口，information.go挂载现行信息模式
  backend/internal/information/   应用登记、校验、HTTP、内存/PG Store
  backend/internal/lineage/       Topic链路聚合、消费证据、缓存与API
  backend/internal/discovery/     Kite发现、MM2解析和快照
  backend/internal/kafkaadmin/    franz-go查询、消费成员证据解码
  backend/internal/registry/      集群登记、连接、查询和范围隔离
  frontend/src/views/            Topology/TopicLineage/InformationApplications等
  frontend/src/components/       X6画布、应用表单及资源组件
  frontend/e2e/                  Mock与显式启用的实验浏览器测试
  migrations/                    PostgreSQL迁移
  scripts/                       本地任务、实验搭建/验证/部署
  docs/                          设计、契约、阶段计划和开发日志
```

关键文件：
- `frontend/src/views/TopicLineage.vue`：Topic入口、集群/Topic/方向/深度、证据详情。
- `frontend/src/components/TopicLineageCanvas.vue`：只读链路画布；不开放节点拖动编辑。
- `frontend/src/components/InformationApplicationForm.vue`：应用、部署位置、关系账号/Client ID/消费组。
- `backend/internal/lineage/engine.go`：Topic唯一身份、逐跳命名、分支、双向查询、真实有向环检测。
- `backend/internal/lineage/consumers.go`：实际消费/历史位点分离；Connect协调组不作业务消费者。
- `backend/internal/lineage/adapter.go`：Kafka读取最多60秒缓存，应用记录不缓存。
- `backend/internal/kafkaadmin/group_evidence.go`：安全解码成员分配/订阅，未知协议不猜测。

## 4. 已完成的交互和语义

### MM2与Topic拓扑

- MM2图支持一到多、多到一、A→B→C，同一Kafka节点唯一、每条MM2任务单独呈现；按实例查看仍保留。
- 初始/显式整理使用ELK；固定左右端口侧并优化端口顺序，拖动采用局部稳定路线。个人布局在浏览器按共享空间/范围/视图保存，不写服务端。
- Topic全链路通过Topic详情「查看链路」或左侧「Topic 链路」进入，可查询上下游、上游、下游，默认2段，支持1～4段。
- 节点身份是**集群+Topic**，同名Topic在不同Kafka中不能合并。
- 证据区分 observed（实际观测）、configured（同步配置）、manual（人工登记）、historical（历史位点）、unknown；目标Topic存在不代表复制健康，更不代表消息端到端追踪。
- 接口：`GET /api/v1/topic-lineage?cluster_id=<登记ID>&topic=<名称>&depth=2&direction=both`。
- 有界查询：8秒、16集群、200节点、400关系、4096配置链路、16384次链路检查；每集群12消费组、全局64消费读取，应用最多1000条。截断/局部失败必须明确显示。
- 自定义策略/过滤类、无法安全解释的Java正则及Identity心跳特例明确未知；旧快照语义缺失时先刷新发现。

### 应用与账号信息

应用字段：名称、负责人、环境、说明、部署位置location；每条关系：cluster_id、topic、mode、可选account/client_id，以及消费关系的consumer_group。

账号只记录名称/Principal，不存密码，不验证认证或ACL。不同集群不同账号不跨集群沿用；同应用同Topic的不同账号关系不能折叠。点击应用查看负责人/位置，点击关系查看账号/Client ID。所有这类绑定仍为人工证据。

生产端没有与消费组对等的“生产组”。Kafka原生管理API不能完整还原生产应用；未登记时不伪造生产节点。真正消费组通过Kafka成员分配/订阅与历史提交位点分别读取，不把历史消费标成正在消费。

### 最近修复的误报

黄色框表示采集局部问题，不是MM2复制失败告警。已修复：
1. MM2的ProtocolType=connect工作协调组被按业务消费组解析，造成4条重复未知告警；现在对成功且新鲜完整的Connect详情跳过业务解析，真实失败仍保留。
2. 同名复制的反向回访、A→B→C/A→C汇入被visited节点误判为环路；现在检查实际有向边是否闭环，正常汇入不告警，真实A→B→A仍告警。
3. 真正未知消费组提示包含集群与组名，避免重复且无法定位的文本。

## 5. 当前真实实验环境

**仅以下已授权专用实验，严禁生产访问。**

| 位置 | 用途 |
| --- | --- |
| 192.168.111.128 | Docker/Minikube、Kite、控制台、A/B Kafka、MM2实例 |
| 192.168.111.129 | 扩展实验Kafka C/D |
| Minikube profile/context | lunaorbit-lab |
| Namespace | lunaorbit-integration，标签purpose=synthetic-integration |
| 远端目录 | /root/lunaorbit-minikube-20261020 |
| 控制台Deployment | lunaorbit-console，api/web/postgres三个容器，PG PVC保留 |
| 共享工作空间 | f60bce6cf6132278da1cf1d5b13c79ba，显式单一目录；不可自动合并历史owner |
| 发现范围 | readonly-lab |
| 当前控制台镜像标签 | information-e47c831d6dee |

账号/SSH密码、Kite Key、数据库密码等**不写入本文或Git**；沿用用户自行保存的凭据和实验机私有文件，不把整个远端目录打包上传。

### 业务场景：只测试同名Topic复制

用户最新要求：实际场景只有同名复制。实验已切换 `org.apache.kafka.connect.mirror.IdentityReplicationPolicy`，不再用默认加前缀命名演示。

```text
A 实验源集群 ──→ B 实验目标集群 ──→ C 实验归档集群 C
       └────────────────────────→ C
D 实验上游集群 D ──→ B ──→ C
```

- A/B/C：`lunaorbit-lab-events`。
- D/B/C：`lunaorbit-mesh-events`。
- 四条配置方向：source→target、source→archive、upstream→target、target→archive；反向禁用。
- 两MM2：mm2、mm2-mesh；B→C只包含上述两个业务Topic。
- 同名策略不能依赖前缀自动阻止复制循环，配置保持有向无环。
- A直达C与A→B→C在同一C Topic汇入可能导致业务重复，本实验不声称去重/exactly-once。必须用B独立唯一消息验证B→C，不能仅凭A在C出现推断经过B。
- 旧`source.*`、`target.source.*`前缀Topic保留为历史数据，没有删除；新规则不再选中它们。查看当前链路请选择无前缀名称。
- 最近验证A根depth2 both：6节点、6关系、issues=[]、truncated=false。
- 测试创建的人工应用已清理，结束时共享应用数0。不要把合成测试数据当成业务登记。

### 回家后访问现有实验页面

`127.0.0.1:8088`是原开发电脑的SSH转发，不是公网地址。家里电脑需要能到达上述内网（同网/VPN等）；代码仓库本身不能提供内网连通性。

确认远端控制台转发仍在运行：远端18088 → console8088。若已不存在，在远端另开终端运行：

```sh
kubectl --context=lunaorbit-lab -n lunaorbit-integration port-forward --address=127.0.0.1 deployment/lunaorbit-console 18088:8088 15432:5432
```

然后家里电脑运行并保持终端：

```sh
ssh -N -L 8088:127.0.0.1:18088 root@192.168.111.128
```

浏览器打开 http://127.0.0.1:8088 。使用已有SSH凭据；不要把密码拼进命令。若远端端口已被正确转发占用，不要重复启动、杀未知进程或改成0.0.0.0。

### 实验部署的特别注意

- `scripts/deploy-information-lab.py`只用于原专用lab，保留数据库/旧Deployment备份、原共享workspace，不能用于生产。
- `scripts/configure-same-topic-lab.py --apply-lab`已执行，属于基础设施变更；不要为“恢复页面”重复执行。它经既定Kite API更新MM2，resourceVersion约束、私有备份，临时精确资源名授权已恢复。
- 控制台运行模式information，Kafka/Kite写开关false、changes disabled；不要通过改回旧部署脚本恢复历史权限。
- **实验机磁盘余量很低**。最近部署因磁盘满导致web镜像未载入、ErrImageNeverPull，服务曾中断；已清理本轮明确生成的中间载荷、重载缺失镜像并恢复3/3 Running。数据、备份、Kafka/MM2均保留。
- 部署前先检查磁盘与CRI镜像清单；脚本已增加镜像载入后的tag存在检查，防止部分载入却继续替换可用控制台。不能通用docker prune、删除PVC、Kafka数据或未知文件。
- 部分旧构建目录仅保留备份，api/frontend载荷已清理，不能假设旧目录可直接重新构建。
- 实验机器时钟与开发电脑日期不一致，所以页面采集时间可能显示10月21日；未修改机器时钟，不要据此覆盖实际时间戳。

## 6. 在新电脑独立本地开发

```powershell
git clone --branch codex/handoff-2026-10-23 https://github.com/AllenxuX/LunaOrbit.git
cd LunaOrbit/lunaorbit-codex-starter
pnpm --dir frontend install --frozen-lockfile
go -C backend mod download
```

两终端分别启动：

```powershell
pwsh -File scripts/tasks.ps1 information-backend
```

```powershell
pwsh -File scripts/tasks.ps1 frontend
```

访问 http://127.0.0.1:5173 。Vite将/api和/healthz代理到后端127.0.0.1:8080。默认本地information-backend使用内存Mock，不需要Kite、LDAP或真实Kafka；重启会清空本地应用记录。它与8088的真实实验不是同一数据源。

持久化开发需要本地PostgreSQL和迁移：复制.env.example为不提交的.env，自行设置数据库密码，`docker compose up -d --wait postgres`，执行`scripts/migrate.ps1`。服务不会自动读取.env；运行Go前仍需设置进程的DATABASE_URL、REGISTRY_MODE=postgres-mock、固定INFORMATION_WORKSPACE_ID等。不要把真实实验私有配置复制进Git。

## 7. 测试、验证和当前证据

在应用目录运行：

```powershell
pwsh -File scripts/tasks.ps1 test
pwsh -File scripts/tasks.ps1 typecheck
pwsh -File scripts/tasks.ps1 lint
pwsh -File scripts/tasks.ps1 build
pnpm --dir frontend exec playwright test --config playwright.information.config.ts
```

说明：Go全量测试中的真实PG测试在未设置INFORMATION_TEST_DATABASE_URL时会skip；不能把默认测试通过解释成真实数据库已验收。浏览器使用playwright.config.ts配置的Edge，换电脑需安装相应浏览器或按已支持方式配置；不要静默修改所有浏览器测试。

最近已实际执行并通过：
- Go `go test ./...`、`go vet ./...`、Linux服务/实验检查器构建；Connect组、同名DAG/真实环、未知组提示有针对性回归。
- 前端52文件258项单元测试、类型检查、lint、build；最后同名修正没有修改前端产品源码，更新了实际实验断言。
- 独立Mock浏览器3项通过。
- 原生PG TestPostgresPersistence真实通过，包含location/account/client_id、revision、更新时间及旧payload兼容。
- 最新真实实验10项浏览器通过（12.2秒）：6项信息平台+4项Topic链路，包含同名不同集群节点、方向/深度、应用账号关系、字段持久化、旧治理阻断及记录清理。
- 4个不同原生Kafka ID、3条随机标记、6项同名复制观察通过；独立B标记证明B→C，D标记验证D→B→C。

真实实验浏览器仅在恢复8088、确认专用实验后显式启用：

```powershell
$env:LUNAORBIT_INFORMATION_LAB_E2E='1'
$env:LUNAORBIT_TOPIC_LINEAGE_LAB_E2E='1'
pnpm --dir frontend exec playwright test e2e/information-platform-lab.spec.ts e2e/topic-lineage-lab.spec.ts
```

这些用例会创建/编辑/删除合成应用登记，finally清理；不会修改Kafka ACL/MM2配置。其他历史lab测试有资源写入，不要通过设置全部LAB标志后直接跑整个e2e目录。

## 8. 已知问题与后续边界

1. 连接Secret Provider仍为内存实现。数据库有秘密引用不代表秘密已持久化；真实认证集群重启后可能需要重新录入连接秘密。当前PLAINTEXT实验不受此影响。不能声称生产Secrets/KMS已完成。
2. 生产者仅人工登记；要实时发现主机/进程/生产速率需另行设计探针、SDK或监控接入，账号绑定不能代替观测。
3. 当前消费详情能力受协议、授权和成员元数据影响；未知保持未知，不能隐藏真实问题或把历史位点当当前消费。
4. Topic链路画布只读、根节点优先保持可读缩放；较长图需平移/适应画布。现有构建有大chunk提示，暂未开展专门体积优化。
5. 内网无登录意味着能访问平台的人共享登记信息；不是公网部署方案。Host/Origin校验、出站范围、请求上限等仍需保留。
6. 自定义MM2策略、完整Kafka版本矩阵、生产业务客户端流量、持续复制健康、逐消息追踪均未验收。
7. 历史LDAP/RBAC/ACL/配置写代码和测试仍在仓库，信息模式下不可达。不要因为看到代码就恢复入口。
8. 家里的机器不会自动拥有当前实验数据、Docker镜像、浏览器依赖或原电脑SSH隧道。Git仅交付源码/文档/迁移/测试，不交付运行中的数据和秘密。

## 9. 设计与日志索引

以下均位于应用子目录：
- `docs/TOPIC_LINEAGE.md`：Topic证据、同名实验、边界和API说明。
- `docs/superpowers/specs/2026-10-23-topology-information-platform.md`：产品收敛依据。
- `docs/superpowers/specs/2026-10-23-application-topic-identity-design.md`：应用账号绑定。
- `docs/superpowers/plans/2026-10-23-topic-lineage.md`、`2026-10-23-application-topic-identity.md`、`2026-10-23-same-topic-lab.md`：已完成阶段。
- `docs/IMPLEMENTATION_LOG.md`：逐批真实命令、结果、部署、误报与磁盘故障恢复记录。
- `ROADMAP.md`：已完成阶段及历史计划，最新用户要求优先。
- `docs/KITE_API_CONTRACT.md`：必须遵守的资源路径/认证/版本契约。

下一次建议先做只读健康检查和磁盘容量盘点，再按用户明确的新任务推进；本次不擅自扩展功能。
