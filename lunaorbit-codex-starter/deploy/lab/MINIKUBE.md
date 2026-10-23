# Kite + Kafka + MM2 实验环境

仅用于本轮用户授权的 Linux Docker 实验机。所有命令明确使用 `lunaorbit-lab` context；现有 `minikube` profile 不启动、不修改。此环境没有生产持久化、容量或高可用保证。

## 组件与隔离

- 独立 profile：`lunaorbit-lab`，Kubernetes v1.35.1，Docker driver，2 CPU / 2400MiB。
- 独立 namespace：`lunaorbit-integration`。
- 源 Kafka 3.7.2、目标 Kafka 3.9.2；各1 Broker/KRaft controller，独立随机 Cluster ID，RF1，PLAINTEXT 仅集群内部。
- MM2 3.9.2：`source -> target`，唯一实验 Topic `lunaorbit-lab-events`，默认策略目标名 `source.lunaorbit-lab-events`。只承诺本次唯一标记复制验证，不承诺持续健康或生产语义。
- Kite v0.16.0：缓存镜像，关闭匿名与analytics；Kubernetes ServiceAccount 仅本 namespace 的资源只读，额外允许 namespaces 列表。API Key 使用单独精确范围的 reader 角色。
- Kite Service 为 ClusterIP。宿主只绑定 `127.0.0.1:28080` port-forward；无 NodePort/公网入口。

脚本中的固定 profile/namespace/Topic 是隔离合成 fixture，不是产品默认环境。平台真实写开关保持false；kubectl仅安装实验基础设施，LunaOrbit资源读取仍经Kite。

## 复现

先核对没有同名既有业务，再在授权实验机启动专用profile（root使用Docker driver时需force）：

```bash
minikube start -p lunaorbit-lab --driver=docker --force --cpus=2 --memory=2400 \
  --kubernetes-version=v1.35.1 --container-runtime=docker --keep-context
kubectl --context=lunaorbit-lab get nodes
minikube -p lunaorbit-lab image load apache/kafka:3.7.2
minikube -p lunaorbit-lab image load apache/kafka:3.9.2
minikube -p lunaorbit-lab image load swr.cn-north-4.myhuaweicloud.com/ddn-k8s/ghcr.io/kite-org/kite:v0.16.0
```

将仓库 `scripts/minikube-lab.py`、`scripts/kite-lab-check.py` 放在实验机私有目录 `/root/lunaorbit-minikube-20261020/`，使用Python3.9+执行。仅适用于本次专用profile；不要针对其他context改名运行。

```bash
python3 /root/lunaorbit-minikube-20261020/minikube-lab.py setup
kubectl --context=lunaorbit-lab -n lunaorbit-integration rollout status deploy/kafka-source --timeout=90s
kubectl --context=lunaorbit-lab -n lunaorbit-integration rollout status deploy/kafka-target --timeout=90s
python3 /root/lunaorbit-minikube-20261020/minikube-lab.py prepare-mm2
kubectl --context=lunaorbit-lab -n lunaorbit-integration rollout status deploy/mm2 --timeout=90s
python3 /root/lunaorbit-minikube-20261020/minikube-lab.py replicate
kubectl --context=lunaorbit-lab -n lunaorbit-integration port-forward --address=127.0.0.1 svc/kite 28080:8080
# 另一个终端
python3 /root/lunaorbit-minikube-20261020/kite-lab-check.py
```

`prepare-mm2`只将本实验目标端三个 `mm2-{configs,offsets,status}.source.internal` Topic设为compact并重启专用MM2，不接触其他Topic。Broker Deployment重建不保留数据，因此重新搭建后必须重新初始化并重跑复制测试。

实验机私有目录保存随机凭据和Key（目录0700、文件0600），不复制到仓库或日志；不要展示原始凭据文件。Kite SQLite同样为临时数据，Pod重建后需重新运行认证检查。API Key创建/赋权路由依据对应版本 `routes.go` / handler核实，只用于搭建实验身份，不加入普通采集账号能力。

## Go实际集成

`backend/internal/discovery/collector/minikube_integration_test.go`新增两个显式启用测试，默认SKIP：

1. `TestMinikubeKiteReadAndDiscovery`：仅接受 `LUNAORBIT_KITE_LAB_URL=http://127.0.0.1:28080`、`LUNAORBIT_KITE_LAB_VERSION=v0.16.0`，Key通过`LUNAORBIT_KITE_LAB_KEY`进程环境安全注入。七条GET、现有collector、MM2证据/图与未升级verified断言。
2. `TestMinikubeKafkaNativeReads`：只在本实验namespace的Pod内、`LUNAORBIT_MINIKUBE_KAFKA=1`时执行；真实franz-go seed/advertised可达、两个不同Cluster ID、实际Topic RF/ISR及消费组读取。

Linux测试二进制通过 `GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go test -c ./internal/discovery/collector` 构建。测试解码器仅依据本次v0.16.0样本，私有于test文件；不会把当前小样本推广为所有Kite版本的生产Decoder。

## 已发现的环境约束

- 默认Kafka cleaner缓冲区超过小堆内存；fixture缩到16MiB缓冲区、192MiB最大堆，未关闭日志压缩。
- Kite默认全局缓存与受限SA范围不兼容；按对应版本源码配置`DISABLE_CACHE=true`，保持最小权限。
- 宿主外部localdomain DNS不可达；MM2只使用集群DNS及内部搜索域、ndots1，未修改宿主或旧集群DNS。
- MM2重建竞态可能将内部Topic自动创建为delete；显式初始化compact后再验证，不将Running当复制成功。
- 实验机系统时间与客户端日期存在偏差，本轮未修改宿主时钟，也不据其做真实延迟/过期验收。

按需从本机建立SSH隧道后访问Kite，密码不在命令中填写：

```bash
ssh -L 28080:127.0.0.1:28080 root@192.168.111.128
```

之后打开 http://127.0.0.1:28080。仅保留实验环境供后续联调；需要停机时用 `minikube stop -p lunaorbit-lab`，不要对现有profile执行删除或清理。

## 控制台真实只读部署

新增scripts/deploy-readonly-lab.py，在专用Linux目录中接收Linux静态API二进制与前端dist根目录tar；只对带synthetic-integration标签的既有namespace部署。脚本从私有kite-reader-key.json读取Key创建Secret，不将Key放镜像上下文或命令行。API与nginx组成同Pod，API监听127.0.0.1:18081，nginx提供8088前端和API同源代理及127.0.0.1:28080 Kite代理；Pod不挂ServiceAccountToken。

```bash
# backend目录构建Linux产物，frontend目录构建dist并打包；上传到实验机后：
python3 /root/lunaorbit-minikube-20261020/deploy-readonly-lab.py \
  /root/lunaorbit-minikube-20261020/lunaorbit-api \
  /root/lunaorbit-minikube-20261020/lunaorbit-frontend.tar
kubectl --context=lunaorbit-lab -n lunaorbit-integration rollout status deployment/lunaorbit-console
kubectl --context=lunaorbit-lab -n lunaorbit-integration port-forward --address=127.0.0.1 service/lunaorbit-console 18088:8088
# 本机另一个终端：
ssh -N -o ServerAliveInterval=15 -o ServerAliveCountMax=3 -L 8088:127.0.0.1:18088 root@192.168.111.128
```

访问http://127.0.0.1:8088。Pod更新后需重新启动port-forward。此独立入口读取真实实验资源，本机5173默认仍按原开发脚本连接本机API。

浏览器验收：`LUNAORBIT_LAB_E2E=1 pnpm exec playwright test e2e/readonly-lab.spec.ts`（Windows PowerShell先设置进程环境）。测试只接入固定回环8088入口，创建当前测试所有者的两套专用登记、实际探测/读取和发现映射；保留合成测试登记供该会话使用，不执行Kafka/MM2配置变更。生产Decoder仅为显式lab适配，版本兼容和分页仍不可推广。


## 已授权的实验管理模式（2026-10-20）

最新工作台验收配置覆盖下文早期无 authorizer 状态：两套合成 Kafka 已显式启用 StandardAuthorizer，ANONYMOUS 为实验 superuser，支持实际 ACL CRUD。`scripts/kafka-acl-lab.py enable` 检查专用 profile/namespace 标签及命名 Broker 镜像；同时将 Broker 容器限额设为640MiB，为192MiB Broker堆及64MiB短时CLI的 native/JVM 开销留余量。实验不验证 SASL 身份或 ACL 的业务拒绝执行。Broker仍是临时数据，滚动重建后用 `minikube-lab.py prepare-mm2` 恢复内部Topic，再用 `replicate` 验证实际复制。

控制台现为 API/web/PostgreSQL 三容器，PostgreSQL监听Pod回环，专用数据库和1GiB PVC保存平台登记、应用资料、团队授权、操作结果与审计。部署前上传 migrations/001–009 到实验私有目录下的 migrations/，部署脚本初始化并重复应用迁移。固定会话/快照密钥与数据库密码由实验机生成，放0600私有文件及Kubernetes Secret，不复制到仓库。应用无ServiceAccount Token。企业Kafka认证凭据仍未接生产SecretStore。

验收命令：`LUNAORBIT_LAB_E2E=1 pnpm exec playwright test e2e/workspace-lab.spec.ts e2e/admin-lab.spec.ts`。PG可选测试通过私有 `verify-workspace-postgres-lab.py` 运行，需临时回环转发 `deployment/lunaorbit-console 15432:5432`；无需暴露数据库Service。`verify-workspace-restart-lab.py prepare` / `verify` 跨Pod重建核验同一会话与应用资料。

用户明确授权后，先运行 scripts/enable-lab-management.py，再用部署脚本发布 admin-lab。Kite 的 Kubernetes Role 只允许更新 mm2-config 和 mm2，应用仍不挂 ServiceAccount Token。MM2 写入通过契约中的 PUT ConfigMap/PATCH Deployment 完成；独立环境开关允许实验管理员自审批。KAFKA_LAB_ALIAS_BINDINGS 配置已部署别名到已探测登记的关联，页面首次无快照会自动发现。普通 Mock/只读及生产默认禁写不变。Kafka Broker 未配置 authorizer，因此实际 ACL 查询/执行正确返回不支持；不以 Mock 数据替代。实验 Kafka 数据仍为临时容器存储，进程重启可丢失数据。运行短时 Kafka CLI 使用64MiB堆，避免与 Broker JVM 争用内存；必要时用 prepare-mm2 恢复隔离内部Topic的compact策略。

## 2026-10-21 SCRAM业务认证验收

运行 `scripts/setup-scram-lab.py setup` 前必须通过既有专用profile、synthetic namespace标签及精确Broker镜像守卫。脚本只为两套合成Broker增加SASL_PLAINTEXT:9094，保留PLAINTEXT:9092供MM2与实验管理；唯一superuser仍为ANONYMOUS，平台创建的SCRAM业务身份是受限账号。脚本不创建/修改任何业务密码。Broker为临时存储，配置重建后运行prepare-mm2及replicate恢复并验证合成链路。

服务端 ACCOUNT_SCRAM_ENDPOINTS 为管理端点到完整业务SASL端点的显式配置，当前两个单Broker按同主机9092→9094映射；所有端点仍受原网络白名单约束。不从Kafka发现结果猜测其他listener端口。真实生产多Broker/SASL_SSL仍需单独实际环境验收。

`verify-scram-access-lab.py` 在两套Kafka实际创建合成应用/账号/ACL，验证正确/错误密码、允许Topic生产/精确读取、禁止Topic写入、消费组允许/拒绝、真实消费组消费和Offset提交，并以第二个平台所有者录入已有账号验证原身份及权限不变。CLI凭据只走stdin，输出固定阶段标记；私有输入、Cookie和固定主密钥留在Linux私有目录，禁止上传仓库。账户密码由平台原生API生成，不再用CLI属性文件写SCRAM。

## 2026-10-21 Kafka 资源管理第2批实验

专用 source 增加 kafka-source-extra（Broker 2），由 scripts/setup-resource-broker-lab.py setup 幂等建立；只读 guard 可重复检查。控制台允许的 advertised 主机增加此合成服务。新 Broker 仍使用临时存储，不视为生产高可用环境。

验证入口：frontend/e2e/resources-lab.spec.ts；Linux 上运行 verify-resource-cancellation-lab.py 和 verify-resource-mm2-lab.py。取消测试只修改自己新建的随机 Topic，以及明确保存/恢复的两个 Broker 复制节流键；所有 Kafka CLI 堆限制为 64 MiB。

原 Pod 的 ndots:5 加 localdomain 搜索后缀会访问不可达宿主 DNS。fix-resource-dns-lab.py 在该专用 profile 的 CoreDNS 中，仅对 *.lunaorbit-integration.svc.cluster.local.localdomain 返回 NXDOMAIN；原配置备份保存在实验私有目录 resource-coredns-before.json，CoreDNS 热加载，Kafka/MM2 不滚动重建。其他 DNS 范围保持原配置。

验收完成：真实两版本配置/删除、双 Broker 迁移及迁移中取消通过，节流已恢复。MM2 回归脚本按 earliest/latest 读取当前有效 Offset 窗口，避免已过期 Offset 0 导致误判；新的唯一 marker 复制已实测通过。

## 2026-10-21 四集群拓扑实验

用户新增授权第二台 Linux 实验机。原 source/target 和原 MM2 保留；第二台运行两个带 `lunaorbit.io.purpose=synthetic-topology` 标签、独立数据卷的 Docker Kafka 3.9.2：archive（C）、upstream（D）。新增专用 namespace 内的 mm2-mesh，同一个进程管理三个连接器，形成 source→archive、upstream→target、target→archive；加上原 source→target，覆盖分叉、汇聚和串联。新 Broker 只在明确指定的私网地址监听，控制器只监听回环。无生产资源接入。

运行方式（SECOND_LAB_IPV4 由操作者指定，不写死到产品）：

```bash
# 第二台：先准备已有 apache/kafka:3.9.2 镜像，再运行
python3 setup-topology-lab.py remote-brokers --host "$SECOND_LAB_IPV4"
# 第一台：检查现有专用 namespace 后，只创建新的 mm2-mesh/config
python3 setup-topology-lab.py mesh --host "$SECOND_LAB_IPV4"
```

第一台私有 `topology-lab.json` 保存非敏感端点与工作负载信息。deploy-readonly-lab.py 读取它，将实验目录扩成四个，增加精确 IP/32、两个端口和六项端点映射；不允许浏览器自由扩大网络范围。实验目录和绑定均限制2～16项，生产默认不开启。原始 MM2 配置没有覆盖。

MM2 使用 Kafka 自带按 source、target 隔离的 Connect 存储，不设置目标级共享 group.id；关闭全局心跳以避免未启用方向也创建 herder，限制 producer buffer。行为已核对 [Kafka 3.9.2 MirrorMakerConfig 源码](https://github.com/apache/kafka/blob/3.9.2/connect/mirror/src/main/java/org/apache/kafka/connect/mirror/MirrorMakerConfig.java)。节点共享进程时，画布按逻辑 Kafka 链路计算层级，不能把共享工作负载误判成 Kafka 环路。

本次给专用 lunaorbit-lab Docker 容器的内存上限从2400MiB调整到3GiB；没有修改其他 profile。原两个 Broker 依旧是早期临时存储实验，不能当成生产持久化/高可用验收；不要随意重建它们。新 C/D 使用单独 Docker 卷。若重启 Minikube，要重新检查内存限额和转发，避免使用原2400MiB上限运行整个拓扑。

验收工具：Linux 静态构建 `backend/cmd/lab-topology-check`，只接受 `--lab` 和标准输入中的私网 remote_host。脚本 `verify-topology-lab.py` 校验专用 namespace 后在控制台 web 容器运行工具。它检查四个不同 Cluster ID、发送两个唯一合成 marker，并验证 A→B、A→C、D→B、A→B→C、D→B→C 五条消息路径；最多读取最近100条，不输出消息正文，不修改已有 Topic 配置或 MM2。

密码维护仅限用户明确指定的实验集群配置：`import-existing-lab-accounts.py --registration <id>` 配合 `cmd/lab-account-import`，使用私有主密钥恢复此前本实验生成且 Kafka 仍存在的账号密码，真实认证后用目标配置的 AAD 重新加密，记录 lab-maintenance/account.import 审计。私有密码和数据库信息只走 stdin，不回显；已有密码不覆盖，Kafka 密码与 ACL 不修改。不把该维护程序作为生产接口或自动跨用户同步机制。
