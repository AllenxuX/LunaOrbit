# UX-v2 唯一接口契约（LUNA-UX-05）

状态：后端交付契约；前端请以此确认字段，不以原型样本猜接口。所有路径前缀 `/api/v1`，information 模式，既有只读边界不变。示例均合成、仅示意列出的字段，不是实际采集结果。

## 1. 范围与重新采集

复用 `GET /discovery/scopes` → `{items:[{id,synthetic,k8s_cluster?,auto?,removed?,removed_at?,kite_error?,namespaces,namespace_mode}]}`。
本轮仅增加 `namespaces:string[]`、`namespace_mode:"allowlist"|"all_accessible"`；前者是静态允许列表，后者在 auto 模式表示该 K8s 集群下可访问且未被采集器排除的 Namespace，不代表全组织。不得把页面 Namespace 筛选当成重新采集粒度。静态模式的 k8s_cluster 可能省略，不自行猜测。

- 重读发现快照：GET snapshot，不刷新发现时间。注意 Topic链路 GET 会读取Kafka元数据（最多60秒缓存）；对象详情 GET 也可能发起只读查询。不得把这些入口统一描述为“完全不访问外部数据”，只能保证不触发发现 runs、不改写MM2配置。
- 重新采集：`POST /discovery/scopes/{id}/runs`，JSON `{}`（不传 Namespace、不传局部失败重试）。202 返回 `{job_id,status:"pending",started_at}`。
- `GET /discovery/scopes/{id}/runs/latest` 返回 `{job_id,status:"pending"|"running"|"done",started_at,finished_at?,error_code?}`。done 仅代表结束；必须检查 error_code，并重新读取快照。任务是每共享空间+scope最新任务，非持久化任务历史；服务重启可能404。
- 目标确认必须显示 scope ID 和 namespaces/namespace_mode；不支持只重试失败 Namespace、取消任务或逐消息验证。批量范围由前端逐scope有界发起，429需退避，不新造批量API。
- 错误 `{error:{code,message,request_id}}`：400 INVALID_ARGUMENT，403 FORBIDDEN，404 NOT_FOUND，409 SCOPE_REMOVED/CONFLICT，429 RATE_LIMITED，503 REGISTRY_UNAVAILABLE/STORAGE_UNAVAILABLE。message 为安全通用说明，不能拼接原始上游错误/配置。

请求示例：`POST /api/v1/discovery/scopes/demo-scope/runs`，Content-Type application/json，body `{}`。
响应字段示例：`{"job_id":"synthetic-job","status":"pending","started_at":"2026-10-27T00:00:00Z"}`。

## 2. 快照、计数与关系状态

`GET /discovery/scopes/{id}/snapshot?page=1&page_size=100` → `{snapshot,total,page,page_size}`；page_size 1～100。`total` 是该scope的 Link总条数，不是MM2实例数、Topic数或故障数。必须读全分页才算范围总量；分页中的 nodes/edges 只对应当页 links。

snapshot 字段：`scope_id,synthetic,observed_at,attempted_at,stale,links,nodes,edges,issues`，auto模式可有 `workloads,scan`。缺失快照404是未知，不是零。

Link关键字段：`id,namespace,workload_uid,workload_name,k8s_cluster?,source_alias,target_alias,enabled,declared,observed,verified,stale,removed?,state,observed_at,runtime,source,target,evidence,notes,declarations,policy,topics_raw,topics_exclude_raw,separator,topic_filter_class,semantics_known`。
- 配置启停用 enabled；运行证据 observed/runtime；验证 verified；过期 stale，互相独立。verified不代表持续健康，也不证明逐条消息复制。禁用但历史verified不能显示当前已同步。
- runtime `{generation,observed_generation,replicas,ready_replicas,pod_candidates,pod_association}` 是K8s部署证据，不是业务流量；过期/采集未知时不能把历史Ready作为当前健康。
- observed_at 是证据时间，attempted_at 是最近尝试；失败可保留历史。GET本身不推进任何时间。stale是服务端采集退化标记，不是实时TTL计时器；前端还需显示实际时间/距今时长，缺失或零时间显示未知，超过前端所声明的新鲜度窗口显示过期而非故障。不要把链路查询生成时间当快照采集时间。
- issues `{code,namespace?,workload_uid?,config_uid?}` 是采集/解析问题，可与异常对象重叠，不可将其数目与未验证MM2相加。
- 单scope关系按 link.id 去重；跨scope按 `(scope_id,link.id)`。实例按 `(scope_id,namespace,workload_uid)` 去重；不按workload_name。MM2未验证数按实例去重其 `!verified` 的非removed关系，明确为“含未验证关系的MM2实例”，不是失败。
- 失败范围按 `(scope_id,namespace)` 去重，缺Namespace按scope整体；同一次失败不要按issue条数当范围数。异常对象只有明确异常证据才计数；未知、过期、未验证不能计为故障。
- Kafka集群复用 `GET /discovery/catalog` 的 `items` 合并结果、以catalog.id计数，不按alias数或图节点数。目录按Cluster ID/地址交集合并；ambiguous不强制合并。全局与Topic查询范围不同不要求数值相等。

策略：仅真实全类名 IdentityReplicationPolicy 可解释为保留源名称，DefaultReplicationPolicy结合 separator；未知类保留原文+待确认。topics_raw保留技术表达式，复杂正则不强译；规则数量不等于实际Topic数。

## 3. Topic链路（不新增接口）

`GET /topic-lineage?cluster_id=<lineage_id或登记id>&topic=orders&depth=2&direction=downstream`。
响应 `{root_id,observed_at,nodes,edges,issues,truncated,depth}`；depth 1～4，direction both/upstream/downstream；不存在的登记/端点404，非法参数400，整体不可用503 LINEAGE_UNAVAILABLE。200亦可能issues非空或truncated=true。

前端查询摘要使用本次请求的起点、方向、depth和返回证据范围；Graph.observed_at为查询生成时间，节点/边 observed_at才是各自证据时间。无返回边不是没有上下游；边界节点不命名为真实源头；反方向未查即范围外。

Node `{id,kind,label,cluster_id?,cluster_name?,topic?,group?,location?,owner?,application_id?,link_id?,exists,existence_note?,evidence,observed_at}`；exists为 `true|false|null`，null绝不显示“不存在”。Topic身份是集群+Topic；不要只按同名合并。
Edge `{id,source,target,kind,evidence,observed_at,scope_id?,link_id?,policy?,rule?,account?,client_id?}`；按id去重；configured/observed/manual/historical/unknown按原证据显示。人工登记account是用户授权登记的账号名称，不是MM2解析出的秘密，也不是认证成功证明。

空图响应形状示例（字段完整；合成）：`{"root_id":"synthetic-root","observed_at":"2026-10-27T00:00:00Z","nodes":[],"edges":[],"issues":[],"truncated":false,"depth":2}`。实际有效根通常仍有根节点，不把此示例当测试fixture。

## 4. 对象详情与局部失败

登记路径：`GET /kafka/clusters/{id}/topics/{topic}` 返回 Kafka Snapshot `{synthetic,observed_at,stale,state,reason?,brokers,topics,groups,offsets,capabilities}`。topics项 `{topic_id?,name,internal,partitions,rf_min,rf_max,error_code?}`；partition `{partition,leader,replicas,isr,error_code?}`。每分区RF=len(replicas)仅在成功元数据时计算；未知/错误不补0，leader负值不是有效Leader。

配置独立读取 `GET /kafka/clusters/{id}/workspace/resource-configs?resource_type=TOPIC&resource_name=<topic>`；沿用现有ConfigSnapshot `{resource:{type,name},entries}`，不增加聚合接口。配置响应没有采集时间字段，前端只能显示请求完成时间并明确其含义，不能冒充Broker证据时间。entries项 value可null，另有 source/sensitive/read_only/type/default_value/synonyms 等现有字段；使用 `min.insync.replicas`、`retention.ms`、`cleanup.policy` 的实际值与来源，未返回/敏感/查询失败保持未知，不填Broker默认值。retention.ms=-1明确无限保留，不当未知。

配置请求失败时保留成功分区元数据；分别显示两请求状态和时间，不把整个Topic详情替换为空。错误为统一error envelope。自动端点用catalog.read_via/registration_id分流：auto只读查询 `/discovery/catalog/{endpoint}/topics`、`/topics/{topic}`、`/groups`；不可读时保持未知，不杜撰配置能力。409 USE_REGISTRATION附registration_id，409 CREDENTIALS_OFF表示不可读，502 ENDPOINT_UNREADABLE表示Kafka读取失败；按已存在登记路径回退，不猜id。

## 5. 安全与边界

不新增健康指标、消息验证、Kafka/K8s写入口或基础设施能力。ConfigMap原文、JAAS、MM2用户名密码、Kite Key不得出现在响应、日志、示例。API新字段仅公开范围元数据。现有个人布局仍由浏览器维护；应用登记复用现有information接口和revision，不影响采集快照。
