# Topic 全链路图

入口：左侧「Topic 链路」或Topic详情「查看链路」。选择集群和准确Topic名称，默认追踪上下游2段，可切换方向和1～4段深度。图中始终保留当前Topic，同名Topic按集群分开。MM2每条同步规则独立展示，支持多源、分支和A→B→C；画布只读，可平移、缩放、全屏和适应画布，点击节点或关系查看详情。下方列表提供文字入口。

## 证据含义

- 实际观测：Kafka返回的Topic存在性及当前消费者分配/订阅；不代表消息内容已被端到端追踪。
- 配置关系：来自现有Kite发现的MM2规则，逐跳应用命名策略。目标Topic存在仍不能证明同步健康。
- 历史关系：只有已提交消费位点；不能当作当前消费。
- 人工登记：来自应用信息中的生产/消费关系；Kafka原生API不能直接还原全部生产应用，不使用ACL推断流量。
- 未知：旧快照、采集失败、不支持的协议/策略或无法安全判断的匹配。局部问题保留其他可读关系，不伪造完整结果。

应用生产/消费关系通过「登记应用关系」维护。每条关系可记录Kafka账号名称（用户名或Principal）、Client ID；消费关系可填写消费组，应用可填写部署位置。账号属于该集群的绑定信息，不会跨集群自动沿用，不保存密码或修改Kafka账号。点击应用节点查看负责人/部署位置，点击应用关系查看账号/Client ID；同应用同Topic的不同账号关系分别保留。以上仍为人工登记，不能证明当前正在收发消息；记录不缓存，修改后重新查询即可生效。人工登记消费组与原生观测消费组使用不同身份，避免将登记冒充观测。

## MM2语义

已支持DefaultReplicationPolicy及IdentityReplicationPolicy常规Topic、配置分隔符、包含/排除规则、默认Topic过滤、内部Topic独立排除与Default别名循环检查。Default命名逐跳计算，例如源Topic `lunaorbit-lab-events` → B的 `source.lunaorbit-lab-events` → C的 `target.source.lunaorbit-lab-events`；A直达C的同名 `source.lunaorbit-lab-events` 是另一条路径。

自定义命名/过滤类、Go无法安全解释的Java正则、Identity心跳特殊行为均明确未知。旧快照缺少语义字段时须刷新发现，不根据目标名称猜测规则。禁用和过期规则不作为有效同步路径。

## 只读API与边界

`GET /api/v1/topic-lineage?cluster_id=<登记ID>&topic=<名称>&depth=2&direction=both`

方向为both/upstream/downstream；深度1～4。响应包含root_id、nodes、edges、issues、truncated和observed_at，证据为observed/configured/manual/historical/unknown；Topic的exists允许null，采集不完整时不返回错误的“不存在”。

单请求最多8秒、16集群、200节点、400关系、4096条配置链路、16384次链路遍历；每集群最多12个消费组，消费读取全局最多64次，应用最多1000条。达到限制显示截断/局部问题。成员与协议数据也设解码大小上限。成功的Kafka只读结果缓存最多1分钟，保留原采集时间，相同请求合并读取；不缓存应用记录。无Kafka/MM2写操作，无新Kite接口。

## 实验验证范围

专用四Kafka/两MM2实验已验证实际A→B→C、A→C分支、反向追踪、深度限制、逐跳Topic存在和人工应用关系；合成应用验证后删除。消费成员解码与当前/历史区分有原生协议fixture自动测试。实验中部分组没有可解释的当前成员分配/订阅，页面如实提示未知；尚未验证企业业务SDK、所有自定义MM2策略、所有Kafka版本和生产流量。平台不采集消息内容，也不提供逐条消息分布式追踪。


## 当前实验：同名Topic复制

2026-10-23按用户实际场景将专用实验两套MM2切换为IdentityReplicationPolicy。当前A、B、C均使用`lunaorbit-lab-events`，D→B→C均使用`lunaorbit-mesh-events`，不添加集群别名前缀。图中仍以“集群+Topic”为唯一身份，不把同名Topic合为一个节点。

测试拓扑保持A→B、A→C、D→B、B→C无环；A→C与A→B→C在同一C Topic汇入，可能收到两条路径复制的相同业务消息，本实验不宣称去重或exactly-once。独立向B写入唯一标记验证B→C，D的消息验证D→B→C，避免仅凭A直达C误认多跳成功。旧前缀Topic作为历史实验数据保留，不删除，也不再被当前同名规则选中。

Kafka Connect工作组用于MM2协调，不作为业务消费组解析；正常的Connect协议不触发“消费证据未知”。真正失败/未知的业务组仍提示，并附集群与组名。反向查询回访节点和多路径汇入不等于环路，只有已展开的实际有向复制关系闭环才提示同名复制环路。

参考：[Kafka 3.9 IdentityReplicationPolicy](https://kafka.apache.org/39/javadoc/org/apache/kafka/connect/mirror/IdentityReplicationPolicy.html)。同名策略无法依靠Topic前缀自动防止复制环路，配置必须避免闭环。
