> 2026-10-23 当前产品调整：内网无登录的共享消息拓扑与信息记录平台。登录、权限、业务账号密码交付、ACL和基础设施写操作退出当前范围；历史条目仅用于理解旧实现，不覆盖当前要求。信息登记可写，Kafka/MM2只采集查询，连接秘密仍仅供后台使用。现行设计见 [信息平台方案](superpowers/specs/2026-10-23-topology-information-platform.md)。

# 安全与变更约束

## 2026-10-21 应用业务凭据受控交付例外

用户明确批准支持应用账号的密码托管和查看。下文“禁止前端返回秘密”对普通列表/查询、集群管理凭据、Kite Key、LDAP凭据继续有效；仅应用业务账号的托管密码允许通过独立 `credentials.read` 权限和显式 POST 操作返回，要求审计、Cache-Control:no-store、关闭/切集群/离开页面清除及30秒自动清除，不进入前端持久化缓存或通用导出。主密钥 ACCOUNT_MASTER_KEY 是独立32字节密钥，不与数据库密文同存；未配置或无效时不启用密码托管/解密。Kafka原生SCRAM接口不返回原始密码，只有平台生成或用户主动录入的密码可以交付；不从敏感ConfigMap自动导入。

`account.manage` 和 `credentials.read` 独立检查且要求无Topic/Group前缀限制的集群管理员；ACL管理权限不推导出密码查看权限。只有显式实验写开关和精确目标目录允许真实账号开通。SCRAM写无create-only CAS：随机用户名、写前检查、持久化请求幂等避免平台重复创建；外部管理员并发仍需人工复核，不能盲目覆盖或重放。

1. **默认只读**：开发时 `ENABLE_KITE_WRITES=false`、`ENABLE_KAFKA_WRITES=false`，所有写操作必须通过平台后端权限检查、审批策略、确认窗口、审计。
2. **Kite Key 最小权限**：按特定 `x-cluster-name`、Namespace、configmaps/deployments/pods 配置 Kite RBAC；读操作使用只读 Key，写操作使用单独限权 Key。不要使用 admin Key 作为常驻运行身份。
3. **服务身份与个人身份分开**：LDAP 认证平台用户并授权；Kite 看到的可能是共享服务 Key，平台必须记录真实审批人/操作者，不能声称 Kite 自动继承 LDAP 用户身份。
4. **机密保护**：禁止密钥出现在代码、`.env` 提交、前端响应、错误日志、链路 Trace、导出文档。ConfigMap 也可能包含敏感内容，需要字段级脱敏。需要保存可回滚快照时使用服务端加密和严格读取授权。
5. **配置写入安全**：先读 ConfigMap 最新 `resourceVersion`，生成 diff，二次校验目标资源与权限，使用有并发控制的更新方式。碰到 409 不自动覆盖。先备份后改动，重启后验证 Pod/同步指标，失败进入人工复核或受控回退。
6. **修改 MM2 的作用域**：限制允许修改的 ConfigMap key、Deployment 和 Namespace；不提供任意资源删除 API，不允许前端提供自由 Kite URL（防 SSRF）。
7. **Kafka ACL**：查询与变更前检查 Broker 已启用合适的认证与授权；Kafka Principal 与 LDAP 平台用户的映射需要显式定义，不能直接用 LDAP 用户名推断 Broker 主体。
8. **证据链**：变更/回滚写入 before/after、资源 UID/resourceVersion、操作者、审批人、Kite 响应、Rollout 验证结果；失败时不可谎报成功。
9. **测试**：初期只能使用本地 mock 或专门测试集群做写入验证；任何生产操作必须由用户显式批准。
10. **运行可靠性**：发现接口故障不等于资源删除；采集降级时显示上次观测时间与 stale，不出现“正常”的误报。


## v2 Kafka 连接安全补充

- 登录平台的 LDAP 凭据和连接 Kafka 的 SASL 凭据是不同安全域；禁止复用默认管理员用户给所有集群。支持按操作类别配置专用低权限服务账号。
- 对既有 `SASL_PLAINTEXT` 明示未加密风险；优先推广 `SASL_SSL`，在 TLS 环境校验证书。PLAIN 和 SCRAM 机制不能互换，具体由集群 listener 决定。
- Kafka Broker 地址是管理端发起出站连接的目标：用户可编辑 bootstrap 不代表允许访问任意主机。上线必须限制内网 CIDR/域名白名单、出站网关、连接超时、DNS 重绑定风险与探测频率。
- TLS 私钥、SASL 密码、Kite API Key 必须采用服务端密钥管理；前端只显示 `configured: true` 和凭据版本/轮换日期，不回传原值。
- Kafka 连接测试不得泄漏失败的凭据字符串；日志、Trace、错误详情、审计、SQL 慢日志全部需要脱敏。
- 实际 Kafka Topic 管理和 ACL 操作必须分别受 Kafka 授权器和 LunaOrbit 平台 RBAC 双重限制。
