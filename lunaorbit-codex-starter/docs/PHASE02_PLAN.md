# Phase 02 实施方案与接口约定

仅实现集群登记、凭据引用、受限只读连接探测及用户独立切换。默认服务仍脱机；REGISTRY_MODE=mock 或 postgres-mock 显式启动开发演示。生产身份与 SecretStore 仅定义接口，不伪造 LDAP/KMS。

## 分工与模块
1. registry：注册服务、HTTP API、开发会话、内存 SecretStore、异步探测调度与审计；当前主会话实施。
2. kafkaadmin：franz-go 工厂、SASL/TLS、IP 固定的允许列表 Dialer、metadata/advertised broker 检查和错误分类；独立实施。
3. registry 存储：PostgreSQL 迁移、内存和 PostgreSQL Store、CAS 与原子审计；独立实施。
4. Vue 集群管理：列表/分页/筛选、注册编辑表单、只写凭据、模拟探测、停用和软删除、按当前浏览器会话切换；独立实施。

## 统一 API（LunaOrbit 自身接口，非 Kite）
所有业务响应仅为安全 DTO。失败：{error:{code,message,request_id}}。会话由 HttpOnly SameSite=Strict Cookie 承载，不接受请求头自报用户或权限。开发会话仅在显式 Mock 模式且回环监听开启；所有资源按创建者 ID 隔离。写请求校验 Origin，禁止带通配 CORS。没有生产可用的身份认证或密钥持久化能力。

postgres-mock 必须提供 32 字节十六进制 DEV_SESSION_KEY；签名 Cookie 在密钥保持不变且未过期时可恢复原所有者。默认内存模式签名密钥每次启动随机。允许精确配置本地 DEV_BROWSER_ORIGIN 兼容 Vite Host 转发，其余跨源请求拒绝。生产身份提供方仍未实现。

- POST /api/v1/dev/session：建立独立开发会话；返回 {user_id,mode:'mock',persistent:boolean}。GET /api/v1/session 查询，未登录 401。
- GET /api/v1/kafka/clusters?page=1&page_size=20&query=：返回 {items:Cluster[],total,page,page_size}。
- POST /api/v1/kafka/clusters：CreateInput；201 Cluster，可保存为 unverified。
- PATCH /api/v1/kafka/clusters/:id：{revision,display_name,room,environment,description,bootstrap_servers,enabled,version_reported}；200 Cluster。revision 必填，过期 409。
- PUT /api/v1/kafka/clusters/:id/credentials：CredentialInput 加 revision；200 Cluster。省略的秘密字段保留旧值，提供空值清除。凭据改变后探测状态重置。
- DELETE /api/v1/kafka/clusters/:id：请求 JSON {revision}；204，逻辑删除。
- POST /api/v1/kafka/clusters/:id/probe：可选 {mock_scenario}（仅 Mock）；202 {job_id,status:'pending'}。
- GET /api/v1/kafka/clusters/:id/probe：{job_id,status:'pending'|'running'|'done',error_code?}；页面随后刷新列表获取结果。

Cluster 字段：id,revision,display_name,room,environment,description,bootstrap_servers:string[],enabled,security_protocol,sasl_mechanism,username_masked,credentials_configured:boolean,tls_ca_configured:boolean,tls_client_configured:boolean,tls_server_name,credential_revision,status,last_probe_at?,last_probe_error_code?,discovered_cluster_id?,version_reported,version_source,capabilities:[{feature,supported,reason}],brokers:[{id,host,port}],created_at,updated_at。永不包含 password、username 原文、PEM、SecretRef 或 JAAS。

CreateInput：display_name,room,environment,description,bootstrap_servers:string[],enabled:boolean,version_reported:string + CredentialInput。
CredentialInput：security_protocol:'PLAINTEXT'|'SSL'|'SASL_PLAINTEXT'|'SASL_SSL',sasl_mechanism:''|'PLAIN'|'SCRAM-SHA-256'|'SCRAM-SHA-512',username?:string,password?:string,tls_ca_pem?:string,tls_cert_pem?:string,tls_key_pem?:string,tls_server_name:string。

Mock 场景：success、authentication_failed、tls_failed、broker_unreachable、advertised_unreachable、authorization_denied、unsupported_api、timeout、kafka37、kafka392。所有结果必须显示为模拟，不根据用户填写版本推断能力或精确版本。界面 SASL_PLAINTEXT 显示风险，优先 SASL_SSL；编辑秘密输入始终为空，取消/提交后清除。

实际交付状态：以上各模块及 Mock 流程已实现。能力未探测和不支持明确分开；成功模拟状态为 mock_connected。凭据可显式清空，未配置时禁止启动探测。全局并行探测上限 2、每登记 2 秒间隔、每用户 10 秒最多 5 次、任务状态上限 256，单任务 5 秒预算，最终状态写入另有 2 秒预算。PostgreSQL 仅允许明确专用开发数据库，禁止用普通业务库名启动演示。

执行结果详见 IMPLEMENTATION_LOG；真实 Kafka 跨版本、完整 SASL/mTLS 成功握手、生产 KMS/LDAP 及出站基础设施未验证。本阶段完成即停，不进入 Phase 03。

## 验证
先写行为测试再实现；全量 Go test/vet/build，前端 test/typecheck/lint/build，HTTP 会话/资源隔离/凭据脱敏/并发探测 smoke。可使用获批 SSH 实验机验证独立数据库迁移，禁止触碰已有 Kubernetes/Kafka；Kafka 3.7/3.9.2 实际环境不可用时用标明合成的 Mock 并记录缺口。完成后更新 ROADMAP 和 IMPLEMENTATION_LOG，只停在 Phase 02。
