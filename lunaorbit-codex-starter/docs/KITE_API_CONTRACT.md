# Kite API 契约（基于 2026-10-19 官网文档）

## 已确认且允许在代码中实现的接口

认证：API Key 全值直接传 HTTP `Authorization`，禁止 `Bearer ` 前缀。K8s 资源请求带 `x-cluster-name: <kite-cluster-name>`。

```http
GET   /api/v1/namespaces/_all
GET   /api/v1/configmaps/{namespace}
GET   /api/v1/configmaps/{namespace}/{name}
GET   /api/v1/deployments/{namespace}
GET   /api/v1/deployments/{namespace}/{name}
GET   /api/v1/pods/{namespace}
GET   /api/v1/pods/{namespace}/{name}
PUT   /api/v1/configmaps/{namespace}/{name}
PATCH /api/v1/configmaps/{namespace}/{name}?patchType=merge
PATCH /api/v1/deployments/{namespace}/{name}?patchType=merge
```

- 内置 Namespace 级资源通用 CRUD 已在 Kite 文档列出；上面只列出当前计划需要的端点。
- `PUT ConfigMap` 是完整更新，需要当前对象 `metadata.resourceVersion`；发生并发更改（409）必须停止并重新比较。
- `PATCH ConfigMap` 接受 `?patchType=merge` 或 `?patchType=json`；生产变更需要防并发冲突的设计，不允许盲覆盖。
- Deployment 重启：更新 `spec.template.metadata.annotations["kite.kubernetes.io/restartedAt"]`，触发新的 rollout；时间戳须为本次操作唯一的 UTC RFC3339 值。
- `/api/v1/admin/clusters/`（GET）需要 `admin` 权限，**不在普通采集服务账号中使用**。平台可以手工登记 Kite 集群显示名与 API `x-cluster-name` 对照。
- Kite API Key 拥有与用户相同的 Kite RBAC 权限，创建密钥并不自动授予资源权限。

## 已知请求样例（只读）

```bash
curl -sS \
  -H "Authorization: ${KITE_API_KEY}" \
  -H "x-cluster-name: ${KITE_CLUSTER_NAME}" \
  "${KITE_BASE_URL}/api/v1/configmaps/${MM2_NAMESPACE}"
```

## 不能猜测的内容

- Pod 日志、滚动发布过程/历史的专门 API 路由、Watch、批量跨 Namespace List；当前所审官方 API 页面没有给出确切路径。可以先只通过已证实的 Pod GET 获取状态，日志功能标为未实现直到验真。
- Kite 具体 API 响应外层封装、错误码细节、分页参数、版本差异。先拿非敏感的真实只读响应样本，构造 Fixture，再决定 parser。
- 不假定 `/api/v1/configmaps/_all` 可跨所有 Namespace 查询；使用 Namespace 列表 + 逐 Namespace 查询。

## 接口文档
- https://kitehq.dev/zh/api/authentication
- https://kitehq.dev/zh/api/resources
- https://kitehq.dev/zh/api/cluster-management
- https://kitehq.dev/zh/api/rbac-management

## v0.16.0 专用实验实测（2026-10-20）

独立Minikube中已实际调用上述七条GET，原样Authorization与x-cluster-name均生效；匿名/错误Bearer前缀401，reader访问admin和其他Namespace403。API Key创建与赋权只在实验搭建脚本使用，路由按对应tag的routes.go与handler核实，不扩大普通采集能力。

该版本本次小样本list外层为items与metadata，metadata含resourceVersion；单ConfigMap为metadata/data，Deployment与Pod为metadata/spec/status（Pod列表项可附带metrics）。未观察到完整分页/版本矩阵，因此仅test私有解码器消费此样本；生产ResourceDecoder保持显式未启用。受限SA使用该tag源码支持的DISABLE_CACHE=true，否则默认全局缓存初始化失败；不能把此时404误判成路由不存在。详见deploy/lab/MINIKUBE.md及开发日志。
