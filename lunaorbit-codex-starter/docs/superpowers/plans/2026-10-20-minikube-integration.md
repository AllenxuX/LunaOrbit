# Minikube Kite/Kafka/MM2 实验验证计划

**Goal:** 在用户授权实验机验证真实 Kite 资源读取和跨 Kafka 的 MM2 合成消息同步。

**Architecture:** 新建 `lunaorbit-lab` profile，与现有停止的集群隔离。专用 namespace 中部署两套单节点 Kafka、一个 MM2 和 Kite；应用资源采集仍只通过 Kite HTTP API。kubectl 仅用于搭建实验基础设施及交叉核验，不成为 LunaOrbit 运行时接入方式。

**Tech Stack:** Minikube Docker driver / Kubernetes v1.35.1、缓存 Apache Kafka 3.7.2/3.9.2、缓存 Kite v0.16.0。

- [x] 检查资源、缓存镜像及现有 profile，不启动或修改旧业务。
- [x] 启动专用 profile，确认节点和 DNS Ready；磁盘不足或 OOM 时停止扩容。
- [x] 部署资源受限的两套 Kafka 与 MM2；只创建合成 Topic/消息，验证目标实际收到数据。
- [x] 部署官方 Kite 镜像，配置实验身份与资源权限，保持匿名访问关闭；秘密随机生成，只留实验机私有文件或 Secret。
- [x] 按官方文档/对应版本源码核实登录及 API Key 操作，调用既有七条 GET 契约，记录脱敏响应结构。未经证实的响应不接入生产解码器。
- [x] 执行项目实际 Reader/transport 检查及同步断言；明确区分基础设施健康、配置声明、实际消息复制。
- [x] 更新开发日志、路线图与复现说明，保留实验环境供后续联调，不触碰生产或开启平台真实写开关。

用户已授权安装启动本次实验组件，不需要逐步确认。若真实资源或镜像无法启动，记录实际失败，不以Mock替代成功结论。


---
> 2026-10-26 追加说明（LUNA-60）：产品定位调整为自动发现优先的内网 Kafka/MM2 拓扑与信息平台：从 Kite 与 MM2 配置自动读出集群和同步关系，人工只负责补充和登记业务信息。本文为历史记录，原文不改；手工集群登记相关内容已被自动发现取代，现行约束见 AGENTS.md 与 SECURITY.md。
