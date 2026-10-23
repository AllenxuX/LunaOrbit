# SCRAM 应用访问管理实施计划

**Goal:** 完成 Kafka 管理 v3 首批，真实SCRAM账号、应用关联、托管密码及完整ACL融合。

**Spec:** ../specs/2026-10-21-kafka-management-v3-design.md。用户已批准并要求继续实施；本会话内实施，不提交推送。

**Architecture:** 保留workspace事务状态与审批/执行底座，账号独立建模，秘密AES-GCM加密及独立读取授权；Kafka操作走原受限franz-go工厂。接口按原HTTP鉴权/同源规则装配。独立模块并行实施，根代理集成与验证。

**Tech Stack:** Go/Gin/franz-go/PostgreSQL，Vue3/script setup/TypeScript/Element Plus。

## 约束与审查重点

- 只修改已授权实验环境，默认真实禁写，不引入GitOps，不增加未记录Kite接口。
- 独立管理凭据/应用凭据安全域；明文不进入普通JSON/日志/审计/前端缓存。
- 同名SCRAM创建不能覆盖；外部并发边界如实记录。
- 录入已有密码只验证，不修改Kafka；导入资料不代表实际认证成功。
- 每次执行重新校验账号权限与写开关；独立审批默认保持。
- 全部ACL包括未归属身份，DENY/Host/通配符/非Topic资源不得绕过范围限制。

## 任务

- [x] 原生适配：新增kafkaadmin/scram.go及协议测试。ListSCRAM/CreateSCRAM/VerifySCRAM；先红后绿验证重复身份、坏密码、safeerrors及网络约束。
- [x] 账号领域：workspace/accounts*.go及tests，账号/关联/加密秘密/审批执行状态。验证加密落库、重启、AAD绑定、导入不写、越权读取、重复执行及部分失败。
- [x] 完整ACL：扩展workspace计划与原生规则能力矩阵；全部规则分页/过滤和精确删除，原生操作组合及范围拒绝测试。
- [x] 集成：registry独立account.manage/credentials.read授权；config主密钥校验、servergateway装配；兼容旧应用单Principal资料。
- [x] 前端：应用内账号创建/录入/关联，全部账号与全部规则，明确密码托管/认证状态、受控密码查看与清除；业务请求不持久化秘密。
- [x] 验证：Go全量test/vet/build，Vuetest/typecheck/lint/build，PostgreSQL加密/重启及迁移兼容，真实SCRAM/受限账号正负权限/browser、MM2复制。
- [x] 独立复核、更新路线图与真实开发日志，列明未验证项；停止首批，不自动开始下一批。
