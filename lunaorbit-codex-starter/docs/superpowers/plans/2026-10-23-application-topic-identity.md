# 应用账号Topic绑定实施计划

Goal：记录应用来源、Kafka账号和Topic关系，并展示于全链路。
Spec：docs/superpowers/specs/2026-10-23-application-topic-identity-design.md
Architecture：复用information JSONB、现有lineage与Vue组件边界。

- [x] 信息字段/校验失败测试，再实现模型与HTTP白名单。
- [x] 不同账号关系分离测试，再扩展lineage属性。
- [x] Vue表单保存/编辑测试，再修改表单、列表与详情。
- [x] 全量检查、专用实验console部署和真实浏览器验证/合成记录清理。
- [x] 更新文档及日志，停止本批。
