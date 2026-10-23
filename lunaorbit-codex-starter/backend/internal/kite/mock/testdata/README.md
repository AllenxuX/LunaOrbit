# 合成 Fixture

catalog.json 完全由本项目生成，仅包含合成元数据，无真实地址、凭据或 ConfigMap 原文。
Mock HTTP 返回 {synthetic_fixture, kind, single, items}；这是本地演示格式，**不是 Kite 响应契约或现场样本**。
Decoder 只供 Mock 使用，不应注册给真实 Kite 实例；现场需脱敏样本后独立实现版本对应 Decoder。
