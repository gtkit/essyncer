# Changelog

本文件记录 essyncer 的版本变更。

## v1.0.0

首个正式版本。GORM → Elasticsearch 8.x 数据同步模块，提供增量同步（GORM Callback + Transactional Outbox + Relay）、全量同步（临时物理索引重建 + alias 原子切换）、ES8 TypedAPI 查询封装与 cobra 运维入口。

### Outbox 租约

- relay 认领 outbox 行时写入一次性 `lease_token`，`sent` / 重试 / `dead` 的写回按 token 做 fencing 判定，不再依赖 `leased_until` 的相等比较。MySQL 按列精度存储 DATETIME，Go 侧的微秒时间戳与 `DATETIME` / `DATETIME(3)` 列相等比较必然失败，会让每次状态写回都报 lease mismatch、行停在 `processing`、relay 陷入重复投递。
- 接入方的 `outbox_events` 表需要包含 `lease_token VARCHAR(32)` 列。新建表的 DDL 与已有表的 `ALTER TABLE` 升级语句见 README「outbox_events 建表」章节。

### 增量同步

- WHERE 条件不含主键的批量 UPDATE / DELETE 不再用零值主键生成 ES 文档。这类写入会被跳过：数据库写照常成功，同时计入 `sync_events_skipped_unidentified` 指标、写一条 `unidentified_rows` 失败样本并打 warn 日志。
- `Updates(map[string]any{...})` 的 key 支持 Go 字段名，写入 ES 前统一归一化成数据库列名。

### Relay 重试

- ES 整体不可达（拿不到 HTTP 响应）不再消耗 `max_attempts` 预算，行留在 `pending` 等待 ES 恢复；HTTP 层的 `429/502/503/504` 仍按 `max_attempts` 计数，耗尽后进入 `dead`。
