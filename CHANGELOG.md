# Changelog

本文件记录 essyncer 的版本变更。

## v1.1.0

### ⚠ 破坏性变更

- `MetricsSnapshot` 删除了 7 个没有任何写入点、恒为 0 的字段：`enqueued_total`、`sync_events_buffered`、`sync_events_flushed`、`sync_events_skipped_tx`、`last_flush_at_unix_ms`、`last_flush_duration_ms`、`last_flush_items`。读取这些字段的代码会编译失败，请改读对应的真实指标（`outbox_events_persisted`、`relay_processed_total`、`bulk_num_flushed` 等）或直接删除相关看板项——它们此前显示的 0 并不代表业务量为 0。
- `EnsureIndex` 的第二个参数由未导出的 `*modelEntry` 改为 `Syncable`。旧签名因为参数类型不可导出，包外无法调用，这次改动对外部调用方没有可编译的旧用法。

### 资源与稳定性

- `New` 把 mapping 文件校验提前到建立 ES 连接与 BulkIndexer 之前。此前校验排在 indexer 创建之后，mapping 配置写错会在返回错误时漏掉 indexer 的 worker goroutine（实测泄漏 12 个）。
- `Shutdown` 在等待 relay 收尾超时时不再提前返回，BulkIndexer 一定会被关闭，两步的错误用 `errors.Join` 合并返回。
- `StartOutboxRelay` 起的后台 goroutine 现在拦截 panic：转成一条失败样本并写入错误日志（含堆栈），`Health(ctx)` 随后持续报错。此前 relay 循环里的 panic 会直接终止接入方的整个进程。
- `FullSync` 的并发 worker 同样拦截 panic，收敛成该模型的 `FullSyncResult.Err`，不影响其余模型的同步结果。

### 新增

- `RunOutboxRelay(ctx) error`：在调用方 goroutine 上阻塞运行 relay，ctx 取消后返回 nil，panic 原样冒泡。供宿主的 worker 管理器统一接管 panic 恢复与关停排序。

### 依赖

- `github.com/gtkit/json` v0.2.10 → `github.com/gtkit/json/v2` v2.1.1
- `go.uber.org/zap` v1.27.0 → v1.28.0，`gorm.io/gorm` v1.30.0 → v1.31.2，`github.com/gin-gonic/gin` v1.10.1 → v1.12.0（与 gin-api 脚手架当前使用的版本一致）
- `github.com/elastic/go-elasticsearch/v8` v8.17.0 → v8.19.7，`github.com/spf13/cobra` v1.8.1 → v1.10.2，`golang.org/x/sync` v0.11.0 → v0.23.0

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
