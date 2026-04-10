# essyncer — GORM → Elasticsearch 8.x 数据同步模块

用于 GORM 数据库与 Elasticsearch 8.x 自动数据同步的 Go 模块。

## 技术栈

| 组件 | 选型 |
|------|------|
| ES 客户端 | `github.com/elastic/go-elasticsearch/v8`（官方 TypedAPI） |
| JSON | Go 标准库 `encoding/json` + `github.com/gtkit/json v0.2.10`（outbox payload path） |
| 日志 | `github.com/gtkit/logger`（兼容 zap.Field 签名） |
| 深拷贝 | `github.com/jinzhu/copier` |
| 查询缓存 | `golang.org/x/sync/singleflight` |
| 配置 | `gopkg.in/yaml.v3` |

## 核心特性

- **全量同步**：临时物理索引重建 + alias 原子切换 + 可配置扫描策略
- **增量同步**：GORM Callback 零侵入 + Transactional Outbox + Relay
- **软删除**：自动检测 `gorm.DeletedAt`；`update` 模式保留软删除文档，`delete` 模式删除 ES 文档
- **ES8 TypedAPI 查询**：泛型链式 API，singleflight 合并重复请求
- **Relay 重试/DLQ**：按 outbox lease / retry / dead 状态推进
- **Metrics 可观测**：全 atomic 无锁，对接 Gin 健康检查
- **TLS 支持**：ES8 安全连接

## 快速开始

```go
cfg, _ := essyncer.LoadConfig("./essyncer.yaml")
syncer, _ := essyncer.New(
    db,
    cfg,
    essyncer.WithLogger(myLogger),
    essyncer.WithFailureBufferSize(256),
    essyncer.WithFailureHook(func(event essyncer.FailureEvent) {
        myLogger.Error("essyncer failure",
            zap.String("source", event.Source),
            zap.String("index", event.Index),
            zap.String("action", event.Action),
            zap.String("document_id", event.DocumentID),
            zap.Int("status", event.Status),
            zap.String("error", event.Error),
        )
    }),
)

syncer.RegisterFromConfig(map[string]essyncer.Syncable{
    "Article": &Article{},
})

syncer.EnableAutoSync(db)
_ = syncer.StartOutboxRelay(ctx)
syncer.FullSync(ctx)

// 或者只对指定模型启用增量同步 / 全量同步
syncer.EnableAutoSync(db, &Article{})
syncer.FullSync(ctx, &Article{})

// 业务操作自动同步
db.Create(&article)                        // → same-tx outbox row
db.Model(&article).Update("title", "new") // → same-tx outbox row
db.Delete(&article)                        // → same-tx outbox row

// 显式事务请走 syncer.Transaction，业务数据和 outbox 会同事务提交
_ = syncer.Transaction(ctx, func(tx *gorm.DB) error {
    return tx.Model(&article).Updates(map[string]any{"title": "tx-safe"}).Error
})

// ES 查询（TypedAPI + singleflight）
result, _ := searcher.NewSearch[Article](syncer.TypedClient(), "articles").
    Must(searcher.Match("title", "Go")).
    Filter(searcher.Term("status", "published")).
    Sort("created_at", false).
    Highlight("title", "content").
    WithSingleflight().
    Page(1, 20).
    Do(ctx)

// Gin 健康检查
r.GET("/health/es", func(c *gin.Context) {
    c.JSON(200, syncer.GetMetrics())
})

r.GET("/health/es/failures", func(c *gin.Context) {
    c.JSON(200, syncer.RecentFailures(20))
})

syncer.Shutdown(ctx)
```

## 事务说明

- 默认 `Create/Update/Delete` 会在当前数据库事务内写入 outbox，由 relay 异步投递到 ES。
- `EnableAutoSync(db)` 表示对所有已注册模型启用增量同步；`EnableAutoSync(db, &Article{}, &User{})` 表示只对指定模型启用。
- `FullSync(ctx)` 表示同步所有已注册模型；`FullSync(ctx, &Article{}, &User{})` 表示只同步指定模型。
- `syncer.Transaction(...)` 和原生 `db.Transaction(...)` 都会让业务数据与 outbox 同事务提交，不再走 `tx_skip`。
- 应用必须自行通过 migration 创建 `outbox_events` 表；库不会在生产路径里自动建表。
- `StartOutboxRelay(ctx)` 需要显式启动；不启动 relay 时，outbox 只会累积，不会投递到 ES。

## Outbox 语义说明

- outbox 行是增量同步的 durable source of truth，ES 变成派生读模型。
- relay 每次 claim 一批 `pending/expired processing` 行，成功标记 `sent`，失败按 backoff 重试，超过阈值进入 `dead`。
- `Shutdown(ctx)` 会先停 relay 拉取，再等待当前 in-flight batch 结束，然后关闭共享 indexer。

## FullSync 语义说明

- `WithIndexName(...)` / `index_name` 现在表示稳定 alias 名，不再表示物理索引名。
- `FullSync`、`FullSyncTable`、`FullSyncWithCheckpoint` 会先写入新的物理索引，成功后再把 alias 原子切换到新索引。
- 默认扫描策略只支持“单个整数主键”作为游标；字符串主键、复合主键会显式报错。
- 可通过 `WithFullSyncScanStrategy(...)` 为单个模型注入自定义扫描策略。
- `FullSyncWithCheckpoint` 仅适用于支持 `int64` checkpoint 的扫描策略。
- `SoftDeleteModeUpdate` 会在全量同步时使用 `Unscoped()`，用于重建带 `deleted_at` 字段的软删除文档。
- `SoftDeleteModeDelete` 与无软删除模型使用默认作用域；全量同步不会把软删除行重新写回 ES。
- alias 解析、临时索引创建、alias 切换任一步失败都会立即终止当前模型的全量同步。

## 生产就绪边界

- 当前版本已修复事务提交时序、TLS 默认安全、失败观测、作用域选择、FullSync correctness blocker，并补上了 transactional outbox + relay 基础闭环。
- 这仍然不是“数据库与 ES 强一致”。当前语义是：DB 真源、ES 最终一致、失败可重试、dead 行可持久化排查。
- 要进一步逼近企业生产级，还需要接入方补齐对账修复、full sync 补尾、outbox 表迁移治理和运行告警。

## 可观测性说明

- `GetMetrics()` 现在除了累计计数，还会返回最近一次错误摘要，以及 outbox/relay 相关计数。
- `RecentFailures(limit)` 返回最近 N 条失败样本，适合挂到内部健康检查或 debug 接口。
- `WithFailureHook(...)` 可把 relay/full sync 失败转发到告警系统、Sentry 或自定义 dead-letter sink。
- 失败样本默认只保存在进程内存里；持久化恢复面是 `outbox_events` 表里的 `pending/dead` 行。

## 架构

```
┌──────────────────────────────────────────┐
│               业务代码 (Gin)               │
│        db.Create / Update / Delete        │
└────────────────────┬─────────────────────┘
                     │ GORM Callback
                     ▼
┌──────────────────────────────────────────┐
│             essyncer.Syncer               │
│                                           │
│  callback ──► outbox_events              │
│                (同事务持久化)              │
│                                           │
│  StartOutboxRelay() ──► ES API           │
│                       claim / retry / dead│
│                                           │
│  FullSync() ──► 新物理索引 rebuild        │
│                 扫描策略 + 并发 workers    │
│                 成功后 alias 原子切换      │
└──────────────────────────────────────────┘

┌──────────────────────────────────────────┐
│      searcher (ES8 TypedAPI)              │
│                                           │
│  NewSearch[T]().Must().Filter().Do()     │
│       ├─ singleflight 合并重复查询        │
│       └─ 默认过滤软删除文档               │
└──────────────────────────────────────────┘
```

## 并发安全

| 组件 | 机制 |
|------|------|
| 模型注册表 | sync.Map |
| Metrics | atomic.Int64 |
| 停止标志 | atomic.Bool |
| Relay 生命周期 | `sync.Mutex` + `sync.WaitGroup` |
| 深拷贝 | copier（每次调用独立） |
| singleflight | x/sync/singleflight |

## 文件结构

```
essyncer/
├── config.go        # 配置 + YAML
├── options.go       # Functional Options
├── log.go           # Logger 接口
├── model.go         # 模型注册 + copier 深拷贝
├── metrics.go       # 运行时指标
├── syncer.go        # 核心（ES8 客户端 + BulkIndexer）
├── callback.go      # GORM Callback 增量同步
├── fullsync.go      # 全量同步
├── searcher/
│   ├── search.go    # 泛型链式查询 + singleflight
│   ├── query.go     # Match/Term/Range/Bool
│   ├── agg.go       # 聚合
│   └── result.go    # 结果结构
├── essyncer.yaml.example
├── mappings/articles.json
├── go.mod
└── README.md
```
