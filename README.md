# essyncer — GORM → Elasticsearch 8.x 数据同步模块

生产级 Go 模块，实现 GORM 数据库与 Elasticsearch 8.x 的自动数据同步。

## 技术栈

| 组件 | 选型 |
|------|------|
| ES 客户端 | `github.com/elastic/go-elasticsearch/v8`（官方 TypedAPI） |
| JSON | Go 标准库 `encoding/json` |
| 日志 | `github.com/gtkit/logger`（兼容 zap.Field 签名） |
| 深拷贝 | `github.com/jinzhu/copier` |
| 查询缓存 | `golang.org/x/sync/singleflight` |
| 配置 | `gopkg.in/yaml.v3` |

## 核心特性

- **全量同步**：游标分页 + Worker Pool 并发 + 断点续传
- **增量同步**：GORM Callback 零侵入，Partial Update
- **软删除**：自动检测 `gorm.DeletedAt`，可选更新/物理删除
- **ES8 TypedAPI 查询**：泛型链式 API，singleflight 合并重复请求
- **BulkIndexer 引擎**：官方自带 worker pool + flush + 重试（无需手写）
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
syncer.FullSync(ctx)

// 业务操作自动同步
db.Create(&article)                        // → index
db.Model(&article).Update("title", "new") // → partial update
db.Delete(&article)                        // → soft delete sync

// 显式事务请走 syncer.Transaction，提交后才会 flush 到 ES
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

- 默认 `Create/Update/Delete` 会在 GORM 语句事务提交后再写入 ES。
- 显式多语句事务请使用 `syncer.Transaction(...)` 包裹；这样只有数据库成功提交后才会 flush 同步事件。
- 若业务直接使用原生 `db.Transaction(...)`，essyncer 会跳过该事务内的自动同步以避免未提交数据提前写入 ES。

## 可观测性说明

- `GetMetrics()` 现在除了累计计数，还会返回最近一次 flush 摘要、最近一次错误摘要，以及事务缓冲/跳过计数。
- `RecentFailures(limit)` 返回最近 N 条失败样本，适合挂到内部健康检查或 debug 接口。
- `WithFailureHook(...)` 可把失败事件转发到告警系统、Sentry 或自定义 dead-letter sink。
- 失败样本默认只保存在进程内存里，重启后会清空；这是一个诊断面，不是持久化队列。

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
│  enqueue() ──► esutil.BulkIndexer        │
│                (官方引擎，自带:)            │
│                 ├─ Worker Pool            │
│                 ├─ Flush (bytes/time)     │
│                 ├─ Retry (status codes)   │
│                 └─ OnFailure callback     │
│                                           │
│  FullSync() ──► 独立 BulkIndexer          │
│                 游标分页 + 并发 workers    │
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
| BulkIndexer.Add() | 官方线程安全（内部有锁） |
| 模型注册表 | sync.Map |
| Metrics | atomic.Int64 |
| 停止标志 | atomic.Bool |
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
