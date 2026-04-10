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

## Cobra 运维入口

可通过 `NewOpsCommand(...)` 把 essyncer 运维命令注入你现有框架的 root cobra command：

```go
rootCmd.AddCommand(essyncer.NewOpsCommand(essyncer.CobraOpsOptions{
    Syncer: syncer,
    Models: map[string]essyncer.Syncable{
        "Article": &Article{},
        "User":    &User{},
    },
}))
```

### 当前能做什么

当前这组 cobra 命令**可以手动触发数据同步相关运维动作**，并且已经支持单条文档级别的手工修复。

- **能做**
  - 手动对单条文档做“创建 / 更新 / 删除 / 查询”修复：
    - `doc create`
    - `doc update`
    - `doc delete`
    - `doc get`
  - 手动执行全量同步：`full-sync`
  - 手动重放 dead outbox：`outbox replay`
  - 手动排空 pending outbox：`relay drain`
  - 手动对账并按模型修复：`reconcile --repair`
  - 查看 / 清理 outbox：`outbox list`、`outbox cleanup`
- **仍然不建议做**
  - 不建议把 cobra 命令当成日常业务写流入口
  - 不建议通过 CLI 绕过 DB 真源，手工拼任意 ES 文档内容

所以，**当前 cobra 命令已经具备“单条 CRUD 运维修复入口”，但它仍然是运维修复工具，不是业务主链路**。

### 适用场景

- ES 索引整体漂移，手动做一次全量重建
- outbox 积压，需要手动 drain 一次
- dead 行需要人工 replay
- DB 和 ES 数量不一致，先对账，再按模型 repair

### 不适用场景

- 日常线上增删改查同步
- 业务侧对单条记录做即时人工补写
- 把 cobra 命令当成正常服务主链路

这些场景应该继续走：

- `EnableAutoSync(...)`
- `StartOutboxRelay(ctx)`
- 正常业务写库事务

### 已提供的运维子命令

- `doc create`
- `doc update`
- `doc delete`
- `doc get`
- `full-sync [model ...]`
- `outbox list`
- `outbox cleanup`
- `outbox replay`
- `relay drain`
- `reconcile [model ...]`

这些命令面向运维/修复场景，不应用来替代正常服务启动时的 `EnableAutoSync(...)` / `StartOutboxRelay(...)` 主链路。

### 子命令详解

#### 1. `doc create`

用途：

- 按模型主键从 DB 读取当前记录
- 生成一条 `index` 类型 outbox 记录
- 用于“ES 缺少这条文档”时的人工补写

常见用法：

```bash
# 只入 outbox，不立即 drain
app essyncer doc create --model Article --pk 1

# 入 outbox 后立刻 drain
app essyncer doc create --model Article --pk 1 --drain

# 对软删除记录走 Unscoped 加载
app essyncer doc create --model Article --pk 1 --unscoped
```

说明：

- `--model` 必填，必须能在 `CobraOpsOptions.Models` 中找到
- `--pk` 必填，当前只支持单主键模型的单条文档修复
- `--drain` 会在入队后立即执行一次 `relay drain`
- 这个命令不会修改 DB，只会把当前 DB 状态重新同步到 ES

#### 2. `doc update`

用途：

- 按模型主键从 DB 读取当前记录
- 生成一条 `update` 类型 outbox 记录
- 用于“ES 已有文档，但字段内容脏了”时做单条修复

常见用法：

```bash
app essyncer doc update --model Article --pk 1
app essyncer doc update --model Article --pk 1 --drain
```

说明：

- 它不是修改 DB，而是把当前 DB 记录作为 ES 修复来源
- 如果你不确定 ES 当前是否已有该文档，更稳的是直接用 `doc create`

#### 3. `doc delete`

用途：

- 生成一条 `delete` 类型 outbox 记录
- 用于“ES 残留了不该存在的文档”时做人工删除修复

常见用法：

```bash
# 直接按 document id 删除
app essyncer doc delete --model Article --id 1

# 先按主键查 DB，再推导 document id
app essyncer doc delete --model Article --pk 1

# 入队后立刻 drain
app essyncer doc delete --model Article --id 1 --drain
```

说明：

- `--id` 和 `--pk` 至少提供一个
- 如果记录已经从 DB 删除了，通常直接用 `--id` 更稳

#### 4. `doc get`

用途：

- 查看单条记录在 DB、ES、outbox 三边的状态
- 这是人工排障时最先该看的命令

常见用法：

```bash
# 通过主键定位
app essyncer doc get --model Article --pk 1

# 直接通过 document id 查看 ES / outbox 状态
app essyncer doc get --model Article --id 1
```

输出字段说明：

- `db_found`：DB 当前是否存在这条记录
- `es_found`：ES alias 下是否存在该文档
- `outbox_pending`：还有多少待投递行
- `outbox_dead`：是否已有 dead 行
- `last_dead_error`：最近一次 dead 错误

#### 5. `full-sync [model ...]`

用途：

- 对所有模型或指定模型执行全量同步
- 走当前的 alias-first rebuild 流程

常见用法：

```bash
# 同步所有已注册模型
app essyncer full-sync

# 只同步指定模型
app essyncer full-sync Article User

# 单模型从 checkpoint 继续
app essyncer full-sync Article --checkpoint 1000
```

说明：

- 不传模型名时，表示同步所有已注册模型
- 传模型名时，必须能在 `CobraOpsOptions.Models` 里找到
- `--checkpoint` 只适用于单模型，且该模型的扫描策略必须支持 `int64` checkpoint

#### 6. `outbox list`

用途：

- 查看当前 outbox 行
- 排查 pending / dead / processing 的积压情况

常见用法：

```bash
# 查看最近 100 条
app essyncer outbox list

# 只看 dead
app essyncer outbox list --status dead

# 只看 pending 和 processing
app essyncer outbox list --status pending --status processing --limit 200
```

输出字段说明：

- `id`：outbox 主键
- `status`：当前状态
- `action`：ES 动作（index / update / delete）
- `alias`：目标 alias
- `doc`：document id
- `attempts`：已重试次数
- `last_error`：最近一次失败原因

#### 7. `outbox cleanup`

用途：

- 删除符合条件的 outbox 行
- 主要用于清理 dead 或历史 sent/dead 记录

常见用法：

```bash
# 清理所有 dead
app essyncer outbox cleanup --status dead

# 只清理 24 小时前的 dead
app essyncer outbox cleanup --status dead --older-than 24h

# 最多清理 1000 条
app essyncer outbox cleanup --status dead --limit 1000
```

说明：

- 默认状态过滤是 `dead`
- 这个命令是删除，不是重放，执行前要确认你不再需要这些行

#### 8. `outbox replay`

用途：

- 把 dead 行重置回 `pending`
- 让 relay 重新尝试投递

常见用法：

```bash
# 重放所有 dead
app essyncer outbox replay --all-dead

# 指定 id 重放
app essyncer outbox replay --id 101 --id 102

# 最多重放 200 条
app essyncer outbox replay --all-dead --limit 200
```

说明：

- replay 会重置 `status / attempts / next_retry_at / last_error / leased_until / sent_at`
- replay 之后需要 relay 在跑，或者你再执行一次 `relay drain`

#### 9. `relay drain`

用途：

- 不走常驻 goroutine，而是命令式地处理一批或多批 outbox
- 适合运维临时排空积压

常见用法：

```bash
# 处理到队列为空
app essyncer relay drain

# 最多处理 10 个 batch
app essyncer relay drain --max-batches 10
```

输出字段说明：

- `batches`：处理了多少批
- `claimed`：claim 到多少行
- `processed`：成功 sent 多少行
- `retried`：回到 pending 多少行
- `dead`：进入 dead 多少行
- `remaining`：当前还剩多少 pending / processing

#### 10. `reconcile [model ...]`

用途：

- 按模型对比 DB 行数和 ES alias 文档数
- 可选在发现不一致时触发 repair

常见用法：

```bash
# 对所有模型做 count 对账
app essyncer reconcile

# 对指定模型做对账
app essyncer reconcile Article User

# 指定模型对账后自动 repair
app essyncer reconcile Article --repair
```

说明：

- 当前是“数量级对账”，不是逐主键逐字段对账
- `--repair` 会对不匹配模型触发 `full-sync`
- 不传模型名时，默认对所有已注册模型做 count 对账

### 推荐运维流程

如果线上出现“DB 已更新，ES 没跟上”：

1. 先执行 `outbox list --status pending --status dead`
2. 如果是 `pending` 积压，执行 `relay drain`
3. 如果是 `dead`，先 `outbox replay`，再 `relay drain`
4. 如果确认索引整体漂移，执行 `reconcile --repair` 或 `full-sync`

如果是“单条记录修复”：

1. 先执行 `doc get --model Article --pk 1`
2. 如果 ES 缺文档，执行 `doc create --model Article --pk 1 --drain`
3. 如果 ES 文档存在但字段脏了，执行 `doc update --model Article --pk 1 --drain`
4. 如果 ES 有脏残留文档，执行 `doc delete --model Article --id 1 --drain`

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
