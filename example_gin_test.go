package essyncer_test

import (
	"context"
	"errors"
	"net/http"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gtkit/essyncer"
	"github.com/gtkit/essyncer/searcher"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// --- 模型定义 ---

type Article struct {
	ID        int64          `gorm:"primaryKey" json:"id"`
	Title     string         `gorm:"size:255" json:"title"`
	Content   string         `gorm:"type:text" json:"content"`
	Category  string         `gorm:"size:50" json:"category"`
	Status    string         `gorm:"size:20;default:draft" json:"status"`
	CreatedAt time.Time      `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt time.Time      `gorm:"autoUpdateTime" json:"updated_at"`
	DeletedAt gorm.DeletedAt `gorm:"index" json:"deleted_at,omitzero"`
}

func (Article) TableName() string { return "articles" }
func (a *Article) GetID() string  { return strconv.FormatInt(a.ID, 10) }

// Example_ginProduction 完整的 Gin 生产集成示例。
func Example_ginProduction() {
	var db *gorm.DB
	logger, _ := zap.NewProduction()

	// 1. 加载配置
	cfg, _ := essyncer.LoadConfig("./essyncer.yaml")

	// 2. 创建 Syncer（注入 gtkit/logger）
	// 假设 gtkit/logger 实现了 essyncer.Logger 接口（Info/Warn/Error/Debug + zap.Field）
	// 这里用 zap 演示，实际替换为：essyncer.WithLogger(gtkitLogger)
	zapAdapter := &zapLoggerAdapter{l: logger}
	sync, _ := essyncer.New(db, cfg, essyncer.WithLogger(zapAdapter))

	// 3. 注册模型
	if err := sync.RegisterFromConfig(map[string]essyncer.Syncable{
		"Article": &Article{},
	}); err != nil {
		panic(err)
	}

	// 4. 启用增量同步
	if err := sync.EnableAutoSync(db); err != nil {
		panic(err)
	}
	if err := sync.StartOutboxRelay(context.Background()); err != nil {
		panic(err)
	}

	// 5. 可选：后台全量同步
	go sync.FullSync(context.Background())

	// 6. Gin 路由
	r := gin.New()
	r.Use(gin.Recovery())

	// 健康检查
	r.GET("/health/es", func(c *gin.Context) {
		ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
		defer cancel()
		if err := sync.Health(ctx); err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"status": "unhealthy", "error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "healthy"})
	})

	// Metrics（对接 Prometheus / 监控）
	r.GET("/metrics/es", func(c *gin.Context) {
		c.JSON(http.StatusOK, sync.GetMetrics())
	})

	// CRUD → 自动同步
	r.POST("/articles", func(c *gin.Context) {
		var a Article
		if err := c.ShouldBindJSON(&a); err != nil {
			c.JSON(400, gin.H{"error": err.Error()})
			return
		}
		db.Create(&a) // → same transaction outbox row
		c.JSON(201, a)
	})

	r.PUT("/articles/:id", func(c *gin.Context) {
		id, _ := strconv.ParseInt(c.Param("id"), 10, 64)
		var updates map[string]any
		if err := c.ShouldBindJSON(&updates); err != nil {
			c.JSON(400, gin.H{"error": err.Error()})
			return
		}
		db.Model(&Article{ID: id}).Updates(updates) // → same transaction outbox row
		c.JSON(200, gin.H{"ok": true})
	})

	r.DELETE("/articles/:id", func(c *gin.Context) {
		id, _ := strconv.ParseInt(c.Param("id"), 10, 64)
		db.Delete(&Article{ID: id}) // → same transaction outbox row
		c.JSON(200, gin.H{"ok": true})
	})

	// ES 搜索（TypedAPI + singleflight）
	r.GET("/articles/search", func(c *gin.Context) {
		keyword := c.Query("q")
		category := c.Query("category")
		page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
		pageSize, _ := strconv.Atoi(c.DefaultQuery("page_size", "20"))

		s := searcher.NewSearch[Article](sync.TypedClient(), "articles")

		if keyword != "" {
			s = s.Must(searcher.MultiMatch(keyword, "title", "content"))
		}
		if category != "" {
			s = s.Filter(searcher.Term("category", category))
		}

		result, err := s.
			Filter(searcher.Term("status", "published")).
			Sort("created_at", false).
			Highlight("title", "content").
			WithSingleflight(). // 合并相同查询的并发请求
			Page(page, pageSize).
			Do(c.Request.Context())

		if err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}

		c.JSON(200, gin.H{
			"total":      result.Total,
			"items":      result.Items,
			"highlights": result.Highlights,
		})
	})

	// 聚合统计
	r.GET("/articles/stats", func(c *gin.Context) {
		aggResult, err := searcher.NewSearch[Article](sync.TypedClient(), "articles").
			Filter(searcher.Term("status", "published")).
			Agg("by_category", searcher.TermsAgg("category", 20)).
			Agg("monthly", searcher.DateHistogramAgg("created_at", "month")).
			DoAgg(c.Request.Context())

		if err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}
		c.JSON(200, aggResult.Aggregations)
	})

	// 7. 优雅关闭
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	srv := &http.Server{Addr: ":8080", Handler: r}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			zapAdapter.Error("listen and serve", zap.Error(err))
		}
	}()

	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		zapAdapter.Error("shutdown http server", zap.Error(err))
	}
	if err := sync.Shutdown(shutdownCtx); err != nil {
		zapAdapter.Error("shutdown essyncer", zap.Error(err))
	}
}

// zapLoggerAdapter 用 zap.Logger 实现 essyncer.Logger 接口。
// 实际项目中直接注入 gtkit/logger 即可。
type zapLoggerAdapter struct{ l *zap.Logger }

func (z *zapLoggerAdapter) Info(msg string, fields ...zap.Field)  { z.l.Info(msg, fields...) }
func (z *zapLoggerAdapter) Warn(msg string, fields ...zap.Field)  { z.l.Warn(msg, fields...) }
func (z *zapLoggerAdapter) Error(msg string, fields ...zap.Field) { z.l.Error(msg, fields...) }
func (z *zapLoggerAdapter) Debug(msg string, fields ...zap.Field) { z.l.Debug(msg, fields...) }
