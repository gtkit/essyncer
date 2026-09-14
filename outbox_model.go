package essyncer

import "time"

const (
	OutboxStatusPending    = "pending"
	OutboxStatusProcessing = "processing"
	OutboxStatusSent       = "sent"
	OutboxStatusDead       = "dead"
)

// OutboxEvent is the durable row persisted in application-owned outbox_events table.
type OutboxEvent struct {
	ID          int64      `gorm:"primaryKey;autoIncrement"`
	TableName   string     `gorm:"column:table_name;type:varchar(128);not null;index"`
	IndexAlias  string     `gorm:"column:index_alias;type:varchar(128);not null"`
	DocumentID  string     `gorm:"column:document_id;type:varchar(191);not null"`
	Action      string     `gorm:"column:action;type:varchar(32);not null"`
	Payload     []byte     `gorm:"column:payload;type:json;not null"`
	Status      string     `gorm:"column:status;type:varchar(32);not null;index"`
	Attempts    int        `gorm:"column:attempts;not null;default:0"`
	NextRetryAt *time.Time `gorm:"column:next_retry_at;index"`
	LastError   string     `gorm:"column:last_error;type:text"`
	LeasedUntil *time.Time `gorm:"column:leased_until;index"`
	// LeaseToken 是每次 claim 生成的租约凭证。它而不是 leased_until 用于后续更新的
	// fencing 判定：DATETIME 列按列精度四舍五入存储，Go 侧的微秒时间戳与之相等比较必然失败。
	LeaseToken string     `gorm:"column:lease_token;type:varchar(32)"`
	CreatedAt  time.Time  `gorm:"column:created_at;autoCreateTime"`
	SentAt     *time.Time `gorm:"column:sent_at"`
}
