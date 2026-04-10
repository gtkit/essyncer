package essyncer

import (
	"context"
	"fmt"
	"reflect"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// FullSyncModelInfo exposes stable model metadata to custom full-sync scan strategies.
type FullSyncModelInfo struct {
	TableName     string
	IndexAlias    string
	HasSoftDelete bool
}

// FullSyncScanStrategy prepares per-run scan cursors for full sync.
type FullSyncScanStrategy interface {
	Prepare(info FullSyncModelInfo, startID int64) (FullSyncScanCursor, error)
}

// FullSyncScanCursor controls query scope and row-to-row progression during a full sync run.
type FullSyncScanCursor interface {
	Scope(query *gorm.DB, batchSize int) *gorm.DB
	Advance(ctx context.Context, row any) error
	Checkpoint() any
}

type unsupportedFullSyncScanStrategy struct {
	err error
}

func (s unsupportedFullSyncScanStrategy) Prepare(info FullSyncModelInfo, _ int64) (FullSyncScanCursor, error) {
	return nil, fmt.Errorf("essyncer: full sync table %s %w", info.TableName, s.err)
}

type integerPrimaryKeyScanStrategy struct {
	key *fullSyncKey
}

func (s integerPrimaryKeyScanStrategy) Prepare(info FullSyncModelInfo, startID int64) (FullSyncScanCursor, error) {
	if s.key == nil {
		return nil, fmt.Errorf("essyncer: full sync table %s requires exactly one integer primary key", info.TableName)
	}

	cursor := &integerPrimaryKeyScanCursor{key: s.key}
	if s.key.unsigned {
		if startID < 0 {
			return nil, fmt.Errorf(
				"essyncer: full sync table %s does not support negative checkpoints for unsigned primary key %s",
				info.TableName,
				s.key.columnName,
			)
		}
		cursor.checkpoint = uint64(startID)
		return cursor, nil
	}

	cursor.checkpoint = startID
	return cursor, nil
}

type integerPrimaryKeyScanCursor struct {
	key        *fullSyncKey
	checkpoint any
}

func (c *integerPrimaryKeyScanCursor) Scope(query *gorm.DB, batchSize int) *gorm.DB {
	return query.
		Where(clause.Gt{Column: clause.Column{Name: c.key.columnName}, Value: c.checkpoint}).
		Order(clause.OrderBy{Columns: []clause.OrderByColumn{{
			Column: clause.Column{Name: c.key.columnName},
		}}}).
		Limit(batchSize)
}

func (c *integerPrimaryKeyScanCursor) Advance(ctx context.Context, row any) error {
	value, err := c.key.valueOf(ctx, reflect.ValueOf(row))
	if err != nil {
		return err
	}
	c.checkpoint = value
	return nil
}

func (c *integerPrimaryKeyScanCursor) Checkpoint() any {
	return c.checkpoint
}
