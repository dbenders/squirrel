package squirrel

import "strings"

type OnConflictFormat interface {
	Replace(sql string) (string, error)
}

var (
	OnConflict = onConflictFormat{}

	OnDuplicate = onDuplicateFormat{}
)

type onConflictFormat struct{}
type onDuplicateFormat struct{}

func (onConflictFormat) Replace(sql string) (string, error) {
	return sql, nil
}

func (onDuplicateFormat) Replace(sql string) (string, error) {
	return strings.Replace(sql, "ON CONFLICT DO UPDATE SET", "ON DUPLICATE KEY UPDATE", -1), nil
}
