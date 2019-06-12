package squirrel

import (
	"fmt"
	"strings"
)

type OnConflictFormat interface {
	String(cols []string) string
}

var (
	OnConflict = onConflictFormat{}

	OnConflictWithKey = onConflictWithKeyFormat{}

	OnDuplicate = onDuplicateFormat{}
)

type onConflictFormat struct{}
type onConflictWithKeyFormat struct{}
type onDuplicateFormat struct{}

func (onConflictFormat) String(cols []string) string {
	return " ON CONFLICT DO UPDATE SET "
}

func (onConflictWithKeyFormat) String(cols []string) string {
	key := ""
	if len(cols) > 0 {
		key = fmt.Sprintf("(%s)", strings.Join(cols, ","))
	}
	return fmt.Sprintf(" ON CONFLICT%s DO UPDATE SET ", key)
}

func (onDuplicateFormat) String(cols []string) string {
	return " ON DUPLICATE KEY UPDATE "
}
