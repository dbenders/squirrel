package squirrel

import "errors"

type Dialect interface {
	Now() expr
	UnixNow() expr
	PlaceholderFormat() PlaceholderFormat
	OnConflictFormat() OnConflictFormat
}

type (
	mySQLDialect      struct{}
	postgreSQLDialect struct{}
	sqlite3Dialect    struct{}
)

var (
	MySQL      Dialect = mySQLDialect{}
	PostgreSQL Dialect = postgreSQLDialect{}
	SQLite3    Dialect = sqlite3Dialect{}
)

var (
	ErrInvalidDialect = errors.New("invalid dialect")
)

// Now is the current datetime
// Ex:
//     .Where(Lt{"ts": Now()})
func (d mySQLDialect) Now() expr      { return Expr("now()") }
func (d postgreSQLDialect) Now() expr { return Expr("now()") }
func (d sqlite3Dialect) Now() expr    { return Expr("datetime('now')") }

// UnixNow is the current datetime in unix epoch format
// Ex:
//     .Where(Lt{"ts": UnixNow()})
func (d mySQLDialect) UnixNow() expr      { return Expr("unix_timestamp()") }
func (d postgreSQLDialect) UnixNow() expr { return Expr("extract(epoch from now())") }
func (d sqlite3Dialect) UnixNow() expr    { return Expr("strftime('%s', 'now')") }

// PlacementHolderFormat is the standard placement format for each dialect
func (d mySQLDialect) PlaceholderFormat() PlaceholderFormat      { return Question }
func (d postgreSQLDialect) PlaceholderFormat() PlaceholderFormat { return Dollar }
func (d sqlite3Dialect) PlaceholderFormat() PlaceholderFormat    { return Question }

// OnConflictFormat is the standard on conflict format for each dialect
func (d mySQLDialect) OnConflictFormat() OnConflictFormat      { return OnDuplicate }
func (d postgreSQLDialect) OnConflictFormat() OnConflictFormat { return OnConflict }
func (d sqlite3Dialect) OnConflictFormat() OnConflictFormat    { return OnConflict }
