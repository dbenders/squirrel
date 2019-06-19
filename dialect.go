package squirrel

import "errors"

type Dialect interface {
	Now() Now
	UnixNow() UnixNow
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

type funcNoParams struct {
	sql string
}

func (f funcNoParams) ToSql() (string, []interface{}, error) {
	return f.sql, nil, nil
}

// Now is the current datetime
// Ex:
//     .Where(Lt{"ts": Now()})
type Now funcNoParams

// UnixNow is the current datetime in unix epoch format
// Ex:
//     .Where(Lt{"ts": UnixNow()})
type UnixNow funcNoParams

func (d mySQLDialect) Now() Now {
	return Now{"now()"}
}

func (d mySQLDialect) UnixNow() UnixNow {
	return UnixNow{"unix_timestamp()"}
}

func (d mySQLDialect) PlaceholderFormat() PlaceholderFormat {
	return Question
}

func (d mySQLDialect) OnConflictFormat() OnConflictFormat {
	return OnDuplicate
}

func (d postgreSQLDialect) Now() Now {
	return Now{"now()"}
}

func (d postgreSQLDialect) UnixNow() UnixNow {
	return UnixNow{"extract(epoch from now())"}
}

func (d postgreSQLDialect) PlaceholderFormat() PlaceholderFormat {
	return Dollar
}

func (d postgreSQLDialect) OnConflictFormat() OnConflictFormat {
	return OnConflict
}

func (d sqlite3Dialect) Now() Now {
	return Now{"datetime('now')"}
}

func (d sqlite3Dialect) UnixNow() UnixNow {
	return UnixNow{"strftime('%s', 'now')"}
}

func (d sqlite3Dialect) PlaceholderFormat() PlaceholderFormat {
	return Question
}

func (d sqlite3Dialect) OnConflictFormat() OnConflictFormat {
	return OnConflictWithKey
}
