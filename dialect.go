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

// Now is the current datetime
// Ex:
//     .Where(Lt{"ts": Now()})
type Now expr

// UnixNow is the current datetime in unix epoch format
// Ex:
//     .Where(Lt{"ts": UnixNow()})
type UnixNow expr

func (d mySQLDialect) Now() Now {
	return Now{sql: "now()"}
}

func (d mySQLDialect) UnixNow() UnixNow {
	return UnixNow{sql: "unix_timestamp()"}
}

func (d mySQLDialect) PlaceholderFormat() PlaceholderFormat {
	return Question
}

func (d mySQLDialect) OnConflictFormat() OnConflictFormat {
	return OnDuplicate
}

func (d postgreSQLDialect) Now() Now {
	return Now{sql: "now()"}
}

func (d postgreSQLDialect) UnixNow() UnixNow {
	return UnixNow{sql: "extract(epoch from now())"}
}

func (d postgreSQLDialect) PlaceholderFormat() PlaceholderFormat {
	return Dollar
}

func (d postgreSQLDialect) OnConflictFormat() OnConflictFormat {
	return OnConflict
}

func (d sqlite3Dialect) Now() Now {
	return Now{sql: "datetime('now')"}
}

func (d sqlite3Dialect) UnixNow() UnixNow {
	return UnixNow{sql: "strftime('%s', 'now')"}
}

func (d sqlite3Dialect) PlaceholderFormat() PlaceholderFormat {
	return Question
}

func (d sqlite3Dialect) OnConflictFormat() OnConflictFormat {
	return OnConflictWithKey
}
