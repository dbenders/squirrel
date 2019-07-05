package squirrel

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/lann/builder"
)

type insertData struct {
	PlaceholderFormat PlaceholderFormat
	RunWith           BaseRunner
	Prefixes          exprs
	Options           []string
	Into              string
	Columns           []string
	Values            [][]interface{}
	Suffixes          exprs
	Select            *SelectBuilder

	OnConflictFormat     OnConflictFormat
	OnConflictKey        []string
	OnConflictSetClauses []setClause
}

func (d *insertData) Exec() (sql.Result, error) {
	if d.RunWith == nil {
		return nil, RunnerNotSet
	}
	return ExecWith(d.RunWith, d)
}

func (d *insertData) Query() (*sql.Rows, error) {
	if d.RunWith == nil {
		return nil, RunnerNotSet
	}
	return QueryWith(d.RunWith, d)
}

func (d *insertData) QueryRow() RowScanner {
	if d.RunWith == nil {
		return &Row{err: RunnerNotSet}
	}
	queryRower, ok := d.RunWith.(QueryRower)
	if !ok {
		return &Row{err: RunnerNotQueryRunner}
	}
	return QueryRowWith(queryRower, d)
}

func (d *insertData) ToSql() (sqlStr string, args []interface{}, err error) {
	if len(d.Into) == 0 {
		err = errors.New("insert statements must specify a table")
		return
	}
	if len(d.Values) == 0 && d.Select == nil {
		err = errors.New("insert statements must have at least one set of values or select clause")
		return
	}

	sql := &bytes.Buffer{}

	if len(d.Prefixes) > 0 {
		args, _ = d.Prefixes.AppendToSql(sql, " ", args)
		sql.WriteString(" ")
	}

	sql.WriteString("INSERT ")

	if len(d.Options) > 0 {
		sql.WriteString(strings.Join(d.Options, " "))
		sql.WriteString(" ")
	}

	sql.WriteString("INTO ")
	sql.WriteString(d.Into)
	sql.WriteString(" ")

	if len(d.Columns) > 0 {
		sql.WriteString("(")
		sql.WriteString(strings.Join(d.Columns, ","))
		sql.WriteString(") ")
	}

	if d.Select != nil {
		args, err = d.appendSelectToSQL(sql, args)
	} else {
		args, err = d.appendValuesToSQL(sql, args)
	}
	if err != nil {
		return
	}

	if len(d.OnConflictSetClauses) > 0 {
		args, err = d.appendOnConflictToSQL(sql, args)
	}

	if len(d.Suffixes) > 0 {
		sql.WriteString(" ")
		args, _ = d.Suffixes.AppendToSql(sql, " ", args)
	}

	sqlStr, err = d.PlaceholderFormat.ReplacePlaceholders(sql.String())
	return
}

func (d *insertData) appendValuesToSQL(w io.Writer, args []interface{}) ([]interface{}, error) {
	if len(d.Values) == 0 {
		return args, errors.New("values for insert statements are not set")
	}

	io.WriteString(w, "VALUES ")

	valuesStrings := make([]string, len(d.Values))
	for r, row := range d.Values {
		valueStrings := make([]string, len(row))
		for v, val := range row {
			e, isExpr := val.(expr)
			if isExpr {
				valueStrings[v] = e.sql
				args = append(args, e.args...)
			} else {
				valueStrings[v] = "?"
				args = append(args, val)
			}
		}
		valuesStrings[r] = fmt.Sprintf("(%s)", strings.Join(valueStrings, ","))
	}

	io.WriteString(w, strings.Join(valuesStrings, ","))

	return args, nil
}

func (d *insertData) appendSelectToSQL(w io.Writer, args []interface{}) ([]interface{}, error) {
	if d.Select == nil {
		return args, errors.New("select clause for insert statements are not set")
	}

	selectClause, sArgs, err := d.Select.ToSql()
	if err != nil {
		return args, err
	}

	io.WriteString(w, selectClause)
	args = append(args, sArgs...)

	return args, nil
}

func (d *insertData) appendOnConflictToSQL(w io.Writer, args []interface{}) ([]interface{}, error) {
	if len(d.OnConflictSetClauses) == 0 {
		return args, errors.New("on conflict clause for insert statements are not set")
	}

	io.WriteString(w, d.OnConflictFormat.String(d.OnConflictKey))

	setSqls := make([]string, len(d.OnConflictSetClauses))
	for i, setClause := range d.OnConflictSetClauses {
		var valSql string
		e, isExpr := setClause.value.(expr)
		if isExpr {
			valSql = e.sql
			args = append(args, e.args...)
		} else {
			valSql = "?"
			args = append(args, setClause.value)
		}
		setSqls[i] = fmt.Sprintf("%s = %s", setClause.column, valSql)
	}
	io.WriteString(w, strings.Join(setSqls, ", "))

	return args, nil
}

// Builder

// InsertBuilder builds SQL INSERT statements.
type InsertBuilder builder.Builder

func init() {
	builder.Register(InsertBuilder{}, insertData{})
}

// Format methods

// PlaceholderFormat sets PlaceholderFormat (e.g. Question or Dollar) for the
// query.
func (b InsertBuilder) PlaceholderFormat(f PlaceholderFormat) InsertBuilder {
	return builder.Set(b, "PlaceholderFormat", f).(InsertBuilder)
}

// OnConflictFormat sets OnConflictFormat (e.g. ON CONFLICT/ON DUPLICATE KEY)
// for the query.
func (b InsertBuilder) OnConflictFormat(f OnConflictFormat) InsertBuilder {
	return builder.Set(b, "OnConflictFormat", f).(InsertBuilder)
}

// Runner methods

// RunWith sets a Runner (like database/sql.DB) to be used with e.g. Exec.
func (b InsertBuilder) RunWith(runner BaseRunner) InsertBuilder {
	return setRunWith(b, runner).(InsertBuilder)
}

// Exec builds and Execs the query with the Runner set by RunWith.
func (b InsertBuilder) Exec() (sql.Result, error) {
	data := builder.GetStruct(b).(insertData)
	return data.Exec()
}

// Query builds and Querys the query with the Runner set by RunWith.
func (b InsertBuilder) Query() (*sql.Rows, error) {
	data := builder.GetStruct(b).(insertData)
	return data.Query()
}

// QueryRow builds and QueryRows the query with the Runner set by RunWith.
func (b InsertBuilder) QueryRow() RowScanner {
	data := builder.GetStruct(b).(insertData)
	return data.QueryRow()
}

// Scan is a shortcut for QueryRow().Scan.
func (b InsertBuilder) Scan(dest ...interface{}) error {
	return b.QueryRow().Scan(dest...)
}

// SQL methods

// ToSql builds the query into a SQL string and bound args.
func (b InsertBuilder) ToSql() (string, []interface{}, error) {
	data := builder.GetStruct(b).(insertData)
	return data.ToSql()
}

// Prefix adds an expression to the beginning of the query
func (b InsertBuilder) Prefix(sql string, args ...interface{}) InsertBuilder {
	return builder.Append(b, "Prefixes", Expr(sql, args...)).(InsertBuilder)
}

// Options adds keyword options before the INTO clause of the query.
func (b InsertBuilder) Options(options ...string) InsertBuilder {
	return builder.Extend(b, "Options", options).(InsertBuilder)
}

// Into sets the INTO clause of the query.
func (b InsertBuilder) Into(from string) InsertBuilder {
	return builder.Set(b, "Into", from).(InsertBuilder)
}

// Columns adds insert columns to the query.
func (b InsertBuilder) Columns(columns ...string) InsertBuilder {
	return builder.Extend(b, "Columns", columns).(InsertBuilder)
}

// Values adds a single row's values to the query.
func (b InsertBuilder) Values(values ...interface{}) InsertBuilder {
	return builder.Append(b, "Values", values).(InsertBuilder)
}

// Suffix adds an expression to the end of the query
func (b InsertBuilder) Suffix(sql string, args ...interface{}) InsertBuilder {
	return builder.Append(b, "Suffixes", Expr(sql, args...)).(InsertBuilder)
}

// SetMap set columns and values for insert builder from a map of column name and value
// note that it will reset all previous columns and values was set if any
func (b InsertBuilder) SetMap(clauses map[string]interface{}) InsertBuilder {
	// Keep the columns in a consistent order by sorting the column key string.
	cols := make([]string, 0, len(clauses))
	for col := range clauses {
		cols = append(cols, col)
	}
	sort.Strings(cols)

	vals := make([]interface{}, 0, len(clauses))
	for _, col := range cols {
		vals = append(vals, clauses[col])
	}

	b = builder.Set(b, "Columns", cols).(InsertBuilder)
	b = builder.Set(b, "Values", [][]interface{}{vals}).(InsertBuilder)

	return b
}

// Select set Select clause for insert query
// If Values and Select are used, then Select has higher priority
func (b InsertBuilder) Select(sb SelectBuilder) InsertBuilder {
	return builder.Set(b, "Select", &sb).(InsertBuilder)
}

// OnConflictKey specify the primary key or columns for the ON CONFLICT DO UPDATE SET clause.
func (b InsertBuilder) OnConflictKey(columns ...string) InsertBuilder {
	for _, col := range columns {
		b = builder.Append(b, "OnConflictKey", col).(InsertBuilder)
	}
	return b
}

// OnConflictSet adds ON CONFLICT DO UPDATE SET clause to the query.
func (b InsertBuilder) OnConflictSet(column string, value interface{}) InsertBuilder {
	return builder.Append(b, "OnConflictSetClauses", setClause{column: column, value: value}).(InsertBuilder)
}

// OnConflictSetSetMap is a convenience method which calls .Set for each key/value pair in clauses.
func (b InsertBuilder) OnConflictSetSetMap(clauses map[string]interface{}) InsertBuilder {
	keys := make([]string, len(clauses))
	i := 0
	for key := range clauses {
		keys[i] = key
		i++
	}
	sort.Strings(keys)
	for _, key := range keys {
		val, _ := clauses[key]
		b = b.OnConflictSet(key, val)
	}
	return b
}

// OnConflictFormat

// OnConflictFormat interface. Encapsulate different phrases used for ON CONFLICT.
type OnConflictFormat interface {
	String(cols []string) string
}

// Global variables for different OnConflictFormats
var (
	OnConflict  = onConflictFormat{}
	OnDuplicate = onDuplicateFormat{}
)

type onConflictFormat struct{}
type onDuplicateFormat struct{}

// String generates the string: ON CONFLICT(col1,col2) DO UPDATE SET
func (onConflictFormat) String(cols []string) string {
	key := ""
	if len(cols) > 0 {
		key = fmt.Sprintf(" (%s)", strings.Join(cols, ","))
	}
	return fmt.Sprintf(" ON CONFLICT%s DO UPDATE SET ", key)
}

// String generates the string: ON DUPLICATE KEY UPDATE
func (onDuplicateFormat) String(cols []string) string {
	return " ON DUPLICATE KEY UPDATE "
}
