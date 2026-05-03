package gorqlite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"
)

// NullString represents a string that may be null.
type NullString struct {
	String string
	Valid  bool // Valid is true if String is not NULL
}

// NullInt64 represents an int64 that may be null.
type NullInt64 struct {
	Int64 int64
	Valid bool // Valid is true if Int64 is not NULL
}

// NullInt32 represents an int32 that may be null.
type NullInt32 struct {
	Int32 int32
	Valid bool // Valid is true if Int32 is not NULL
}

// NullInt16 represents an int16 that may be null.
type NullInt16 struct {
	Int16 int16
	Valid bool // Valid is true if Int16 is not NULL
}

// NullFloat64 represents a float64 that may be null.
type NullFloat64 struct {
	Float64 float64
	Valid   bool // Valid is true if Float64 is not NULL
}

// NullBool represents a bool that may be null.
type NullBool struct {
	Bool  bool
	Valid bool // Valid is true if Bool is not NULL
}

// NullTime represents a time.Time that may be null.
type NullTime struct {
	Time  time.Time
	Valid bool // Valid is true if Time is not NULL
}

/* *****************************************************************

   method: Connection.Query()

	This is the JSON we get back:

{
    "results": [
        {
            "columns": ["id", "name"],
            "types": ["integer", "text"],
            "values": [[1, "fiona"], [2, "sinead"]],
            "time": 0.0150043
        }
    ],
    "time": 0.0220043
}

	or

{
    "results": [
        {"error": "near \"nonsense\": syntax error"}
    ],
    "time": 2.478862
}

 * *****************************************************************/

// resultJSON is the unmarshal target for one element of the "results"
// array returned by /db/query, /db/execute and /db/request. It holds
// every field any of those endpoints may return.
type resultJSON struct {
	Columns      []string        `json:"columns,omitempty"`
	Types        []string        `json:"types,omitempty"`
	Values       [][]interface{} `json:"values,omitempty"`
	LastInsertID *int64          `json:"last_insert_id,omitempty"`
	RowsAffected *int64          `json:"rows_affected,omitempty"`
	Time         float64         `json:"time,omitempty"`
	Error        string          `json:"error,omitempty"`
}

// responseJSON is the top-level shape returned by /db/query,
// /db/execute and /db/request.
type responseJSON struct {
	Results        []resultJSON `json:"results"`
	Error          string       `json:"error,omitempty"`
	Time           float64      `json:"time,omitempty"`
	SequenceNumber *int64       `json:"sequence_number,omitempty"`
}

// QueryOne wraps Query into a single-statement method.
//
// QueryOne uses context.Background() internally; to specify the context, use QueryOneContext.
func (conn *Connection) QueryOne(sqlStatement string) (qr QueryResult, err error) {
	return conn.QueryOneContext(context.Background(), sqlStatement)
}

// QueryOneContext wraps Query into a single-statement method.
func (conn *Connection) QueryOneContext(ctx context.Context, sqlStatement string) (qr QueryResult, err error) {
	qra, err := conn.QueryContext(ctx, []string{sqlStatement})
	return firstQueryResult(qra), err
}

// QueryOneParameterized wraps QueryParameterized into a single-statement method.
//
// QueryOneParameterized uses context.Background() internally;
// to specify the context, use QueryOneParameterizedContext.
func (conn *Connection) QueryOneParameterized(statement ParameterizedStatement) (qr QueryResult, err error) {
	return conn.QueryOneParameterizedContext(context.Background(), statement)
}

// QueryOneParameterizedContext wraps QueryParameterizedContext into a single-statement method.
func (conn *Connection) QueryOneParameterizedContext(ctx context.Context, statement ParameterizedStatement) (qr QueryResult, err error) {
	qra, err := conn.QueryParameterizedContext(ctx, []ParameterizedStatement{statement})
	return firstQueryResult(qra), err
}

// firstQueryResult returns qra[0] if it exists, otherwise a zero QueryResult.
// Avoids panics when QueryOne wrappers receive an empty slice from an
// unexpected/empty server response.
func firstQueryResult(qra []QueryResult) QueryResult {
	if len(qra) == 0 {
		return QueryResult{}
	}
	return qra[0]
}

// Query is used to perform SELECT operations in the database. It takes an array of SQL statements and
// executes them in a single transaction, returning an array of QueryResult.
//
// Query uses context.Background() internally; to specify the context, use QueryContext.
func (conn *Connection) Query(sqlStatements []string) (results []QueryResult, err error) {
	return conn.QueryContext(context.Background(), sqlStatements)
}

// QueryContext is used to perform SELECT operations in the database. It takes an array of SQL statements and
// executes them in a single transaction, returning an array of QueryResult.
func (conn *Connection) QueryContext(ctx context.Context, sqlStatements []string) (results []QueryResult, err error) {
	parameterizedStatements := make([]ParameterizedStatement, 0, len(sqlStatements))
	for _, sqlStatement := range sqlStatements {
		parameterizedStatements = append(parameterizedStatements, ParameterizedStatement{
			Query: sqlStatement,
		})
	}

	return conn.QueryParameterizedContext(ctx, parameterizedStatements)
}

// QueryParameterized is used to perform SELECT operations in the database.
//
// It takes an array of parameterized SQL statements and executes them in a single transaction,
// returning an array of QueryResult vars.
//
// QueryParameterized uses context.Background() internally; to specify the context, use QueryParameterizedContext.
func (conn *Connection) QueryParameterized(sqlStatements []ParameterizedStatement) (results []QueryResult, err error) {
	return conn.QueryParameterizedContext(context.Background(), sqlStatements)
}

// parseQueryResult turns a single rqlite result element into a QueryResult.
func (conn *Connection) parseQueryResult(r resultJSON) QueryResult {
	var qr QueryResult
	qr.conn = conn
	qr.rowNumber = -1
	qr.Timing = r.Time

	if r.Error != "" {
		trace("%s: have an error on this result: %s", conn.ID, r.Error)
		qr.Err = errors.New(r.Error)
		return qr
	}

	qr.columns = r.Columns
	qr.types = r.Types
	qr.values = r.Values

	trace("%s: this result (#col,time) %d %f", conn.ID, len(qr.columns), qr.Timing)
	return qr
}

// QueryParameterizedContext is used to perform SELECT operations in the database.
//
// It takes an array of parameterized SQL statements and executes them in a single transaction,
// returning an array of QueryResult vars.
func (conn *Connection) QueryParameterizedContext(ctx context.Context, sqlStatements []ParameterizedStatement) (results []QueryResult, err error) {
	results = make([]QueryResult, 0)

	if conn.isClosed() {
		results = append(results, QueryResult{Err: ErrClosed})
		return results, ErrClosed
	}

	trace("%s: Query() for %d statements", conn.ID, len(sqlStatements))

	response, err := conn.rqliteApiPost(ctx, api_QUERY, sqlStatements)
	if err != nil {
		trace("%s: rqliteApiCall() ERROR: %s", conn.ID, err.Error())
		results = append(results, QueryResult{Err: err})
		return results, err
	}
	trace("%s: rqliteApiCall() OK", conn.ID)

	var resp responseJSON
	if err := json.Unmarshal(response, &resp); err != nil {
		trace("%s: json.Unmarshal() ERROR: %s", conn.ID, err.Error())
		results = append(results, QueryResult{Err: err})
		return results, err
	}

	if resp.Error != "" {
		trace("%s: api ERROR: %s", conn.ID, resp.Error)
		apiErr := errors.New(resp.Error)
		results = append(results, QueryResult{Err: apiErr})
		return results, apiErr
	}

	trace("%s: I have %d result(s) to parse", conn.ID, len(resp.Results))

	var errs []error
	for n, r := range resp.Results {
		trace("%s: parsing result %d", conn.ID, n)
		qr := conn.parseQueryResult(r)
		results = append(results, qr)
		if qr.Err != nil {
			errs = append(errs, qr.Err)
		}
	}

	trace("%s: finished parsing, returning %d results", conn.ID, len(results))

	return results, joinErrors(errs...)
}

/* *****************************************************************

   type: QueryResult

 * *****************************************************************/

// QueryResult holds the results of a call to Query().  You could think of it as a rowset.
//
// So if you were to query:
//
//	SELECT id, name FROM some_table;
//
// then a QueryResult would hold any errors from that query, a list of columns and types, and the actual row values.
//
// Query() returns an array of QueryResult vars, while QueryOne() returns a single variable.
type QueryResult struct {
	conn      *Connection
	Err       error
	columns   []string
	types     []string
	Timing    float64
	values    [][]interface{}
	rowNumber int64
}

// Columns returns a list of the column names for this QueryResult.
func (qr *QueryResult) Columns() []string {
	return qr.columns
}

// Map returns the current row (as advanced by Next()) as a map[string]interface{}.
//
// The key is a string corresponding to a column name.
// The value is the corresponding column.
//
// Note that only json values are supported, so you will need to type the interface{} accordingly.
func (qr *QueryResult) Map() (map[string]interface{}, error) {
	trace("%s: Map() called for row %d", qr.conn.ID, qr.rowNumber)
	ans := make(map[string]interface{})

	if qr.rowNumber == -1 {
		return ans, errors.New("you need to Next() before you Map(), sorry, it's complicated")
	}

	thisRowValues := qr.values[qr.rowNumber]
	for i := 0; i < len(qr.columns); i++ {
		switch qr.types[i] {
		case "date", "datetime":
			if thisRowValues[i] != nil {
				t, err := toTime(thisRowValues[i])
				if err != nil {
					return ans, err
				}
				ans[qr.columns[i]] = t
			} else {
				ans[qr.columns[i]] = nil
			}
		default:
			ans[qr.columns[i]] = thisRowValues[i]
		}
	}

	return ans, nil
}

// Slice returns the current row (as advanced by Next()) as a []interface{}.
//
// The slice is a shallow copy of the internal representation of the row data.
//
// Note that only json values are supported, so you will need to type the interface{} accordingly.
func (qr *QueryResult) Slice() ([]interface{}, error) {
	trace("%s: Slice() called", qr.conn.ID)

	if qr.rowNumber == -1 {
		return nil, errors.New("you need to Next() before you Slice(), sorry, it's complicated")
	}

	thisRowValues := qr.values[qr.rowNumber]
	ans := make([]interface{}, len(thisRowValues))
	for i, v := range thisRowValues {
		switch qr.types[i] {
		case "date", "datetime":
			if v != nil {
				t, err := toTime(v)
				if err != nil {
					return ans, err
				}
				ans[i] = t
			} else {
				ans[i] = nil
			}
		default:
			ans[i] = v
		}
	}
	return ans, nil
}

// Next positions the QueryResult result pointer so that Scan() or Map() is ready.
//
// You should call Next() first, but gorqlite will fix it if you call Map() or Scan() before
// the initial Next().
//
// A common idiom:
//
//	rows := conn.Write(something)
//	for rows.Next() {
//	    // your Scan/Map and processing here.
//	}
func (qr *QueryResult) Next() bool {
	if qr.rowNumber >= int64(len(qr.values)-1) {
		return false
	}

	qr.rowNumber += 1
	return true
}

// NumRows returns the number of rows returned by the query.
func (qr *QueryResult) NumRows() int64 {
	return int64(len(qr.values))
}

// RowNumber returns the current row number as Next() iterates through the result's rows.
func (qr *QueryResult) RowNumber() int64 {
	return qr.rowNumber
}

func toTime(src interface{}) (time.Time, error) {
	switch src := src.(type) {
	case string:
		const layout = "2006-01-02 15:04:05"
		if t, err := time.Parse(layout, src); err == nil {
			return t, nil
		}
		return time.Parse(time.RFC3339, src)
	case float64:
		return time.Unix(int64(src), 0), nil
	case int64:
		return time.Unix(src, 0), nil
	}
	return time.Time{}, fmt.Errorf("invalid time type:%T val:%v", src, src)
}

// Scan takes a list of pointers and then updates them to reflect the current row's data.
//
// Note that only the following data types are used, and they
// are a subset of the types JSON uses:
//
//	string, for JSON strings
//	float64, for JSON numbers
//	int64, as a convenient extension
//	nil for JSON null
//
// booleans, JSON arrays, and JSON objects are not supported,
// since sqlite does not support them.
func (qr *QueryResult) Scan(dest ...interface{}) error {
	trace("%s: Scan() called for %d vars", qr.conn.ID, len(dest))

	if qr.rowNumber == -1 {
		return errors.New("you need to Next() before you Scan(), sorry, it's complicated")
	}

	if len(dest) != len(qr.columns) {
		return fmt.Errorf("expected %d columns but got %d vars", len(qr.columns), len(dest))
	}

	thisRowValues := qr.values[qr.rowNumber]
	for n, d := range dest {
		src := thisRowValues[n]
		switch d := d.(type) {
		case *time.Time:
			if src == nil {
				continue
			}
			t, err := toTime(src)
			if err != nil {
				return fmt.Errorf("%v: bad time col:(%d/%s) val:%v", err, n, qr.Columns()[n], src)
			}
			*d = t
		case *int:
			switch src := src.(type) {
			case float64:
				*d = int(src)
			case int64:
				*d = int(src)
			case string:
				i, err := strconv.Atoi(src)
				if err != nil {
					return err
				}
				*d = i
			case nil:
				trace("%s: skipping nil scan data for variable #%d (%s)", qr.conn.ID, n, qr.columns[n])
			default:
				return fmt.Errorf("invalid int col:%d type:%T val:%v", n, src, src)
			}
		case *int64:
			switch src := src.(type) {
			case float64:
				*d = int64(src)
			case int64:
				*d = src
			case string:
				i, err := strconv.ParseInt(src, 10, 64)
				if err != nil {
					return err
				}
				*d = i
			case nil:
				trace("%s: skipping nil scan data for variable #%d (%s)", qr.conn.ID, n, qr.columns[n])
			default:
				return fmt.Errorf("invalid int64 col:%d type:%T val:%v", n, src, src)
			}
		case *float64:
			switch src := src.(type) {
			case float64:
				*d = src
			case int64:
				*d = float64(src)
			case string:
				f, err := strconv.ParseFloat(src, 64)
				if err != nil {
					return err
				}
				*d = f
			case nil:
				trace("%s: skipping nil scan data for variable #%d (%s)", qr.conn.ID, n, qr.columns[n])
			default:
				return fmt.Errorf("invalid float64 col:%d type:%T val:%v", n, src, src)
			}
		case *string:
			switch src := src.(type) {
			case string:
				*d = src
			case nil:
				trace("%s: skipping nil scan data for variable #%d (%s)", qr.conn.ID, n, qr.columns[n])
			default:
				return fmt.Errorf("invalid string col:%d type:%T val:%v", n, src, src)
			}
		case *bool:
			// Note: Rqlite does not support bool, but this is a loop from dest
			// meaning, the user might be targeting to a bool-type variable.
			// Per Go convention, and per strconv.ParseBool documentation, bool might be
			// coming from value of "1", "t", "T", "TRUE", "true", "True", for `true` and
			// "0", "f", "F", "FALSE", "false", "False" for `false`
			switch src := src.(type) {
			case float64:
				b, err := strconv.ParseBool(strconv.FormatFloat(src, 'g', -1, 64))
				if err != nil {
					return err
				}
				*d = b
			case int64:
				b, err := strconv.ParseBool(strconv.FormatInt(src, 10))
				if err != nil {
					return err
				}
				*d = b
			case string:
				b, err := strconv.ParseBool(src)
				if err != nil {
					return err
				}
				*d = b
			case nil:
				trace("%s: skipping nil scan data for variable #%d (%s)", qr.conn.ID, n, qr.columns[n])
			default:
				return fmt.Errorf("invalid bool col:%d type:%T val:%v", n, src, src)
			}
		case *[]byte:
			switch src := src.(type) {
			case []byte:
				*d = src
			case string:
				*d = []byte(src)
			default:
				return fmt.Errorf("invalid []byte col:%d type:%T val:%v", n, src, src)
			}
		case *NullString:
			switch src := src.(type) {
			case string:
				*d = NullString{Valid: true, String: src}
			case nil:
				*d = NullString{Valid: false}
			default:
				return fmt.Errorf("invalid string col:%d type:%T val:%v", n, src, src)
			}
		case *NullInt64:
			switch src := src.(type) {
			case float64:
				*d = NullInt64{Valid: true, Int64: int64(src)}
			case int64:
				*d = NullInt64{Valid: true, Int64: src}
			case string:
				i, err := strconv.ParseInt(src, 10, 64)
				if err != nil {
					return err
				}
				*d = NullInt64{Valid: true, Int64: i}
			case nil:
				*d = NullInt64{Valid: false}
			default:
				return fmt.Errorf("invalid int64 col:%d type:%T val:%v", n, src, src)
			}
		case *NullInt32:
			switch src := src.(type) {
			case float64:
				*d = NullInt32{Valid: true, Int32: int32(src)}
			case int64:
				*d = NullInt32{Valid: true, Int32: int32(src)}
			case string:
				i, err := strconv.ParseInt(src, 10, 32)
				if err != nil {
					return err
				}
				*d = NullInt32{Valid: true, Int32: int32(i)}
			case nil:
				*d = NullInt32{Valid: false}
			default:
				return fmt.Errorf("invalid int32 col:%d type:%T val:%v", n, src, src)
			}
		case *NullInt16:
			switch src := src.(type) {
			case float64:
				*d = NullInt16{Valid: true, Int16: int16(src)}
			case int64:
				*d = NullInt16{Valid: true, Int16: int16(src)}
			case string:
				i, err := strconv.ParseInt(src, 10, 16)
				if err != nil {
					return err
				}
				*d = NullInt16{Valid: true, Int16: int16(i)}
			case nil:
				*d = NullInt16{Valid: false}
			default:
				return fmt.Errorf("invalid int16 col:%d type:%T val:%v", n, src, src)
			}
		case *NullFloat64:
			switch src := src.(type) {
			case float64:
				*d = NullFloat64{Valid: true, Float64: src}
			case int64:
				*d = NullFloat64{Valid: true, Float64: float64(src)}
			case string:
				f, err := strconv.ParseFloat(src, 64)
				if err != nil {
					return err
				}
				*d = NullFloat64{Valid: true, Float64: f}
			case nil:
				*d = NullFloat64{Valid: false}
			default:
				return fmt.Errorf("invalid float64 col:%d type:%T val:%v", n, src, src)
			}
		case *NullBool:
			switch src := src.(type) {
			case float64:
				b, err := strconv.ParseBool(strconv.FormatFloat(src, 'g', -1, 64))
				if err != nil {
					return err
				}
				*d = NullBool{Valid: true, Bool: b}
			case int64:
				b, err := strconv.ParseBool(strconv.FormatInt(src, 10))
				if err != nil {
					return err
				}
				*d = NullBool{Valid: true, Bool: b}
			case string:
				b, err := strconv.ParseBool(src)
				if err != nil {
					return err
				}
				*d = NullBool{Valid: true, Bool: b}
			case nil:
				*d = NullBool{Valid: false}
			default:
				return fmt.Errorf("invalid bool col:%d type:%T val:%v", n, src, src)
			}
		case *NullTime:
			if src == nil {
				*d = NullTime{Valid: false}
			} else {
				t, err := toTime(src)
				if err != nil {
					return fmt.Errorf("%v: bad time col:(%d/%s) val:%v", err, n, qr.Columns()[n], src)
				}
				*d = NullTime{Valid: true, Time: t}
			}
		default:
			return fmt.Errorf("unknown destination type (%T) to scan into in variable #%d", d, n)
		}
	}

	return nil
}

// Types returns an array of the column's types.
//
// Note that sqlite will repeat the type you tell it, but in many cases, it's ignored.  See https://www.sqlite.org/datatype3.html
//
// This info may additionally conflict with the reality that your data is being JSON encoded/decoded.
func (qr *QueryResult) Types() []string {
	return qr.types
}
