package gorqlite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

/* *****************************************************************

   method: Connection.Write()

	This is the JSON we get back:

{
    "results": [
        {
            "last_insert_id": 1,
            "rows_affected": 1,
            "time": 0.00759015
        },
        {
            "last_insert_id": 2,
            "rows_affected": 1,
            "time": 0.00669015
        }
    ],
    "time": 0.869015
}

	or

{
    "results": [
        {
            "error": "table foo already exists"
        }
    ],
    "time": 0.18472685400000002
}

	We don't care about the overall time.  We just want the results,
	so we'll take those and put each into a WriteResult

	Because the results themselves are smaller than the JSON
	(which repeats strings like "last_insert_id" frequently),
	we'll just parse everything at once.

 * *****************************************************************/

// WriteOne wraps Write() into a single-statement method.
//
// WriteOne uses context.Background() internally; to specify the context, use WriteOneContext.
func (conn *Connection) WriteOne(sqlStatement string) (wr WriteResult, err error) {
	wra, err := conn.Write([]string{sqlStatement})
	return wra[0], err
}

// WriteOneContext wraps WriteContext() into a single-statement
func (conn *Connection) WriteOneContext(ctx context.Context, sqlStatement string) (wr WriteResult, err error) {
	wra, err := conn.WriteContext(ctx, []string{sqlStatement})
	return wra[0], err
}

// WriteOneParameterized wraps WriteParameterized() into a single-statement method.
//
// WriteOneParameterized uses context.Background() internally; to specify the context, use WriteOneParameterizedContext.
func (conn *Connection) WriteOneParameterized(statement ParameterizedStatement) (wr WriteResult, err error) {
	wra, err := conn.WriteParameterized([]ParameterizedStatement{statement})
	return wra[0], err
}

// WriteOneParameterizedContext wraps WriteParameterizedContext into
// a single-statement method.
func (conn *Connection) WriteOneParameterizedContext(ctx context.Context, statement ParameterizedStatement) (wr WriteResult, err error) {
	wra, err := conn.WriteParameterizedContext(ctx, []ParameterizedStatement{statement})
	return wra[0], err
}

func (conn *Connection) WriteStmt(ctx context.Context, sqlStatements ...*Statement) (results []WriteResult, err error) {
	return conn.WriteParameterizedContext(ctx, makeParameterizedStatements(sqlStatements))
}

// Write is used to perform DDL/DML in the database synchronously without parameters.
//
// Write uses context.Background() internally; to specify the context, use WriteContext.
// To use Write with parameterized queries, use WriteParameterized.
func (conn *Connection) Write(sqlStatements []string) (results []WriteResult, err error) {
	parameterizedStatements := make([]ParameterizedStatement, 0, len(sqlStatements))
	for _, sqlStatement := range sqlStatements {
		parameterizedStatements = append(parameterizedStatements, ParameterizedStatement{
			Query: sqlStatement,
		})
	}

	return conn.WriteParameterized(parameterizedStatements)
}

// WriteContext is used to perform DDL/DML in the database synchronously without parameters.
//
// To use WriteContext with parameterized queries, use WriteParameterizedContext.
func (conn *Connection) WriteContext(ctx context.Context, sqlStatements []string) (results []WriteResult, err error) {
	parameterizedStatements := make([]ParameterizedStatement, 0, len(sqlStatements))
	for _, sqlStatement := range sqlStatements {
		parameterizedStatements = append(parameterizedStatements, ParameterizedStatement{
			Query: sqlStatement,
		})
	}

	return conn.WriteParameterizedContext(ctx, parameterizedStatements)
}

// WriteParameterized is used to perform DDL/DML in the database synchronously.
//
// WriteParameterized takes an array of SQL statements, and returns an equal-sized array of WriteResults,
// each corresponding to the SQL statement that produced it.
//
// All statements are executed as a single transaction.
//
// WriteParameterized returns an error if one is encountered during its operation.
// If it's something like a call to the rqlite API, then it'll return that error.
// If one statement out of several has an error, it will return a generic
// "there were %d statement errors" and you'll have to look at the individual statement's Err for more info.
//
// WriteParameterized uses context.Background() internally; to specify the context, use WriteParameterizedContext.
func (conn *Connection) WriteParameterized(sqlStatements []ParameterizedStatement) (results []WriteResult, err error) {
	return conn.WriteParameterizedContext(context.Background(), sqlStatements)
}

// WriteParameterizedContext is used to perform DDL/DML in the database synchronously.
//
// WriteParameterizedContext takes an array of SQL statements, and returns an equal-sized array of WriteResults,
// each corresponding to the SQL statement that produced it.
//
// All statements are executed as a single transaction.
//
// WriteParameterizedContext returns an error if one is encountered during its operation.
// If it's something like a call to the rqlite API, then it'll return that error.
// If one statement out of several has an error, it will return a generic
// "there were %d statement errors" and you'll have to look at the individual statement's Err for more info.
func (conn *Connection) WriteParameterizedContext(ctx context.Context, sqlStatements []ParameterizedStatement) (results []WriteResult, err error) {
	results = make([]WriteResult, 0)

	if conn.hasBeenClosed {
		results = append(results, WriteResult{Err: ErrClosed})
		return results, ErrClosed
	}

	trace("%s: Write() for %d statements", conn.ID, len(sqlStatements))

	response, err := conn.rqliteApiPost(ctx, api_WRITE, sqlStatements)
	if err != nil {
		trace("%s: rqliteApiCall() ERROR: %s", conn.ID, err.Error())
		results = append(results, WriteResult{Err: err})
		return results, err
	}
	trace("%s: rqliteApiCall() OK", conn.ID)

	var sections map[string]interface{}
	err = json.Unmarshal(response, &sections)
	if err != nil {
		trace("%s: json.Unmarshal() ERROR: %s", conn.ID, err.Error())
		results = append(results, WriteResult{Err: err})
		return results, err
	}
	// stop if we got an error from the api
	if errMsg, ok := sections["error"].(string); ok && errMsg != "" {
		trace("%s: api ERROR: %s", conn.ID, errMsg)

		err = fmt.Errorf("%s", errMsg)
		results = append(results, WriteResult{Err: err})
		return results, err
	}

	// at this point, we have a "results" section and
	// a "time" section.  we can ignore the latter.

	resultsArray, ok := sections["results"].([]interface{})
	if !ok {
		err = errors.New("result key is missing from response")
		trace("%s: sections[\"results\"] ERROR: %s", conn.ID, err)
		results = append(results, WriteResult{Err: err})
		return results, err
	}

	trace("%s: I have %d result(s) to parse", conn.ID, len(resultsArray))
	numStatementErrors := 0
	for n, k := range resultsArray {
		trace("%s: starting on result %d", conn.ID, n)
		thisWR := conn.makeWriteResult(k.(map[string]interface{}))
		if thisWR.Err != nil {
			numStatementErrors++
		} else {
			trace("%s: this result (LII,RA,T): %d %d %f", conn.ID, thisWR.LastInsertID, thisWR.RowsAffected, thisWR.Timing)
		}
		results = append(results, thisWR)
	}

	trace("%s: finished parsing, returning %d results", conn.ID, len(results))
	if numStatementErrors > 0 {
		return results, fmt.Errorf("there were %d statement errors", numStatementErrors)
	}

	return results, nil
}

func (conn *Connection) makeWriteResult(thisResult map[string]interface{}) WriteResult {
	thisWR := WriteResult{
		ID: conn.ID,
	}

	// did we get an error?
	_, ok := thisResult["error"]
	if ok {
		trace("%s: have an error on this result: %s", conn.ID, thisResult["error"].(string))
		thisWR.Err = errors.New(thisResult["error"].(string))
		return thisWR
	}

	_, ok = thisResult["last_insert_id"]
	if ok {
		thisWR.LastInsertID = int64(thisResult["last_insert_id"].(float64))
	}

	_, ok = thisResult["rows_affected"] // could be zero for a CREATE
	if ok {
		thisWR.RowsAffected = int64(thisResult["rows_affected"].(float64))
	}
	_, ok = thisResult["time"] // could be nil
	if ok {
		thisWR.Timing = thisResult["time"].(float64)
	}
	return thisWR
}

// QueueOne is a convenience method that wraps Queue into a single-statement.
//
// QueueOne uses context.Background() internally; to specify the context, use QueueOneContext.
func (conn *Connection) QueueOne(sqlStatement string) (seq int64, err error) {
	sqlStatements := make([]string, 0)
	sqlStatements = append(sqlStatements, sqlStatement)
	return conn.Queue(sqlStatements)
}

// QueueOneContext is a convenience method that wraps QueueContext into a single-statement
func (conn *Connection) QueueOneContext(ctx context.Context, sqlStatement string) (seq int64, err error) {
	return conn.QueueContext(ctx, []string{sqlStatement})
}

// QueueOneParameterized is a convenience method that wraps QueueParameterized into a single-statement method.
//
// QueueOneParameterized uses context.Background() internally; to specify the context, use QueueOneParameterizedContext.
func (conn *Connection) QueueOneParameterized(statement ParameterizedStatement) (seq int64, err error) {
	return conn.QueueParameterized([]ParameterizedStatement{statement})
}

// QueueOneParameterizedContext is a convenience method that wraps QueueParameterizedContext() into a single-statement method.
func (conn *Connection) QueueOneParameterizedContext(ctx context.Context, statement ParameterizedStatement) (seq int64, err error) {
	return conn.QueueParameterizedContext(ctx, []ParameterizedStatement{statement})
}

// Queue is used to perform asynchronous writes to the rqlite database as defined in the documentation:
// https://github.com/rqlite/rqlite/blob/master/DOC/QUEUED_WRITES.md
//
// Queue uses context.Background() internally; to specify the context, use QueueContext.
// To use Queue with parameterized queries, use QueueParameterized.
func (conn *Connection) Queue(sqlStatements []string) (seq int64, err error) {
	return conn.QueueContext(context.Background(), sqlStatements)
}

// QueueContext is used to perform asynchronous writes to the rqlite database as defined in the documentation:
// https://github.com/rqlite/rqlite/blob/master/DOC/QUEUED_WRITES.md
//
// To use QueueContext with parameterized queries, use QueueParameterizedContext.
func (conn *Connection) QueueContext(ctx context.Context, sqlStatements []string) (seq int64, err error) {
	parameterizedStatements := make([]ParameterizedStatement, 0)
	for _, sqlStatement := range sqlStatements {
		parameterizedStatements = append(parameterizedStatements, ParameterizedStatement{Query: sqlStatement})
	}

	return conn.QueueParameterizedContext(ctx, parameterizedStatements)
}

// QueueParameterized is used to perform asynchronous writes with parameterized queries
// to the rqlite database as defined in the documentation:
// https://github.com/rqlite/rqlite/blob/master/DOC/QUEUED_WRITES.md
//
// QueueParameterized uses context.Background() internally; to specify the context, use QueueParameterizedContext.
func (conn *Connection) QueueParameterized(sqlStatements []ParameterizedStatement) (seq int64, err error) {
	return conn.QueueParameterizedContext(context.Background(), sqlStatements)
}

// QueueParameterizedContext is used to perform asynchronous writes with parameterized queries
// to the rqlite database as defined in the documentation:
// https://github.com/rqlite/rqlite/blob/master/DOC/QUEUED_WRITES.md
func (conn *Connection) QueueParameterizedContext(ctx context.Context, sqlStatements []ParameterizedStatement) (seq int64, err error) {
	if conn.hasBeenClosed {
		return 0, ErrClosed
	}

	trace("%s: Write() for %d statements", conn.ID, len(sqlStatements))

	// Set queuing mode just for this call.
	conn.wantsQueueing = true
	defer func() {
		conn.wantsQueueing = false
	}()

	response, err := conn.rqliteApiPost(ctx, api_WRITE, sqlStatements)
	if err != nil {
		trace("%s: rqliteApiCall() ERROR: %s", conn.ID, err.Error())
		return 0, err
	}
	trace("%s: rqliteApiCall() OK", conn.ID)

	var sections map[string]interface{}
	err = json.Unmarshal(response, &sections)
	if err != nil {
		trace("%s: json.Unmarshal() ERROR: %s", conn.ID, err.Error())
		return 0, err
	}

	return int64(sections["sequence_number"].(float64)), nil
}

// WriteResult holds the result of a single statement sent to Write().
//
// Write() returns an array of WriteResult vars, while WriteOne() returns a single WriteResult.
type WriteResult struct {
	ID           string  // ID of connection
	Err          error   // don't trust the rest if this isn't nil
	Timing       float64 // timing
	RowsAffected int64   // affected by the change
	LastInsertID int64   // if relevant, otherwise zero value
}

func (w *WriteResult) IsZero() bool {
	return w.Timing == 0 && w.RowsAffected == 0 && w.LastInsertID == 0 && w.Err == nil
}
