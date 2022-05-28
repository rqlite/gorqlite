package gorqlite

/*
	this file has
		Write()
		WriteResult and its methods
*/

import (
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

/*
WriteOne() is a convenience method that wraps Write() into a single-statement
method.
*/

func (conn *Connection) WriteOne(sqlStatement string) (wr WriteResult, err error) {
	if conn.hasBeenClosed {
		wr.Err = errClosed
		return wr, errClosed
	}
	sqlStatements := make([]string, 0)
	sqlStatements = append(sqlStatements, sqlStatement)
	wra, err := conn.Write(sqlStatements)
	return wra[0], err
}

func (conn *Connection) QueueOne(sqlStatement string) (seq int64, err error) {
	if conn.hasBeenClosed {
		return 0, errClosed
	}
	sqlStatements := make([]string, 0)
	sqlStatements = append(sqlStatements, sqlStatement)
	return conn.Queue(sqlStatements)
}

/*
Write() is used to perform DDL/DML in the database.  ALTER, CREATE, DELETE, DROP, INSERT, UPDATE, etc. all go through Write().

Write() takes an array of SQL statements, and returns an equal-sized array of WriteResults, each corresponding to the SQL statement that produced it.

All statements are executed as a single transaction.

Write() returns an error if one is encountered during its operation.  If it's something like a call to the rqlite API, then it'll return that error.  If one statement out of several has an error, it will return a generic "there were %d statement errors" and you'll have to look at the individual statement's Err for more info.
*/
func (conn *Connection) Write(sqlStatements []string) (results []WriteResult, err error) {
	results = make([]WriteResult, 0)

	if conn.hasBeenClosed {
		var errResult WriteResult
		errResult.Err = errClosed
		results = append(results, errResult)
		return results, errClosed
	}

	trace("%s: Write() for %d statements", conn.ID, len(sqlStatements))

	response, err := conn.rqliteApiPost(api_WRITE, sqlStatements)
	if err != nil {
		trace("%s: rqliteApiCall() ERROR: %s", conn.ID, err.Error())
		var errResult WriteResult
		errResult.Err = err
		results = append(results, errResult)
		return results, err
	}
	trace("%s: rqliteApiCall() OK", conn.ID)

	var sections map[string]interface{}
	err = json.Unmarshal(response, &sections)
	if err != nil {
		trace("%s: json.Unmarshal() ERROR: %s", conn.ID, err.Error())
		var errResult WriteResult
		errResult.Err = err
		results = append(results, errResult)
		return results, err
	}

	/*
		at this point, we have a "results" section and
		a "time" section.  we can igore the latter.
	*/

	resultsArray, ok := sections["results"].([]interface{})
	if !ok {
		err = errors.New("Result key is missing from response")
		trace("%s: sections[\"results\"] ERROR: %s", conn.ID, err)
		var errResult WriteResult
		errResult.Err = err
		results = append(results, errResult)
		return results, err
	}
	trace("%s: I have %d result(s) to parse", conn.ID, len(resultsArray))
	numStatementErrors := 0
	for n, k := range resultsArray {
		trace("%s: starting on result %d", conn.ID, n)
		thisResult := k.(map[string]interface{})

		var thisWR WriteResult
		thisWR.conn = conn

		// did we get an error?
		_, ok := thisResult["error"]
		if ok {
			trace("%s: have an error on this result: %s", conn.ID, thisResult["error"].(string))
			thisWR.Err = errors.New(thisResult["error"].(string))
			results = append(results, thisWR)
			numStatementErrors += 1
			continue
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

		trace("%s: this result (LII,RA,T): %d %d %f", conn.ID, thisWR.LastInsertID, thisWR.RowsAffected, thisWR.Timing)
		results = append(results, thisWR)
	}

	trace("%s: finished parsing, returning %d results", conn.ID, len(results))

	if numStatementErrors > 0 {
		return results, errors.New(fmt.Sprintf("there were %d statement errors", numStatementErrors))
	} else {
		return results, nil
	}
}

func (conn *Connection) Queue(sqlStatements []string) (seq int64, err error) {
	if conn.hasBeenClosed {
		return 0, errClosed
	}

	trace("%s: Write() for %d statements", conn.ID, len(sqlStatements))

	// Set queuing mode just for this call.
	conn.wantsQueueing = true
	defer func() {
		conn.wantsQueueing = false
	}()

	response, err := conn.rqliteApiPost(api_WRITE, sqlStatements)
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

/* *****************************************************************

   type: WriteResult

 * *****************************************************************/

/*
A WriteResult holds the result of a single statement sent to Write().

Write() returns an array of WriteResult vars, while WriteOne() returns a single WriteResult.
*/
type WriteResult struct {
	Err          error // don't trust the rest if this isn't nil
	Timing       float64
	RowsAffected int64 // affected by the change
	LastInsertID int64 // if relevant, otherwise zero value
	conn         *Connection
}
