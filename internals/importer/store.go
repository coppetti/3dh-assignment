package importer

import (
	"fmt"
	"io"

	bolt "github.com/johnnadratowski/golang-neo4j-bolt-driver"
	"github.com/johnnadratowski/golang-neo4j-bolt-driver/structures/graph"
)

const (
	URI = "bolt://neo4j:admin@localhost:7687"
)

// var cfg, _ = configmanager.GetConfig()

func createConnection() bolt.Conn {
	driver := bolt.NewDriver()
	con, err := driver.OpenNeo(URI)
	handleError(err)
	return con
}

// Here we prepare a new statement. This gives us the flexibility to
// cancel that statement without any request sent to Neo
func prepareStatement(query string, con bolt.Conn) bolt.Stmt {
	st, err := con.PrepareNeo(query)
	handleError(err)
	return st
}

// Executing a statement just returns summary information
// receives a map i.e: map[string]interface{}{"foo": 1, "bar": 2.2}
func executeStatement(st bolt.Stmt, vars map[string]interface{}) {
	_, err := st.ExecNeo(vars)
	handleError(err)
}
func queryStatement(st bolt.Stmt) bolt.Rows {
	// Even once I get the rows, if I do not consume them and close the
	// rows, Neo will discard and not send the data
	rows, err := st.QueryNeo(nil)
	handleError(err)

	return rows
}
func consumeRows(rows bolt.Rows, st bolt.Stmt) {
	// This interface allows you to consume rows one-by-one, as they
	// come off the bolt stream. This is more efficient especially
	// if you're only looking for a particular row/set of rows, as
	// you don't need to load up the entire dataset into memory
	var err error
	err = nil
	fmt.Println("calculated_at | supplier_id | review | acceptance_ratio")
	for err == nil {
		var row []interface{}
		row, _, err = rows.NextNeo()
		if err != nil && err != io.EOF {
			panic(err)
		} else if err != io.EOF {
			fmt.Printf("%+v\n", row) // Prints all paths
		}
	}
}
func consumePathData(rows bolt.Rows, st bolt.Stmt) []graph.Path {
	// Here we loop through the rows until we get the metadata object
	// back, meaning the row stream has been fully consumed

	var err error
	err = nil
	data := []graph.Path{}
	for err == nil {
		var row []interface{}
		row, _, err = rows.NextNeo()
		if err != nil && err != io.EOF {
			panic(err)
		} else if err != io.EOF {
			// fmt.Printf("PATH: %#v\n", row) // Prints all paths
			// for _, r := range row {
			// 	data = append(data, r.(graph.Path))
			// }
			data = append(data, row[0].(graph.Path))
		}
	}
	st.Close()
	return data
}

func getPathData(query string) []graph.Path {
	st := prepareStatement(query, conn)
	rows := queryStatement(st)
	p := consumePathData(rows, st)
	return p
}

func getRowData(query string) {
	st := prepareStatement(query, conn)
	rows := queryStatement(st)
	consumeRows(rows, st)
}

// Here we create a simple function that will take care of errors, helping with some code clean up
func handleError(err error) {
	if err != nil {
		panic(err)
	}
}
