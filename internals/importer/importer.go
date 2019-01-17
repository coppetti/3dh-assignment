package importer

import (
	"database/sql"
	"encoding/json"
	"log"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

var conn = createConnection()

type SQLData struct {
	Event                    string    `json:"event"`
	SupplierID               string    `json:"supplier_id"`
	OrderID                  string    `json:"order_id"`
	Timestamp                time.Time `json:"timestamp"`
	PriceCustomer            string    `json:"price_customer"`
	OrderStationType         string    `json:"orderStationType"`
	OrderStationModel        string    `json:"orderStationModel"`
	ContextTraitsUID         string    `json:"context_traits_uid"`
	ReviewValueSpeed         string    `json:"review_value_speed"`
	ContextTraitsPersona     string    `json:"context_traits_persona"`
	OrderStationManufacterer string    `json:"orderStationManufacterer"`
}

func Import() {
	// get from sql
	rows := getDataFromSQL()
	var rowsData string
	for rows.Next() {
		rows.Scan(&rowsData)
		// insert into struct
		jsondata := &SQLData{}
		err := json.Unmarshal([]byte(rowsData), jsondata)
		log.Println(rowsData)
		if err != nil {
			log.Println(">>>>>> ", err)
		}
		log.Printf("%+v", jsondata)
		// load in neo4j
		loadIntoNeo4j(jsondata)

	}
}

func getDataFromSQL() *sql.Rows {
	db, err := sql.Open("sqlite3", "./data/database.db")
	if err != nil {
		return nil
	}
	rows, err := db.Query("SELECT data FROM my_table")
	if err != nil {
		return nil
	}
	return rows
}

func loadIntoNeo4j(d *SQLData) {
	// todo
	event := strings.Split(d.Event, "/")
	e := event[len(event)-1]

	if d.SupplierID != "" && d.Event != "" && d.OrderID != "" {

		if d.Timestamp.String() == "" {
			d.Timestamp = time.Now()
		}
		if d.PriceCustomer == "" {
			d.PriceCustomer = "na"
		}
		if d.OrderStationType == "" {
			d.OrderStationType = "na"
		}
		if d.OrderStationModel == "" {
			d.OrderStationModel = "na"
		}
		if d.ContextTraitsPersona == "" {
			d.ContextTraitsPersona = "na"
		}
		if d.ContextTraitsUID == "" {
			d.ContextTraitsUID = "na"
		}
		if d.ReviewValueSpeed == "" {
			d.ReviewValueSpeed = "na"
		}
		if d.OrderStationManufacterer == "" {
			d.OrderStationManufacterer = "na"
		}

		dt := strings.Split(d.Timestamp.String(), " ")[0]

		query := `MERGE (s:Suplier{id: "` + d.SupplierID + `"})
		MERGE (o:Order{id: "` + d.OrderID + `"})
		MERGE (d:Date{date: "` + dt + `"})
		MERGE (e:Event{timestamp: "` + d.Timestamp.String() + `", price_customer: "` + d.PriceCustomer + `", order_station_type: "` + d.OrderStationType + `",
			order_station_model: "` + d.OrderStationModel + `", 
			context_traits_id: "` + d.ContextTraitsUID + `", review_value_speed: "` + d.ReviewValueSpeed + `", 
			context_traits_persona: "` + d.ContextTraitsPersona + `", order_station_manufacturer: "` + d.OrderStationManufacterer + `"})
		MERGE (o)-[:` + e + `]->(e)<-[:` + e + `]-(s)
		MERGE (e)-[:at_date]->(d)
		`

		stmt := prepareStatement(query, conn)

		// strmap := map[string]interface{}{"ts": d.Timestamp, "pc": d.PriceCustomer, "ost": d.OrderStationType,
		// 	"osm": d.OrderStationModel, "ctid": d.ContextTraitsUID, "rvs": d.ReviewValueSpeed, "ctp": d.ContextTraitsPersona,
		// 	"ostm": d.OrderStationManufacterer}

		_, err := stmt.ExecNeo(map[string]interface{}{})
		if err != nil {
			log.Printf("%+v", d)
			panic(query)
		}

		stmt.Close()
	}
}
