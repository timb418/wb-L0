package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/lib/pq"
	"github.com/nats-io/nats.go"
	"html/template"
	"log"
	"nats-test/pkg/model"
	"net/http"
	"net/url"
)

var cache map[string]model.Order

const (
	host     = "localhost"
	port     = 5432
	user     = "tim"
	password = "1111"
	dbname   = "postgres"
)

func main() {
	cache = make(map[string]model.Order)
	restoreCache()
	nc, err := nats.Connect("nats://localhost:4222")
	log.Printf("nats was connected - %t\n", nc.IsConnected())
	if err == nil {
		go NatsHandler(nc)
	} else {
		log.Fatal("Could not connect to nats-streaming")
		return
	}

	http.HandleFunc("/getorderbyid", getOrderById)

	err = http.ListenAndServe(":8090", nil)
	if err != nil {
		return
	}

}

func restoreCache() {
	var counter = 0
	db := getDbConnection()
	defer db.Close()

	_ = db.QueryRow("select count(*) from orders").Scan(&counter)
	if len(cache) < counter {
		rows, err := db.Query("select order_data from orders")
		if err != nil {
			fmt.Println("error with query")
		}
		//fmt.Println(rows)

		var order model.Order
		for rows.Next() {
			var jsStr string
			err := rows.Scan(&jsStr)
			if err != nil {
				fmt.Println("error while scanning")
			}
			err = json.Unmarshal([]byte(jsStr), &order)
			cache[order.OrderUid] = order
		}
	}
	log.Print("cache was restored")
	log.Print(len(cache))
}

func getOrderById(writer http.ResponseWriter, request *http.Request) {
	addr, err := url.Parse(request.RequestURI)
	if err != nil {
		log.Fatal(err)
	}
	values := addr.Query()
	id := values.Get("id")
	var order model.Order
	//order := getOrderFromDB(id)

	if id != "" {
		if cache[id].OrderUid != "" {
			order = cache[id]
		} else {
			order = getOrderFromDB(id)
		}
	}
	tmpl, _ := template.ParseFiles("ui/static/order.html")
	err = tmpl.Execute(writer, order)
	if err != nil {
		log.Fatalf("Error with template: %v", err)
	}

}

func getOrderFromDB(id string) model.Order {
	db := getDbConnection()
	fmt.Println("get order from db")
	rows, err := db.Query("select order_data from orders where order_uid = $1 limit 1", id)
	if err != nil {
		fmt.Println("error with query")
	}

	var order model.Order
	//order := model.Order{}
	for rows.Next() {
		var jsStr string
		err := rows.Scan(&jsStr)
		fmt.Println("jsStr is")
		fmt.Println(jsStr)
		if err != nil {
			fmt.Println("error while scanning")
		}
		err = json.Unmarshal([]byte(jsStr), &order)
	}
	fmt.Println("order loaded from db is")
	fmt.Println(order)
	return order
}

func getDbConnection() *sql.DB {
	psqlconn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)
	db, _ := sql.Open("postgres", psqlconn)
	return db
}

func NatsHandler(nc *nats.Conn) {
	_, err := nc.Subscribe("foo", func(m *nats.Msg) {
		var order model.Order
		err := json.Unmarshal(m.Data, &order)

		if err == nil {
			sqlStatement := `INSERT INTO orders (order_uid, order_data)
								VALUES ($1, $2)`
			db := getDbConnection()

			_, err = db.Exec(sqlStatement, order.OrderUid, m.Data)
			cache[order.OrderUid] = order
		} else {
			log.Printf("Incorrect data was passed - wrong data structure - %s", m.Data)
		}

	})
	if err != nil {
		return
	}
}
