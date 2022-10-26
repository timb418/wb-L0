package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/lib/pq"
	"github.com/nats-io/nats.go"
	"html/template"
	"log"
	"math/rand"
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
	nc, err := getNatsConn()

	log.Printf("nats was connected - %t\n", nc.IsConnected())
	if err == nil {
		go NatsHandler(nc)
	} else {
		log.Fatal("Could not connect to nats-streaming")
		return
	}

	http.HandleFunc("/getorderbyid", getOrderById)
	http.HandleFunc("/insert", sendNatsMessage)

	err = http.ListenAndServe(":8090", nil)
	if err != nil {
		return
	}

}

func getNatsConn() (*nats.Conn, error) {
	return nats.Connect("nats://localhost:4222")
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

func randSeq(n int) string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func sendNatsMessage(writer http.ResponseWriter, request *http.Request) {
	randomUid := randSeq(20)
	log.Println("random uid is " + randomUid)
	orderJson := `{"order_uid":"` + randomUid + `","track_number":"WBILMTESTTRACK","entry":"WBIL","delivery":{"name":"TestTestov","phone":"+9720000000","zip":"2639809","city":"KiryatMozkin","address":"PloshadMira15","region":"Kraiot","email":"test@gmail.com"},"payment":{"transaction":"` + randomUid + `","request_id":"","currency":"USD","provider":"wbpay","amount":1817,"payment_dt":1637907727,"bank":"alpha","delivery_cost":1500,"goods_total":317,"custom_fee":0},"items":[{"chrt_id":9934930,"track_number":"WBILMTESTTRACK","price":453,"rid":"ab4219087a764ae0btest","name":"Mascaras","sale":30,"size":"0","total_price":317,"nm_id":2389212,"brand":"VivienneSabo","status":202}],"locale":"en","internal_signature":"","customer_id":"test","delivery_service":"meest","shardkey":"9","sm_id":99,"date_created":"2021-11-26T06:22:19Z","oof_shard":"1"}`
	nc, err := getNatsConn()
	if err == nil {
		err := nc.Publish("foo", []byte(orderJson))
		if err != nil {
			log.Println("could not publish")
			return
		}
	} else {
		log.Println(err)
	}
}
