package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	_ "github.com/lib/pq"
)

const (
	port      = ":1234"
	url       = "/"
	host      = "localhost"
	portDB    = 5432
	user      = "postgres"
	dbname    = "data"
	tableName = "json_data"
	password  = "user password"
)

type jsonPostData struct {
	Data string //`json:"data"`
}
type dbSendData struct {
	Data string
	Time int64
}

func getMethod(db *sql.DB, max string, min string) ([]byte, error) {
	query := fmt.Sprintf("SELECT * FROM %s WHERE time >= %s AND time <= %s", tableName, min, max)
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var rs = make([]*dbSendData, 0)
	for rows.Next() {
		rec := new(dbSendData)
		var id int64
		err = rows.Scan(&id, &rec.Data, &rec.Time)
		if err != nil {
			return nil, err
		}
		rs = append(rs, rec)
	}

	js, err := json.Marshal(rs)
	if err != nil {
		return nil, err
	}

	return js, nil
}

func postMethod(db *sql.DB, data jsonPostData) error {
	if data.Data != "" {
		query := fmt.Sprintf("INSERT INTO %s (data, time) VALUES ('%s', %d)", tableName, data.Data, time.Now().Unix())
		_, err := db.Exec(query)
		if err != nil {
			return err
		}
	}
	return nil
}

func handler(w http.ResponseWriter, r *http.Request, db *sql.DB) {
	err := db.Ping()
	if err != nil {
		//fmt.Println("11!", err.Error())
		http.Error(w, err.Error(), 400)
		return
	}

	if r.Method == "POST" {
		data := jsonPostData{}
		if r.Body == nil {
			//fmt.Println("2!")
			http.Error(w, "Please send a request body", 400)
			return
		}
		err := json.NewDecoder(r.Body).Decode(&data)
		if err != nil {
			//fmt.Println("3!")
			http.Error(w, err.Error(), 400)
			return
		}
		err = postMethod(db, data)
		if err != nil {
			//fmt.Println("4!")
			http.Error(w, err.Error(), 400)
			return
		}
		fmt.Fprintf(w, "Ok")
	} else if r.Method == "GET" {
		w.Header().Set("Content-Type", "application/json")

		max := r.URL.Query().Get("max")
		min := r.URL.Query().Get("min")
		if max == "" {
			max = strconv.Itoa(int(time.Now().Unix()))
		}
		if min == "" {
			min = "0"
		}

		js, err := getMethod(db, max, min)
		if err != nil {
			//fmt.Println("5!")
			http.Error(w, err.Error(), 400)
			return
		}
		w.Write(js)
	}

}

func main() {

	//connect to db
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s dbname=%s sslmode=disable password=%s",
		host, portDB, user, dbname, password)

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		//fmt.Println("10!")
		panic(err)
	}
	defer db.Close()

	http.HandleFunc(url, func(w http.ResponseWriter, r *http.Request) {
		r.Close = true
		handler(w, r, db)
	})
	err = http.ListenAndServe(port, nil)
	if err != nil {
		//fmt.Println("1!")
		log.Fatal("ListenAndServe", err)
	}
}
