package modul_index

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/lib/pq"
)

var subordinateInfo = dbconf{
	host:     "mobilink-2018-subordinate-1.cb2q7f7mvbwr.ap-south-1.rds.amazonaws.com",
	port:     5432,
	user:     "operations_team",
	password: "fqzq5PfXjAyuMvR2",
	dbname:   "mobilink",
}

var localInfo = dbconf{
	host:     "localhost",
	port:     5432,
	user:     "postgres",
	password: "123456",
	dbname:   "op_request",
}

type dbconf struct {
	host     string
	port     int
	user     string
	password string
	dbname   string
}

func init() {
	time.LoadLocation("Asia/Karachi")
}

func connDB(dbInfo dbconf) *sql.DB {
	conn := fmt.Sprintf(
		`host=%s port=%d user=%s password=%s dbname=%s sslmode=disable`,
		dbInfo.host, dbInfo.port, dbInfo.user, dbInfo.password, dbInfo.dbname,
	)

	db, err := sql.Open("postgres", conn)
	checkError("error connect database", err)

	err = db.Ping()
	checkError("error while ping database", err)

	return db
}

func getIdxMin(values [][]string) []string {
	f := values[0]
	for i, v := range values {
		if v[0] < f[0] {
			f = values[i]
		}
	}
	return f
}

func getIdxMax(values [][]string) []string {
	f := values[0]
	for i, v := range values {
		if v[0] > f[0] {
			f = values[i]
		}
	}
	return f
}

func Date(year, month, day int) time.Time {
	return time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
}

func checkError(message string, err error) {
	if err != nil {
		log.Fatal(message, err)
	}
}
