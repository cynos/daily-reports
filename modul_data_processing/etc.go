package modul_data_processing

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/lib/pq"
)

type dbconf struct {
	host     string
	port     int
	user     string
	password string
	dbname   string
}

var localInfo = dbconf{
	host:     "localhost",
	port:     5432,
	user:     "postgres",
	password: "123456",
	dbname:   "op_request",
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

func checkError(msg string, err error) {
	if err != nil {
		log.Fatal(msg, err)
	}
}
