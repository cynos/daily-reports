package modul_slave_local

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/lib/pq"
)

var slaveInfo = dbconf{
	host:     "mobilink-2018-slave-1.cb2q7f7mvbwr.ap-south-1.rds.amazonaws.com",
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

func checkError(message string, err error) {
	if err != nil {
		log.Fatal(message, err)
	}
}
