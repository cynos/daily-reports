package modul_subordinate_local

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
)

func Subscriptions(date string) {
	dbs := connDB(subordinateInfo)
	dbl := connDB(localInfo)

	defer dbs.Close()
	defer dbl.Close()

	tableName := "subscriptions_all"
	stmt, err := dbl.Prepare("INSERT INTO " + tableName + " VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21)")
	checkError("failed to prepare statement |", err)

	fmt.Println("starting insert subordinate to local data subscriptions |", date)

	date = strings.Replace(date, " 00:00:00", "", -1)
	startTime := " 00:00:00"
	endTime := " 23:59:59"

	offset := 5000
	counter := 0
	for i := 0; true; i++ {
		preoffset := offset * i

		rows, err := getDataSubscriptions(dbs, date+startTime, date+endTime, offset, preoffset)
		if err != nil {
			log.Fatal(err)
		}

		var (
			c2, c3, c4, c5, c6, c8, c11, c13, c14, c17, c18, c21, c22 sql.NullString
			c1, c7, c10, c12, c15, c16, c20                           sql.NullInt64
			c19                                                       sql.NullBool
		)

		notLast := rows.Next()

		if !notLast {
			break
		}

		for notLast {
			rows.Scan(&c1, &c2, &c3, &c4, &c5, &c6, &c7, &c8, &c10, &c11, &c12, &c13, &c14, &c15, &c16, &c17, &c18, &c19, &c20, &c21, &c22)

			_, err := stmt.Exec(c1, c2, c3, c4, c5, c6, c7, c8, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22)
			checkError("error execute query", err)

			notLast = rows.Next()
			counter++
		}
		fmt.Println("+ row inserted >", counter)
	}
	stmt.Close()
	fmt.Println("------ data saved ------")
}

func getDataSubscriptions(dbs *sql.DB, start, end string, offset, preoffset int) (*sql.Rows, error) {
	query := fmt.Sprintf(
		`set time zone "Asia/Karachi"; select * from mobilink_subscriptions
		where created_at between '%v' and '%v' order by id limit %v offset %v ;`,
		start, end, offset, preoffset)

	rows, err := dbs.Query(query)
	if err != nil {
		return rows, err
	}

	return rows, nil
}
