package modul_slave_local

import (
	"database/sql"
	"fmt"
)

func Dispatch(date string) {
	dbl := connDB(localInfo)
	dbs := connDB(slaveInfo)

	defer dbs.Close()
	defer dbl.Close()

	tableName := "dispatch_all"
	fid, lid := getIdxDispatch(date, dbl)

	fmt.Println("starting insert slave to local data dispatch |", date)
	offset := 10000
	counter := 0
	for i := 0; true; i++ {
		preoffset := offset * i

		rows, err := getDataDispatch(dbs, fid, lid, offset, preoffset)
		if err != nil {
			checkError("error get data dispatch", err)
		}

		query := `
			insert into ` + tableName + `
				(id, created_at, msisdn, sub_id, action, cycle)
			values `

		var c1, c2, c3, c5, c6 sql.NullString
		var c4 sql.NullInt64

		notLast := rows.Next()

		if !notLast {
			break
		}

		for notLast {
			rows.Scan(&c1, &c2, &c3, &c4, &c5, &c6)

			notLast = rows.Next()
			if notLast {
				query += fmt.Sprintf("('%v', '%v', '%v', %v, '%v', '%v'), ",
					c1.String, c2.String, c3.String, c4.Int64, c5.String, c6.String)
			} else {
				query += fmt.Sprintf("('%v', '%v', '%v', %v, '%v', '%v')",
					c1.String, c2.String, c3.String, c4.Int64, c5.String, c6.String)
			}

			counter++
		}
		pushDataDispatch(dbl, query)
		fmt.Println("+ row inserted >", counter)
	}
}

func getIdxDispatch(date string, dbl *sql.DB) (fid, lid int) {
	query := `select idx_min, idx_max from dispatch_index where date = '` + date + `'`
	row := dbl.QueryRow(query)
	err := row.Scan(&fid, &lid)
	checkError("failed to get idx - ", err)
	return
}

func getDataDispatch(dbs *sql.DB, fid, lid, offset, preoffset int) (*sql.Rows, error) {
	query := fmt.Sprintf(
		`select
		id, created_at, msisdn, sub_id, action, cycle
		from mobilink_dispatcher
		where id between %v and %v limit %v offset %v`,
		fid, lid, offset, preoffset)

	rows, err := dbs.Query(query)
	if err != nil {
		return rows, err
	}

	return rows, nil
}

func pushDataDispatch(dbl *sql.DB, query string) {
	_, err := dbl.Exec(query)
	checkError("insert failed - ", err)
}
