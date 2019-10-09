package modul_slave_local

import (
	"database/sql"
	"fmt"
	"strings"
)

func Injections(date string) {
	dbs := connDB(slaveInfo)
	dbl := connDB(localInfo)

	defer dbs.Close()
	defer dbl.Close()

	tableName := "injections_all"
	fid, lid := getIdxInjections(date, dbl)

	fmt.Println("starting insert slave to local data injections |", date)
	offset := 5000
	counter := 0
	for i := 0; true; i++ {
		preoffset := offset * i

		rows, err := getDataInjections(dbs, fid, lid, offset, preoffset)
		if err != nil {
			checkError("error get data injections", err)
		}

		query := `
		insert into ` + tableName + `
		(id, date, created_at, msisdn, price, paid, error, type_charge)
		values `

		var c1, c2, c3, c4, c5, c6, c7, c8 sql.NullString

		notLast := rows.Next()

		if !notLast {
			break
		}

		for notLast {
			rows.Scan(&c1, &c2, &c3, &c4, &c5, &c6, &c7, &c8)

			notLast = rows.Next()
			if notLast {
				query += fmt.Sprintf("('%v', '%v', '%v', '%v', '%v', '%v', E'%v', '%v'), ",
					c1.String, c2.String, c3.String, c4.String, c5.String, c6.String, strings.Replace(c7.String, "'", "\\'", -1), c8.String)
			} else {
				query += fmt.Sprintf("('%v', '%v', '%v', '%v', '%v', '%v', E'%v', '%v')",
					c1.String, c2.String, c3.String, c4.String, c5.String, c6.String, strings.Replace(c7.String, "'", "\\'", -1), c8.String)
			}

			counter++
		}
		pushDataInjections(dbl, query)
		fmt.Println("+ row inserted >", counter)
	}
	fmt.Println("------ data saved ------")
}

func getIdxInjections(date string, dbl *sql.DB) (fid, lid int) {
	query := `select idx_min, idx_max from injections_index where date = '` + date + `'`
	row := dbl.QueryRow(query)
	err := row.Scan(&fid, &lid)
	checkError("failed to get idx - ", err)
	return
}

func getDataInjections(dbs *sql.DB, fid, lid, offset, preoffset int) (*sql.Rows, error) {
	query := fmt.Sprintf(
		`select
		id, date, created_at, msisdn, price, paid, error, type_charge 
		from mobilink_injections
		where id between %v and %v limit %v offset %v`,
		fid, lid, offset, preoffset)

	rows, err := dbs.Query(query)
	if err != nil {
		return rows, err
	}

	return rows, nil
}

func pushDataInjections(dbl *sql.DB, query string) {
	_, err := dbl.Exec(query)
	checkError("insert failed - ", err)
}
