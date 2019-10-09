package modul_slave_local

import (
	"database/sql"
	"fmt"
)

func Charge(date string) {
	dbs := connDB(slaveInfo)
	dbl := connDB(localInfo)

	defer dbs.Close()
	defer dbl.Close()

	tableName := "charge_all"
	fid, lid := getIdxCharge(date, dbl)

	fmt.Println("starting insert slave to local data charge |", date)
	offset := 5000
	counter := 0
	for i := 0; true; i++ {
		preoffset := offset * i

		rows, err := getDataCharge(dbs, fid, lid, offset, preoffset)
		if err != nil {
			checkError("error get data charge", err)
		}

		query := `
		insert into ` + tableName + `
			(id, msisdn, created_at, price, paid, type_charge, date, cycle, disp_id, sub_id, sub_status_code)
		values `

		var c1, c2, c3, c6, c7, c8 sql.NullString
		var c4, c9, c10, c11 sql.NullInt64
		var c5 sql.NullBool

		notLast := rows.Next()

		if !notLast {
			break
		}

		for notLast {
			rows.Scan(&c1, &c2, &c3, &c4, &c5, &c6, &c7, &c8, &c9, &c10, &c11)

			notLast = rows.Next()
			if notLast {
				query += fmt.Sprintf("('%v', '%v', '%v', %v, %v, '%v', '%v', '%v', %v, %v, %v), ",
					c1.String, c2.String, c3.String, c4.Int64, c5.Bool, c6.String, c7.String, c8.String, c9.Int64, c10.Int64, c11.Int64)
			} else {
				query += fmt.Sprintf("('%v', '%v', '%v', %v, %v, '%v', '%v', '%v', %v, %v, %v)",
					c1.String, c2.String, c3.String, c4.Int64, c5.Bool, c6.String, c7.String, c8.String, c9.Int64, c10.Int64, c11.Int64)
			}

			counter++
		}
		pushDataCharge(dbl, query)
		fmt.Println("+ row inserted >", counter)
	}
	fmt.Println("------ data saved ------")
}

func getIdxCharge(date string, dbl *sql.DB) (fid, lid int) {
	query := `select idx_min, idx_max from charge_index where date = '` + date + `'`
	row := dbl.QueryRow(query)
	err := row.Scan(&fid, &lid)
	checkError("failed to get idx - ", err)
	return
}

func getDataCharge(dbs *sql.DB, fid, lid, offset, preoffset int) (*sql.Rows, error) {
	query := fmt.Sprintf(
		`select
		id, msisdn, created_at, price, paid, type_charge, date, cycle, disp_id, sub_id, sub_status_code 
		from mobilink_charge_query
		where id between %v and %v limit %v offset %v`,
		fid, lid, offset, preoffset)

	rows, err := dbs.Query(query)
	if err != nil {
		return rows, err
	}

	return rows, nil
}

func pushDataCharge(dbl *sql.DB, query string) {
	_, err := dbl.Exec(query)
	checkError("insert failed - ", err)
}
