package modul_data_processing

import (
	"database/sql"
	"fmt"
	"strings"
)

func InjectionsOverUnder(date string) {
	dbl := connDB(localInfo)
	defer dbl.Close()

	firstidx, lastidx := getIdxInjections(date, dbl)

	date = strings.Replace(date, " 00:00:00", "", -1)

	query := fmt.Sprintf(`select distinct msisdn from injections_all where id between %v and %v`, firstidx, lastidx)
	rows, err := dbl.Query(query)
	checkError("error getting distinct msisdn | ", err)

	var ij = struct {
		Msisdn   string
		Date     string
		Total    sql.NullInt64
		Status   string
		BillType sql.NullString
	}{}

	fmt.Println("> start data processing - injections over under -", date)

	counter := 0
	for i := 0; rows.Next(); i++ {
		rows.Scan(&ij.Msisdn)

		q1 := fmt.Sprintf(`select sum(price) as total from injections_all where msisdn = %v and paid = true`, ij.Msisdn)
		row := dbl.QueryRow(q1)
		row.Scan(&ij.Total)

		if ij.Total.Int64 == 0 {
			continue
		}

		if ij.Total.Valid {
			if ij.Total.Int64 < 4000 {
				ij.Status = "UNDER"
			} else {
				ij.Status = "OVER"
			}
		}

		q2 := fmt.Sprintf(`select billing_type from subscriptions_all where msisdn = '%v' order by created_at desc limit 1`, ij.Msisdn)
		row = dbl.QueryRow(q2)
		row.Scan(&ij.BillType)
		if !ij.BillType.Valid {
			ij.BillType.String = "prepaid"
		}

		ij.Date = date

		queryInsert := fmt.Sprintf("insert into report_injections_overunder_1 (msisdn, date, total, status, billtype) values ('%v','%v',%v,'%v','%v')",
			ij.Msisdn, ij.Date, ij.Total.Int64, ij.Status, ij.BillType.String)

		_, err := dbl.Exec(queryInsert)
		checkError("error insert ", err)
		counter++
	}
	fmt.Println("+ Data saved,", counter, "row inserted")
	fmt.Println("==============================")
}

func getIdxInjections(date string, dbl *sql.DB) (fid, lid int) {
	query := `select idx_min, idx_max from injections_index where date = '` + date + `'`
	row := dbl.QueryRow(query)
	err := row.Scan(&fid, &lid)
	checkError("failed to get idx - ", err)
	return
}
