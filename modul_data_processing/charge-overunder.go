package modul_data_processing

import (
	"database/sql"
	"fmt"
	"strings"
)

func ChargeOverUnder(date string) {
	dbl := connDB(localInfo)
	defer dbl.Close()

	firstidx, lastidx := getIdxCharge(date, dbl)

	date = strings.Replace(date, " 00:00:00", "", -1)

	query := fmt.Sprintf(`select distinct sub_id from charge_all where id between %v and %v`, firstidx, lastidx)
	rows, err := dbl.Query(query)
	checkError("error getting distinct id | ", err)

	var charge = struct {
		Msisdn   string
		SubID    string
		Date     string
		Total    sql.NullInt64
		Status   string
		BillType sql.NullString
	}{}

	fmt.Println("> start data processing - charge over under -", date)

	counter := 0
	for i := 0; rows.Next(); i++ {
		rows.Scan(&charge.SubID)

		q1 := fmt.Sprintf(`select sum(price) as total from charge_all where sub_id = %v and paid = true`, charge.SubID)
		row := dbl.QueryRow(q1)
		row.Scan(&charge.Total)

		if charge.Total.Int64 == 0 {
			continue
		}

		if charge.Total.Valid {
			if charge.Total.Int64 < 4000 {
				charge.Status = "UNDER"
			} else {
				charge.Status = "OVER"
			}
		}

		q2 := fmt.Sprintf(`select msisdn, billing_type from subscriptions_all where id = %v`, charge.SubID)
		row = dbl.QueryRow(q2)
		row.Scan(&charge.Msisdn, &charge.BillType)
		if !charge.BillType.Valid {
			charge.BillType.String = "prepaid"
		}

		charge.Date = date

		queryInsert := fmt.Sprintf("insert into report_charge_overunder (msisdn, sub_id, date, total, status, billtype) values ('%v','%v','%v',%v,'%v','%v')",
			charge.Msisdn, charge.SubID, charge.Date, charge.Total.Int64, charge.Status, charge.BillType.String)

		_, err := dbl.Exec(queryInsert)
		checkError("error insert ", err)
		counter++
	}
	fmt.Println("+ Data saved,", counter, "row inserted")
	fmt.Println("==============================")
}

func getIdxCharge(date string, dbl *sql.DB) (fid, lid int) {
	query := `select idx_min, idx_max from charge_index where date = '` + date + `'`
	row := dbl.QueryRow(query)
	err := row.Scan(&fid, &lid)
	checkError("failed to get idx - ", err)
	return
}
