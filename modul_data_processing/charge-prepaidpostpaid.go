package modul_data_processing

import (
	"database/sql"
	"fmt"
	"strings"
)

func ChargePrepaidPostpaid(date string) {
	dbl := connDB(localInfo)
	defer dbl.Close()

	firstidx, lastidx := getIdxCharge(date, dbl)

	date = strings.Replace(date, " 00:00:00", "", -1)

	query := fmt.Sprintf(`select distinct sub_id from charge_all where paid = true and id between %v and %v`, firstidx, lastidx)
	rows, err := dbl.Query(query)
	checkError("error getting distinct id | ", err)

	var charge = struct {
		Msisdn   string
		SubID    string
		Date     string
		BillType sql.NullString
	}{}

	fmt.Println("> start data processing - charge prepaid postpaid -", date)

	counter := 0
	for i := 0; rows.Next(); i++ {
		rows.Scan(&charge.SubID)

		q1 := fmt.Sprintf(`select msisdn, billing_type from subscriptions_all where id = %v`, charge.SubID)
		row := dbl.QueryRow(q1)
		row.Scan(&charge.Msisdn, &charge.BillType)
		if charge.BillType.Valid == false || charge.BillType.String == "" {
			charge.BillType.String = "prepaid"
		}

		charge.Date = date

		qi := fmt.Sprintf("insert into report_charge_prepaidpostpaid (msisdn, sub_id, date, billtype) values ('%v','%v','%v','%v')",
			charge.Msisdn, charge.SubID, charge.Date, charge.BillType.String)

		_, err := dbl.Exec(qi)
		checkError("error insert ", err)
		counter++
	}
	fmt.Println("+ Data saved,", counter, "row inserted")
	fmt.Println("==============================")
}
