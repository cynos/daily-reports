package modul_index

import (
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"time"
)

func Dispatch(first, last string) {
	fmt.Println("..... Processing index dispatcher .....")

	dbs := connDB(subordinateInfo)
	dbl := connDB(localInfo)

	defer dbs.Close()
	defer dbl.Close()

	layout := "2006-01-02 00:00:00"

	f, _ := time.Parse(layout, first)
	l, _ := time.Parse(layout, last)

	date := map[string]string{
		"lowdate": f.Format(layout),
		"topdate": f.AddDate(0, 0, 1).Format(layout),
	}

	days, _ := strconv.Atoi(fmt.Sprintf("%v", l.Sub(f).Hours()/24+1))

	for i := 0; i < days; i++ {
		fmt.Println("> Process date " + date["lowdate"])

		var res [][]string

		for k, v := range date {
			res = append(res, idxDispatch(k, v, dbs))
		}

		fmt.Println("+", res)
		pushDispatch(date["lowdate"], res, dbl)

		lowdate, _ := time.Parse(layout, date["lowdate"])
		topdate, _ := time.Parse(layout, date["topdate"])
		date["lowdate"] = lowdate.AddDate(0, 0, 1).Format(layout)
		date["topdate"] = topdate.AddDate(0, 0, 1).Format(layout)
	}
	fmt.Println("Done, index dispatcher")
	fmt.Println("================================================")
}

func idxDispatch(keyDate, valDate string, db *sql.DB) []string {
	dobreak := false
	index := 10000000
	plus := 1000000
	minus := 500000
	lastoperation := "plus"

	for dobreak == false {
		allOver := false
		allUnder := false

		query := `select id, (created_at at time zone 'Asia/Karachi') from mobilink_dispatcher where id < ` + strconv.Itoa(index) + " order by id desc limit 100;"

		rows, err := db.Query(query)
		if err != nil {
			log.Fatal(err)
		}

		var id, dat string
		for rows.Next() {
			rows.Scan(&id, &dat)

			if dat > valDate {
				allOver = true
			} else {
				allUnder = true
			}
		}

		if allOver && allUnder {
			//index in range
			dobreak = true
			query := `select id, (created_at at time zone 'Asia/Karachi') from mobilink_dispatcher where id < ` + strconv.Itoa(index) + " order by id desc limit 100;"
			rows, _ := db.Query(query)

			var res [][]string

			for rows.Next() {
				rows.Scan(&id, &dat)
				if dat > valDate && keyDate == "lowdate" {
					res = append(res, []string{id, dat})
				}
				if dat < valDate && keyDate == "topdate" {
					res = append(res, []string{id, dat})
				}
			}

			if keyDate == "lowdate" {
				return getIdxMin(res)
			}
			if keyDate == "topdate" {
				return getIdxMax(res)
			}
		} else {
			if allOver {
				//over range
				index -= minus

				if lastoperation == "plus" {
					lastoperation = "minus"
					plus = plus / 10
				}
			} else {
				//under range
				index += plus

				if lastoperation == "minus" {
					lastoperation = "plus"
					minus = minus / 10
				}
			}
		}
	}
	return nil
}

func pushDispatch(date string, data [][]string, db *sql.DB) {
	query := `insert into dispatch_index (date, idx_min, idx_max) values ($1, $2, $3)`
	_, err := db.Exec(query, date, data[0][0], data[1][0])
	checkError("failed to insert dispatch index - ", err)
}
