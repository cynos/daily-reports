package main

import (
	"fmt"
	"time"

	index "./modul_index"
	subordinateLocal "./modul_subordinate_local"
	dataprocess "./modul_data_processing"

	"github.com/jasonlvhit/gocron"
)

func main() {
	gocron.Every(1).Day().At("12:00").Do(task)
	<-gocron.Start()
}

func task() {
	layout := "2006-01-02 00:00:00"
	yesterday := time.Now().Add(-24 * time.Hour).Format(layout)

	fmt.Println("### stage 1 -", yesterday)
	gettingIndex(yesterday)

	fmt.Println("### stage 2 -", yesterday)
	subordinateToLocal(yesterday)

	fmt.Println("### stage 3 -", yesterday)
	dataProcessing(yesterday)
}

func gettingIndex(date string) {
	index.Charge(date, date)
	index.Injections(date, date)
	index.Dispatch(date, date)
}

func subordinateToLocal(date string) {
	subordinateLocal.Charge(date)
	subordinateLocal.Injections(date)
	subordinateLocal.Subscriptions(date)
	subordinateLocal.Dispatch(date)
}

func dataProcessing(date string) {
	dataprocess.ChargeOverUnder(date)
	dataprocess.InjectionsOverUnder(date)
	dataprocess.ChargePrepaidPostpaid(date)
}
