package main

import (
	"fmt"
	"time"

	index "./modul_index"
	slaveLocal "./modul_slave_local"
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
	slaveToLocal(yesterday)

	fmt.Println("### stage 3 -", yesterday)
	dataProcessing(yesterday)
}

func gettingIndex(date string) {
	index.Charge(date, date)
	index.Injections(date, date)
	index.Dispatch(date, date)
}

func slaveToLocal(date string) {
	slaveLocal.Charge(date)
	slaveLocal.Injections(date)
	slaveLocal.Subscriptions(date)
	slaveLocal.Dispatch(date)
}

func dataProcessing(date string) {
	dataprocess.ChargeOverUnder(date)
	dataprocess.InjectionsOverUnder(date)
	dataprocess.ChargePrepaidPostpaid(date)
}
