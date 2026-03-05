package main

import (
	"fmt"
	"time"

	bf "github.com/leksas/binance-futures-connector-go"
)

func main() {
	WsContinuousKlineExample()
}

func WsContinuousKlineExample() {
	websocketStreamClient := bf.NewWebsocketStreamClient(false)
	wsContinuousKlineHandler := func(event *bf.WsContinuousKlineEvent) {
		fmt.Println(bf.PrettyPrint(event))
	}
	errHandler := func(err error) {
		fmt.Println(err)
	}
	doneCh, stopCh, err := websocketStreamClient.WsContinuousKlineServe(
		"BTCUSDT",
		bf.PERPETUAL,
		bf.Interval1s,
		wsContinuousKlineHandler,
		errHandler,
	)
	if err != nil {
		fmt.Println(err)
		return
	}
	// use stopCh to exit
	go func() {
		time.Sleep(10 * time.Second)
		stopCh <- struct{}{}
	}()
	<-doneCh
}
